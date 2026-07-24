import { Readable } from 'stream'

const NON_CANCELLATION_ERROR = Symbol('nonCancellationError')

/**
 * Marks an error as NOT being a consequence of cancellation, so `runStoreWithSignal` surfaces it
 * as-is even when the signal has aborted. Backends use this for irreversible commit/cleanup-phase
 * failures (e.g. a committed-but-unreconciled storage state): those are never caused by abort
 * teardown — the source is fully consumed before the commit begins — and masking them behind the
 * cancellation reason would hide the repair/quarantine signal from callers and operators.
 * The marker preserves the error's identity and class, only tagging it.
 */
export function markAsNonCancellationError<T>(error: T): T {
  if (error !== null && typeof error === 'object') {
    try {
      Object.defineProperty(error, NON_CANCELLATION_ERROR, { value: true, enumerable: false })
    } catch {
      // A frozen/sealed error cannot be tagged; it may then be translated on abort — acceptable
      // degradation, and no storage code throws frozen errors.
    }
  }
  return error
}

function isNonCancellationError(error: unknown): boolean {
  return error !== null && typeof error === 'object' && NON_CANCELLATION_ERROR in error
}

function abortReasonOf(signal: AbortSignal): unknown {
  // `??` would also replace an explicit `null` abort reason; the caller must observe their own
  // cancellation cause, so only default when no reason was provided at all.
  return signal.reason === undefined ? new Error('The store operation was aborted.') : signal.reason
}

/**
 * Runs a store operation under an optional cancellation signal.
 *
 * On abort the source stream is destroyed — settling any consumer awaiting its data — and the
 * optional `onAbort` hook runs so callers can tear down transport that no longer depends on the
 * source (e.g. an S3 managed upload whose remaining parts are already buffered). The operation's
 * rejection is then surfaced as the signal's reason, so callers observe their own cancellation
 * cause rather than a transport-specific error. A store that completes before consuming the abort
 * is allowed to succeed: content is addressed by its id, so a committed write is never harmful.
 *
 * The source is destroyed without an error: consumers observe a premature close, and an
 * already-ended (fully consumed) source is left untouched instead of emitting an 'error' that
 * may no longer have observers.
 */
export async function runStoreWithSignal<T>(
  stream: Readable,
  signal: AbortSignal | undefined,
  operation: () => Promise<T>,
  onAbort?: () => void
): Promise<T> {
  if (!signal) {
    return operation()
  }
  if (signal.aborted) {
    try {
      stream.destroy()
    } catch {
      // teardown is best-effort; the abort reason below must win
    }
    throw abortReasonOf(signal)
  }
  const abort = (): void => {
    // Exception-safe: this runs inside the signal's event dispatch, where a throw would escape as
    // an uncaught exception — and a failing stream teardown must not prevent the backend hook from
    // running (nor vice versa). The operation's rejection path owns error reporting.
    try {
      stream.destroy()
    } catch {
      // best-effort
    }
    try {
      onAbort?.()
    } catch {
      // best-effort
    }
  }
  signal.addEventListener('abort', abort, { once: true })
  try {
    return await operation()
  } catch (error) {
    // Translate teardown-caused rejections (destroyed source, aborted transport) to the caller's
    // cancellation reason — but NEVER errors marked as commit/cleanup-phase failures: those carry
    // repair/quarantine information that the abort did not cause and must not hide.
    throw signal.aborted && !isNonCancellationError(error) ? abortReasonOf(signal) : error
  } finally {
    signal.removeEventListener('abort', abort)
  }
}
