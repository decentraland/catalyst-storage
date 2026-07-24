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

/**
 * True for the rejection shape a signalled `stream/promises` pipeline (or `throwIfAborted`)
 * produces on abort. Used to distinguish an abort-caused teardown from a real failure that merely
 * RACED the abort (ENOSPC, EACCES, zlib errors, …): only the former may be suppressed or
 * reinterpreted as a cancellation outcome.
 */
export function isAbortError(error: unknown): boolean {
  const err = error as { name?: unknown; code?: unknown } | null
  return err?.name === 'AbortError' || err?.code === 'ABORT_ERR'
}

/**
 * True only for rejections provably produced by this module's abort teardown: the signal's own
 * reason (checkpoint throws), abort errors (signalled pipelines, `throwIfAborted` defaults), a
 * prematurely closed stream — but ONLY when this run's teardown actually destroyed the live source
 * (`ERR_STREAM_PREMATURE_CLOSE` is a public shape: a source can close prematurely for a real
 * upstream fault and then race the abort, and that failure must surface as itself) — or an aborted
 * S3 managed upload (`RequestAbortedError` only arises from `ManagedUpload.abort()`, which nothing
 * but this module's hook calls on these uploads). Anything else — fs, zlib, transport or logic
 * errors that merely raced the abort — is NOT teardown-caused and must surface as itself.
 */
function isAbortTeardownError(error: unknown, signal: AbortSignal, teardownDestroyedSource: boolean): boolean {
  if (error === signal.reason) return true
  if (isAbortError(error)) return true
  const err = error as { name?: unknown; code?: unknown } | null
  if (teardownDestroyedSource && err?.code === 'ERR_STREAM_PREMATURE_CLOSE') return true
  // aws-sdk v2 ManagedUpload.abort()
  if (err?.code === 'RequestAbortedError' || err?.name === 'RequestAbortedError') return true
  return false
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
 * source (e.g. an S3 managed upload whose remaining parts are already buffered). A rejection that
 * is provably caused by that teardown is surfaced as the signal's reason, so callers observe their
 * own cancellation cause rather than a transport-specific error; a rejection that merely RACED the
 * abort (fs, zlib, transport or logic failures) surfaces as itself — cancellation must never mask
 * a real storage error. A store that completes before consuming the abort is allowed to succeed:
 * content is addressed by its id, so a committed write is never harmful.
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
  // Whether THIS run's teardown destroyed a live source. A premature-close rejection is only
  // provably ours when it did: if the source was already dead (ended, or destroyed by a real
  // upstream fault) when the abort fired, our destroy was a no-op and a premature-close failure
  // belongs to that fault, not to the cancellation.
  let teardownDestroyedSource = false
  const abort = (): void => {
    // Exception-safe: this runs inside the signal's event dispatch, where a throw would escape as
    // an uncaught exception — and a failing stream teardown must not prevent the backend hook from
    // running (nor vice versa). The operation's rejection path owns error reporting.
    try {
      if (!stream.destroyed) {
        teardownDestroyedSource = true
        stream.destroy()
      }
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
    // Translate ONLY rejections provably caused by the abort teardown (destroyed source, aborted
    // transport, checkpoint throws) to the caller's cancellation reason. Real backend/storage
    // failures that merely raced the abort — an S3 rejection, ENOSPC on a staged write, a zlib
    // error, a source that closed prematurely on its own — surface as themselves; the marker check
    // is a hard override guaranteeing commit-phase errors are never translated even if one ever
    // matched a teardown shape.
    throw signal.aborted &&
      isAbortTeardownError(error, signal, teardownDestroyedSource) &&
      !isNonCancellationError(error)
      ? abortReasonOf(signal)
      : error
  } finally {
    signal.removeEventListener('abort', abort)
  }
}
