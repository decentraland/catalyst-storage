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

/** What this run's abort teardown actually did, used to prove a rejection was caused by it. */
type TeardownProvenance = {
  /** The listener destroyed a source that was still live (a destroy on a dead stream is a no-op). */
  destroyedSource: boolean
  /** The `onAbort` hook reported tearing down in-flight transport (e.g. an S3 managed upload). */
  abortedTransport: boolean
}

/**
 * True only for rejections provably produced by this module's abort teardown. Every shape a store
 * can reject with is public — a source can close prematurely, an SDK can report an aborted request,
 * a custom stream or transport can raise an `AbortError` of its own — so no shape is credited on
 * appearance alone. The signal's own reason needs no provenance (only our checkpoints throw it, and
 * the one place that hands a signal to an abortable pipeline converts that pipeline's abort into the
 * reason at its call site, where the attribution is known); the remaining shapes are credited only
 * when this run's teardown actually performed the corresponding action. Anything else — fs, zlib,
 * transport or logic errors — is NOT teardown-caused and surfaces as itself.
 */
function isAbortTeardownError(error: unknown, signal: AbortSignal, teardown: TeardownProvenance): boolean {
  if (error === signal.reason) return true
  const err = error as { name?: unknown; code?: unknown } | null
  if (teardown.destroyedSource && err?.code === 'ERR_STREAM_PREMATURE_CLOSE') return true
  // aws-sdk v2 ManagedUpload.abort(). NOTE for a future v3 migration (v3 was tried in #66 and
  // rolled back in #74): v3 rejects aborted requests with an `AbortError` from @smithy, a shape this
  // function deliberately does NOT credit — a custom transport can raise one coincidentally. A v3
  // port must therefore attribute the abort where it is known, as the compression pipeline does at
  // its call site (or rely on v3's native `abortSignal` request option), or cancelled uploads will
  // surface as raw abort errors instead of the caller's reason.
  if (teardown.abortedTransport && (err?.code === 'RequestAbortedError' || err?.name === 'RequestAbortedError')) {
    return true
  }
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
 * source (e.g. an S3 managed upload whose remaining parts are already buffered); the hook returns
 * `true` when it actually tore something down, which is what lets a transport-shaped rejection be
 * credited to this cancellation rather than to a coincident fault. A rejection that
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
  onAbort?: () => boolean | void
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
  // What this run's teardown actually did. A premature-close or aborted-transport rejection is only
  // provably ours when the corresponding action happened: if the source was already dead (ended, or
  // destroyed by a real upstream fault), or the hook never tore any transport down, a rejection with
  // that shape belongs to the underlying fault, not to the cancellation.
  const teardown: TeardownProvenance = { destroyedSource: false, abortedTransport: false }
  const abort = (): void => {
    // Exception-safe: this runs inside the signal's event dispatch, where a throw would escape as
    // an uncaught exception — and a failing stream teardown must not prevent the backend hook from
    // running (nor vice versa). The operation's rejection path owns error reporting.
    try {
      if (!stream.destroyed) {
        // Only a destroy that interrupts a source still delivering data can cause a premature
        // close. Destroying one that has already emitted 'end' (reachable with `autoDestroy: false`,
        // or in the tick before an auto-destroy lands) is just resource cleanup and earns no
        // provenance — otherwise a premature-close rejection from elsewhere would be credited to us.
        teardown.destroyedSource = !stream.readableEnded
        stream.destroy()
      }
    } catch {
      // best-effort
    }
    try {
      teardown.abortedTransport = onAbort?.() === true
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
    throw signal.aborted && isAbortTeardownError(error, signal, teardown) && !isNonCancellationError(error)
      ? abortReasonOf(signal)
      : error
  } finally {
    signal.removeEventListener('abort', abort)
  }
}
