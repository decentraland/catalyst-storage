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
}

/**
 * True only for rejections provably produced by this module's abort teardown. Every shape a store
 * can reject with is public — a source can close prematurely, an SDK can report an aborted request,
 * a custom stream or transport can raise an `AbortError` of its own — so no shape is credited on
 * appearance alone. The signal's own reason needs no provenance (only our checkpoints throw it); the
 * premature close is credited only when this run's teardown actually destroyed a live source.
 * Anything else — fs, zlib, transport or logic errors — is NOT teardown-caused and surfaces as
 * itself.
 *
 * Transport-shaped rejections are deliberately NOT matched here. The AWS SDK v3 rejects an aborted
 * request with an `AbortError` from @smithy, a shape a custom transport can raise for its own
 * reasons, so it cannot be credited on appearance. Both places that hand a signal to abortable
 * machinery — the S3 managed upload and the compression pipeline — instead attribute the abort at
 * their own call site, where they know they caused it, and convert it to the caller's reason there.
 */
function isAbortTeardownError(error: unknown, signal: AbortSignal, teardown: TeardownProvenance): boolean {
  if (error === signal.reason) return true
  const err = error as { name?: unknown; code?: unknown } | null
  return teardown.destroyedSource && err?.code === 'ERR_STREAM_PREMATURE_CLOSE'
}

/**
 * Destroys a stream that nothing else is listening to yet.
 *
 * The listener is attached BEFORE destroying, and is not optional. A stream whose `open(2)` is still
 * in flight goes on to emit 'error' even after `destroy()` — an `fs.ReadStream` over a missing or
 * unreadable path is the ordinary case — and on the pre-aborted path below `operation()` never runs,
 * so NOTHING else ever attaches a listener: the emit becomes an uncaught exception, which terminates
 * the process by default. The same window exists for the abort listener, which can fire before the
 * backend's `pipeline` has taken ownership of the source.
 *
 * Whatever arrives here is post-mortem noise — the caller is about to observe the cancellation
 * reason, or the operation's own rejection. Mirrors the teardown in `compressContentFile` and
 * `inflateGzipItemInto`, which guard the identical hazard.
 */
function destroyQuietly(stream: Readable): void {
  stream.on('error', () => undefined)
  stream.destroy()
}

/**
 * Releases a source a FAILED store never took ownership of.
 *
 * Every backend validates the id before it starts piping, and a rejection there — `PathNotContainedError`
 * for a traversing, empty, over-long or reserved id — left the caller's stream open forever: nothing had
 * piped it, so nothing destroyed it, and an `fs.ReadStream` has no finalizer, so not even GC reclaimed
 * it. A service that passes untrusted ids to `storeStream` (exactly the threat model that error exists
 * for) leaked one descriptor — or one undrained request socket — per rejected call until EMFILE took
 * down all storage with it. Measured at 30 leaked descriptors per 30 rejected calls, on every backend.
 *
 * Safe on every rejection path, not just that one: a store that failed mid-pipe has already had its
 * source destroyed by `pipeline`, and destroying an ended or already-destroyed stream is a no-op. Runs
 * AFTER the error is in hand and never replaces it — the teardown is silent by construction.
 */
function releaseUnconsumedSource(stream: Readable): void {
  try {
    if (!stream.destroyed) destroyQuietly(stream)
  } catch {
    // Best-effort: the operation's own rejection is what the caller needs to see.
  }
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
 * source (e.g. an S3 managed upload whose remaining parts are already buffered). A rejection that is
 * provably caused by that teardown is surfaced as the signal's reason, so callers observe their own
 * cancellation cause rather than a transport-specific error; a rejection that merely RACED the abort
 * (fs, zlib, transport or logic failures) surfaces as itself — cancellation must never mask a real
 * storage error. A backend whose transport rejects with its own abort shape attributes that at its
 * call site, where it knows it caused it, rather than relying on shape matching here. A store that
 * completes before consuming the abort is allowed to succeed: content is addressed by its id, so a
 * committed write is never harmful.
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
    try {
      return await operation()
    } catch (error) {
      releaseUnconsumedSource(stream)
      throw error
    }
  }
  if (signal.aborted) {
    try {
      destroyQuietly(stream)
    } catch {
      // teardown is best-effort; the abort reason below must win
    }
    throw abortReasonOf(signal)
  }
  // What this run's teardown actually did. A premature-close rejection is only provably ours when
  // the corresponding action happened: if the source was already dead (ended, or destroyed by a real
  // upstream fault), a rejection with that shape belongs to the underlying fault, not to the
  // cancellation.
  const teardown: TeardownProvenance = { destroyedSource: false }
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
        destroyQuietly(stream)
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
    //
    // Released AFTER the translation decision is made from state already captured: the abort path has
    // destroyed this source already (so this is a no-op there), and the paths that have NOT — an id
    // rejected before any piping started — are the leak this closes.
    releaseUnconsumedSource(stream)
    throw signal.aborted && isAbortTeardownError(error, signal, teardown) && !isNonCancellationError(error)
      ? abortReasonOf(signal)
      : error
  } finally {
    signal.removeEventListener('abort', abort)
  }
}
