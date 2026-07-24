import { Readable } from 'stream'

function abortReasonOf(signal: AbortSignal): unknown {
  return signal.reason ?? new Error('The store operation was aborted.')
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
    stream.destroy()
    throw abortReasonOf(signal)
  }
  const abort = (): void => {
    stream.destroy()
    onAbort?.()
  }
  signal.addEventListener('abort', abort, { once: true })
  try {
    return await operation()
  } catch (error) {
    throw signal.aborted ? abortReasonOf(signal) : error
  } finally {
    signal.removeEventListener('abort', abort)
  }
}
