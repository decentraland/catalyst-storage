import { Readable, Writable } from 'stream'

/**
 * Absorbs a stream's 'error' after it has been torn down.
 *
 * A stream destroyed while its `open(2)` is still in flight goes on to emit 'error', and with no
 * listener attached that is an uncaught exception which terminates the process by default (measured at
 * 200/200 escapes without it, 0 with it). Shared rather than re-declared per module so there is one
 * thing to reason about.
 *
 * @internal
 */
export const ignoreStreamError = (): void => undefined

/**
 * Tears a stream down and swallows whatever it emits afterwards.
 *
 * The listener is attached BEFORE the destroy, and that order is not optional: see
 * {@link ignoreStreamError}. The whole thing is exception-safe because every caller reaches it on a
 * failure path where the error already in hand is the one that matters — a custom stream whose
 * `destroy()` throws must not replace it.
 *
 * `undefined` is accepted so callers can tear down streams whose construction may not have happened.
 *
 * @internal
 */
export function destroyQuietly(stream: Readable | Writable | undefined): void {
  if (!stream) return
  try {
    stream.on('error', ignoreStreamError)
    stream.destroy()
  } catch {
    // Best-effort: the failure that brought us here is what the caller needs to see.
  }
}

/**
 * {@link destroyQuietly} over several streams, for the pipeline teardown paths that have two or three
 * to release and must not let one failure skip the rest.
 *
 * @internal
 */
export function destroyAllQuietly(...streams: (Readable | Writable | undefined)[]): void {
  for (const stream of streams) destroyQuietly(stream)
}
