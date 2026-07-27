/**
 * An id that does not resolve to a servable path of its own under the storage root: it escapes the
 * root, it lands in the reserved staging namespace, or it normalizes onto ANOTHER id's path (an
 * empty, `.` or `..` segment, or an absolute path). Typed so a read can report it as "nothing to
 * serve" without having to recognize an error message, while writes and existence checks reject
 * loudly.
 *
 * @public
 */
export class PathNotContainedError extends Error {}

/**
 * A gzip item refused to inflate within the configured cap (a decompression bomb, or content that
 * genuinely exceeds `decompressMaxFileSize`). Typed so a read reports it as a miss: there is nothing
 * servable and nothing an operator can repair on this request.
 *
 * @public
 */
export class DecompressionLimitExceededError extends Error {}

/**
 * Thrown when a commit rename failed AND its pre-rename intent journal could not be cleared. The staged
 * file it names is then the only PROOF that the rename never landed: the storage preserves that exact
 * path instead of its usual staging cleanup, so the next construction discards the intent as pre-rename
 * rather than applying the failed commit as a completed transition.
 *
 * Lives here, alongside the other two, because it REACHES CALLERS: `storeStream` and
 * `storeStreamAndCompress` rethrow it. It used to be declared in the intent journal, which the entry
 * point does not re-export, so the one error carrying an actionable payload (`stagedPath`) could only be
 * matched on its message — the exact gap the two errors above were exported to close.
 *
 * @public
 */
export class UncommittedIntentSurvivedError extends Error {
  constructor(
    readonly stagedPath: string,
    message: string
  ) {
    super(message)
    this.name = 'UncommittedIntentSurvivedError'
  }
}
