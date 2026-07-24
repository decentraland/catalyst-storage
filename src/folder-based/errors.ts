/**
 * An id that does not resolve to a servable path under the storage root: it escapes the root, or it
 * lands in the reserved staging namespace. Typed so a read can report it as "nothing to serve"
 * without having to recognize an error message, while writes and existence checks reject loudly.
 */
export class PathNotContainedError extends Error {}

/**
 * A gzip item refused to inflate within the configured cap (a decompression bomb, or content that
 * genuinely exceeds `decompressMaxFileSize`). Typed so a read reports it as a miss: there is nothing
 * servable and nothing an operator can repair on this request.
 */
export class DecompressionLimitExceededError extends Error {}
