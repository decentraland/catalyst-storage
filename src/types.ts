import { Readable } from 'stream'
import { IFileSystemComponent } from './fs/types'
import { IBaseComponent, IConfigComponent, ILoggerComponent } from '@well-known-components/interfaces'
/**
 * @public
 */
export type AppComponents = {
  fs: IFileSystemComponent
  config: IConfigComponent
  logs: ILoggerComponent
}

/**
 * @public
 */
export type IContentStorageComponent = IBaseComponent & {
  /**
   * Stores the stream under the given id.
   *
   * The source must be one nothing has read from yet. A stream that has already been consumed — even
   * partially, as when a caller hashes or sniffs the body first — is REFUSED rather than stored, because
   * what it can still supply is not the content. `unshift`-ing the bytes back does not make it storable
   * again; hand over a fresh source instead.
   *
   * A REJECTED store consumes the source: it is destroyed on the way out, so a caller retrying after
   * correcting the id must supply a new stream. Without that, a service passing untrusted ids leaked one
   * descriptor — or one undrained request socket — per rejected call, since nothing had piped the stream and
   * so nothing would ever close it.
   *
   * @param signal Optional cancellation signal. When it aborts, the store stops consuming the
   * stream, tears down any in-flight transport (e.g. the S3 upload), and rejects with the
   * signal's reason. A store that completes before observing the abort is allowed to succeed;
   * either way no partial content is ever observable under the id, and the previous version of the
   * id stays intact on cancellation.
   *
   * ONE EXCEPTION to "the previous version stays intact", on the S3 backend only: a request S3 has
   * already received IN FULL when the abort fires cannot be un-sent, so the service may still apply it.
   * The residue is bounded — S3 object writes are atomic, so the key holds either the previous content or
   * the complete new content, never a mixture — but a cancelled store is not a guaranteed rollback there.
   * The folder-based backend has no such window: its commit is a local rename it fully controls.
   */
  storeStream(fileId: string, content: Readable, signal?: AbortSignal): Promise<void>
  /**
   * Stores the stream under the given id, compressed when the backend supports it.
   *
   * @param signal Optional cancellation signal with the same semantics as {@link storeStream}.
   */
  storeStreamAndCompress(fileId: string, content: Readable, signal?: AbortSignal): Promise<void>
  delete(fileIds: string[]): Promise<void>
  retrieve(fileId: string, range?: { start: number; end: number }): Promise<ContentItem | undefined>
  fileInfo(fileId: string): Promise<FileInfo | undefined>
  fileInfoMultiple(fileIds: string[]): Promise<Map<string, FileInfo | undefined>>
  exist(fileId: string): Promise<boolean>
  existMultiple(fileIds: string[]): Promise<Map<string, boolean>>
  /**
   * Yields the ids this storage holds — ids, not filenames, so one containing path separators round-trips
   * instead of collapsing onto its last segment. `prefix` filters those ids, never the on-disk name.
   *
   * Every id present for the whole enumeration is yielded AT LEAST ONCE, and only ids the point lookups accept
   * are yielded. It is not guaranteed to be a set: **an id can be yielded twice**, so a consumer acting on the
   * output must be idempotent. That happens in one case — a single flat-mode directory holding more than
   * `MAX_BUFFERED_DIRECTORY_ENTRIES` entries, where the compressed-name snapshot is capped and a raw file whose
   * `.gzip` sibling fell outside it is yielded rather than probed. Absence from a partial snapshot is not
   * evidence of absence on disk, so the choice there is a possible duplicate or a missing id, and a duplicate
   * costs an idempotent repeat while an omission under-reports what the node holds. With hash prefixes it
   * cannot arise: a shard holds total/65,536 entries, so every directory is decided from a single read.
   */
  allFileIds(prefix?: string): AsyncIterable<string>
}

/**
 * @public
 */
export type FileInfo = {
  encoding: string | null
  size: number | null
  /**
   * Logical content size (uncompressed). Same as size when encoding is null. Null if unknown.
   *
   * SECURITY: for gzip-encoded content this is read from the gzip ISIZE trailer, which is part of
   * the stored (potentially attacker-controlled) file and is only accurate mod 2^32. Treat it as a
   * hint for display only — never rely on it for buffer allocation or size-limit enforcement.
   */
  contentSize: number | null
}

/**
 * Thrown when a backend cannot serve a RANGE of an object it can otherwise serve whole.
 *
 * Distinct from `RangeError`, which means the requested bounds are invalid: here the request is
 * well-formed and the content is present, but this backend has no way to apply logical bounds to it
 * (S3 ranges address the stored bytes, and S3 keeps no uncompressed-size metadata for encoded
 * objects).
 *
 * BOTH map to 416, not just this one. `RangeError` reaches the caller from every backend — the
 * folder-based and S3 `retrieve()` implementations re-raise it ahead of their failure logging, and the
 * in-memory one simply lets `clampRange` propagate it — because bounds the caller got wrong are the
 * caller's problem, and `clampRange` throws it for a `start` past the end of the object, which is
 * precisely HTTP "Range Not Satisfiable". The read contract's "5xx for any other rejection" is about
 * STORAGE faults and was written as though `RangeError` could not escape; a service that took it
 * literally answered 500, and paged an operator, for a malformed `Range` header. The rule is:
 * `RangeError` and `RangeNotSupportedError` are 416, `undefined` is 404, anything else is 5xx.
 *
 * @public
 */
export class RangeNotSupportedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'RangeNotSupportedError'
  }
}

/**
 * Validates that a range is well-formed (start >= 0 and start <= end).
 *
 * @internal
 */
export function validateRange(range: { start: number; end: number }): void {
  // Safe integers only: NaN/Infinity/fractional bounds would otherwise surface as low-level stream
  // errors or an invalid ContentItem.size instead of a clear RangeError.
  if (!Number.isSafeInteger(range.start) || !Number.isSafeInteger(range.end)) {
    throw new RangeError(`Invalid range: start=${range.start}, end=${range.end}`)
  }
  if (range.start < 0 || range.start > range.end) {
    throw new RangeError(`Invalid range: start=${range.start}, end=${range.end}`)
  }
}

/**
 * Clamps range.end to the file size and validates that start is within bounds.
 * Returns the clamped end value.
 *
 * @internal
 */
export function clampRange(range: { start: number; end: number }, size: number): number {
  validateRange(range)
  const clampedEnd = Math.min(range.end, size - 1)
  if (range.start > clampedEnd) {
    throw new RangeError(`Range start ${range.start} exceeds size ${size}`)
  }
  return clampedEnd
}

/**
 * @public
 */
export type ContentItem = FileInfo & {
  /**
   * Gets the readable stream, uncompressed if necessary.
   */
  asStream(): Promise<Readable>

  /**
   * Used to get the raw stream, no matter how it is stored.
   * That may imply that the stream may be compressed.
   */
  asRawStream(): Promise<Readable>
}
