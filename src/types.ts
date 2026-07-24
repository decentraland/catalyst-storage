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
   * @param signal Optional cancellation signal. When it aborts, the store stops consuming the
   * stream, tears down any in-flight transport (e.g. the S3 upload), and rejects with the
   * signal's reason. A store that completes before observing the abort is allowed to succeed;
   * either way no partial content is ever observable under the id.
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
 * @public
 */
/**
 * Validates that a range is well-formed (start >= 0 and start <= end).
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
 */
export function clampRange(range: { start: number; end: number }, size: number): number {
  validateRange(range)
  const clampedEnd = Math.min(range.end, size - 1)
  if (range.start > clampedEnd) {
    throw new RangeError(`Range start ${range.start} exceeds size ${size}`)
  }
  return clampedEnd
}

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
