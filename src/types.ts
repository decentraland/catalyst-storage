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
  storeStream(fileId: string, content: Readable): Promise<void>
  storeStreamAndCompress(fileId: string, content: Readable): Promise<void>
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
}

/**
 * @public
 */
/**
 * Validates that a range is well-formed (start >= 0 and start <= end).
 */
export function validateRange(range: { start: number; end: number }): void {
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
