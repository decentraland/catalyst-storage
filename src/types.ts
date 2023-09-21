import { Readable } from 'stream'
import { IFileSystemComponent } from './fs/types'
import { IConfigComponent, ILoggerComponent } from '@well-known-components/interfaces'
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
export type IContentStorageComponent = {
  storeStream(fileId: string, content: Readable): Promise<void>
  storeStreamAndCompress(fileId: string, content: Readable): Promise<void>
  delete(fileIds: string[]): Promise<void>
  retrieve(fileId: string): Promise<ContentItem | undefined>
  fileInfo(fileId: string): Promise<FileInfo | undefined>
  fileInfoMultiple(fileIds: string[]): Promise<Map<string, FileInfo | undefined>>
  exist(fileId: string): Promise<boolean>
  existMultiple(fileIds: string[]): Promise<Map<string, boolean>>
  allFileIds(prefix?: string): AsyncIterable<string>
}

export type FileInfo = {
  encoding: string | null
  size: number | null
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
