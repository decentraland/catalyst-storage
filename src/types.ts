import { Readable } from "stream"
import { IFileSystemComponent } from "./fs/types"
import { IConfigComponent } from "@well-known-components/interfaces"

export type AppComponents = {
  fs: IFileSystemComponent
  config: IConfigComponent
}

export type IContentStorageComponent = {
  storeStream(fileId: string, content: Readable): Promise<void>
  storeStreamAndCompress(fileId: string, content: Readable): Promise<void>
  delete(fileIds: string[]): Promise<void>
  retrieve(fileId: string): Promise<ContentItem | undefined>
  exist(fileId: string): Promise<boolean>
  existMultiple(fileIds: string[]): Promise<Map<string, boolean>>
}

export interface ContentItem {
  encoding: string | null
  size: number | null
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
