import { Readable } from 'stream'
import { ContentItem, IContentStorageComponent } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'

/**
 * @public
 */
export function createInMemoryStorage(): IContentStorageComponent {
  const storage: Map<string, Uint8Array> = new Map()

  return {
    async storeStreamAndCompress(fileId: string, content: Readable): Promise<void> {
      storage.set(fileId, await streamToBuffer(content))
    },
    async exist(fileId: string): Promise<boolean> {
      return storage.has(fileId)
    },
    async storeStream(fileId: string, content: Readable): Promise<void> {
      storage.set(fileId, await streamToBuffer(content))
    },
    async delete(ids: string[]): Promise<void> {
      ids.forEach((id) => storage.delete(id))
    },
    async retrieve(fileId: string): Promise<ContentItem | undefined> {
      const content = storage.get(fileId)
      return content ? SimpleContentItem.fromBuffer(content) : undefined
    },
    async existMultiple(fileIds: string[]): Promise<Map<string, boolean>> {
      return new Map(fileIds.map((fileId) => [fileId, storage.has(fileId)]))
    },
    async *allFileIds(prefix?: string): AsyncIterable<string> {
      for (const key of storage.keys()) {
        if (!prefix || key.startsWith(prefix)) {
          yield key
        }
      }
    }
  }
}
