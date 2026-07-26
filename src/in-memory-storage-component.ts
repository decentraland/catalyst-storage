import { Readable } from 'stream'
import { clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { runStoreWithSignal } from './cancellation'
import { assertAddressableContentId } from './content-id'
import { PathNotContainedError } from './folder-based/errors'

/**
 * @public
 *
 * Ids are validated to the same rules as the folder-based backend (see `assertAddressableContentId`)
 * — an id this backend accepts is one the others accept too, so a service whose id handling is
 * exercised here in tests behaves the same way in production.
 */
export function createInMemoryStorage(): IContentStorageComponent {
  const storage: Map<string, Uint8Array> = new Map()

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    assertAddressableContentId(id)
    const buffer = storage.get(id)
    return buffer ? { encoding: null, size: buffer.length, contentSize: buffer.length } : undefined
  }

  // Shared by both store methods (this backend does not compress). The checkpoint before the commit
  // matters for the same reason it does in the other backends: once the source is consumed,
  // destroying it cancels nothing, so without it an abort observed during the read would still
  // commit content for a cancelled request.
  const storeBuffered = (fileId: string, content: Readable, signal?: AbortSignal): Promise<void> =>
    runStoreWithSignal(content, signal, async () => {
      assertAddressableContentId(fileId)
      const buffer = await streamToBuffer(content)
      signal?.throwIfAborted()
      storage.set(fileId, buffer)
    })

  return {
    storeStreamAndCompress: storeBuffered,
    async exist(fileId: string): Promise<boolean> {
      assertAddressableContentId(fileId)
      return storage.has(fileId)
    },
    storeStream: storeBuffered,
    async delete(ids: string[]): Promise<void> {
      ids.forEach((id) => assertAddressableContentId(id))
      ids.forEach((id) => storage.delete(id))
    },
    async retrieve(fileId: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> {
      if (range) validateRange(range)
      // Matches the folder-based read contract: an id that does not name a storable object is
      // "nothing to serve" here, while `exist`/`fileInfo` reject it loudly.
      try {
        assertAddressableContentId(fileId)
      } catch (error) {
        if (error instanceof PathNotContainedError) return undefined
        throw error
      }
      const content = storage.get(fileId)
      if (!content) return undefined
      if (range) {
        const clampedEnd = clampRange(range, content.length)
        return SimpleContentItem.fromBuffer(content.subarray(range.start, clampedEnd + 1))
      }
      return SimpleContentItem.fromBuffer(content)
    },
    async existMultiple(fileIds: string[]): Promise<Map<string, boolean>> {
      fileIds.forEach((fileId) => assertAddressableContentId(fileId))
      return new Map(fileIds.map((fileId) => [fileId, storage.has(fileId)]))
    },
    async *allFileIds(prefix?: string): AsyncIterable<string> {
      for (const key of storage.keys()) {
        if (!prefix || key.startsWith(prefix)) {
          yield key
        }
      }
    },
    fileInfo,
    async fileInfoMultiple(fileIds: string[]): Promise<Map<string, FileInfo | undefined>> {
      return new Map(
        await Promise.all(
          fileIds.map(async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)])
        )
      )
    }
  }
}
