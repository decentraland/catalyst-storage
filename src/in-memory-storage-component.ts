import { Readable } from 'stream'
import { clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { runStoreWithSignal } from './cancellation'
import { assertAddressableContentId, assertStorableContentId } from './content-id'
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
      // This backend has no filesystem to refuse an unstorable name for it, so without this it accepted
      // ids the folder-based and S3 backends cannot store — the divergence these shared checks exist to
      // prevent. Reads deliberately do NOT enforce it; see `assertStorableContentId`.
      assertStorableContentId(fileId)
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
      // Validated and deleted id by id, NOT validated-all-then-deleted-all. The folder-based backend
      // resolves and removes each id in turn, so `delete(['victim', '../evil'])` removes `victim` and
      // then rejects; validating up front here left `victim` in place for the same call, so a service
      // exercised against this backend in tests saw the opposite outcome in production. `delete` is
      // idempotent, so retrying the whole list is the recovery in both.
      for (const id of ids) {
        assertAddressableContentId(id)
        storage.delete(id)
      }
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
      // COPIED, not aliased. `fromBuffer` streams the buffer it is given as-is, so handing over the
      // stored one let a consumer that writes into a chunk it received rewrite stored content in
      // place — `for await (const chunk of await item.asStream()) chunk.write('…')` changed what the
      // next `retrieve` returned. A `subarray` range is a view over the same memory, so it aliased
      // too. Every other backend reads bytes off a disk or a socket and so cannot be aliased; this
      // one has to copy to give the same guarantee.
      if (range) {
        const clampedEnd = clampRange(range, content.length)
        return SimpleContentItem.fromBuffer(Buffer.from(content.subarray(range.start, clampedEnd + 1)))
      }
      return SimpleContentItem.fromBuffer(Buffer.from(content))
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
