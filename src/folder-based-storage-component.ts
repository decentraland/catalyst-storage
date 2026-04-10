import { createHash } from 'crypto'
import path from 'path'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { compressContentFile } from './extras/compression'

const pipe = promisify(pipeline)

const ONE_HOUR_IN_MS = 60 * 60 * 1000
const FIVE_MINUTES_IN_MS = 5 * 60 * 1000
const FIVE_GB_IN_BYTES = 5 * 1024 * 1024 * 1024

/** @public */
export type FolderStorageOptions = {
  /// by default FALSE, disables the sha1 prefix for all files. @see getFilePath
  disablePrefixHash: boolean
  /** TTL in milliseconds for cached decompressed files. Default: 1 hour. */
  decompressCacheTTL?: number
  /** Max total size in bytes for cached decompressed files. Default: 5GB. */
  decompressCacheMaxSize?: number
  /** How often to run the eviction check in milliseconds. Default: 5 minutes. */
  decompressCacheEvictionInterval?: number
}

/**
 * @public
 */
export async function createFolderBasedFileSystemContentStorage(
  components: Pick<AppComponents, 'fs' | 'logs'>,
  root: string,
  options?: Partial<FolderStorageOptions>
): Promise<IContentStorageComponent> {
  const logger = components.logs.getLogger('folder-based-content-storage')

  // remove path separators / \ from the end of the folder
  while (root.endsWith(path.sep)) {
    root = root.slice(0, -1)
  }

  await components.fs.mkdir(root, { recursive: true })

  const USE_HASH_PREFIX = !(options?.disablePrefixHash ?? false)
  const CACHE_TTL = options?.decompressCacheTTL ?? ONE_HOUR_IN_MS
  const CACHE_MAX_SIZE = options?.decompressCacheMaxSize ?? FIVE_GB_IN_BYTES
  const CACHE_EVICTION_INTERVAL = options?.decompressCacheEvictionInterval ?? FIVE_MINUTES_IN_MS

  // LRU cache tracker for decompressed gzip files written to disk
  const decompressCache = new Map<string, { size: number; lastAccess: number }>()
  let totalCacheSize = 0

  // Concurrency guard: prevents multiple simultaneous decompressions of the same file
  const inflightDecompressions = new Map<string, Promise<void>>()

  let evicting = false
  async function evictCache() {
    if (evicting) return
    evicting = true
    try {
      await runEviction()
    } finally {
      evicting = false
    }
  }

  async function runEviction() {
    const now = Date.now()

    // TTL eviction
    for (const [filePath, entry] of decompressCache) {
      if (now - entry.lastAccess > CACHE_TTL) {
        await noFailUnlink(filePath)
        totalCacheSize -= entry.size
        decompressCache.delete(filePath)
      }
    }

    // Size eviction (LRU)
    if (totalCacheSize > CACHE_MAX_SIZE) {
      const sorted = [...decompressCache.entries()].sort((a, b) => a[1].lastAccess - b[1].lastAccess)
      for (const [filePath, entry] of sorted) {
        if (totalCacheSize <= CACHE_MAX_SIZE) break
        await noFailUnlink(filePath)
        totalCacheSize -= entry.size
        decompressCache.delete(filePath)
      }
    }
  }

  let evictionTimer: ReturnType<typeof setInterval> | undefined

  async function getFilePath(id: string): Promise<string> {
    // We are sharding the files using the first 4 digits of its sha1 hash, because it generates collisions
    // for the file system to handle millions of files in the same directory.
    // This way, asuming that sha1 hash distribution is ~uniform we are reducing by 16^4 the max amount of files in a directory.
    const hash = createHash('sha1').update(id).digest('hex').substring(0, 4)

    const directoryPath = path.normalize(USE_HASH_PREFIX ? path.join(root, hash) : root)

    const finalPath = path.normalize(path.join(directoryPath, id))

    // recursively creates the directory structure if needed
    const dirname = path.dirname(finalPath)

    if (!finalPath.startsWith(directoryPath)) {
      throw new Error('Cannot manipulate files outside of the root storage folder')
    }

    if (!(await components.fs.existPath(dirname))) {
      await components.fs.mkdir(dirname, { recursive: true })
    }

    return finalPath
  }

  const retrieveWithEncoding = async (
    id: string,
    encoding: string | null,
    range?: { start: number; end: number }
  ): Promise<ContentItem | undefined> => {
    const extension = encoding ? '.' + encoding : ''
    const filePath = (await getFilePath(id)) + extension

    if (await components.fs.existPath(filePath)) {
      const stat = await components.fs.stat(filePath)

      if (range) {
        const clampedEnd = clampRange(range, stat.size)
        return new SimpleContentItem(
          async () => components.fs.createReadStream(filePath, { start: range.start, end: clampedEnd }),
          clampedEnd - range.start + 1,
          encoding
        )
      }

      return new SimpleContentItem(async () => components.fs.createReadStream(filePath), stat.size, encoding)
    }

    return undefined
  }

  const noFailUnlink = async (path: string): Promise<boolean> => {
    try {
      await components.fs.unlink(path)
      return true
    } catch (error) {
      return false
    }
  }

  const storeStream = async (id: string, stream: Readable): Promise<void> => {
    const filePath = await getFilePath(id)
    try {
      await pipe(stream, components.fs.createWriteStream(filePath))
    } catch (err) {
      await noFailUnlink(filePath)
      throw err
    }
  }

  async function removeCacheEntry(filePath: string): Promise<boolean> {
    const entry = decompressCache.get(filePath)
    if (entry) {
      await noFailUnlink(filePath)
      totalCacheSize -= entry.size
      decompressCache.delete(filePath)
      return true
    }
    return false
  }

  function touchCacheEntry(filePath: string) {
    const entry = decompressCache.get(filePath)
    if (entry) {
      entry.lastAccess = Date.now()
    }
  }

  const retrieve = async (id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> => {
    if (range) validateRange(range)
    try {
      let contentItem: ContentItem | undefined = undefined
      if (!range) contentItem = await retrieveWithEncoding(id, 'gzip')
      if (!contentItem) {
        contentItem = await retrieveWithEncoding(id, null, range)
        if (contentItem && range) {
          // Update last access if this file is in the cache
          touchCacheEntry(await getFilePath(id))
        }
      }

      // If range was requested but uncompressed file doesn't exist, fall back to
      // decompressing the gzip file, writing it to disk as a cache, and serving the range.
      if (!contentItem && range) {
        const uncompressedPath = await getFilePath(id)

        // Wait for any in-flight decompression of the same file, or start one
        let decompressPromise = inflightDecompressions.get(uncompressedPath)
        const isOwner = !decompressPromise
        if (!decompressPromise) {
          const gzipItem = await retrieveWithEncoding(id, 'gzip')
          if (gzipItem) {
            decompressPromise = (async () => {
              try {
                await pipe(await gzipItem.asStream(), components.fs.createWriteStream(uncompressedPath))
              } catch (err) {
                // Remove partial file to prevent serving corrupt data
                await noFailUnlink(uncompressedPath)
                throw err
              }

              const stat = await components.fs.stat(uncompressedPath)
              decompressCache.set(uncompressedPath, { size: stat.size, lastAccess: Date.now() })
              totalCacheSize += stat.size
            })()
            inflightDecompressions.set(uncompressedPath, decompressPromise)
          }
        }
        if (decompressPromise) {
          try {
            await decompressPromise
          } finally {
            if (isOwner) inflightDecompressions.delete(uncompressedPath)
          }

          // Serve range from the cached uncompressed file
          contentItem = await retrieveWithEncoding(id, null, range)
        }
      }

      return contentItem
    } catch (error: any) {
      if (error instanceof RangeError) throw error
      logger.error(error)
    }
    return undefined
  }

  async function exist(id: string): Promise<boolean> {
    const filePath = await getFilePath(id)
    return (await components.fs.existPath(filePath + '.gzip')) || (await components.fs.existPath(filePath))
  }

  const allFileIdsRec = async function* (folder: string, prefix?: string): AsyncIterable<string> {
    const dirEntries = await components.fs.opendir(folder, { bufferSize: 4000 })
    for await (const entry of dirEntries) {
      if (entry.isDirectory()) {
        yield* allFileIdsRec(path.resolve(folder, entry.name), prefix)
      } else if (!prefix || entry.name.startsWith(prefix)) {
        const baseName = entry.name.replace(/\.gzip$/, '')
        // Skip cached uncompressed files when the .gzip version also exists
        if (baseName !== entry.name || !(await components.fs.existPath(path.resolve(folder, baseName + '.gzip')))) {
          yield baseName
        }
      }
    }
  }

  async function readGzipOriginalSize(filePath: string, gzipSize: number): Promise<number | null> {
    // The gzip format (RFC 1952) stores the original uncompressed size in its
    // trailer — the last 4 bytes (ISIZE field, uint32 little-endian).
    // This works for files < 4GB (ISIZE is mod 2^32).
    if (gzipSize < 8) return null // Too small to be a valid gzip file
    try {
      const stream = components.fs.createReadStream(filePath, {
        start: gzipSize - 4,
        end: gzipSize - 1
      })
      const buffer = await streamToBuffer(stream)
      return buffer.readUInt32LE(0)
    } catch {
      return null
    }
  }

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    const possibleEncondings = ['gzip', null]
    const baseFilePath = await getFilePath(id)

    for (const encoding of possibleEncondings) {
      const extension = encoding ? '.' + encoding : ''
      const filePath = baseFilePath + extension
      if (await components.fs.existPath(filePath)) {
        const stat = await components.fs.stat(filePath)
        if (encoding === 'gzip') {
          const contentSize = await readGzipOriginalSize(filePath, stat.size)
          return {
            size: stat.size,
            encoding,
            contentSize
          }
        }
        return {
          size: stat.size,
          encoding,
          contentSize: stat.size
        }
      }
    }

    return undefined
  }

  return {
    async start(_startOptions: any) {
      evictionTimer = setInterval(evictCache, CACHE_EVICTION_INTERVAL)
      evictionTimer.unref()
    },
    async stop() {
      if (evictionTimer) {
        clearInterval(evictionTimer)
        evictionTimer = undefined
      }
      // Wait for any inflight decompressions to finish before cleaning up
      await Promise.allSettled(inflightDecompressions.values())
      // Evict all cached files on shutdown to prevent disk leaks across restarts
      for (const [filePath, entry] of decompressCache) {
        await noFailUnlink(filePath)
        totalCacheSize -= entry.size
        decompressCache.delete(filePath)
      }
    },
    storeStream,
    retrieve,
    exist,
    async storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
      const filePath = await getFilePath(id)
      await removeCacheEntry(filePath)
      await storeStream(id, stream)
      if (await compressContentFile(filePath)) {
        // try to remove original file if present
        const contentItem = await retrieve(id)
        if (contentItem?.encoding) {
          await noFailUnlink(filePath)
        }
      }
    },
    async delete(ids: string[]): Promise<void> {
      for (const id of ids) {
        const filePath = await getFilePath(id)
        const wasCached = await removeCacheEntry(filePath)
        if (!wasCached) {
          await noFailUnlink(filePath)
        }
        await noFailUnlink(filePath + '.gzip')
      }
    },
    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      const entries = await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]))
      return new Map(entries)
    },
    allFileIds: (prefix?: string) => allFileIdsRec(root, prefix),
    fileInfo,
    async fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
      return new Map(
        await Promise.all(cids.map(async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)]))
      )
    }
  }
}
