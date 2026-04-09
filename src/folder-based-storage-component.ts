import { createHash } from 'crypto'
import path from 'path'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import { AppComponents, ContentItem, FileInfo, IContentStorageComponent } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { compressContentFile } from './extras/compression'

const pipe = promisify(pipeline)

const ONE_HOUR_IN_MS = 60 * 60 * 1000
const FIVE_MINUTES_IN_MS = 5 * 60 * 1000
const ONE_GB_IN_BYTES = 1024 * 1024 * 1024

/** @public */
export type FolderStorageOptions = {
  /// by default FALSE, disables the sha1 prefix for all files. @see getFilePath
  disablePrefixHash: boolean
  /** TTL in milliseconds for cached decompressed files. Default: 1 hour. */
  decompressCacheTTL: number
  /** Max total size in bytes for cached decompressed files. Default: 1GB. */
  decompressCacheMaxSize: number
  /** How often to run the eviction check in milliseconds. Default: 5 minutes. */
  decompressCacheEvictionInterval: number
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
  const CACHE_MAX_SIZE = options?.decompressCacheMaxSize ?? ONE_GB_IN_BYTES
  const CACHE_EVICTION_INTERVAL = options?.decompressCacheEvictionInterval ?? FIVE_MINUTES_IN_MS

  // LRU cache tracker for decompressed gzip files written to disk
  const decompressCache = new Map<string, { size: number; lastAccess: number }>()
  let totalCacheSize = 0

  // Concurrency guard: prevents multiple simultaneous decompressions of the same file
  const inflightDecompressions = new Map<string, Promise<void>>()

  async function evictCache() {
    const now = Date.now()

    // TTL eviction
    for (const [filePath, entry] of decompressCache) {
      if (now - entry.lastAccess > CACHE_TTL) {
        if (await noFailUnlink(filePath)) {
          totalCacheSize -= entry.size
        }
        decompressCache.delete(filePath)
      }
    }

    // Size eviction (LRU)
    if (totalCacheSize > CACHE_MAX_SIZE) {
      const sorted = [...decompressCache.entries()].sort((a, b) => a[1].lastAccess - b[1].lastAccess)
      for (const [filePath, entry] of sorted) {
        if (totalCacheSize <= CACHE_MAX_SIZE) break
        if (await noFailUnlink(filePath)) {
          totalCacheSize -= entry.size
        }
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
        if (range.start < 0 || range.start > range.end) {
          throw new RangeError(`Invalid range: start=${range.start}, end=${range.end}`)
        }
        const clampedEnd = Math.min(range.end, stat.size - 1)
        if (range.start > clampedEnd) {
          throw new RangeError(`Range start ${range.start} exceeds file size ${stat.size}`)
        }
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

  async function removeCacheEntry(filePath: string) {
    const entry = decompressCache.get(filePath)
    if (entry) {
      if (await noFailUnlink(filePath)) {
        totalCacheSize -= entry.size
      }
      decompressCache.delete(filePath)
    }
  }

  function touchCacheEntry(filePath: string) {
    const entry = decompressCache.get(filePath)
    if (entry) {
      entry.lastAccess = Date.now()
    }
  }

  const retrieve = async (id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> => {
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
        const gzipItem = await retrieveWithEncoding(id, 'gzip')
        if (gzipItem) {
          if (range.start < 0 || range.start > range.end) {
            throw new RangeError(`Invalid range: start=${range.start}, end=${range.end}`)
          }

          const uncompressedPath = await getFilePath(id)

          // Wait for any in-flight decompression of the same file, or start one
          let decompressPromise = inflightDecompressions.get(uncompressedPath)
          const isOwner = !decompressPromise
          if (!decompressPromise) {
            decompressPromise = (async () => {
              const decompressed = await streamToBuffer(await gzipItem.asStream())
              try {
                await pipe(Readable.from(decompressed), components.fs.createWriteStream(uncompressedPath))
              } catch (err) {
                // Remove partial file to prevent serving corrupt data
                await noFailUnlink(uncompressedPath)
                throw err
              }

              const size = decompressed.length
              decompressCache.set(uncompressedPath, { size, lastAccess: Date.now() })
              totalCacheSize += size
            })()
            inflightDecompressions.set(uncompressedPath, decompressPromise)
          }
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
    return !!(await retrieve(id))
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

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    const possibleEncondings = ['gzip', null]
    const baseFilePath = await getFilePath(id)

    for (const encoding of possibleEncondings) {
      const extension = encoding ? '.' + encoding : ''
      const filePath = baseFilePath + extension
      if (await components.fs.existPath(filePath)) {
        const stat = await components.fs.stat(filePath)
        return {
          size: stat.size,
          encoding
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
      // Evict all cached files on shutdown to prevent disk leaks across restarts
      for (const [filePath, entry] of decompressCache) {
        if (await noFailUnlink(filePath)) {
          totalCacheSize -= entry.size
        }
        decompressCache.delete(filePath)
      }
    },
    storeStream,
    retrieve,
    exist,
    async storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
      await removeCacheEntry(await getFilePath(id))
      await storeStream(id, stream)
      if (await compressContentFile(await getFilePath(id))) {
        // try to remove original file if present
        const contentItem = await retrieve(id)
        if (contentItem?.encoding) {
          await noFailUnlink(await getFilePath(id))
        }
      }
    },
    async delete(ids: string[]): Promise<void> {
      for (const id of ids) {
        const filePath = await getFilePath(id)
        await removeCacheEntry(filePath)
        await noFailUnlink(filePath)
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
