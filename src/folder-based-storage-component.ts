import { createHash } from 'crypto'
import path from 'path'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import { AppComponents, ContentItem, FileInfo, IContentStorageComponent } from './types'
import { SimpleContentItem } from './content-item'
import { compressContentFile } from './extras/compression'

const pipe = promisify(pipeline)

/** @public */
export type FolderStorageOptions = {
  /// by default FALSE, disables the sha1 prefix for all files. @see getFilePath
  disablePrefixHash: boolean
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

  const noFailUnlink = async (path: string) => {
    try {
      await components.fs.unlink(path)
    } catch (error) {
      // Ignore these errors
    }
  }

  const storeStream = async (id: string, stream: Readable): Promise<void> => {
    await pipe(stream, components.fs.createWriteStream(await getFilePath(id)))
  }

  const retrieve = async (id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> => {
    try {
      let contentItem: ContentItem | undefined = undefined
      if (!range) contentItem = await retrieveWithEncoding(id, 'gzip')
      if (!contentItem) contentItem = await retrieveWithEncoding(id, null, range)

      // If range was requested but uncompressed file doesn't exist, fall back to
      // decompressing the gzip file and extracting only the requested range.
      // We stream-decompress, skip bytes before range.start, and collect bytes
      // until range.end — only the range portion is buffered in memory.
      if (!contentItem && range) {
        const gzipItem = await retrieveWithEncoding(id, 'gzip')
        if (gzipItem) {
          if (range.start < 0 || range.start > range.end) {
            throw new RangeError(`Invalid range: start=${range.start}, end=${range.end}`)
          }
          const decompressedStream = await gzipItem.asStream()
          const chunks: Buffer[] = []
          let offset = 0

          await new Promise<void>((resolve, reject) => {
            decompressedStream.on('error', reject)
            decompressedStream.on('data', (chunk: Buffer) => {
              const chunkStart = offset
              const chunkEnd = offset + chunk.length - 1
              offset += chunk.length

              if (chunkEnd < range.start) return // before range, skip
              if (chunkStart > range.end) {
                decompressedStream.destroy() // past range, stop
                return
              }

              const sliceStart = Math.max(0, range.start - chunkStart)
              const sliceEnd = Math.min(chunk.length, range.end - chunkStart + 1)
              chunks.push(chunk.subarray(sliceStart, sliceEnd))
            })
            decompressedStream.on('end', resolve)
            decompressedStream.on('close', resolve)
          })

          const result = Buffer.concat(chunks)
          if (result.length > 0) {
            return SimpleContentItem.fromBuffer(result)
          }
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
        yield entry.name.replace(/\.gzip/, '')
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
    storeStream,
    retrieve,
    exist,
    async storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
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
        await noFailUnlink(await getFilePath(id))
        await noFailUnlink((await getFilePath(id)) + '.gzip')
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
