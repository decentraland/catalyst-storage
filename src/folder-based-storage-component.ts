import { createHash } from "crypto"
import path from "path"
import { pipeline, Readable } from "stream"
import { promisify } from "util"
import { AppComponents } from "./types"
import { SimpleContentItem } from "./content-item"
import { compressContentFile } from "./extras/compression"
import { ContentItem, IContentStorageComponent } from "./types"

const pipe = promisify(pipeline)

export type FolderBasedContentStorage = IContentStorageComponent & {
  allFileIds(): AsyncIterable<string>
}

export async function createFolderBasedFileSystemContentStorage(
  components: Pick<AppComponents, "fs">,
  root: string
): Promise<FolderBasedContentStorage> {
  // remove path separators / \ from the end of the folder
  while (root.endsWith(path.sep)) {
    root = root.slice(0, -1)
  }

  await components.fs.mkdir(root, { recursive: true })

  const getFilePath = async (id: string): Promise<string> => {
    // We are sharding the files using the first 4 digits of its sha1 hash, because it generates collisions
    // for the file system to handle millions of files in the same directory.
    // This way, asuming that sha1 hash distribution is ~uniform we are reducing by 16^4 the max amount of files in a directory.
    const directoryPath = path.join(root, createHash("sha1").update(id).digest("hex").substring(0, 4))
    if (!(await components.fs.existPath(directoryPath))) {
      await components.fs.mkdir(directoryPath, { recursive: true })
    }
    return path.join(directoryPath, id)
  }

  const retrieveWithEncoding = async (id: string, encoding: string | null): Promise<ContentItem | undefined> => {
    const extension = encoding ? "." + encoding : ""
    const filePath = (await getFilePath(id)) + extension

    if (await components.fs.existPath(filePath)) {
      const stat = await components.fs.stat(filePath)
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

  const retrieve = async (id: string): Promise<ContentItem | undefined> => {
    try {
      return (await retrieveWithEncoding(id, "gzip")) || (await retrieveWithEncoding(id, null))
    } catch (error) {
      console.error(error)
    }
    return undefined
  }

  async function exist(id: string): Promise<boolean> {
    return !!(await retrieve(id))
  }

  const allFileIdsRec = async function* (folder: string): AsyncIterable<string> {
    const dirEntries = await components.fs.opendir(folder, { bufferSize: 4000 })
    for await (const entry of dirEntries) {
      entry.isDirectory()
        ? yield* allFileIdsRec(path.resolve(folder, entry.name))
        : yield entry.name.replace(/\.gzip/, "")
    }
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
        await noFailUnlink((await getFilePath(id)) + ".gzip")
      }
    },
    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      const entries = await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]))
      return new Map(entries)
    },
    allFileIds: () => allFileIdsRec(root),
  }
}
