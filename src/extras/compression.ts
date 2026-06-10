import destroy from 'destroy'
import * as fs from 'fs'
import * as path from 'path'
import { pipeline } from 'stream'
import { promisify } from 'util'
import { createGzip } from 'zlib'
const pipe = promisify(pipeline)

/**
 * @public
 */
export type CompressionResult = {
  originalSize: number
  compressedSize: number
}

/**
 * @public
 */
export async function compressContentFile(contentFilePath: string): Promise<boolean> {
  const result = await gzipCompressFile(contentFilePath, contentFilePath + '.gzip')
  return !!result
}

async function gzipCompressFile(input: string, output: string): Promise<CompressionResult | null> {
  if (path.resolve(input) === path.resolve(output)) {
    throw new Error("Can't compress a file using src==dst")
  }
  const gzip = createGzip()
  const source = fs.createReadStream(input)
  const destination = fs.createWriteStream(output)

  try {
    try {
      await pipe(source, gzip, destination)
    } finally {
      destroy(source)
      destroy(destination)
    }

    const originalSize = await fs.promises.lstat(input)
    const newSize = await fs.promises.lstat(output)

    if (newSize.size * 1.1 > originalSize.size) {
      // if the new file is bigger than the original file then we delete the compressed file
      // the 1.1 magic constant is to establish a gain of at least 10% of the size to justify the
      // extra CPU of the decompression. Awaited so the .gzip is gone before we return.
      await fs.promises.unlink(output).catch(() => undefined)
      return null
    }

    return {
      originalSize: originalSize.size,
      compressedSize: newSize.size
    }
  } catch (err) {
    // On any failure (read/write/gzip error) remove the partial .gzip so it can't shadow the
    // source file and be served as corrupt content on a later read.
    await fs.promises.unlink(output).catch(() => undefined)
    throw err
  }
}
