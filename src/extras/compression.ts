import destroy from 'destroy'
import * as nodeFs from 'fs'
import * as path from 'path'
import { Readable, Writable } from 'stream'
import { pipeline } from 'stream/promises'
import { createGzip } from 'zlib'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { IFileSystemComponent } from '../fs/types'

/**
 * The filesystem surface compression needs: the two streams, a size probe and a cleanup unlink.
 * Narrow on purpose, so any adapter that can stream and stat its own paths can be compressed
 * through — not just real local files.
 *
 * @public
 */
export type CompressionFileSystem = Pick<
  IFileSystemComponent,
  'createReadStream' | 'createWriteStream' | 'unlink' | 'stat' | 'lstat'
>

/** Native `fs`, used when no filesystem is supplied. Only valid for real local paths. */
const NATIVE_FS: CompressionFileSystem = {
  createReadStream: nodeFs.createReadStream,
  createWriteStream: nodeFs.createWriteStream,
  unlink: nodeFs.promises.unlink,
  stat: nodeFs.promises.stat,
  lstat: nodeFs.promises.lstat
}

/**
 * What a successful compression measured. Internal: `compressContentFile` reports only whether the
 * compression was kept, so this shape has never been reachable from any exported signature.
 */
type CompressionResult = {
  originalSize: number
  compressedSize: number
}

/**
 * @public
 */
export async function compressContentFile(
  contentFilePath: string,
  logger?: ILoggerComponent.ILogger,
  output?: string,
  signal?: AbortSignal,
  fs: CompressionFileSystem = NATIVE_FS
): Promise<boolean> {
  // `fs` reads and writes through the caller's filesystem component, so an adapter that virtualizes
  // paths gets compressed stores too — not only atomic raw writes. It defaults to native node `fs`,
  // which is only valid for real local paths.
  // `output` lets callers stage the compressed file elsewhere (e.g. a temp dir) and rename it into
  // place themselves, so a process killed mid-compression cannot leave a partial .gzip at the
  // canonical path. Defaults to the in-place `<contentFilePath>.gzip` for backward compatibility.
  // `signal` aborts the read→gzip→write pipeline mid-flight (tearing its streams down) instead of
  // letting a cancelled request keep paying CPU/disk until the compression completes; the partial
  // output is removed before the rejection propagates.
  const result = await gzipCompressFile(contentFilePath, output ?? contentFilePath + '.gzip', fs, logger, signal)
  return !!result
}

/**
 * Removes the (possibly partial) compressed output. A missing file (ENOENT) is expected and
 * ignored; any other failure leaves a stray .gzip on disk, so it is surfaced via the logger.
 */
async function removeOutput(
  output: string,
  reason: string,
  fs: CompressionFileSystem,
  logger?: ILoggerComponent.ILogger
): Promise<void> {
  try {
    await fs.unlink(output)
  } catch (err: any) {
    if (err?.code !== 'ENOENT') {
      logger?.warn(`Failed to remove compressed file after ${reason}`, { output, error: err?.message ?? String(err) })
    }
  }
}

async function gzipCompressFile(
  input: string,
  output: string,
  fs: CompressionFileSystem,
  logger?: ILoggerComponent.ILogger,
  signal?: AbortSignal
): Promise<CompressionResult | null> {
  if (path.resolve(input) === path.resolve(output)) {
    throw new Error("Can't compress a file using src==dst")
  }
  const gzip = createGzip()
  // Constructed INSIDE the try: native fs reports open failures asynchronously, but a custom adapter
  // may throw synchronously — and a `createReadStream` that throws after the destination was opened
  // would otherwise leak that stream AND leave the (already created, empty) output behind, which in
  // the in-place mode is a canonical `.gzip` that reads would prefer over the real content.
  let source: Readable | undefined
  let destination: Writable | undefined

  try {
    try {
      source = fs.createReadStream(input)
      destination = fs.createWriteStream(output)
      // This @types/node version requires `signal` in PipelineOptions, so branch instead of
      // passing an options object without one.
      if (signal) {
        await pipeline(source, gzip, destination, { signal })
      } else {
        await pipeline(source, gzip, destination)
      }
    } finally {
      // Either may be undefined when its construction threw.
      //
      // The listener is attached BEFORE destroying, and is not optional. A stream whose `open(2)` is
      // still in flight goes on to emit 'error' even after `destroy()` — the path being removed in
      // the meantime is enough — and with no listener that is an uncaught exception, which
      // terminates the process by default. Measured at 200/200 escapes without it and 0 with it.
      // Whatever arrives here is post-mortem noise; the failure that brought us here, or the value
      // already produced, is what the caller needs.
      for (const stream of [source, destination]) {
        if (!stream) continue
        stream.on('error', () => undefined)
        destroy(stream)
      }
    }

    // lstat when the adapter has it (the bundled component does): both paths are files this
    // storage created, and measuring a link rather than its target keeps a symlinked path from
    // reporting someone else's size. `stat` is the fallback, since lstat is optional. Each is called
    // ON the component, never detached into a local: an adapter whose methods rely on `this` (a class
    // instance, for example) would break if the function were pulled off the object first.
    const originalSize = fs.lstat ? await fs.lstat(input) : await fs.stat(input)
    const newSize = fs.lstat ? await fs.lstat(output) : await fs.stat(output)

    if (newSize.size * 1.1 > originalSize.size) {
      // if the new file is bigger than the original file then we delete the compressed file
      // the 1.1 magic constant is to establish a gain of at least 10% of the size to justify the
      // extra CPU of the decompression. Awaited so the .gzip is gone before we return.
      await removeOutput(output, 'a non-beneficial compression ratio', fs, logger)
      return null
    }

    return {
      originalSize: originalSize.size,
      compressedSize: newSize.size
    }
  } catch (err) {
    // On any failure (read/write/gzip error) remove the partial .gzip so it can't shadow the
    // source file and be served as corrupt content on a later read.
    await removeOutput(output, 'a compression failure', fs, logger)
    throw err
  }
}
