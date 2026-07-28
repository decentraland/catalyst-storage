import * as nodeFs from 'fs'
import * as path from 'path'
import { Readable, Transform, Writable } from 'stream'
import { pipeline } from 'stream/promises'
import { createGzip } from 'zlib'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { IFileSystemComponent } from '../fs/types'
import { destroyAllQuietly, ignoreStreamError } from '../stream-teardown'

/**
 * The filesystem surface compression needs: the two streams, a size probe and a cleanup unlink.
 * Narrow on purpose, so any adapter that can stream and stat its own paths can be compressed
 * through — not just real local files.
 *
 * @internal
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
 * Stops a compression that has already grown too large to satisfy the 1.1 rule. A sentinel, not a
 * failure: it never escapes `gzipCompressFile`, which turns it into the same "not compressed" answer
 * the post-hoc ratio check produces.
 */
const NOT_WORTH_COMPRESSING = new Error('The compressed output cannot beat the original by enough to keep it')

/**
 * What a successful compression measured. Internal: `compressContentFile` reports only whether the
 * compression was kept, so this shape has never been reachable from any exported signature.
 */
type CompressionResult = {
  originalSize: number
  compressedSize: number
}

/**
 * @internal
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
  // Built once the original's size is known, so it is declared out here for the teardown below.
  let counter: Transform | undefined
  let originalSize = 0
  let compressedSize = 0

  try {
    try {
      source = fs.createReadStream(input)
      // ABSORBED FROM THE MOMENT IT EXISTS, not only once the teardown below runs: a read stream reports a
      // failed `open(2)` asynchronously and the very next line AWAITS, so an emit landing in that window is an
      // unhandled 'error' event, which terminates the process by default. The teardown cannot cover it — it
      // does not run until the await has settled. Reachable with no adapter and no corruption, since a MISSING
      // input fails both the open and the probe and load decides which lands first; it surfaced twice here as
      // an intermittent uncaught ENOENT. An extra listener does not displace `pipeline`'s own.
      source.on('error', ignoreStreamError)
      // Probed AFTER the source is constructed but BEFORE the destination, and both halves of that are
      // load-bearing:
      // - after the source, so a failure here runs the teardown that releases it rather than leaking the
      //   descriptor.
      // - before the destination, so a failure here never leaves an output file behind. Opening the
      //   destination first meant its `open(2)` could still be in flight when this probe rejected, and
      //   complete AFTER the cleanup unlink — re-creating the very `.gzip` that was just removed, which
      //   in the in-place mode is a canonical empty `.gzip` that reads prefer over the real content.
      //
      // lstat when the adapter has it (the bundled component does): both paths are files this storage
      // created, and measuring a link rather than its target keeps a symlinked path from reporting
      // someone else's size. `stat` is the fallback, since lstat is optional. Called ON the component,
      // never detached into a local: an adapter whose methods rely on `this` (a class instance, for
      // example) would break if the function were pulled off the object first.
      originalSize = (fs.lstat ? await fs.lstat(input) : await fs.stat(input)).size
      // Stops the compression the moment its output can no longer satisfy the 1.1 rule. 8MB of
      // incompressible media (a PNG, a JPEG, a GLB — the bulk of Decentraland content by bytes) cost a
      // measured 128ms of CPU at every zlib level, plus an 8MB staged write and an 8MB read back, to
      // produce a file that was then deleted. EXACT, not a heuristic: the pipeline is abandoned only
      // once the output has passed the point where `compressedSize * 1.1 > originalSize` is already
      // guaranteed, so which files end up compressed does not change. It also counts the output size, so
      // the second post-pipeline size probe is gone too.
      const usefulOutputLimit = originalSize / 1.1
      counter = new Transform({
        transform(chunk: Buffer, _encoding, callback) {
          compressedSize += chunk.length
          if (compressedSize > usefulOutputLimit) {
            callback(NOT_WORTH_COMPRESSING)
            return
          }
          callback(null, chunk)
        }
      })
      destination = fs.createWriteStream(output)
      // This @types/node version requires `signal` in PipelineOptions, so branch instead of
      // passing an options object without one.
      if (signal) {
        await pipeline(source, gzip, counter, destination, { signal })
      } else {
        await pipeline(source, gzip, counter, destination)
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
      // `gzip` is included even though it is constructed above the try: when `createReadStream` or
      // `createWriteStream` throws, `pipeline` never takes ownership of it, so nothing else will
      // ever destroy it and its native zlib deflate state (~16KB) is held until GC — on a failure
      // loop, once per attempt.
      // `destroyAllQuietly`, not a hand-rolled loop. This was the one teardown site in the package that
      // was not exception-safe: a `destroy()` that THROWS — a custom adapter's stream, an already-detached
      // handle — replaced the real pipeline error (an ENOSPC on the staged write is the one that matters)
      // with the teardown's own, skipped the remaining streams so the staged-gzip write descriptor and
      // zlib's deflate state stayed held, and, because the throw escaped the `finally` rather than the
      // `try`, meant `removeOutput` never ran and a partial staged `.gzip` was left behind. The shared
      // helper exists for exactly this and every sibling teardown already used it.
      destroyAllQuietly(source, gzip, counter, destination)
    }

    // Reaching here means the 1.1 rule is already satisfied, so there is nothing left to check: the
    // counter above bails out on `compressedSize > originalSize / 1.1`, which is the same condition as
    // the `compressedSize * 1.1 > originalSize` this used to re-test afterwards. The rule — a gain of at
    // least 10% to justify the CPU of decompressing on every read — is enforced in exactly one place
    // now, at the earliest point it can be decided, and the compressed size is COUNTED as the bytes flow
    // through rather than probed from the filesystem afterwards.
    return {
      originalSize,
      compressedSize
    }
  } catch (err) {
    // The early bail-out arrives here too, but it is NOT a failure: it is the 1.1 rule reaching its
    // verdict before the whole file had to be compressed to prove it. It gets its own reason so a
    // stray output that cannot be cleaned up is not reported to an operator as a compression fault.
    const notWorthCompressing = err === NOT_WORTH_COMPRESSING
    // On any failure (read/write/gzip error) remove the partial .gzip so it can't shadow the
    // source file and be served as corrupt content on a later read.
    await removeOutput(
      output,
      notWorthCompressing ? 'a non-beneficial compression ratio' : 'a compression failure',
      fs,
      logger
    )
    if (notWorthCompressing) return null
    throw err
  }
}
