import { Readable } from 'stream'
import { ILoggerComponent } from '@well-known-components/interfaces'

// Workaround: TS "commonjs" transforms import() to require().
// This indirection preserves the native import() needed for ESM-only packages.
const _importDynamic = Function('modulePath', 'return import(modulePath)') as (modulePath: string) => Promise<any>

/** file-type v21 needs no more than the first ~4100 bytes to identify any type it knows. */
export const MIME_DETECTION_BYTES = 4100

export const DEFAULT_MIME_TYPE = 'application/octet-stream'

/** Loads the module that supplies `fileTypeFromBuffer`. Injectable so callers can supply their own. */
export type FileTypeLoader = () => Promise<{ fileTypeFromBuffer: (buffer: Uint8Array) => Promise<{ mime?: string }> }>

/**
 * Reads the first `byteCount` bytes of the stream for inspection (fewer only when the source ends
 * first), then returns those bytes together with a Readable that re-emits them followed by the
 * remainder of the original stream. This lets us detect the MIME type from the head while streaming
 * the body straight to S3, so large files are never buffered in memory in full.
 *
 * The head is cut at exactly `byteCount` rather than at a chunk boundary, because a source is free to
 * deliver the whole file as a single chunk — `Readable.from(buffer)` does, including this library's
 * own `bufferToStream` — and the entire point of this function is that it never holds more than the
 * detection window in memory.
 */
export async function peekHead(stream: Readable, byteCount: number): Promise<{ head: Buffer; body: Readable }> {
  const iterator = stream[Symbol.asyncIterator]()
  const headChunks: Buffer[] = []
  let headLength = 0
  let finished = false
  // The tail of a chunk that ran past the detection window, re-emitted ahead of the rest of the
  // source so no bytes are lost by cutting mid-chunk.
  let remainder: Buffer | undefined

  while (headLength < byteCount) {
    const next = await iterator.next()
    if (next.done) {
      finished = true
      break
    }
    const chunk = Buffer.isBuffer(next.value) ? next.value : Buffer.from(next.value)
    const needed = byteCount - headLength
    if (chunk.length > needed) {
      headChunks.push(chunk.subarray(0, needed))
      remainder = chunk.subarray(needed)
      break
    }
    headChunks.push(chunk)
    headLength += chunk.length
  }

  const head = Buffer.concat(headChunks)

  const body = Readable.from(
    (async function* () {
      try {
        yield head
        if (remainder) yield remainder
        if (!finished) {
          let next = await iterator.next()
          while (!next.done) {
            yield Buffer.isBuffer(next.value) ? next.value : Buffer.from(next.value)
            next = await iterator.next()
          }
        }
      } finally {
        // Release the source stream whenever consumption stops — normal end, or early
        // termination such as the body being destroyed after a failed upload — so its
        // underlying resources (e.g. file descriptors) are not leaked.
        await iterator.return?.()
      }
    })()
  )

  return { head, body }
}

/**
 * The memoized `file-type` module. Loading it entered the ESM loader on EVERY store; it is resolved
 * once per process instead.
 *
 * A REJECTED load is deliberately not cached: a transient resolution failure must not permanently
 * downgrade every later store to `application/octet-stream`. The `catch` that clears the memo also
 * guarantees a warm-up call can never raise an unhandled rejection.
 */
let fileTypeModule: Promise<any> | undefined
export const loadFileType: FileTypeLoader = () => {
  if (!fileTypeModule) {
    const attempt = _importDynamic('file-type')
    fileTypeModule = attempt
    attempt.catch(() => {
      if (fileTypeModule === attempt) fileTypeModule = undefined
    })
  }
  return fileTypeModule
}

/**
 * Detects the MIME type of content from its leading bytes, falling back to
 * `application/octet-stream` when nothing recognizable is found or the detector is unavailable.
 *
 * `loadModule` is injectable because the real loader cannot run under a Jest sandbox: the dynamic
 * `import()` is issued from a `Function`-compiled helper, which the runtime cannot attribute to a
 * referencing module, so it rejects with "trying to `import` a file outside of the scope of the test
 * code". Production always uses the default.
 */
export async function detectMimeTypeFromBuffer(
  buffer: Buffer | Uint8Array,
  logger: ILoggerComponent.ILogger,
  loadModule: FileTypeLoader = loadFileType
): Promise<string> {
  const detectionBuffer = buffer.subarray(0, Math.min(MIME_DETECTION_BYTES, buffer.length))

  try {
    const { fileTypeFromBuffer } = await loadModule()
    const mime = await fileTypeFromBuffer(detectionBuffer)
    return mime?.mime || DEFAULT_MIME_TYPE
  } catch (error: any) {
    // NEVER silent. A failure here is indistinguishable from "this content has no recognizable
    // signature", so swallowing it stored every object as application/octet-stream with nothing in
    // the logs to say detection had stopped working at all. The store itself is still valid — the
    // content type is metadata — so fall back, but say so.
    logger.warn(`MIME type detection failed; storing as ${DEFAULT_MIME_TYPE}`, {
      error: error?.message ?? String(error)
    })
    return DEFAULT_MIME_TYPE
  }
}
