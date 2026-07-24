import {
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandInput,
  ListObjectsV2CommandOutput,
  S3Client
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { Readable } from 'stream'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem } from './content-item'
import { isAbortError, runStoreWithSignal } from './cancellation'

// Workaround: TS "commonjs" transforms import() to require().
// This indirection preserves the native import() needed for ESM-only packages.
const _importDynamic = Function('modulePath', 'return import(modulePath)') as (modulePath: string) => Promise<any>

const MIME_DETECTION_BYTES = 4100

/**
 * Reads at least the first `byteCount` bytes from the stream for inspection (chunk-granular, so it
 * may read a bit more — up to a whole chunk past the target), then returns those bytes together
 * with a Readable that re-emits them followed by the remainder of the original stream. This lets us
 * detect the MIME type from the head while streaming the body straight to S3, so large files are
 * never buffered in memory in full.
 */
async function peekHead(stream: Readable, byteCount: number): Promise<{ head: Buffer; body: Readable }> {
  const iterator = stream[Symbol.asyncIterator]()
  const headChunks: Buffer[] = []
  let headLength = 0
  let finished = false

  while (headLength < byteCount) {
    const next = await iterator.next()
    if (next.done) {
      finished = true
      break
    }
    const chunk = Buffer.isBuffer(next.value) ? next.value : Buffer.from(next.value)
    headChunks.push(chunk)
    headLength += chunk.length
  }

  const head = Buffer.concat(headChunks)

  const body = Readable.from(
    (async function* () {
      try {
        yield head
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
 * Detects the MIME type from a buffer.
 * Uses only the first bytes of the buffer for detection.
 * file-type v21 only needs the first ~4100 bytes to detect any file type.
 */
async function detectMimeTypeFromBuffer(buffer: Buffer | Uint8Array): Promise<string> {
  const maxBytesForDetection = 4100
  const bytesToUse = Math.min(maxBytesForDetection, buffer.length)
  const detectionBuffer = buffer.slice(0, bytesToUse)

  try {
    const { fileTypeFromBuffer } = await _importDynamic('file-type')
    const mime = await fileTypeFromBuffer(detectionBuffer)
    return mime?.mime || 'application/octet-stream'
  } catch (error: any) {
    return 'application/octet-stream'
  }
}

/**
 * @public
 */
export async function createAwsS3BasedFileSystemContentStorage(
  components: Pick<AppComponents, 'config' | 'logs'>,
  bucket: string
): Promise<IContentStorageComponent> {
  const { config, logs } = components

  const s3 = new S3Client({
    region: await config.requireString('AWS_REGION')
  })

  const getKey = (hash: string) => hash

  return createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: bucket, getKey })
}

/** Whether an S3 response body is a Node-style readable stream this storage can serve. */
function isReadable(body: unknown): body is Readable {
  return body instanceof Readable || typeof (body as Readable | undefined)?.pipe === 'function'
}

/** Names what arrived instead of a stream, for an actionable error. */
function describeBody(body: unknown): string {
  if (body === undefined) return 'undefined'
  if (body === null) return 'null'
  const name = (body as object)?.constructor?.name
  return name ? `a ${name}` : typeof body
}

/**
 * S3-backed content storage.
 *
 * `s3` must be a real `S3Client`, not a structural stand-in. Stores go through
 * `@aws-sdk/lib-storage`'s managed upload, which reads the client's own `config` (endpoint and
 * region resolvers, credentials) and runs its middleware stack — a hand-rolled object with just a
 * `send` method fails at runtime, not at compile time. Tests that need to intercept requests should
 * construct an `S3Client` with dummy credentials and replace `send` on it (see
 * `test/fake-s3-client.ts`), which keeps that machinery intact.
 *
 * Cancellation scope: a cancelled store aborts the in-flight request itself, so the object does not
 * appear at its key. The one case this cannot cover is a request S3 has ALREADY received in full
 * when the abort fires — tearing down the connection does not un-send those bytes, and the service
 * may still apply them. That residue is bounded: S3 object writes are atomic, so the key is either
 * absent or holds the complete content, never a partial or mixed state, and since content is
 * addressed by its own hash the worst outcome is the correct bytes existing under their own id after
 * a store reported as cancelled. The folder-based backend has no such window — its commit is a local
 * rename it fully controls.
 *
 * @public
 */
export async function createS3BasedFileSystemContentStorage(
  components: Pick<AppComponents, 'logs'>,
  s3: S3Client,
  options: { Bucket: string; getKey?: (hash: string) => string }
): Promise<IContentStorageComponent> {
  const logger = components.logs.getLogger('s3-based-content-storage')
  const getKey = options.getKey || ((hash: string) => hash)
  const Bucket = options.Bucket

  async function exist(id: string): Promise<boolean> {
    try {
      const obj = await s3.send(new HeadObjectCommand({ Bucket, Key: getKey(id) }))
      return !!obj.ETag
    } catch {
      return false
    }
  }

  async function storeStream(id: string, stream: Readable, signal?: AbortSignal): Promise<void> {
    // Destroying the source on abort is not enough to cancel the upload: once the SDK has
    // buffered the bytes (always, for files below the part size) the request no longer depends
    // on the source, so the managed upload itself must be aborted to tear down its transport.
    let upload: Upload | undefined
    // The controller the managed upload observes. Owned here (rather than left to lib-storage's
    // internal one) so the SAME signal that stops the upload's own loop is also handed to every
    // request it issues — see `abortableClient`.
    const uploadAbort = new AbortController()
    // Set only when WE abort the upload. `Upload.done()` races the upload against an abort watcher
    // that rejects with lib-storage's own `AbortError` as soon as abort() fires, so every rejection
    // after our teardown carries that shape — including one that would otherwise have been a real
    // S3 failure, which the SDK discards above this layer and we therefore cannot recover.
    // a shape the shared translator deliberately refuses to credit (a transport can raise one for
    // its own reasons). Tracking our own teardown here is the provenance that lets this call site
    // attribute the rejection, the same way the compression pipeline attributes its own.
    let abortedUpload = false
    const abortUpload = (): boolean => {
      if (!upload) return false
      try {
        // Guarded: a custom double's abort() must not replace the caller's cancellation reason.
        void upload.abort()
      } catch {
        // best-effort teardown
      }
      // Also abort directly: a custom double's abort() may not touch the controller, and it is the
      // signal — not `Upload.abort()` — that tears the in-flight request down.
      uploadAbort.abort()
      abortedUpload = true
      return true
    }
    // lib-storage issues `client.send(command)` with NO request options, so its PutObject,
    // UploadPart and CompleteMultipartUpload calls carry no abortSignal: `abort()` only wins the
    // race inside `done()` while the HTTP request it started keeps going and can still commit the
    // object AFTER this store has rejected as cancelled. Delegating through a wrapper that attaches
    // the signal makes those requests genuinely abortable.
    //
    // `Object.create` so every other client property lib-storage reads — `config` and its endpoint
    // resolver, `requestHandler`, `forcePathStyle` — resolves to the real client's own values
    // through the prototype chain, and only `send` is replaced.
    //
    // AbortMultipartUpload is deliberately EXEMPT: it is the cleanup that removes uploaded parts,
    // and lib-storage issues it precisely when the upload was aborted. Attaching the (already
    // aborted) signal would cancel the cleanup and leave the parts to accumulate.
    const abortableClient: S3Client = Object.assign(Object.create(s3) as S3Client, {
      send: ((command: any, sendOptions?: any) =>
        // Matched by name rather than `instanceof`: lib-storage takes client-s3 as a PEER
        // dependency, so an install that hoists two copies would fail an identity check and
        // silently cancel the cleanup — the one outcome this exemption exists to prevent.
        command?.constructor?.name === 'AbortMultipartUploadCommand'
          ? s3.send(command, sendOptions)
          : s3.send(command, { ...sendOptions, abortSignal: uploadAbort.signal })) as S3Client['send']
    })
    await runStoreWithSignal(
      stream,
      signal,
      async () => {
        // Inspect only the head for MIME detection, then stream the body straight to S3 so large
        // files are never buffered in memory. The AWS SDK's managed upload performs a multipart
        // upload, buffering only part-sized chunks rather than the whole file.
        const { head, body } = await peekHead(stream, MIME_DETECTION_BYTES)
        const mimeType = await detectMimeTypeFromBuffer(head)
        signal?.throwIfAborted()

        upload = new Upload({
          client: abortableClient,
          abortController: uploadAbort,
          params: {
            Bucket,
            Key: getKey(id),
            Body: body,
            ContentType: mimeType
          }
        })
        // The abort listener can only tear the upload down once `upload` is assigned: an abort
        // landing before this point found it undefined and did nothing, and — with a small source
        // already fully buffered into the head — the upload no longer needs the source, so it would
        // complete and commit content for an already-cancelled store. Re-check here, where the
        // upload provably exists, and tear it down ourselves.
        if (signal?.aborted) {
          abortUpload()
          signal.throwIfAborted()
        }
        try {
          await upload.done()
        } catch (error) {
          // Release the source stream if the upload stopped consuming the body (e.g. it failed
          // before reading anything, so peekHead's generator never started and can't self-clean).
          // Destroying the source releases its underlying resources (e.g. file descriptors).
          // No-op if already ended/destroyed. Guarded so a custom stream whose destroy() throws
          // cannot replace the upload error the caller needs to see.
          try {
            stream.destroy()
          } catch {
            // best-effort cleanup; the upload error below is what matters
          }
          // We aborted this upload, so its AbortError is provably our teardown: report the caller's
          // reason. An abort error we did NOT cause (an SDK-internal request abort racing the
          // cancellation) falls through and surfaces as itself.
          if (abortedUpload && isAbortError(error)) {
            signal?.throwIfAborted()
          }
          throw error
        }
      },
      // Tears the managed upload down; the local `abortedUpload` flag it sets is what lets the catch
      // above attribute an AbortError to this cancellation.
      abortUpload
    )
  }

  async function retrieve(id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> {
    if (range) validateRange(range)
    try {
      const obj = await s3.send(new HeadObjectCommand({ Bucket, Key: getKey(id) }))

      const size = obj.ContentLength ?? null
      const clampedEnd = range && size !== null ? clampRange(range, size) : undefined

      return new SimpleContentItem(
        async () => {
          const output = await s3.send(
            new GetObjectCommand({
              Bucket,
              Key: getKey(id),
              Range: range ? `bytes=${range.start}-${clampedEnd ?? range.end}` : undefined
            })
          )
          // `Body` is optional in the v3 types and the runtime shape depends on the platform: in
          // Node it is a Readable (an SdkStream wrapping the response), in a browser build a Blob or
          // web ReadableStream. Verify rather than cast blindly, so a misconfigured client or an
          // unexpected response becomes a clear storage error at the point of failure instead of a
          // non-stream reaching consumers and breaking somewhere further away.
          const body = output.Body
          if (!isReadable(body)) {
            throw new Error(
              `S3 returned no readable body for ${getKey(id)}; received ${describeBody(body)}. This storage ` +
                `requires a Node-style stream body — check that the client is an @aws-sdk/client-s3 S3Client ` +
                `running on Node.`
            )
          }
          return body
        },
        range ? (clampedEnd !== undefined ? clampedEnd - range.start + 1 : null) : size,
        obj.ContentEncoding || null
      )
    } catch (error: any) {
      if (error instanceof RangeError) throw error
      // A missing object returns NotFound (404) when the principal has s3:ListBucket; there is
      // nothing to serve, so fall through and return undefined.
      // v3 reports the error code as `name` and the HTTP status under `$metadata`.
      const statusCode = error.$metadata?.httpStatusCode
      const code = error.name
      if (code !== 'NotFound' && statusCode !== 404) {
        const logContext = { key: getKey(id), code, statusCode }
        if (statusCode === 403) {
          // S3 returns 403 (with an empty body, hence a null message on HEAD) instead of 404 for a
          // missing key when the principal lacks s3:ListBucket. It can also be a genuine access
          // denial. Surface it as an actionable warning rather than a bare, message-less error.
          logger.warn(
            'S3 returned 403 Forbidden retrieving content; returning not-found. If the object is simply missing, grant the principal s3:ListBucket so missing keys return 404; otherwise check the object/bucket permissions.',
            logContext
          )
        } else {
          logger.error(`Failed to retrieve content from S3: ${error.message || code || 'unknown error'}`, logContext)
        }
      }
    }
    return undefined
  }

  async function storeStreamAndCompress(id: string, stream: Readable, signal?: AbortSignal): Promise<void> {
    // In AWS S3 we don't compress, we directly store the stream
    await storeStream(id, stream, signal)
  }

  async function deleteFn(ids: string[]): Promise<void> {
    await s3.send(
      new DeleteObjectsCommand({
        Bucket,
        Delete: {
          Objects: ids.map(($) => ({ Key: getKey($) }))
        }
      })
    )
  }

  async function existMultiple(cids: string[]): Promise<Map<string, boolean>> {
    return new Map(await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)])))
  }

  async function* allFileIds(prefix?: string): AsyncIterable<string> {
    const params: ListObjectsV2CommandInput = {
      Bucket,
      ContinuationToken: undefined
    }

    if (prefix) {
      params.Prefix = prefix
    }

    let output: ListObjectsV2CommandOutput
    do {
      output = await s3.send(new ListObjectsV2Command(params))
      if (output.Contents) {
        for (const content of output.Contents) {
          yield content.Key!
        }
      }
      params.ContinuationToken = output.NextContinuationToken
    } while (output.IsTruncated)
  }

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    try {
      const obj = await s3.send(new HeadObjectCommand({ Bucket, Key: getKey(id) }))
      const size = obj.ContentLength ?? null
      return {
        encoding: obj.ContentEncoding || null,
        size,
        contentSize: obj.ContentEncoding ? null : size
      }
    } catch {
      return undefined
    }
  }

  async function fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
    return new Map(
      await Promise.all(cids.map(async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)]))
    )
  }

  return {
    exist,
    fileInfo,
    fileInfoMultiple,
    storeStream,
    retrieve,
    storeStreamAndCompress,
    delete: deleteFn,
    existMultiple,
    allFileIds
  }
}
