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
import { mapWithConcurrency } from './concurrency'
import {
  DEFAULT_MIME_TYPE,
  detectMimeTypeFromBuffer,
  loadFileType,
  MIME_DETECTION_BYTES,
  peekHead
} from './mime-detection'

/** S3's hard per-request limit for `DeleteObjects`; the SDK does not split a larger list. */
const DELETE_OBJECTS_MAX_KEYS = 1000

/**
 * How many ids a batch surface (`existMultiple`, `fileInfoMultiple`) may have in flight at once. Well
 * under the SDK's default connection pool, so a large batch queues instead of drawing 503 SlowDown.
 */
const BATCH_CONCURRENCY = 32

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

  const storage = await createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: bucket, getKey })

  return {
    ...storage,
    // This factory CONSTRUCTED the client, so releasing its socket pool is part of shutting the
    // component down — the SDK's own guidance is to destroy a client explicitly in Node, or sockets
    // stay open long after the last request. Nothing offered a way to do that, so the agent outlived
    // every consumer of this factory.
    //
    // `createS3BasedFileSystemContentStorage` deliberately does NOT do this: there the client is
    // injected, the caller owns it and may share it with other components.
    async stop() {
      await storage.stop?.()
      s3.destroy()
    }
  }
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
 * The request-level options `S3Client.send` accepts, derived from the SDK's own signature rather than
 * hand-declared, so it tracks the installed client version.
 */
type SendOptions = NonNullable<Parameters<S3Client['send']>[1]>

/**
 * Returns a client that behaves exactly like `s3` but attaches `signal` to the requests it issues.
 *
 * This exists because `@aws-sdk/lib-storage` calls `client.send(command)` with no request options, so
 * its PutObject, UploadPart and CompleteMultipartUpload requests are not cancellable: aborting the
 * managed upload only wins the race inside `done()` while the HTTP request it started keeps going and
 * can still commit the object. Attaching the signal here makes those requests genuinely abortable.
 *
 * `Object.create` so every other client member lib-storage reads — `config` and its endpoint
 * resolver, `requestHandler`, `forcePathStyle` — resolves to the real client's own values through the
 * prototype chain, with only `send` replaced.
 *
 * AbortMultipartUpload is deliberately EXEMPT: it is the cleanup that removes uploaded parts, and
 * lib-storage issues it precisely when the upload was aborted, so attaching the already-aborted
 * signal would cancel the cleanup and leave the parts to accumulate. It is matched by constructor
 * name rather than `instanceof` because client-s3 is a PEER dependency of lib-storage: an install
 * that hoists two copies would fail an identity check and silently break this exemption.
 *
 * `command` is intentionally untyped. The SDK's own parameter type for it is the service-wide
 * `Command<ServiceInputTypes, ServiceInputTypes, ServiceOutputTypes, ServiceOutputTypes, …>`
 * instantiation, which concrete commands are NOT assignable to (their `middlewareStack.add` is
 * invariant in the input/output types), so annotating it would reject every real caller. The options
 * argument — the part this shim actually constructs, and the only place a mistake here could silently
 * disable cancellation — is fully type-checked.
 */
function createAbortableClient(s3: S3Client, signal: AbortSignal): S3Client {
  const send = (command: any, sendOptions?: SendOptions): unknown => {
    if (command?.constructor?.name === 'AbortMultipartUploadCommand') {
      return s3.send(command, sendOptions)
    }
    const withAbortSignal: SendOptions = { ...sendOptions, abortSignal: signal }
    return s3.send(command, withAbortSignal)
  }
  return Object.assign(Object.create(s3) as S3Client, { send: send as S3Client['send'] })
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

  // Warm the ESM loader now rather than on the first store, so a resolution problem shows up in the
  // logs before traffic arrives instead of silently degrading every object's content type.
  void loadFileType().catch((error: any) =>
    logger.warn(
      `Could not preload the MIME detection module; stores will retry and fall back to ${DEFAULT_MIME_TYPE}`,
      { error: error?.message ?? String(error) }
    )
  )

  /**
   * Whether an S3 failure means "there is no object to serve" rather than "this storage could not
   * answer the question".
   *
   * Only a definitive not-found qualifies: `NotFound`/`NoSuchKey`, or a 404. 403 is included because
   * a principal without `s3:ListBucket` gets 403 instead of 404 for a MISSING key, and the response
   * carries nothing that separates that from a genuine denial — so "not here" is the only answer the
   * bucket policy leaves available. It is logged as actionable (see `warnIfForbidden`).
   *
   * Everything else — 500, 503 SlowDown, a network fault, expired credentials — is the STORAGE
   * failing. Reporting those as absent is what made a broken or throttled bucket look like an empty
   * one to callers, so they are surfaced; this is the same contract the folder-based backend gives.
   */
  function isNotFound(error: any): boolean {
    const statusCode = error?.$metadata?.httpStatusCode
    const code = error?.name
    return code === 'NotFound' || code === 'NoSuchKey' || statusCode === 404 || statusCode === 403
  }

  function logContextFor(id: string, error: any): Record<string, string | number> {
    const context: Record<string, string | number> = { key: getKey(id) }
    if (error?.name) context.code = error.name
    if (error?.$metadata?.httpStatusCode) context.statusCode = error.$metadata.httpStatusCode
    return context
  }

  /** A 403 read as not-found is worth an operator's attention: it may be a real permission problem. */
  function warnIfForbidden(operation: string, id: string, error: any): void {
    if (error?.$metadata?.httpStatusCode !== 403) return
    logger.warn(
      `S3 returned 403 Forbidden while ${operation}; reporting the content as not found. If the object is simply ` +
        `missing, grant the principal s3:ListBucket so missing keys return 404; otherwise check the object/bucket ` +
        `permissions.`,
      logContextFor(id, error)
    )
  }

  function logStorageFailure(operation: string, id: string, error: any): void {
    logger.error(
      `Failed while ${operation} in S3: ${error?.message || error?.name || 'unknown error'}`,
      logContextFor(id, error)
    )
  }

  async function exist(id: string): Promise<boolean> {
    try {
      await s3.send(new HeadObjectCommand({ Bucket, Key: getKey(id) }))
      // A HeadObject that succeeds IS the existence answer. Requiring an ETag on top of it is
      // stricter than the contract and reports a present object as absent on any S3-compatible
      // implementation that omits the header.
      return true
    } catch (error: any) {
      if (isNotFound(error)) {
        warnIfForbidden('checking whether content exists', id, error)
        return false
      }
      logStorageFailure('checking whether content exists', id, error)
      throw error
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
    const abortUpload = (): void => {
      if (!upload) return
      try {
        // `abort()` is declared async, so awaiting it here would stall the signal's event dispatch:
        // it is deliberately fire-and-forget. A rejected promise must still be absorbed — an
        // unhandled rejection terminates the process by default — and `Promise.resolve` also
        // normalizes a double whose abort() returns nothing at all. The surrounding try keeps the
        // guard against a synchronous throw, which happens before either wrapper applies. Teardown
        // is best-effort either way: a failing abort() must not replace the caller's reason, and the
        // controller aborted below is what actually tears the request down.
        void Promise.resolve(upload.abort()).catch(() => undefined)
      } catch {
        // best-effort teardown
      }
      // Also abort directly: a custom double's abort() may not touch the controller, and it is the
      // signal — not `Upload.abort()` — that tears the in-flight request down.
      uploadAbort.abort()
      abortedUpload = true
    }
    // Requests issued by the managed upload carry this signal, so cancelling tears the in-flight
    // request down instead of merely losing the race inside `done()`. See createAbortableClient.
    const abortableClient = createAbortableClient(s3, uploadAbort.signal)
    await runStoreWithSignal(
      stream,
      signal,
      async () => {
        // Inspect only the head for MIME detection, then stream the body straight to S3 so large
        // files are never buffered in memory. The AWS SDK's managed upload performs a multipart
        // upload, buffering only part-sized chunks rather than the whole file.
        const { head, body } = await peekHead(stream, MIME_DETECTION_BYTES)
        const mimeType = await detectMimeTypeFromBuffer(head, logger)
        // LOAD-BEARING, and it must stay immediately before the upload is constructed. An abort
        // landing during the two awaits above found `upload` still undefined, so the listener's
        // teardown did nothing — and with a small source already fully buffered into the head the
        // upload no longer needs that source, so it would run to completion and commit content for
        // an already-cancelled store. This is the checkpoint that stops it.
        //
        // Nothing below re-checks `signal.aborted` before `done()` because nothing can change it:
        // the constructor is synchronous, so no job — including the signal's own event dispatch —
        // runs between here and there. Anyone introducing an `await` in that gap has to add a
        // checkpoint back.
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
      const encoding = obj.ContentEncoding || null
      const itemSize = range ? (clampedEnd !== undefined ? clampedEnd - range.start + 1 : null) : size

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
        itemSize,
        encoding,
        // Same rule `fileInfo` applies: S3 keeps no uncompressed-size metadata, so for encoded
        // content the logical size is genuinely unknown. Letting SimpleContentItem default it to
        // `itemSize` passed the COMPRESSED byte count off as the content size to callers doing
        // `contentSize ?? size`. `null` is the documented "unknown".
        encoding ? null : itemSize
      )
    } catch (error: any) {
      if (error instanceof RangeError) throw error
      // v3 reports the error code as `name` and the HTTP status under `$metadata`.
      if (isNotFound(error)) {
        warnIfForbidden('retrieving content', id, error)
        return undefined
      }
      // The STORAGE failed, not the object missing. Answering `undefined` would tell the caller the
      // content is permanently absent while it may well be there, so a throttled or unreachable
      // bucket would read as an empty node and stop being retried.
      logStorageFailure('retrieving content', id, error)
      throw error
    }
  }

  async function storeStreamAndCompress(id: string, stream: Readable, signal?: AbortSignal): Promise<void> {
    // In AWS S3 we don't compress, we directly store the stream
    await storeStream(id, stream, signal)
  }

  async function deleteFn(ids: string[]): Promise<void> {
    // S3 caps DeleteObjects at DELETE_OBJECTS_MAX_KEYS keys per request and the SDK does not split
    // the list, so a larger batch was rejected outright as MalformedXML. An EMPTY list is rejected
    // the same way, and simply never enters the loop — matching the other backends, where deleting
    // nothing is a no-op rather than an error.
    for (let from = 0; from < ids.length; from += DELETE_OBJECTS_MAX_KEYS) {
      const batch = ids.slice(from, from + DELETE_OBJECTS_MAX_KEYS)
      const output = await s3.send(
        new DeleteObjectsCommand({
          Bucket,
          Delete: {
            Objects: batch.map(($) => ({ Key: getKey($) }))
          }
        })
      )
      // DeleteObjects reports PER-KEY failures inside a 200 response, so a resolved send is not a
      // completed delete. Ignoring them let `delete()` report success while the objects were still
      // readable; this backend now gives the same guarantee as the folder-based one, where a delete
      // that resolves means nothing readable survived it.
      const errors = output.Errors ?? []
      if (errors.length > 0) {
        const shown = errors
          .slice(0, 5)
          .map((each) => `${each.Key} (${each.Code ?? 'unknown'})`)
          .join(', ')
        throw new Error(
          `Failed to delete ${errors.length} of ${batch.length} object(s) from S3: ${shown}` +
            (errors.length > 5 ? ', …' : '')
        )
      }
    }
  }

  async function existMultiple(cids: string[]): Promise<Map<string, boolean>> {
    return new Map(
      await mapWithConcurrency(
        cids,
        BATCH_CONCURRENCY,
        async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]
      )
    )
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
      // A truncated page with no continuation token would otherwise re-request the FIRST page
      // forever, yielding the same keys on every pass.
    } while (output.IsTruncated && params.ContinuationToken)
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
    } catch (error: any) {
      if (isNotFound(error)) {
        warnIfForbidden('reading content metadata', id, error)
        return undefined
      }
      logStorageFailure('reading content metadata', id, error)
      throw error
    }
  }

  async function fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
    return new Map(
      await mapWithConcurrency(
        cids,
        BATCH_CONCURRENCY,
        async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)]
      )
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
