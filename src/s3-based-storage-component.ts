import {
  AbortMultipartUploadCommand,
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

import {
  AppComponents,
  clampRange,
  ContentItem,
  FileInfo,
  IContentStorageComponent,
  RangeNotSupportedError,
  validateRange
} from './types'
import { contentCodingOf, normalizeContentEncoding, SimpleContentItem } from './content-item'
import { isAbortError, runStoreWithSignal } from './cancellation'
import { mapWithConcurrency } from './concurrency'
import {
  DEFAULT_MIME_TYPE,
  detectMimeTypeFromBuffer,
  FileTypeLoader,
  loadFileType,
  MIME_DETECTION_BYTES,
  peekHead
} from './mime-detection'

/** Absorbs a torn-down stream's trailing 'error', which would otherwise be an uncaught exception. */
const ignoreStreamError = (): void => undefined

/**
 * How ids map to bucket keys.
 *
 * The mapping is either the default identity or a MATCHED PAIR: `getKey` and `getId` must be supplied
 * together, which the type enforces so a TypeScript caller migrating to this version gets the error
 * at compile time rather than at construction. The runtime check remains for JavaScript callers.
 *
 * The pairing is not bookkeeping. `allFileIds()` enumerates the bucket, which yields KEYS, while
 * every other surface takes IDS: without the inverse, a GC sweep that enumerated and then deleted
 * issued a double-prefixed key, and because `DeleteObjects` is idempotent S3 reported success while
 * deleting nothing, forever.
 *
 * @public
 */
export type S3ContentStorageOptions = {
  Bucket: string
  /**
   * Supplies the module that identifies content types, defaulting to the bundled `file-type`.
   *
   * `file-type` is ESM-only, so the default reaches it through a dynamic import. Injecting a loader
   * lets a consumer use their own detector, skip the ESM dependency entirely, or — in a test — avoid
   * a dynamic import whose resolution is not tied to the caller's lifecycle. Resolved once, during
   * construction.
   */
  fileTypeLoader?: FileTypeLoader
} & (
  | { getKey?: undefined; getId?: undefined }
  | {
      /** Maps a COMPLETE content id to its bucket key. Never called with a partial id. */
      getKey: (hash: string) => string
      /** The exact inverse of `getKey`. Keys that do not round-trip are skipped by `allFileIds()`. */
      getId: (key: string) => string
    }
)

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

  // Keys are ids verbatim here, which is the default mapping — and the one case where `allFileIds`
  // round-trips without an explicit inverse.
  //
  // Destroyed on a failed construction: this factory OWNS the client, and construction can now throw
  // (a missing bucket, an invalid getKey/getId pair) before the `stop()` below exists to release it.
  // A supervisor retrying a misconfigured deployment would otherwise leak a socket pool per attempt.
  let storage: IContentStorageComponent
  try {
    storage = await createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: bucket })
  } catch (error) {
    s3.destroy()
    throw error
  }

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
 * Requests the cancellation signal must NOT be attached to, keyed by command constructor name.
 *
 * - `AbortMultipartUploadCommand` is the cleanup that removes uploaded parts, and lib-storage issues
 *   it precisely when the upload was aborted; attaching the already-aborted signal would cancel the
 *   cleanup and leave the parts to accumulate.
 * `CompleteMultipartUploadCommand` is deliberately NOT exempt. Exempting it would close the
 * part-leak window described in `abortMultipartUpload` below, but at the cost of letting a cancelled
 * store run the complete request to completion — committing the object for the entire duration of
 * that request, which for a multi-GB upload is seconds to minutes, not the last-packet race the
 * cancellation contract documents. The leak is closed at the call site instead.
 */
const UNCANCELLABLE_COMMANDS = new Set(['AbortMultipartUploadCommand'])

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
 * Two commands are deliberately EXEMPT (see `UNCANCELLABLE_COMMANDS`). They are matched by
 * constructor name rather than `instanceof` because client-s3 is a PEER dependency of lib-storage: an
 * install that hoists two copies would fail an identity check and silently break the exemption.
 *
 * `command` is intentionally untyped. The SDK's own parameter type for it is the service-wide
 * `Command<ServiceInputTypes, ServiceInputTypes, ServiceOutputTypes, ServiceOutputTypes, …>`
 * instantiation, which concrete commands are NOT assignable to (their `middlewareStack.add` is
 * invariant in the input/output types), so annotating it would reject every real caller. The options
 * argument — the part this shim actually constructs, and the only place a mistake here could silently
 * disable cancellation — is fully type-checked.
 */
function createAbortableClient(
  s3: S3Client,
  signal: AbortSignal,
  onMultipartCreated: (uploadId: string) => void
): S3Client {
  const send = (command: any, sendOptions?: SendOptions): unknown => {
    if (UNCANCELLABLE_COMMANDS.has(command?.constructor?.name)) {
      return s3.send(command, sendOptions)
    }
    const withAbortSignal: SendOptions = { ...sendOptions, abortSignal: signal }
    const result = s3.send(command, withAbortSignal)
    if (command?.constructor?.name === 'CreateMultipartUploadCommand') {
      // Capture the upload id as it is issued. lib-storage keeps it private and only cleans it up on
      // the paths that run BEFORE `CompleteMultipartUpload`, so this is what lets the caller abort an
      // upload whose complete request was torn down — without reaching into the SDK's internals.
      return Promise.resolve(result).then((output: any) => {
        if (typeof output?.UploadId === 'string') onMultipartCreated(output.UploadId)
        return output
      })
    }
    return result
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
  options: S3ContentStorageOptions
): Promise<IContentStorageComponent> {
  const logger = components.logs.getLogger('s3-based-content-storage')
  const getKey = options.getKey || ((hash: string) => hash)
  const Bucket = options.Bucket
  // Both or neither. The type already enforces this, so only a JavaScript caller reaches it — and
  // BOTH halves matter. Without `getId`, `allFileIds()` yields keys that do not resolve back through
  // retrieve/exist/delete, and a delete of an enumerated id silently removes nothing because
  // `DeleteObjects` is idempotent. Without `getKey`, the mapping is mixed: reads and writes address
  // identity keys while enumeration decodes them, so every key fails the round-trip check below and
  // enumeration reports an empty bucket — the silent-empty answer this component exists to avoid.
  if (!!options.getKey !== !!options.getId) {
    throw new Error(
      `getKey and getId must be supplied together; received only ${options.getKey ? 'getKey' : 'getId'}. ` +
        'They are inverses: retrieve/exist/delete map an id to a key with getKey, while allFileIds() maps a ' +
        'listed key back with getId, and each is unusable without the other.'
    )
  }
  const getId = options.getId || ((key: string) => key)

  // Warm the ESM loader now rather than on the first store, so a resolution problem shows up in the
  // logs before traffic arrives instead of silently degrading every object's content type.
  //
  // AWAITED, not fire-and-forget. `file-type` is ESM-only, so this enters the dynamic loader, and
  // leaving it in flight meant the import could still be resolving after whatever started it had gone
  // away: under Jest that surfaced as `You are trying to import a file after the Jest environment has
  // been torn down` — 77 of them in one run — and, because the memo is cleared when the load rejects,
  // it could also poison detection for a store that raced it, silently storing content as
  // `application/octet-stream`. Construction is the right place to pay for it: the module is memoized
  // per process, so only the first component built here waits, and a failure still only warns —
  // detection is metadata, and a store must not fail because the detector is unavailable.
  const fileTypeLoader = options.fileTypeLoader ?? loadFileType
  await fileTypeLoader().catch((error: any) =>
    logger.warn(
      `Could not preload the MIME detection module; stores will retry and fall back to ${DEFAULT_MIME_TYPE}`,
      { error: error?.message ?? String(error) }
    )
  )

  /**
   * Whether a 403 on a read may be reported as absence.
   *
   * Only ONE situation justifies that: a principal without `s3:ListBucket` gets 403 instead of 404
   * for a MISSING key, and the response carries nothing separating that from a real denial. Every
   * other 403 — rotated credentials, a bad signature, clock skew, a revoked policy — is the storage
   * refusing to answer, and reporting it as absence is what makes a broken node look like an empty
   * one.
   *
   * Defaults to FALSE and is only enabled on positive evidence (see `probeBucketAccess`), because the
   * two readings are not symmetric: answering "cannot read" for content that is genuinely absent
   * costs a retry, while answering "absent" for content that is present and unreadable is a silent
   * data-loss report. Resolved ONCE at construction, so it costs nothing per read.
   */
  let report403AsAbsent = false
  await probeBucketAccess()

  async function probeBucketAccess(): Promise<void> {
    try {
      await s3.send(new ListObjectsV2Command({ Bucket, MaxKeys: 1 }))
      // Listing works, so missing keys answer 404 and every 403 is a real authorization failure.
      return
    } catch (error: any) {
      const statusCode = error?.$metadata?.httpStatusCode
      const code = error?.name
      // A missing or misnamed bucket answers `HeadObject` with a 404 byte-identical to a missing key,
      // so without this probe EVERY id reported absent and nothing was logged — a silently empty
      // node. It is a deployment error, not a runtime condition, so it fails construction where an
      // operator will see it.
      if (code === 'NoSuchBucket' || statusCode === 404) {
        throw new Error(
          `Refusing to start: the S3 bucket '${Bucket}' does not exist or is not reachable by this principal. ` +
            `Every read would report content as absent, which is indistinguishable from an empty node. ` +
            `Original error: ${error?.message ?? String(error)}`
        )
      }
      // A 403 is only evidence about `s3:ListBucket` when it is an authorization DENIAL. The
      // credential and clock failures below are 403s too, and they say nothing about which
      // permissions this principal holds — crediting them would switch reads into the lenient mode on
      // a startup blip, and the node would then answer "absent" for every id for its whole lifetime.
      // That is precisely the empty-node failure this probe exists to remove, so it must not be
      // reachable through the probe itself.
      //
      // `ListObjectsV2` is a GET with an XML error body, so the SDK surfaces the real code here —
      // unlike `HeadObject`, whose empty body is why the ambiguity exists in the first place.
      if (statusCode === 403 && code === 'AccessDenied') {
        report403AsAbsent = true
        logger.warn(
          `This principal cannot s3:ListBucket on '${Bucket}', so a 403 on a read cannot be distinguished from a ` +
            `missing key and will be reported as absent. Grant s3:ListBucket so missing keys return 404 and a real ` +
            `authorization failure (rotated credentials, clock skew, a revoked policy) surfaces as an error ` +
            `instead of an empty node.`,
          { error: error?.message ?? String(error) }
        )
        return
      }
      // Anything else — a credential or clock 403, a network blip, a throttle — says nothing about
      // permissions and must not stop a component whose reads and writes would surface the same fault
      // themselves. Reads stay STRICT: an unverified 403 rejects, which is the answer that keeps a
      // broken node distinguishable from an empty one.
      logger.warn(
        `Could not verify s3:ListBucket on '${Bucket}'; a 403 on a read will be surfaced as an error rather than ` +
          `reported as absence. If this principal genuinely lacks s3:ListBucket, grant it so missing keys return ` +
          `404, or restart once the underlying problem is resolved.`,
        { error: error?.message ?? String(error) }
      )
    }
  }

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
    if (code === 'NotFound' || code === 'NoSuchKey' || statusCode === 404) return true
    // 403 is "absent" ONLY while this principal's read of a missing key is genuinely indistinguishable
    // from a denial — see `canListBucket`. Once the startup probe has proven `s3:ListBucket`, S3
    // answers a missing key with 404, so every remaining 403 is a real authorization failure
    // (`InvalidAccessKeyId`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed`, a revoked policy) and
    // must REJECT. Reporting those as absent made a node whose key had been rotated, or whose clock
    // had drifted, answer "I hold nothing" for every id — while its writes rejected loudly.
    return statusCode === 403 && report403AsAbsent
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
    // Set once lib-storage escalates to a multipart upload, so a failed or cancelled store can clean
    // its parts up itself.
    let multipartUploadId: string | undefined
    // Requests issued by the managed upload carry this signal, so cancelling tears the in-flight
    // request down instead of merely losing the race inside `done()`. See createAbortableClient.
    const abortableClient = createAbortableClient(s3, uploadAbort.signal, (uploadId) => {
      multipartUploadId = uploadId
    })
    await runStoreWithSignal(
      stream,
      signal,
      async () => {
        // Everything from the first read of the source onward is inside this try, so ANY failure
        // releases it. `peekHead` has already pulled up to the detection window by then, and its
        // body generator has not started — `Readable.from` does not pull until first read — so its
        // own `iterator.return()` cleanup never runs and the source's descriptor would be held for
        // the life of the process. The reachable throw here is a caller-supplied `getKey`, evaluated
        // inside the params literal below.
        try {
          await uploadTo(id, stream, signal)
        } catch (error) {
          // Release the source stream if the upload stopped consuming the body (e.g. it failed
          // before reading anything, so peekHead's generator never started and can't self-clean).
          // Destroying the source releases its underlying resources (e.g. file descriptors).
          // No-op if already ended/destroyed. Guarded so a custom stream whose destroy() throws
          // cannot replace the upload error the caller needs to see. The listener is attached first:
          // a stream torn down mid-open still emits 'error' afterwards.
          try {
            stream.on('error', ignoreStreamError)
            stream.destroy()
          } catch {
            // best-effort cleanup; the upload error below is what matters
          }
          // Remove any parts this upload left behind. lib-storage issues its own
          // `AbortMultipartUpload` only on the paths that run BEFORE `CompleteMultipartUpload`, so a
          // complete request that was cancelled or failed leaves every uploaded part in the bucket —
          // invisible, and billed until a lifecycle rule reaps it. Idempotent (a completed or
          // already-aborted upload answers `NoSuchUpload`), issued on the REAL client so the signal
          // that caused the teardown cannot cancel the cleanup too, and best-effort: it must never
          // replace the error the caller needs to see.
          if (multipartUploadId) {
            try {
              await s3.send(new AbortMultipartUploadCommand({ Bucket, Key: getKey(id), UploadId: multipartUploadId }))
            } catch (cleanupError: any) {
              logger.warn(`Could not abort the multipart upload of ${id}; its parts may persist in the bucket`, {
                key: getKey(id),
                uploadId: multipartUploadId,
                error: cleanupError?.message ?? String(cleanupError)
              })
            }
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

    async function uploadTo(id: string, stream: Readable, signal: AbortSignal | undefined): Promise<void> {
      // Inspect only the head for MIME detection, then stream the body straight to S3 so large
      // files are never buffered in memory. The AWS SDK's managed upload performs a multipart
      // upload, buffering only part-sized chunks rather than the whole file.
      const { head, body } = await peekHead(stream, MIME_DETECTION_BYTES)
      const mimeType = await detectMimeTypeFromBuffer(head, logger, fileTypeLoader)
      // LOAD-BEARING, and it must stay immediately before the upload is constructed. An abort
      // landing during the two awaits above found `upload` still undefined, so the listener's
      // teardown did nothing — and with a small source already fully buffered into the head the
      // upload no longer needs that source, so it would run to completion and commit content for
      // an already-cancelled store. This is the checkpoint that stops it.
      //
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
      // Re-checked because `getKey` above is CALLER-supplied code evaluated inside the params
      // literal, i.e. after the checkpoint and before `upload` is assigned. A `getKey` that aborts
      // the signal lands while the abort listener still sees `upload === undefined` and so tears
      // nothing down, leaving an already-cancelled store to run to completion and commit. Nothing
      // else in this gap can move the signal — the constructor and `done()`'s prologue are
      // synchronous — but the caller's own callback can, so the upload is torn down here, where it
      // provably exists.
      if (signal?.aborted) {
        abortUpload()
        signal.throwIfAborted()
      }
      await upload.done()
    }
  }

  async function retrieve(id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> {
    if (range) validateRange(range)
    try {
      const obj = await s3.send(new HeadObjectCommand({ Bucket, Key: getKey(id) }))

      const size = obj.ContentLength ?? null
      const encoding = obj.ContentEncoding || null

      // A range over ENCODED content cannot be served from S3. The `Range` header slices the STORED
      // (compressed) bytes, so the item would hand the caller a fragment of a gzip stream — which
      // `asStream()` then fails to inflate (Z_DATA_ERROR), or, for a range starting at 0, inflates
      // into the WHOLE object while advertising the requested length. Both are silently wrong
      // answers to a request whose logical bounds S3 has no way to apply, because it stores no
      // uncompressed-size metadata. This storage never writes `ContentEncoding` itself; an object
      // that has one was put there by a migration or an operator.
      //
      // Rejecting, not `undefined`: the content IS here, this backend cannot serve this VIEW of it —
      // the same distinction the rest of the read contract draws. The folder-based backend answers
      // the identical call by inflating to its decompression cache, which has no S3 equivalent.
      // Tested against the NORMALIZED coding: `identity` (and an empty header) mean the bytes are not
      // encoded at all, so such an object is perfectly rangeable — rejecting it would have made any
      // object an operator tagged `Content-Encoding: identity` permanently un-rangeable.
      if (range && contentCodingOf(encoding) !== null) {
        throw new RangeNotSupportedError(
          `Cannot serve a range of ${getKey(id)}: it is stored with Content-Encoding '${encoding}', and S3 ranges ` +
            `address the compressed bytes. Read it whole with retrieve(id) and slice the decoded stream, or store ` +
            `it unencoded.`
        )
      }

      const clampedEnd = range && size !== null ? clampRange(range, size) : undefined
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
        // Same rule `fileInfo` applies, via the same predicate: S3 keeps no uncompressed-size
        // metadata, so for genuinely encoded content the logical size is unknown, and letting this
        // default to `itemSize` would pass the COMPRESSED byte count off as the content size to
        // callers doing `contentSize ?? size`. Codings that encode nothing — `identity`, and the
        // `aws-chunked` S3 writes itself — must NOT be caught by that: those bytes are the content,
        // and their size is known.
        contentCodingOf(encoding) === null ? itemSize : null
      )
    } catch (error: any) {
      // Caller-facing range problems, not storage faults: re-raised BEFORE `logStorageFailure`, which
      // would otherwise emit one ERROR per request for what is a 416.
      if (error instanceof RangeError || error instanceof RangeNotSupportedError) throw error
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
            Objects: batch.map(($) => ({ Key: getKey($) })),
            // Only `Errors` is read below, and quiet mode still returns those in full. Without it S3
            // echoes a <Deleted> element for every key — 1000 per request — all parsed out of XML and
            // discarded.
            Quiet: true
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
        // Names the CALLER's scope, not this chunk's. The request may span many chunks: earlier ones
        // are already deleted and later ones are never attempted, so a message counting only this
        // batch left callers unable to tell what remains. `delete` is idempotent, so retrying the
        // whole list is safe and is the intended recovery.
        throw new Error(
          `Failed to delete ${errors.length} object(s) from S3 while deleting ${ids.length} requested ` +
            `(failing in the chunk starting at index ${from}; objects before it are already deleted and ` +
            `objects after it were not attempted): ${shown}` +
            (errors.length > 5 ? ', …' : '')
        )
      }
    }
  }

  async function existMultiple(cids: string[]): Promise<Map<string, boolean>> {
    return new Map(
      await mapWithConcurrency(cids, BATCH_CONCURRENCY, async (cid): Promise<[string, boolean]> => [
        cid,
        await exist(cid)
      ])
    )
  }

  /**
   * Walks the bucket yielding stored IDS, not keys.
   *
   * `prefix` filters IDS, matching the folder-based contract. It is pushed to S3 as a server-side
   * `Prefix` only for the DEFAULT identity mapping, where an id prefix provably is a key prefix.
   * `getKey` maps a complete id to a key, so applying it to a partial one is a category error: under
   * a sharding mapping it yields a prefix no real key starts with, and every prefixed enumeration
   * came back empty. A custom mapping therefore lists unprefixed and filters on the id here.
   *
   * Only keys the mapping round-trips are yielded — see the loop below.
   */
  async function* allFileIds(prefix?: string): AsyncIterable<string> {
    const params: ListObjectsV2CommandInput = {
      Bucket,
      ContinuationToken: undefined
    }

    // Server-side filtering ONLY for the identity mapping, where an id prefix provably is a key
    // prefix. `getKey` maps a COMPLETE id to a key, so handing it a partial one is a category error:
    // with a sharding mapping like `h => h.slice(0,4) + '/' + h`, `getKey('ab')` produced a Prefix no
    // real key starts with, S3 returned nothing, and a prefix-sharded GC sweep concluded the bucket
    // was empty. A local re-filter cannot repair that — it can only narrow what the server returned.
    if (prefix && !options.getKey) {
      params.Prefix = prefix
    }

    // Warned about once per enumeration rather than per key: a foreign bucket would otherwise emit a
    // line per object, and one is enough to act on.
    let warnedAboutForeignKeys = false

    let output: ListObjectsV2CommandOutput
    do {
      output = await s3.send(new ListObjectsV2Command(params))
      if (output.Contents) {
        for (const content of output.Contents) {
          const key = content.Key!
          let id: string | undefined
          let notOwnedBecause: string | undefined
          try {
            const candidate = getId(key)
            const roundTripped = getKey(candidate)
            if (roundTripped === key) id = candidate
            else notOwnedBecause = `round-tripped to ${roundTripped}`
          } catch (error: any) {
            // A strict inverse that parses its own namespace THROWS on a key from outside it, which
            // is a clear "not mine" rather than a fault. Letting it propagate would fail the entire
            // enumeration over one object this storage does not own — turning a shared bucket into
            // an unenumerable one. `getKey` is caller code too, so it is inside the same guard.
            notOwnedBecause = `mapping threw: ${error?.message ?? String(error)}`
          }
          // ROUND TRIP, not just an inverse call. `getId` is applied to every key the bucket returns,
          // including ones this mapping never produced — and a lossy inverse maps those onto ids that
          // look real. With `getKey: h => h.slice(0,4) + '/' + h`, a foreign `zz/abcdef` yields the id
          // `abcdef`, whose actual key is `ab/abcdef`: enumeration reported that id TWICE, and a GC
          // sweep that deleted what it enumerated destroyed the real object while leaving the foreign
          // one untouched. Requiring `getKey(id) === key` is the same invariant the folder-based
          // backend enforces on its paths, and it makes storing and enumerating provably inverse.
          //
          // Filtering rather than yielding is the safe direction: a key that does not round-trip is
          // already unreachable through `retrieve`/`delete`, which would compute a different key for
          // that id, so nothing reachable is being hidden.
          if (id === undefined) {
            if (!warnedAboutForeignKeys) {
              warnedAboutForeignKeys = true
              logger.warn(
                `Skipping bucket keys that this storage's getKey/getId mapping does not round-trip; they are not ` +
                  `ids of content it owns. Enumerating them would report phantom ids, and a caller deleting what ` +
                  `it enumerated could remove real content stored under a different key.`,
                { key, reason: notOwnedBecause ?? 'unknown' }
              )
            }
            continue
          }
          // Always re-checked against the ID. For a custom mapping this is the ONLY prefix filter,
          // since the request above was unprefixed; for the identity mapping it is a cheap
          // confirmation.
          if (prefix && !id.startsWith(prefix)) continue
          yield id
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
      const encoding = obj.ContentEncoding || null
      // Normalized exactly as `retrieve` normalizes it, so the two surfaces never disagree about one
      // id: an object tagged `Content-Encoding: identity` is not encoded, so it reports a known
      // `contentSize` and a `null` encoding rather than claiming its size is unknown.
      const coding = contentCodingOf(encoding)
      return {
        encoding: normalizeContentEncoding(encoding),
        size,
        contentSize: coding === null ? size : null
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
      await mapWithConcurrency(cids, BATCH_CONCURRENCY, async (cid): Promise<[string, FileInfo | undefined]> => [
        cid,
        await fileInfo(cid)
      ])
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
