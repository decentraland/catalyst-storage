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
import { assertStorableStream, contentCodingOf, normalizeContentEncoding, SimpleContentItem } from './content-item'
import { assertStorableContentId } from './content-id'
import { isAbortError, runStoreWithSignal } from './cancellation'
import { destroyQuietly } from './stream-teardown'
import { forEachWithConcurrency, mapWithConcurrency } from './concurrency'
import {
  DEFAULT_MIME_TYPE,
  detectMimeTypeFromBuffer,
  FileTypeLoader,
  loadFileType,
  MIME_DETECTION_BYTES,
  peekHead
} from './mime-detection'

/**
 * An id IS its bucket key.
 *
 * There is deliberately no key-mapping hook. `allFileIds()` enumerates the bucket, which yields keys,
 * while every other surface takes ids — so a custom mapping is only sound as a matched, total,
 * round-tripping pair, and getting it wrong produced silent data loss: a GC sweep that enumerated and
 * then deleted issued a double-prefixed key, and because `DeleteObjects` is idempotent S3 reported
 * success while deleting nothing, forever. A caller who wants a key prefix should point the storage at
 * a bucket (or a dedicated bucket) rather than reshaping ids underneath it.
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
   * a dynamic import whose resolution is not tied to the caller's lifecycle.
   *
   * Called ONCE, during construction, and the module it returns is reused by every store. The single
   * exception is a loader that REJECTS: construction then only warns (detection is metadata, and a
   * store must not fail because the detector is unavailable) and stores retry it, so a transient
   * failure does not permanently downgrade every later store.
   */
  fileTypeLoader?: FileTypeLoader
  /**
   * Answers a 403 on a READ as "the content is absent" instead of rejecting. Defaults to `false`.
   *
   * Set this ONLY for a principal that genuinely lacks `s3:ListBucket`. Without that permission S3
   * answers a MISSING key with 403 instead of 404, and nothing in the response separates that from a
   * real denial — so for such a principal every read of absent content would otherwise reject.
   *
   * It is a deliberate opt-in rather than something this component infers, because the signal it would
   * have to infer from is not sound. A 403 `AccessDenied` on the startup `ListObjectsV2` probe is
   * ALSO what an IAM policy scoped to `arn:aws:s3:::my-bucket` returns for a bucket that is not that
   * one — including a MISSPELLED one, since the implicit deny is evaluated before S3 ever checks
   * whether the bucket exists, so the `NoSuchBucket`/404 guard never fires. Crediting it turned a
   * one-character typo in `Bucket` into a node that answered "I hold nothing" for every id, for its
   * whole lifetime, while its writes rejected loudly — precisely the silently-empty node the startup
   * probe exists to prevent, reached through the probe itself.
   *
   * Granting `s3:ListBucket` is the better fix: missing keys then return 404 and a genuine
   * authorization failure (rotated credentials, clock skew, a revoked policy) surfaces as an error.
   */
  report403AsAbsent?: boolean
  /**
   * Part size in bytes for the managed multipart upload. Defaults to lib-storage's own 5 MiB minimum.
   *
   * This is what sets the largest object a single store can write. Bodies reach lib-storage wrapped by the
   * MIME head-peek, and that wrapper exposes no length — it is not an `fs.ReadStream` and has no
   * `length`/`size`/`byteLength` — so lib-storage cannot right-size parts the way it does for a Buffer or
   * a file stream. It falls back to the 5 MiB minimum, and with S3's 10,000-part limit that caps a store at
   * roughly 50 GiB, which is only reported as `Exceeded 10000 parts` AFTER 50 GiB has crossed the wire.
   *
   * Raise it for a deployment that stores objects near or past that ceiling (64 MiB parts lift it to about
   * 640 GiB) at the cost of proportionally more memory per concurrent upload, since lib-storage buffers a
   * part at a time. Scene assets are nowhere near it, hence the unchanged default.
   */
  partSize?: number
}

/** S3's hard per-request limit for `DeleteObjects`; the SDK does not split a larger list. */
const DELETE_OBJECTS_MAX_KEYS = 1000

/**
 * How many `DeleteObjects` requests a single `delete()` may have in flight. Each carries up to 1000
 * keys, so this is already 8000 keys of work at once — well clear of the SDK's default socket pool while
 * turning a 1000-round-trip sweep into 125.
 */
const DELETE_CHUNK_CONCURRENCY = 8

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
  bucket: string,
  /** Forwarded to {@link createS3BasedFileSystemContentStorage}; see the options documented there. */
  options?: Pick<S3ContentStorageOptions, 'fileTypeLoader' | 'report403AsAbsent'>
): Promise<IContentStorageComponent> {
  const { config, logs } = components

  const s3 = new S3Client({
    region: await config.requireString('AWS_REGION')
  })

  // Destroyed on a failed construction: this factory OWNS the client, and construction can throw (a
  // missing or unreachable bucket) before the `stop()` below exists to release it.
  // A supervisor retrying a misconfigured deployment would otherwise leak a socket pool per attempt.
  let storage: IContentStorageComponent
  try {
    storage = await createS3BasedFileSystemContentStorage({ logs }, s3, {
      Bucket: bucket,
      fileTypeLoader: options?.fileTypeLoader,
      report403AsAbsent: options?.report403AsAbsent
    })
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
 * Requests the cancellation signal must NOT be attached to, identified by command constructor name.
 *
 * `AbortMultipartUploadCommand` is the cleanup that removes uploaded parts, and lib-storage issues it
 * precisely when the upload was aborted; attaching the already-aborted signal would cancel the cleanup
 * and leave the parts to accumulate.
 *
 * `CompleteMultipartUploadCommand` is deliberately NOT exempt. Exempting it would close the
 * part-leak window the store's own failure path closes, but at the cost of letting a cancelled
 * store run the complete request to completion — committing the object for the entire duration of
 * that request, which for a multi-GB upload is seconds to minutes, not the last-packet race the
 * cancellation contract documents. The leak is closed at the call site instead.
 */
const UNCANCELLABLE_COMMANDS = new Set(['AbortMultipartUploadCommand'])

/**
 * Whether this command is the multipart create whose `UploadId` the store needs to capture.
 *
 * `CreateMultipartUploadCommand` is not imported (nothing here constructs one — lib-storage does), so this
 * cannot use `instanceof` the way the abort exemption now does. Matching the input shape as well as the
 * name gives it a second, mangling-proof signal: the create is the only command lib-storage issues that
 * carries a `Bucket` and `Key` with no `UploadId` and no `Body`.
 */
function isCreateMultipartUpload(command: any): boolean {
  if (command?.constructor?.name === 'CreateMultipartUploadCommand') return true
  const input = command?.input
  return (
    !!input &&
    typeof input.Bucket === 'string' &&
    typeof input.Key === 'string' &&
    input.UploadId === undefined &&
    input.Body === undefined &&
    input.Delete === undefined &&
    input.Prefix === undefined
  )
}

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
 * One command is deliberately EXEMPT (see `UNCANCELLABLE_COMMANDS`). It is matched by
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
  onMultipartCreated: (uploadId: string) => void,
  /**
   * Called when the multipart create is ISSUED, before its response is known.
   *
   * The upload id can only be captured from a create that RESOLVES, so an abort landing while the create
   * is in flight leaves a store that cannot clean up after itself: the `.then` that captures the id and
   * the one in which lib-storage builds its own abort command are both on the rejected path, so zero
   * `AbortMultipartUpload` requests are issued for an upload S3 may well have created. Knowing the create
   * was attempted is what lets that be reported rather than passing silently.
   */
  onMultipartCreateIssued: () => void
): S3Client {
  const send = (command: any, sendOptions?: SendOptions): unknown => {
    // `instanceof` FIRST, name second. The name check exists because client-s3 is a peer dependency of
    // lib-storage and a hoisted second copy would fail an identity test — sound reasoning, but it settled
    // on `constructor.name`, which `esbuild --minify` and webpack's `mangleExports` rewrite. Lambda-bundled
    // consumers do exactly that, and the failure is SILENT: the cleanup inherits the already-aborted
    // signal, `AbortMultipartUpload` never leaves the process, and every cancelled multipart store leaks
    // parts that are billed until a lifecycle rule reaps them. The identity check costs nothing when it
    // holds, and the name check still covers the duplicated-copy case when it does not.
    if (command instanceof AbortMultipartUploadCommand || UNCANCELLABLE_COMMANDS.has(command?.constructor?.name)) {
      return s3.send(command, sendOptions)
    }
    const withAbortSignal: SendOptions = { ...sendOptions, abortSignal: signal }
    const isCreate = isCreateMultipartUpload(command)
    if (isCreate) onMultipartCreateIssued()
    const result = s3.send(command, withAbortSignal)
    if (isCreate) {
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
  const Bucket = options.Bucket

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
  const configuredFileTypeLoader = options.fileTypeLoader ?? loadFileType
  let resolvedDetector: Awaited<ReturnType<FileTypeLoader>> | undefined
  try {
    resolvedDetector = await configuredFileTypeLoader()
  } catch (error: any) {
    logger.warn(
      `Could not preload the MIME detection module; stores will retry and fall back to ${DEFAULT_MIME_TYPE}`,
      { error: error?.message ?? String(error) }
    )
  }
  // The RESOLVED detector is what stores use, not the loader that produced it. Handing the loader
  // itself to every store meant "resolved once, during construction" only held for the bundled
  // loader, which memoizes internally; an injected one was re-invoked per store and could fail after
  // construction had already succeeded — the caller having no way to know their loader was on the
  // hot path at all.
  //
  // The FAILED case deliberately keeps calling the original loader. A transient resolution failure
  // must not permanently downgrade every later store to `application/octet-stream`, which is the same
  // reason `loadFileType` refuses to cache a rejection.
  const detector = resolvedDetector
  const fileTypeLoader: FileTypeLoader = detector ? async () => detector : configuredFileTypeLoader

  /**
   * Whether a 403 on a read may be reported as absence.
   *
   * Only ONE situation justifies that: a principal without `s3:ListBucket` gets 403 instead of 404
   * for a MISSING key, and the response carries nothing separating that from a real denial. Every
   * other 403 — rotated credentials, a bad signature, clock skew, a revoked policy — is the storage
   * refusing to answer, and reporting it as absence is what makes a broken node look like an empty
   * one.
   *
   * CONFIGURED, never inferred (see `report403AsAbsent` in the options). The startup probe cannot tell
   * "this principal cannot list an existing bucket" from "this bucket is not the one the policy grants"
   * — a scoped policy denies both identically — so inferring it made a misspelled bucket name enable
   * the lenient mode and report every id absent forever. The two readings are not symmetric: answering
   * "cannot read" for content that is genuinely absent costs a retry, while answering "absent" for
   * content that is present and unreadable is a silent data-loss report, so the default is the strict
   * one and widening it is the operator's explicit choice. Read once per request from a constant, so it
   * costs nothing.
   */
  const report403AsAbsent = options.report403AsAbsent === true
  await probeBucketAccess()

  async function probeBucketAccess(): Promise<void> {
    try {
      await s3.send(new ListObjectsV2Command({ Bucket, MaxKeys: 1 }))
      // Listing works, so missing keys answer 404 and every 403 is a real authorization failure.
      if (report403AsAbsent) {
        logger.warn(
          `report403AsAbsent is enabled, but this principal CAN s3:ListBucket on '${Bucket}', so S3 answers a ` +
            `missing key with 404 and every 403 is a genuine authorization failure. Leaving the option on turns ` +
            `rotated credentials, clock skew or a revoked policy into "this node holds nothing" for every id. ` +
            `Remove it.`
        )
      }
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
      // A 403 here is NOT evidence that this principal lacks `s3:ListBucket` on an existing bucket, and
      // it used to be credited as exactly that. An IAM policy scoped to `arn:aws:s3:::my-bucket`
      // returns 403 `AccessDenied` for any OTHER bucket — including a misspelled one — because the
      // implicit deny is evaluated before S3 checks whether the bucket exists, so the `NoSuchBucket`
      // branch above never fires. Crediting it turned a one-character typo into a node that answered
      // "absent" for every id for its whole lifetime while its writes rejected loudly: the silently
      // empty node this probe exists to prevent, produced BY the probe. The credential and clock 403s
      // (`InvalidAccessKeyId`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed`) say nothing about
      // permissions either.
      //
      // So the probe no longer decides. It reports what it saw and reads stay STRICT unless the
      // operator has said otherwise, which keeps a broken node distinguishable from an empty one.
      if (statusCode === 403) {
        logger.warn(
          report403AsAbsent
            ? `Could not verify s3:ListBucket on '${Bucket}', and report403AsAbsent is enabled: a 403 on a read ` +
                `will be reported as absent content. If this 403 is a misspelled bucket, expired credentials or ` +
                `clock skew rather than a missing s3:ListBucket grant, this node will report that it holds ` +
                `NOTHING for every id. Verify the bucket name and the credentials.`
            : `S3 returned 403 for the startup s3:ListBucket probe on '${Bucket}'. Reads stay strict, so a 403 on ` +
                `a read will surface as an error rather than as absent content. If the bucket name is correct and ` +
                `this principal genuinely cannot list it, grant s3:ListBucket so missing keys return 404 — or set ` +
                `report403AsAbsent if you accept that a 403 can no longer be told apart from a missing object.`,
          { error: error?.message ?? String(error), code: code ?? 'unknown' }
        )
        return
      }
      // A network blip or a throttle says nothing about permissions and must not stop a component
      // whose reads and writes would surface the same fault themselves.
      logger.warn(
        `Could not verify s3:ListBucket on '${Bucket}'; the probe failed for a reason unrelated to permissions. ` +
          `Reads follow the configured report403AsAbsent setting (${report403AsAbsent}).`,
        { error: error?.message ?? String(error), code: code ?? 'unknown' }
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
    // 403 is "absent" only when the operator has said this principal cannot `s3:ListBucket` — see
    // `report403AsAbsent`. Otherwise S3 answers a missing key with 404, so a 403 is a real
    // authorization failure (`InvalidAccessKeyId`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed`, a
    // revoked policy) and must REJECT. Reporting those as absent made a node whose key had been
    // rotated, or whose clock had drifted, answer "I hold nothing" for every id — while its writes
    // rejected loudly.
    return statusCode === 403 && report403AsAbsent
  }

  function logContextFor(id: string, error: any): Record<string, string | number> {
    const context: Record<string, string | number> = { key: id }
    if (error?.name) context.code = error.name
    if (error?.$metadata?.httpStatusCode) context.statusCode = error.$metadata.httpStatusCode
    return context
  }

  /** A 403 read as not-found is worth an operator's attention: it may be a real permission problem. */
  function warnIfForbidden(operation: string, id: string, error: any): void {
    if (error?.$metadata?.httpStatusCode !== 403) return
    logger.warn(
      `S3 returned 403 Forbidden while ${operation}; reporting the content as not found because report403AsAbsent ` +
        `is enabled. If the object is simply missing, grant the principal s3:ListBucket so missing keys return 404 ` +
        `and this option can be removed; otherwise check the bucket name, the credentials and the object/bucket ` +
        `permissions — this node is reporting content as absent that it may simply be unable to read.`,
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
      await s3.send(new HeadObjectCommand({ Bucket, Key: id }))
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
    // S3 failure, which the SDK discards above this layer and we therefore cannot recover. It is also
    // a shape the shared translator deliberately refuses to credit (a transport can raise one for its
    // own reasons). Tracking our own teardown here is the provenance that lets this call site
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
    // Set when the multipart create was issued, whether or not its response ever arrived.
    let multipartCreateIssued = false
    // Requests issued by the managed upload carry this signal, so cancelling tears the in-flight
    // request down instead of merely losing the race inside `done()`. See createAbortableClient.
    const abortableClient = createAbortableClient(
      s3,
      uploadAbort.signal,
      (uploadId) => {
        multipartUploadId = uploadId
      },
      () => {
        multipartCreateIssued = true
      }
    )
    await runStoreWithSignal(
      stream,
      signal,
      async () => {
        // Everything from the first read of the source onward is inside this try, so ANY failure
        // releases it. `peekHead` has already pulled up to the detection window by then, and its
        // body generator has not started — `Readable.from` does not pull until first read — so its
        // own `iterator.return()` cleanup never runs and the source's descriptor would be held for
        // the life of the process.
        try {
          await uploadTo(id, stream, signal)
        } catch (error) {
          // Release the source stream if the upload stopped consuming the body (e.g. it failed
          // before reading anything, so peekHead's generator never started and can't self-clean).
          // Destroying the source releases its underlying resources (e.g. file descriptors).
          // No-op if already ended/destroyed, and guarded so a custom stream whose destroy() throws
          // cannot replace the upload error the caller needs to see.
          destroyQuietly(stream)
          // Remove any parts this upload left behind. lib-storage issues its own
          // `AbortMultipartUpload` only on the paths that run BEFORE `CompleteMultipartUpload`, so a
          // complete request that was cancelled or failed leaves every uploaded part in the bucket —
          // invisible, and billed until a lifecycle rule reaps it. Idempotent (a completed or
          // already-aborted upload answers `NoSuchUpload`), issued on the REAL client so the signal
          // that caused the teardown cannot cancel the cleanup too, and best-effort: it must never
          // replace the error the caller needs to see.
          if (multipartUploadId) {
            try {
              await s3.send(new AbortMultipartUploadCommand({ Bucket, Key: id, UploadId: multipartUploadId }))
            } catch (cleanupError: any) {
              // `NoSuchUpload` (or a 404) means there is nothing left to abort — lib-storage already
              // aborted it on every path that runs BEFORE `CompleteMultipartUpload`, which is the
              // ordinary `UploadPart` failure (a 503 SlowDown). Reporting that as "its parts may
              // persist in the bucket" told operators to hunt a part leak that does not exist, on the
              // most common failure of all. Only a cleanup that genuinely could not run is worth a
              // warning; the window this call exists for is the cancelled-or-failed COMPLETE request,
              // which lib-storage does not clean up.
              const cleanupCode = cleanupError?.name
              const cleanupStatus = cleanupError?.$metadata?.httpStatusCode
              if (cleanupCode === 'NoSuchUpload' || cleanupStatus === 404) {
                logger.debug(`The multipart upload of ${id} was already aborted; no parts remain`, {
                  key: id,
                  uploadId: multipartUploadId
                })
              } else {
                logger.warn(`Could not abort the multipart upload of ${id}; its parts may persist in the bucket`, {
                  key: id,
                  uploadId: multipartUploadId,
                  error: cleanupError?.message ?? String(cleanupError)
                })
              }
            }
          } else if (multipartCreateIssued) {
            // The create was issued but never answered — the abort tore it down in flight. S3 may still
            // have processed the request, in which case a multipart upload exists that NOTHING can abort:
            // its id only ever existed in the response that was discarded.
            //
            // Deliberately not "cleaned up" automatically. The only lookup available,
            // `ListMultipartUploads` for this key, cannot distinguish this orphan from a CONCURRENT
            // legitimate upload of the same id, and aborting that would fail a healthy store. Since no
            // part was uploaded, nothing is billed for storage — the cost is an entry in
            // `ListMultipartUploads` — so reporting it is the proportionate response.
            logger.warn(
              `The multipart upload of ${id} was cancelled while it was being created, so its upload id was ` +
                `never received and it cannot be aborted from here. No parts were uploaded, so nothing is billed ` +
                `for storage, but a stale multipart upload may remain listed for this key. A bucket lifecycle ` +
                `rule for incomplete multipart uploads reclaims it.`,
              { key: id }
            )
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
      // A source that cannot supply content is refused, not uploaded: `peekHead` sees an immediate
      // `{done: true}` from an already-consumed stream and reports "no content", so this stored a
      // 0-byte object under the id and resolved. Same rule the folder-based and in-memory backends
      // apply — see `assertStorableStream`.
      //
      // The id is checked FIRST, as on the other two backends: one bad call must not produce a different
      // typed error depending on which backend is behind it. An S3 key has no per-segment limit of its
      // own, so this rule is enforced purely to keep the id namespace identical across backends — an id
      // this accepted and the folder-based backend cannot store is the divergence these shared checks
      // exist to prevent.
      assertStorableContentId(id)
      assertStorableStream(stream)
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
        // Omitted unless configured, so lib-storage keeps its own default rather than this passing an
        // explicit `undefined` that could shadow it. See `partSize` for why the default caps a store at
        // roughly 50 GiB.
        ...(options.partSize !== undefined ? { partSize: options.partSize } : {}),
        params: {
          Bucket,
          Key: id,
          Body: body,
          ContentType: mimeType
        }
      })
      // Re-checked because the abort listener tears down `upload`, and `upload` was undefined for the
      // whole of the two awaits above — so a signal that fired during them found nothing to tear down.
      // The constructor and `done()`'s prologue are synchronous, so this is the first point at which
      // the upload provably exists and can be cancelled.
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
      const obj = await s3.send(new HeadObjectCommand({ Bucket, Key: id }))

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
          `Cannot serve a range of ${id}: it is stored with Content-Encoding '${encoding}', and S3 ranges ` +
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
              Key: id,
              Range: range ? `bytes=${range.start}-${clampedEnd ?? range.end}` : undefined,
              // Pins the bytes to the VERSION the metadata above was read from. `size`, `encoding` and
              // `contentSize` come from that HeadObject, while this GetObject runs whenever the consumer
              // opens the stream — so a re-store in between served one version's bytes under another
              // version's advertised length, with no error: a 100-byte object re-stored at 95 bytes
              // answered a `{start:90,end:99}` range with 5 bytes under `size: 10`, which a caller
              // forwarding `size` as Content-Length turns into a truncated response with a mismatched
              // header. A shrink past `start` instead surfaced as a raw SDK `InvalidRange`.
              //
              // With the precondition the same race fails loudly as a 412 instead, which the read
              // contract's "treat a failing stream as a retryable miss" already covers — and a retry
              // re-heads the object and gets a consistent pair. The folder-based backend documents this
              // same window as unavoidable for itself; S3 can actually close it, so it does.
              //
              // Omitted when the head returned no ETag (an S3-compatible endpoint that does not send one),
              // which degrades to the previous behaviour rather than making every read fail.
              IfMatch: obj.ETag
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
              `S3 returned no readable body for ${id}; received ${describeBody(body)}. This storage ` +
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
    // the list, so a larger batch was rejected outright as MalformedXML. An EMPTY list produces no
    // chunks at all — matching the other backends, where deleting nothing is a no-op rather than the
    // MalformedXML an empty request would return.
    const chunks: string[][] = []
    for (let from = 0; from < ids.length; from += DELETE_OBJECTS_MAX_KEYS) {
      chunks.push(ids.slice(from, from + DELETE_OBJECTS_MAX_KEYS))
    }
    // Issued concurrently rather than one after another: the chunks share no state, and serializing
    // them made a million-id GC sweep 1000 sequential round trips — minutes of pure latency for work
    // that parallelizes perfectly. The folder-based backend already bounds its equivalent loop the
    // same way. `forEachWithConcurrency` starts no new chunk after the first failure.
    await forEachWithConcurrency(chunks, DELETE_CHUNK_CONCURRENCY, async (batch) => {
      const output = await s3.send(
        new DeleteObjectsCommand({
          Bucket,
          Delete: {
            Objects: batch.map(($) => ({ Key: $ })),
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
        // Names the CALLER's scope, not this chunk's, and deliberately claims NOTHING about which
        // other keys were removed. The chunks are issued concurrently, so an earlier index is not
        // necessarily done and a later one is not necessarily untouched — the previous wording said
        // exactly that, and keeping it after parallelising the loop would have made the library lie
        // about its own state. `delete` is idempotent, so retrying the whole list is the recovery.
        throw new Error(
          `Failed to delete ${errors.length} object(s) from S3 while deleting ${ids.length} requested. ` +
            `Chunks are issued concurrently, so the remaining keys may be partially deleted; delete is ` +
            `idempotent, so retry the whole list. Failures: ${shown}` +
            (errors.length > 5 ? ', …' : '')
        )
      }
    })
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
   * Walks the bucket yielding stored ids.
   *
   * An id IS its key, so enumeration round-trips by construction and `prefix` is pushed down to S3 as a
   * server-side `Prefix` — the whole listing never crosses the wire just to be filtered locally.
   */
  async function* allFileIds(prefix?: string): AsyncIterable<string> {
    const params: ListObjectsV2CommandInput = {
      Bucket,
      ContinuationToken: undefined,
      Prefix: prefix
    }

    let output: ListObjectsV2CommandOutput
    // Every token this enumeration has already followed. Relaxing the stop condition to `IsTruncated
    // !== false` (see below) traded a guaranteed termination for one that depends on the server issuing a
    // FRESH token each page — and the endpoints that relaxation exists for are exactly the ones where that
    // is not guaranteed: a gateway echoing the request's `ContinuationToken` back as
    // `NextContinuationToken` made this loop re-request the same page forever, re-yielding the same ids on
    // every pass, so a GC or sync sweep never terminated and kept deleting the same ids.
    const followedTokens = new Set<string>()
    do {
      output = await s3.send(new ListObjectsV2Command(params))
      if (output.Contents) {
        for (const content of output.Contents) {
          // The SDK types `Key` as optional and it flows straight into a caller's sweep, where
          // `delete([...ids])` would build `{ Key: undefined }` and fail or silently no-op the whole batch.
          // AWS always sets it, so this only fires for an S3-compatible endpoint or a truncated parse.
          if (typeof content.Key !== 'string') {
            logger.warn('Skipping a listed object with no key while enumerating', {
              bucket: Bucket,
              prefix: prefix ?? ''
            })
            continue
          }
          yield content.Key
        }
      }
      const nextToken = output.NextContinuationToken
      if (nextToken !== undefined && followedTokens.has(nextToken)) {
        logger.warn(
          'Stopping enumeration: the endpoint returned a continuation token it had already issued, which ' +
            'would repeat the same page indefinitely. The listing may be incomplete.',
          { bucket: Bucket, prefix: prefix ?? '', pagesListed: followedTokens.size }
        )
        return
      }
      if (nextToken !== undefined) followedTokens.add(nextToken)
      params.ContinuationToken = nextToken
      // Continue on any continuation token unless the server explicitly said this page was the last.
      //
      // Both halves matter, in opposite directions. Requiring a TOKEN stops the infinite loop a
      // truncated page without one would cause, re-requesting the first page forever and yielding the
      // same keys on every pass. But requiring `IsTruncated` to be TRUE silently truncated the
      // enumeration on an S3-compatible endpoint (MinIO, Ceph, a gateway) that returns
      // `NextContinuationToken` and omits `IsTruncated`: exactly one page was listed and the iterator
      // ended normally, so a GC or sync sweep concluded the bucket held only its first 1000 keys.
      // AWS S3 itself always sets the flag, so `!== false` costs nothing there.
    } while (params.ContinuationToken && output.IsTruncated !== false)
  }

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    try {
      const obj = await s3.send(new HeadObjectCommand({ Bucket, Key: id }))
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
