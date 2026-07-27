/**
 * The public surface of this package.
 *
 * Listed EXPLICITLY rather than re-exported wholesale. Wildcard re-exports made every helper a module
 * happened to export public by accident — the MIME detector's ESM loader memo, the id validators, the
 * bounded-map helper, the stream-teardown utilities, the internal error classes — none of which any
 * consumer imports, and each of which is something this package would then have to keep working
 * forever. What stays is what the four factories genuinely need a caller to be able to name.
 *
 * Everything withdrawn still exists and is still used internally; it is simply no longer contract.
 * Paired with the `exports` map in package.json, which stops `dist/*` paths from being importable, so
 * this list is now the whole of what a consumer can reach.
 */

// The four backends and the filesystem adapter they take.
export { createFolderBasedFileSystemContentStorage } from './folder-based-storage-component'
export type { FolderStorageOptions } from './folder-based-storage-component'
export {
  createAwsS3BasedFileSystemContentStorage,
  createS3BasedFileSystemContentStorage
} from './s3-based-storage-component'
export type { S3ContentStorageOptions } from './s3-based-storage-component'
export { createInMemoryStorage } from './in-memory-storage-component'
export { createFsComponent } from './fs/fs-component'
export type { IFileSystemComponent } from './fs/types'

// The component contract every backend implements, and the shapes it hands back.
export type { AppComponents, ContentItem, FileInfo, IContentStorageComponent } from './types'

/**
 * The errors a public method can actually REACH A CALLER with, exported as runtime values so an
 * `instanceof` check against them is stable.
 *
 * The test is escape, not existence: an error class that is only ever thrown and caught inside this
 * package is not something a consumer can branch on, so exporting it would be inventing contract.
 * Verified per class rather than assumed —
 *
 * - `PathNotContainedError` escapes `exist`, `fileInfo`, `delete`, `storeStream` and
 *   `storeStreamAndCompress` on the folder-based and in-memory backends. (`retrieve` deliberately
 *   converts it to `undefined`: an id that names no storable object has nothing to serve.)
 * - `RangeNotSupportedError` escapes the S3 `retrieve` when a range is asked of an object that has a
 *   `Content-Encoding`; the read contract asks callers to answer 416 for it, which requires this.
 * - `UncommittedIntentSurvivedError` escapes `storeStream`/`storeStreamAndCompress` when a commit
 *   failed AND its journal could not be cleared. Its `stagedPath` is the actionable part, and it is
 *   the one error here that tells an operator a retry is safe.
 *
 * `DecompressionLimitExceededError` is deliberately NOT here: every path that raises it is caught by
 * `retrieve`, which answers `undefined`. It became unreachable when the legacy no-rename range path
 * was removed — that was the one place it surfaced on a consumer's stream.
 */
export { RangeNotSupportedError } from './types'
export { PathNotContainedError, UncommittedIntentSurvivedError } from './folder-based/errors'

// Stream helpers: a caller has to be able to build a source for `storeStream` and drain what
// `retrieve` returns, and `SimpleContentItem` is how a consumer implementing its own storage produces
// a conforming `ContentItem`.
export { bufferToStream, SimpleContentItem, streamToBuffer } from './content-item'

/**
 * Lets a caller ask, BEFORE calling `storeStream`, whether the source it holds is still storable.
 *
 * Exported because the rule it enforces is one a caller can otherwise only discover by failing: a body
 * that has been read from — to hash it, to sniff its type, to measure it — cannot supply the content any
 * more, and every backend refuses it. A caller that needs to inspect a body first can now check the
 * source it is about to hand over rather than finding out from a rejected store.
 */
export { assertStorableStream } from './content-item'

// Names the injectable detector in `S3ContentStorageOptions`, so that option is usable from a typed
// callback rather than only from an inline literal.
export type { FileTypeLoader } from './mime-detection'
