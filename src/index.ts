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

// Stream helpers: a caller has to be able to build a source for `storeStream` and drain what
// `retrieve` returns, and `SimpleContentItem` is how a consumer implementing its own storage produces
// a conforming `ContentItem`.
export { bufferToStream, SimpleContentItem, streamToBuffer } from './content-item'

// Names the injectable detector in `S3ContentStorageOptions`, so that option is usable from a typed
// callback rather than only from an inline literal.
export type { FileTypeLoader } from './mime-detection'
