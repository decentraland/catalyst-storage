import { createHash } from 'crypto'
import path from 'path'
import { pipeline, Readable, Transform, Writable } from 'stream'
import { promisify } from 'util'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { isAbortError, markAsNonCancellationError, runStoreWithSignal } from './cancellation'
import { mapWithConcurrency } from './concurrency'
import { compressContentFile } from './extras/compression'
import { createFsInvariants } from './folder-based/fs-invariants'
import { createDecompressCache, InvalidationToken } from './folder-based/decompress-cache'
import { createIntentJournal, TEMP_DIR_NAME, UncommittedIntentSurvivedError } from './folder-based/intent-journal'
import { DecompressionLimitExceededError, PathNotContainedError } from './folder-based/errors'

const pipe = promisify(pipeline)

const ONE_HOUR_IN_MS = 60 * 60 * 1000
const FIVE_MINUTES_IN_MS = 5 * 60 * 1000
const FIVE_GB_IN_BYTES = 5 * 1024 * 1024 * 1024
const TWO_HUNDRED_FIFTY_SIX_MB_IN_BYTES = 256 * 1024 * 1024

/** @public */
export type FolderStorageOptions = {
  /// by default FALSE, disables the sha1 prefix for all files. @see getFilePath
  disablePrefixHash: boolean
  /** TTL in milliseconds for cached decompressed files. Default: 1 hour. */
  decompressCacheTTL?: number
  /** Max total size in bytes for cached decompressed files. Default: 5GB. */
  decompressCacheMaxSize?: number
  /** How often to run the eviction check in milliseconds. Default: 5 minutes. */
  decompressCacheEvictionInterval?: number
  /**
   * Max size in bytes a single gzip item may inflate to when serving a range request. Inflation is
   * aborted past this limit, preventing a decompression bomb from writing an unbounded amount to
   * disk. Defaults to 256MB — comfortably above any realistic single compressible content file
   * while keeping a malicious gzip's footprint small (and far below the whole-cache budget). Raise
   * it only if legitimate gzipped content can be larger.
   */
  decompressMaxFileSize?: number
  /**
   * Name of the reserved directory (directly under the storage root) where atomic writes stage
   * their temp files. The name is reserved: ids resolving into it are rejected. Configurable so a
   * flat-mode (disablePrefixHash) deployment that already holds content under the default name can
   * pick a different reserved name instead of migrating that content. Must be a single path
   * segment. Default: '.tmp-writes'. Only meaningful when the filesystem component provides
   * `rename` (atomic mode); without it no staging happens and the namespace is neither created nor
   * enforced.
   */
  tempDirectoryName?: string
}

/**
 * A Transform that passes bytes through unchanged but errors once more than `maxBytes` have flowed
 * through it. Used to cap how much a gzip item may inflate to, so a decompression bomb cannot write
 * an unbounded amount of data to disk.
 */
function createSizeLimitTransform(maxBytes: number): Transform {
  let total = 0
  return new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      total += chunk.length
      if (total > maxBytes) {
        callback(
          new DecompressionLimitExceededError(`Decompressed size exceeds the maximum allowed of ${maxBytes} bytes`)
        )
        return
      }
      callback(null, chunk)
    }
  })
}

/**
 * Filesystem-backed content storage.
 *
 * The crash-recovery journal and the decompress cache live in `./folder-based/`; this module is the
 * storage surface over them.
 *
 * Operational contract:
 * - **Exclusive root ownership** — a storage root must be owned by exactly one live storage
 *   instance. In-memory state (path locks, decompress-cache tracking, staged-write ownership,
 *   directory tracking) is per-instance; two instances over one root can delete each other's staged
 *   files and race their caches. Shared roots are not supported.
 * - **Crash-atomic writes require `fs.rename`** — when the filesystem component provides `rename`,
 *   every write stages into a reserved directory and renames into place, so an interrupted write
 *   can never leave a partial file at a canonical path. Without `rename` (legacy custom adapters)
 *   writes fall back to non-atomic direct writes; a warning is logged at construction.
 * - **Atomicity covers process crashes, NOT power-loss durability** — staged data is deliberately
 *   not `fsync`'d before the commit rename. Against process death this is airtight (a canonical path
 *   holds the previous file or the complete new one, never a partial). Against a power loss / kernel
 *   panic it is not: `rename` orders metadata, so the directory entry can survive while the staged
 *   data blocks never reached the disk, leaving the file missing, zero-length or partial. Content is
 *   content-addressed and re-downloadable, so durability past process death is intentionally out of
 *   contract — but consumers must detect and discard unreadable content rather than trust presence.
 * - **Reserved staging namespace** — one directory name directly under the root (default
 *   `.tmp-writes`, see {@link FolderStorageOptions.tempDirectoryName}) is reserved; ids resolving
 *   into it are rejected. With `disablePrefixHash` the factory REFUSES TO START if that directory
 *   pre-exists with content it cannot prove it owns, so an upgrade can never silently hide
 *   pre-existing addressable content.
 *
 * @public
 */
export async function createFolderBasedFileSystemContentStorage(
  components: Pick<AppComponents, 'fs' | 'logs'>,
  root: string,
  options?: Partial<FolderStorageOptions>
): Promise<IContentStorageComponent> {
  const logger = components.logs.getLogger('folder-based-content-storage')

  // remove path separators / \ from the end of the folder
  while (root.endsWith(path.sep)) {
    root = root.slice(0, -1)
  }

  const USE_HASH_PREFIX = !(options?.disablePrefixHash ?? false)

  // Atomic-write support requires `rename` on the filesystem component. Without it (legacy custom
  // adapters) every write falls back to the in-place direct write and NONE of the staging machinery
  // applies — so the reserved temp namespace is neither created nor enforced: a legacy no-rename
  // deployment that stored ids under the default reserved name keeps working unchanged, it just
  // gets none of the crash-atomicity or reconciliation guarantees.
  const ATOMIC_MODE = !!components.fs.rename

  // ALL configuration validation happens before the first filesystem mutation, so an invalid
  // configuration fails without side effects (no root creation, no reserved dir, no marker write).
  const tempDirName = options?.tempDirectoryName ?? TEMP_DIR_NAME
  const tempDir = path.join(root, tempDirName)
  if (ATOMIC_MODE) {
    if (tempDirName === '' || tempDirName === '.' || tempDirName === '..' || /[/\\]/.test(tempDirName)) {
      throw new Error(`tempDirectoryName must be a single path segment, got: ${JSON.stringify(tempDirName)}`)
    }
    if (USE_HASH_PREFIX && /^[0-9a-f]{4}$/i.test(tempDirName)) {
      throw new Error(
        `tempDirectoryName must not look like a shard directory (4 hex characters) when hash prefixes are enabled, got: ${JSON.stringify(tempDirName)}`
      )
    }
  }
  // NaN/Infinity/non-positive values would silently disable the decompression-bomb cap, or create
  // tight eviction loops and pathological cache behavior.
  for (const [optionName, value] of Object.entries({
    decompressCacheTTL: options?.decompressCacheTTL,
    decompressCacheMaxSize: options?.decompressCacheMaxSize,
    decompressCacheEvictionInterval: options?.decompressCacheEvictionInterval,
    decompressMaxFileSize: options?.decompressMaxFileSize
  })) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
      throw new Error(`${optionName} must be a positive safe integer, got: ${String(value)}`)
    }
  }

  if (!ATOMIC_MODE) {
    logger.warn(
      'The filesystem component does not provide rename: writes will NOT be crash-atomic, and the reserved ' +
        'staging directory, orphan sweep and crash reconciliation are disabled (legacy direct-write mode).'
    )
  }

  const { existsForInvariant, noFailUnlink } = createFsInvariants(components.fs)

  const CACHE_EVICTION_INTERVAL = options?.decompressCacheEvictionInterval ?? FIVE_MINUTES_IN_MS
  const MAX_DECOMPRESSED_SIZE = options?.decompressMaxFileSize ?? TWO_HUNDRED_FIFTY_SIX_MB_IN_BYTES

  const cache = createDecompressCache(
    { logger, fsInvariants: { existsForInvariant, noFailUnlink } },
    {
      ttl: options?.decompressCacheTTL ?? ONE_HOUR_IN_MS,
      maxSize: options?.decompressCacheMaxSize ?? FIVE_GB_IN_BYTES
    }
  )
  const { withPathLock } = cache

  // Directories this instance has already created or observed. `getFilePath` runs on EVERY
  // operation — including every read — and its directory check was one syscall per call (~30% of an
  // `exist`, ~35% of a `retrieve`, which calls it twice). Caching is sound under the documented
  // exclusive-root ownership: nothing else removes our directories. If one disappears anyway, the
  // operation that needs it fails loudly and `forgetDirectory` lets the retry recreate it.
  // Bounded by construction with hash prefixes (16^4 = 65,536 shards) and capped for flat mode,
  // where slash-containing ids can nest arbitrarily.
  /** This storage's own suffix for an id's compressed representation. Reserved: ids may not end in it. */
  const GZIP_EXTENSION = '.gzip'

  const MAX_KNOWN_DIRECTORIES = 100_000
  const knownDirectories = new Set<string>()

  function forgetDirectory(dirname: string): void {
    knownDirectories.delete(dirname)
  }

  /**
   * Runs a write that depends on a cached directory, dropping the cache entry when the write fails
   * because that directory is not usable. Without this the entry would stay cached forever and every
   * retry would keep skipping the `mkdir`, so one damaged shard would fail permanently instead of
   * healing once the damage is repaired.
   *
   * Both ENOENT (the directory was removed) and ENOTDIR (something replaced it with a file) count:
   * either way the cached entry no longer describes a directory writes can land in. ENOTDIR matters
   * even though this storage will not clear the obstruction itself — once an operator does, the next
   * write has to be able to recreate the tree rather than fail on a stale entry.
   *
   * Every write path goes through here rather than repeating the check: the atomic and legacy
   * direct-write paths, and both compressed variants, all resolve their target through the same
   * cache, so an invalidation that only covered one of them would leave the others stuck.
   */
  async function writingUnder<T>(filePath: string, write: () => Promise<T>): Promise<T> {
    try {
      return await write()
    } catch (err) {
      const code = (err as { code?: string } | null)?.code
      if (code === 'ENOENT' || code === 'ENOTDIR') {
        forgetDirectory(path.dirname(filePath))
      }
      throw err
    }
  }

  await components.fs.mkdir(root, { recursive: true })

  // Prepares (and refuses to start over an unsafe) staging area, so it must run after the root
  // exists and after all configuration has been validated.
  const journal = await createIntentJournal(
    {
      fs: components.fs,
      logger,
      fsInvariants: { existsForInvariant, noFailUnlink },
      withPathLock,
      // Resolved lazily: reconciliation runs after construction, so getFilePath's state is ready by
      // then (it is declared above this call for that reason).
      resolveFilePath: (id: string) => getFilePath(id)
    },
    { tempDir, tempDirName, atomic: ATOMIC_MODE, useHashPrefix: USE_HASH_PREFIX }
  )

  let evictionTimer: ReturnType<typeof setInterval> | undefined
  // Tracks the in-flight eviction tick so `stop()` can await one that is already running.
  let evictionTick: Promise<void> = Promise.resolve()
  // Tracks the detached startup temp-file sweep so `stop()` can await it (rather than leaving a
  // promise dangling past shutdown). Repeated start() calls CHAIN onto it instead of replacing it.
  let tempFileSweep: Promise<void> = Promise.resolve()

  /**
   * Read-path existence probe: ONE stat instead of `existPath` followed by `stat`, since a stat
   * answers both questions.
   *
   * Only ENOENT/ENOTDIR count as absent. Every other failure — EACCES, EIO, an adapter fault — means
   * the file may well be there and we cannot read it, which is not the same answer: returning
   * `undefined` would report a present-but-unreadable file as missing and put the "broken storage
   * looks like 404" behaviour right back into the read path this contract exists to fix. Same rule as
   * `existsForInvariant`; the difference is only that recovery paths need the boolean.
   */
  async function statForRead(
    filePath: string,
    parentAlreadyProvenIntact = false
  ): Promise<{ size: number } | undefined> {
    try {
      return await components.fs.stat(filePath)
    } catch (err: any) {
      // ENAMETOOLONG joins ENOENT/ENOTDIR as PROVABLY absent: no file of that name can exist, so it
      // is a miss rather than a storage fault. `exist()` answered `false` for it before switching to
      // this probe, and turning that into a throw failed whole `existMultiple` batches.
      if (err?.code !== 'ENOENT' && err?.code !== 'ENOTDIR' && err?.code !== 'ENAMETOOLONG') throw err
      // An id's two representations live in the SAME directory, so a caller probing both only needs
      // that directory proven once: `parentAlreadyProvenIntact` says an earlier probe in this
      // operation already resolved (rather than threw), which is exactly that proof. Without it,
      // `exist()` on a missing id paid four stats where three suffice, on a batch API. The window it
      // gives up — the directory being destroyed between two adjacent probes of the same operation,
      // downgrading a fault to a miss — is narrower than the races the read path already tolerates.
      if (parentAlreadyProvenIntact) return undefined
      // A missing-file error here has two very different meanings, and only one of them is a miss.
      // `getFilePath` created the parent directory (or served it from the cache) immediately before
      // this probe, so an INTACT DIRECTORY must be sitting there. Anything else means the tree this
      // instance owns was damaged underneath it — the shard was removed, taking every id inside it,
      // or something replaced it with a file — which is a storage fault, not this id being absent.
      // Reporting absence would hand back a 404 for a broken store.
      //
      // The parent must be proven to be a DIRECTORY, not merely present: an access check passes for a
      // regular file left at the shard path, while every stat beneath it fails with ENOTDIR, so
      // "present" alone would classify a corrupted tree as a miss. A probe that fails for any reason
      // is likewise not proof, so it does not earn one either.
      //
      // Costs one syscall, and only after a stat has already failed — hits, the hot path this cache
      // exists for, are untouched. Invalidating also lets a write recreate the tree once whatever is
      // occupying the path is gone (a foreign file is never removed here: destroying something this
      // storage cannot prove it owns is exactly what the reserved-namespace checks refuse to do).
      const dirname = path.dirname(filePath)
      let parentIsIntact = false
      try {
        parentIsIntact = (await components.fs.stat(dirname)).isDirectory()
      } catch {
        parentIsIntact = false
      }
      if (!parentIsIntact) {
        forgetDirectory(dirname)
        logger.warn(`Refusing to report ${filePath} as absent: its parent directory is missing or is not a directory`)
        throw err
      }
      return undefined
    }
  }

  async function getFilePath(id: string): Promise<string> {
    // An empty id resolves to the containment directory itself, which is a directory and not anyone's
    // content. It is also the one input the round-trip check below cannot reject on its own, because
    // an empty id and an empty relative path are equal.
    if (id.length === 0) {
      throw new PathNotContainedError('The id is empty, so it does not name a file inside the storage folder')
    }

    // `<id>.gzip` is this storage's own name for the compressed representation of `<id>`, so an id
    // ending in it is not addressable: it occupies another id's second path. The damage is not
    // hypothetical — storing `foo` and `foo.gzip` made `retrieve('foo')` serve `foo.gzip`'s bytes
    // (inflating them, with a contentSize read out of the wrong file's last four bytes),
    // `exist('foo.gzip')` answer false, and `allFileIds()` report a phantom `foo` twice while never
    // listing `foo.gzip` — so a consumer syncing or GC-ing from it would delete real content.
    if (id.endsWith(GZIP_EXTENSION)) {
      throw new PathNotContainedError(
        `The id ends in ${GZIP_EXTENSION}, which names the compressed representation of another id: ${JSON.stringify(id)}`
      )
    }

    // A NUL byte cannot be part of a filename; `fs` rejects it with ERR_INVALID_ARG_VALUE, which is
    // not one of the "provably absent" codes, so it would surface from `exist()` as a storage fault.
    if (id.includes('\0')) {
      throw new PathNotContainedError(`The id contains a NUL byte, which no filename can hold`)
    }

    // We are sharding the files using the first 4 digits of its sha1 hash, because it generates collisions
    // for the file system to handle millions of files in the same directory.
    // This way, asuming that sha1 hash distribution is ~uniform we are reducing by 16^4 the max amount of files in a directory.
    const hash = createHash('sha1').update(id).digest('hex').substring(0, 4)

    const directoryPath = path.normalize(USE_HASH_PREFIX ? path.join(root, hash) : root)

    const finalPath = path.normalize(path.join(directoryPath, id))

    // recursively creates the directory structure if needed
    const dirname = path.dirname(finalPath)

    // ALIASING check: the id must resolve to EXACTLY its own path. `path.join` normalizes what it
    // builds, so several distinct id strings can land on one file — `a/../victim`, `./victim`,
    // `/victim` and `a//../victim` all reach the path of `victim`, and `a//victim` reaches that of
    // `a/victim`. A caller accepting untrusted ids could then overwrite, read or delete another id's
    // content: directly in flat mode, and with hash prefixes after finding a prefix whose first four
    // SHA-1 hex digits match the victim's shard, which is only ~2^16 work.
    //
    // Stated as the invariant rather than as a list of the bad forms, which would only be as good as
    // the enumeration: every aliasing form fails this equality by construction, because normalizing
    // is exactly what makes the resolved path differ from the id that produced it. It is also the
    // precise inverse of how `allFileIds` recovers an id from a path, so storing and enumerating are
    // provably round-trip.
    if (path.relative(directoryPath, finalPath) !== id) {
      throw new PathNotContainedError(
        `The id does not name a path of its own: ${JSON.stringify(id)} resolves onto ` +
          `${JSON.stringify(path.relative(directoryPath, finalPath))}`
      )
    }

    // CONTAINMENT check, orthogonal to the one above: an id like `../evil` resolves to exactly its own
    // path and so round-trips cleanly, it is simply outside the root. We compare against
    // `directoryPath + path.sep` (not a bare `startsWith`) so a sibling directory that merely shares
    // the prefix — e.g. id "../<root>-evil/x" resolving to "<root>-evil" — cannot pass:
    // "/data/contents-evil".startsWith("/data/contents") is true, but it is outside "/data/contents/".
    if (!finalPath.startsWith(directoryPath + path.sep)) {
      throw new PathNotContainedError('Cannot manipulate files outside of the root storage folder')
    }

    // The temp-write namespace is reserved: an id resolving into it (reachable when
    // disablePrefixHash makes the root itself the containment dir, e.g. '.tmp-writes/foo') would be
    // hidden from allFileIds and could be deleted by the startup sweep.
    if (ATOMIC_MODE && (finalPath === tempDir || finalPath.startsWith(tempDir + path.sep))) {
      throw new PathNotContainedError('Cannot manipulate files inside the reserved temp-write folder')
    }

    if (!knownDirectories.has(dirname)) {
      if (!(await components.fs.existPath(dirname))) {
        await components.fs.mkdir(dirname, { recursive: true })
      }
      // Clear wholesale rather than evicting one entry: the cache only holds directory names, so a
      // rebuild costs one syscall per directory touched afterwards.
      if (knownDirectories.size >= MAX_KNOWN_DIRECTORIES) {
        knownDirectories.clear()
      }
      knownDirectories.add(dirname)
    }

    return finalPath
  }

  /**
   * Builds the `ContentItem` for ONE representation of an id, or `undefined` when that
   * representation is not there.
   *
   * `resolveContentSize` controls the gzip trailer read that makes the item report its LOGICAL
   * (uncompressed) size. Callers that only want the gzip stream — the range-cache inflation, which
   * discards the item's metadata immediately — pass `false` to skip that read.
   */
  const retrieveWithEncoding = async (
    id: string,
    encoding: string | null,
    range?: { start: number; end: number },
    resolveContentSize = true
  ): Promise<ContentItem | undefined> => {
    const extension = encoding ? '.' + encoding : ''
    const filePath = (await getFilePath(id)) + extension

    const stat = await statForRead(filePath)
    if (!stat) return undefined

    if (range) {
      const clampedEnd = clampRange(range, stat.size)
      return new SimpleContentItem(
        async () => components.fs.createReadStream(filePath, { start: range.start, end: clampedEnd }),
        clampedEnd - range.start + 1,
        encoding
      )
    }

    // A gzip item's `asStream()` yields DECOMPRESSED bytes, so its `contentSize` — documented as the
    // logical, uncompressed size — must come from the trailer, exactly as `fileInfo` reads it.
    // Leaving it to SimpleContentItem's `contentSize = size` fallback would hand callers the
    // COMPRESSED byte count under that field, and at least one bounds range requests with
    // `contentSize ?? size`. An `undefined` trailer means the gzip vanished mid-read: report this
    // representation as absent so the caller falls through to the raw one, just as `fileInfo` does.
    // For a gzip item the logical size is the trailer's, or `null` when the caller opted out of
    // reading it — never `stat.size`, which is the COMPRESSED count and is exactly the confusion
    // SimpleContentItem's own `encoding ? null : size` default exists to prevent.
    let contentSize: number | null = encoding === 'gzip' ? null : stat.size
    if (encoding === 'gzip' && resolveContentSize) {
      const originalSize = await readGzipOriginalSize(filePath, stat.size)
      if (originalSize === undefined) return undefined
      contentSize = originalSize
    }

    return new SimpleContentItem(async () => components.fs.createReadStream(filePath), stat.size, encoding, contentSize)
  }

  async function gzipSourceVanishedForRead(gzipPath: string): Promise<boolean> {
    // Was a failed inflation the SOURCE disappearing under a concurrent delete — an expected race —
    // or this storage's own machinery breaking? The error cannot answer that: neither its code nor
    // its identity is evidence, because `pipeline` destroys upstream streams WITH the downstream
    // error, so a staging write that fails ENOENT arrives on the gzip stream as the very same object
    // a vanished source would produce. Attributing by listener would credit every broken staging
    // directory to a deleted file and retry it into a reported absence.
    //
    // The on-disk state does answer it, and only costs a probe on the failure path: if the gzip we
    // were inflating is gone, the id really is being deleted; if it is still there, the failure
    // belongs to us.
    //
    // Answered by `statForRead`, the same probe the rest of the read path uses, so absence means the
    // same thing everywhere: the file is gone AND its parent is still proven to be a directory. A
    // weaker test — plain "the path does not exist" — would call a removed or file-obstructed shard
    // a vanished source, swallow the inflation and retry it into a reported absence, which is
    // precisely the misclassification this contract removes. Its rejection (a parent that cannot be
    // proven intact) is not evidence of a vanish either: it invalidates the stale cache entry on the
    // way through, and the caller's original inflate error is what surfaces.
    try {
      return (await statForRead(gzipPath)) === undefined
    } catch {
      return false
    }
  }

  async function inflateGzipItemInto(gzipItem: ContentItem, target: string): Promise<void> {
    // Both streams are created inside the try and torn down if anything fails before `pipe` takes
    // ownership: arguments evaluate left to right, so the source (and the file descriptor behind it)
    // already exists by the time the destination is constructed, and a custom adapter may throw
    // synchronously there where native fs would report asynchronously. Without this the source is
    // left paused mid-read, holding its descriptor for the life of the process. When `pipe` did run
    // it has already destroyed both, so the teardown here is a no-op in the common failure case.
    let source: Readable | undefined
    let destination: Writable | undefined
    try {
      source = await gzipItem.asStream()
      destination = components.fs.createWriteStream(target)
      // Cap how much the gzip may inflate to so a decompression bomb cannot write an unbounded file
      // to disk. The gzip trailer's declared size is attacker-controllable, so the limit is enforced
      // on the actual inflated bytes.
      await pipe(source, createSizeLimitTransform(MAX_DECOMPRESSED_SIZE), destination)
    } catch (err) {
      for (const stream of [source, destination]) {
        try {
          // Listener first: a stream destroyed while its `open(2)` is still in flight still emits
          // 'error' afterwards, and with none attached that becomes an uncaught exception rather
          // than the failure being rethrown below.
          stream?.on('error', () => undefined)
          stream?.destroy()
        } catch {
          // best-effort teardown; the failure below is what matters
        }
      }
      throw err
    }
  }

  async function materializeRangeCacheFromGzip(
    id: string,
    uncompressedPath: string,
    token: InvalidationToken
  ): Promise<void> {
    // `false`: only the stream is used here, so the trailer read that resolves the logical size
    // would be pure overhead on the decompression path.
    const gzipItem = await retrieveWithEncoding(id, 'gzip', undefined, false)
    if (!gzipItem) {
      return
    }

    const gzipPath = uncompressedPath + '.gzip'
    const sourceVanished = () => gzipSourceVanishedForRead(gzipPath)
    const { rename } = components.fs
    if (rename) {
      // Stage the inflation in the temp dir so a process killed mid-decompress can never leave a
      // partial file at the canonical uncompressed path — a later range request would silently serve
      // its truncated bytes as valid content.
      const writePath = journal.newTempPath()
      try {
        await inflateGzipItemInto(gzipItem, writePath)
        // Commit under the path lock so this rename can never interleave with a store or delete on
        // the same canonical path; discard when the source gzip was replaced or the id deleted while
        // inflating.
        const committed = await withPathLock(uncompressedPath, async () => {
          if (token.invalidated) return false
          await rename(writePath, uncompressedPath)
          const stat = await components.fs.stat(uncompressedPath)
          cache.record(uncompressedPath, stat.size)
          return true
        })
        if (!committed) {
          await noFailUnlink(writePath)
        }
      } catch (err) {
        // Remove the partial staged file; the canonical path was never touched.
        await noFailUnlink(writePath)
        // An invalidated token means the id was overwritten/deleted while inflating — the failure
        // belongs to the replaced gzip, not to the caller's request. Resolving lets the retry loop
        // observe the new representation instead of the error bubbling into a spurious undefined for
        // a valid id.
        if (token.invalidated || (await sourceVanished())) return
        throw err
      }
      return
    }

    // In-place (no rename) legacy path: there is no staging, so the ENTIRE inflate/register sequence
    // runs under the path lock and honors the invalidation token — a concurrent store/delete
    // completing first must not be overwritten by a stale decompression, and the cleanup of a failed
    // inflation must not race a newer writer.
    await withPathLock(uncompressedPath, async () => {
      if (token.invalidated) return
      try {
        await inflateGzipItemInto(gzipItem, uncompressedPath)
      } catch (err) {
        // Under the lock the partial file is provably ours to remove.
        await noFailUnlink(uncompressedPath)
        // Defensive symmetry with the staged branch: writers take this same lock, so the token cannot
        // flip mid-section today.
        if (token.invalidated || (await sourceVanished())) return
        throw err
      }
      const stat = await components.fs.stat(uncompressedPath)
      cache.record(uncompressedPath, stat.size)
    })
  }

  // Shared no-rename (legacy) direct write. MUST be called while holding the path lock. Writes the
  // raw in place and enforces the same successful-write invariant as the atomic path: never resolve
  // while the preferred gzip counterpart survives. There is no journal in this mode, so a surviving
  // gzip rolls the in-place store back through the catch — the previous gzip version stays cleanly
  // intact (the raw overwritten by the pipe can only have been that gzip's own re-derivable cache).
  async function writeRawInPlaceLocked(
    id: string,
    filePath: string,
    stream: Readable,
    signal?: AbortSignal
  ): Promise<void> {
    // Cancellation is only honored BEFORE the destructive in-place write begins — outside the
    // rollback path below, since nothing has been touched yet. Once the pipe has replaced the
    // canonical raw, the previous version is already gone (in-place semantics): an abort observed
    // after that point treats the store as completed, because "rolling back" would unlink the only
    // committed object rather than restore anything. A mid-write abort destroys the source, and the
    // resulting pipe failure follows this mode's usual non-atomic handling (the partial overwrite
    // is removed; the previous raw version cannot be preserved without rename support).
    signal?.throwIfAborted()
    try {
      await pipe(stream, components.fs.createWriteStream(filePath))
      await noFailUnlink(filePath + '.gzip')
      if (await existsForInvariant(filePath + '.gzip')) {
        // A post-write invariant failure, never abort-caused: keep it visible past cancellation.
        throw markAsNonCancellationError(
          new Error(
            `Failed to remove the previous gzip representation of ${id}; the in-place store was rolled back ` +
              `and reads keep serving the previous version.`
          )
        )
      }
      cache.forget(filePath)
      cache.invalidateInflight(filePath)
    } catch (err) {
      // Clean up the partial output while still holding the lock: doing it after release could
      // delete a queued writer's freshly committed content for the same id.
      await noFailUnlink(filePath)
      throw err
    }
  }

  const doStoreStream = async (id: string, stream: Readable, signal?: AbortSignal): Promise<void> => {
    const filePath = await getFilePath(id)
    const { rename } = components.fs
    // A custom fs adapter that predates the optional `rename` falls back to the original direct
    // write. It isn't crash-atomic, but keeps the public IFileSystemComponent backward-compatible;
    // the bundled createFsComponent provides rename and so takes the atomic path below.
    if (!rename) {
      await writingUnder(filePath, () =>
        withPathLock(filePath, () => writeRawInPlaceLocked(id, filePath, stream, signal))
      )
      return
    }
    // Stage the write in the reserved temp dir under a random name, then atomically rename it into
    // place. A direct write to the final path leaves a truncated/zero-byte file if the process dies
    // mid-write (OOM-kill, eviction, crash); since `exist()` only checks for the path, that partial
    // file would then be treated as a valid cached copy and never re-fetched. `rename` within a
    // filesystem is atomic, so a reader always sees either the previous file or the fully-written new
    // one. Temp files live outside the content namespace, so they cannot collide with an addressable
    // id. (Data is not fsync'd before the rename, so a power loss can still lose it — content is
    // content-addressed and simply re-downloaded, so durability past process death isn't needed.)
    const tempPath = journal.newTempPath()
    await writingUnder(filePath, async () => {
      try {
        await pipe(stream, components.fs.createWriteStream(tempPath))
        // An abort observed once the source is consumed must still cancel the store before the
        // commit; the catch below removes the staged file and the canonical path stays untouched.
        signal?.throwIfAborted()
        await withPathLock(filePath, async () => {
          // Re-check INSIDE the lock: an abort landing while this store was queued on the path lock
          // (after the checkpoint above, with the source already consumed) must still cancel before
          // the irreversible commit below. Nothing has touched the canonical paths yet, so throwing
          // here is handled exactly like the pre-lock throw.
          signal?.throwIfAborted()
          try {
            // The raw and its .gzip are one versioned object: a gzip left from a previous version
            // would be preferred by retrieve() and serve stale bytes over the content just stored
            // (intent-journaled so even a crash mid-cleanup cannot leave the stale gzip preferred).
            await journal.commitRepresentation('raw', id, tempPath, filePath, filePath + '.gzip', rename, signal)
          } finally {
            // Run the bookkeeping even when the commit throws (a failed counterpart cleanup reports
            // failure AFTER the rename landed): drop any stale decompress-cache tracking so eviction
            // can never delete the new content, and tell an in-flight decompression it is outdated.
            cache.forget(filePath)
            cache.invalidateInflight(filePath)
          }
        })
      } catch (err) {
        // On a write error the temp file may be partial; on a rename error it still exists. Either way
        // remove it so a failed store never leaves a stray file behind (the final path is untouched) —
        // EXCEPT when the temp file is the preserved proof of an uncommitted intent that could not be
        // cleared: destroying it would let the next reconciliation apply the failed commit.
        if (!(err instanceof UncommittedIntentSurvivedError && err.stagedPath === tempPath)) {
          await noFailUnlink(tempPath)
        }
        throw err
      }
    })
  }

  // Concurrent-read contract: reads are deliberately NOT serialized against writes (locking the hot
  // read path would be far too costly). IN ATOMIC MODE every read observes some COMPLETE committed
  // version of the id — commits are atomic renames and a version's raw/gzip transition happens under
  // the path lock — but a read that overlaps a commit may still serve the previous version (e.g. its
  // gzip, which retrieve prefers, in the instant before the committing section unlinks it). Reads
  // started after a store/delete promise resolves observe that operation's outcome.
  //
  // WITHOUT `rename` (legacy no-rename adapters) that completeness guarantee DOES NOT HOLD, and not
  // only after a crash: the in-place write streams straight onto the canonical path, so a concurrent
  // reader can observe a partially written file during normal operation — a truncated raw, or a gzip
  // that fails to inflate. There is no way to make an in-place write atomic for readers; the mode
  // exists for backward compatibility, and a deployment that serves reads concurrently with writes
  // needs `rename`.
  //
  // METADATA AND BYTES CAN COME FROM DIFFERENT VERSIONS. The returned ContentItem opens its stream
  // LAZILY, while `size`/`contentSize` were measured at retrieve() time. A store landing in between
  // can unlink the observed file, making asStream() fail (typically ENOENT) — callers should treat
  // that as a retryable miss, exactly like retrieve() having returned undefined — but it can also
  // REPLACE the file, in which case the stream yields the new version's bytes under the previous
  // version's advertised size (and, for a gzip item, a `contentSize` read from what is no longer the
  // file's trailer, so it may be an arbitrary number). This only arises when an id is overwritten
  // with DIFFERENT content, which the content-addressed model this storage is built for does not do;
  // callers that both allow it and forward `size` as an HTTP Content-Length must re-check after
  // streaming rather than trust the advertised value. Closing the window entirely needs the stream
  // opened eagerly at retrieve() and read through that descriptor, which the filesystem component
  // has no capability for today.
  //
  // Ids quarantined by
  // a failed post-rename cleanup are repaired before serving, and REJECT when they cannot be — a
  // read never exposes a known-mixed state (see the intent journal's quarantine).
  //
  // Error contract: `undefined` means "there is nothing to serve for this id" — it is absent, it
  // does not resolve to a servable path, it exceeded the decompression cap, or it vanished mid-read.
  // A failure of the storage ITSELF (EACCES/EIO/ENOSPC on its own directories, a corrupt gzip, a
  // failed decompression commit, an unrepairable mixed state) REJECTS, so callers can distinguish
  // "not here" from "cannot be read right now" instead of turning an unreadable disk into a 404.
  const retrieve = async (id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> => {
    if (range) validateRange(range)
    try {
      await assertNotQuarantined(id)
      let contentItem: ContentItem | undefined = undefined
      if (!range) contentItem = await retrieveWithEncoding(id, 'gzip')
      if (!contentItem) {
        contentItem = await retrieveWithEncoding(id, null, range)
        if (contentItem && range) {
          // Update last access if this file is in the cache
          cache.touch(await getFilePath(id))
        }
      }

      // If range was requested but uncompressed file doesn't exist, fall back to
      // decompressing the gzip file, writing it to disk as a cache, and serving the range.
      // Two attempts: a decompression can be invalidated by a concurrent overwrite committing while
      // it inflates (its stale output is correctly discarded), leaving this request with neither a
      // cached file nor its result — the second attempt re-reads the id's current representation
      // instead of returning a spurious undefined for a valid id.
      for (let attempt = 0; attempt < 2 && !contentItem && range; attempt++) {
        const uncompressedPath = await getFilePath(id)

        // Deduplicated across concurrent callers of the same path, and handed the invalidation token
        // that says whether the gzip this inflation started from is still the current version.
        await cache.deduplicateInflation(uncompressedPath, (token) =>
          materializeRangeCacheFromGzip(id, uncompressedPath, token)
        )

        // Serve range from the cached uncompressed file (undefined when the gzip didn't exist or
        // the decompression was discarded; the loop then retries once)
        contentItem = await retrieveWithEncoding(id, null, range)
      }

      return contentItem
    } catch (error: any) {
      if (error instanceof RangeError) throw error
      // Expected misses, reported as "absent" exactly like an unknown id: an id that does not
      // resolve to a servable path (the pinned containment contract — note that `exist` and
      // `fileInfo` reject those loudly instead), and content that refuses to inflate within the
      // decompression cap. Nothing is servable and nothing about the request is retryable.
      //
      // A file vanishing under a concurrent delete is deliberately NOT classified here. An ENOENT is
      // only a miss when the content itself is provably gone, which is decided at the inflation by
      // re-probing the source (see `sourceVanished`) and resolves into a retry. Treating every ENOENT
      // as a miss here would also absorb one raised by the staging directory, a rename or a missing
      // shard — storage faults wearing the same shape, which is exactly what this contract removes.
      if (error instanceof PathNotContainedError || error instanceof DecompressionLimitExceededError) {
        logger.warn(`Cannot serve ${id}`, { reason: error?.message ?? String(error) })
        return undefined
      }
      // Everything else is the STORAGE failing, not the id missing: EACCES/EIO/ENOSPC on our own
      // directories, a corrupt gzip, a failed decompression commit. Answering "not found" would
      // tell the caller the content is permanently absent while `exist()` still reports it present,
      // so a broken disk would read as an empty node and stop being retried. Surface it instead.
      logger.error(error)
      throw error
    }
  }

  /**
   * Refuses a read of an id whose on-disk state is known-mixed and could not be repaired.
   *
   * The id is PRESENT — both of its representations are on disk — and this storage simply cannot
   * decide which one is current, so this is a "cannot be read", not a "not here". Reporting absence
   * handed back a 404 for content whose bytes are sitting on the disk and which `allFileIds()` still
   * enumerates, and contradicted the store that had already failed loudly announcing the quarantine.
   * Repairing first (under the path lock) means a recoverable id is served normally and only a
   * genuinely stuck one reaches the throw.
   */
  async function assertNotQuarantined(id: string): Promise<void> {
    if (!journal.isQuarantined(id)) return
    if (await journal.ensureReconciled(id)) return
    throw new Error(
      `Cannot serve ${id}: its raw and gzip representations are in a mixed state that could not be repaired. ` +
        `Reads are refused for this id until a retried store, a later repair or a restart completes the cleanup.`
    )
  }

  async function exist(id: string): Promise<boolean> {
    await assertNotQuarantined(id)
    const filePath = await getFilePath(id)
    // Probed with `statForRead`, the same rule the rest of the read path uses: only a file PROVABLY
    // gone is absent. `existPath` tests F_OK|R_OK, so a present-but-unreadable file (mode/ACL damage,
    // EIO) read as `false` — the "a broken store looks like an empty one" answer this storage's read
    // contract exists to remove, and one `fileInfo()` already refuses to give for the very same id.
    if ((await statForRead(filePath + '.gzip')) !== undefined) return true
    // The gzip probe resolving already proved the shared parent directory intact.
    return (await statForRead(filePath, true)) !== undefined
  }

  /**
   * Walks the content tree yielding stored ids.
   *
   * An id is the path of its file RELATIVE TO the directory `getFilePath` resolves ids against — the
   * shard directory when hash prefixes are on, the storage root in flat mode. Yielding the bare
   * basename instead would collapse an id containing path separators (which nests it into
   * subdirectories) onto its last segment, so `allFileIds()` reported ids that do not resolve back to
   * the file they came from. `prefix` filters those ids, not the on-disk filenames — matching it
   * against the filename let it match the `.gzip` extension of a compressed representation.
   */
  const allFileIdsRec = async function* (folder: string, idBase: string, prefix?: string): AsyncIterable<string> {
    const dirEntries = await components.fs.opendir(folder, { bufferSize: 4000 })
    for await (const entry of dirEntries) {
      const entryPath = path.resolve(folder, entry.name)
      if (entry.isDirectory()) {
        // The reserved temp-write dir only exists directly under the storage root; skip it there and
        // only there, so a deeper same-named directory (reachable via a slash-containing id) is not
        // silently hidden from enumeration.
        if (ATOMIC_MODE && folder === root && entry.name === tempDirName) continue
        // With hash prefixes the SHARD is the id namespace root, so ids nested inside it are relative
        // to the shard rather than to the storage root.
        yield* allFileIdsRec(entryPath, USE_HASH_PREFIX && folder === root ? entryPath : idBase, prefix)
        continue
      }
      const isGzip = entry.name.endsWith(GZIP_EXTENSION)
      const id = path.relative(idBase, isGzip ? entryPath.slice(0, -GZIP_EXTENSION.length) : entryPath)
      if (prefix && !id.startsWith(prefix)) continue
      // Skip cached uncompressed files when the .gzip version also exists: both are one id.
      if (!isGzip && (await components.fs.existPath(entryPath + GZIP_EXTENSION))) continue
      yield id
    }
  }

  /**
   * The gzip trailer's declared original size: a `number` when the format supplies one, `null` when it
   * cannot, or `undefined` when the file disappeared while being read (the caller then falls through
   * to the id's other representation). A storage failure rejects rather than answering any of these.
   */
  async function readGzipOriginalSize(filePath: string, gzipSize: number): Promise<number | null | undefined> {
    // The gzip format (RFC 1952) stores the original uncompressed size in its
    // trailer — the last 4 bytes (ISIZE field, uint32 little-endian).
    // This works for files < 4GB (ISIZE is mod 2^32).
    // SECURITY: the trailer is part of the stored (possibly attacker-controlled) file, so this is
    // only a hint — it is never used to bound decompression (see createSizeLimitTransform) and
    // callers must not trust it for allocation or limits.
    if (gzipSize < 8) return null // Too small to be a valid gzip file
    try {
      const stream = components.fs.createReadStream(filePath, {
        start: gzipSize - 4,
        end: gzipSize - 1
      })
      const buffer = await streamToBuffer(stream)
      if (buffer.length === 4) return buffer.readUInt32LE(0)

      // Fewer than 4 bytes means the file SHRANK between the stat that produced `gzipSize` and this
      // read of its last four bytes — a concurrent overwrite with smaller content. Reading on would
      // throw ERR_BUFFER_OUT_OF_BOUNDS, and the caller's probe cannot recognise the race either,
      // because the path still EXISTS (it was replaced, not removed).
      //
      // Re-read against a FRESH stat rather than reporting a non-answer. The two non-answers are both
      // actively harmful here: `undefined` means "this representation is gone, try the other one",
      // and when the new version is gzip-primary there is no other one, so a present id was reported
      // ABSENT (~1.3% of reads under concurrent compressing writes). `null` means "size unknown", and
      // the consumer this field exists for bounds range requests with `contentSize ?? size`, so it
      // would silently substitute the COMPRESSED size and serve a truncated range. Re-reading gives
      // the real answer for the overwrite that actually happened.
      const current = await statForRead(filePath)
      if (current === undefined) return undefined
      if (current.size < 8) return null
      const retried = await streamToBuffer(
        components.fs.createReadStream(filePath, { start: current.size - 4, end: current.size - 1 })
      )
      // Still short: the file is being rewritten repeatedly. Only now is the size genuinely unknown,
      // and `null` says exactly that rather than inventing a number.
      return retried.length === 4 ? retried.readUInt32LE(0) : null
    } catch (err) {
      // `null` is a legitimate answer — content whose size the format cannot express (a >4GB original,
      // where ISIZE wraps) genuinely has no declared size — so it must not double as "we could not
      // read it". Callers cannot tell those apart, and at least one uses `contentSize ?? size` to bound
      // range requests, where a masked failure silently substitutes the COMPRESSED size.
      //
      // Same rule as every other read, via the same probe: a file provably gone with its parent intact
      // is the documented mid-read race, and everything else — EIO, EACCES, a damaged shard (which
      // `statForRead` rejects on) — is a fault that surfaces.
      if ((await statForRead(filePath)) === undefined) return undefined
      throw err
    }
  }

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    await assertNotQuarantined(id)
    const possibleEncondings = ['gzip', null]
    const baseFilePath = await getFilePath(id)

    // Both representations share a parent directory, so it only has to be proven intact once.
    let parentProven = false
    for (const encoding of possibleEncondings) {
      const extension = encoding ? '.' + encoding : ''
      const filePath = baseFilePath + extension
      const stat = await statForRead(filePath, parentProven)
      parentProven = true
      if (stat) {
        if (encoding === 'gzip') {
          const contentSize = await readGzipOriginalSize(filePath, stat.size)
          // The gzip vanished between the stat and the trailer read: try the raw representation
          // instead of reporting a file that is no longer there (a store transitioning gzip -> raw
          // lands exactly here), and report the id absent only if that is gone too.
          if (contentSize === undefined) continue
          return {
            size: stat.size,
            encoding,
            contentSize
          }
        }
        return {
          size: stat.size,
          encoding,
          contentSize: stat.size
        }
      }
    }

    return undefined
  }

  const doStoreStreamAndCompress = async (id: string, stream: Readable, signal?: AbortSignal): Promise<void> => {
    const filePath = await getFilePath(id)
    const { rename } = components.fs
    // Without rename (legacy custom fs adapter) everything is necessarily in place, so the whole
    // sequence runs under the path lock: no concurrent store/delete can interleave between the
    // raw write, the compression and the raw cleanup (which would otherwise be able to delete a
    // newer writer's file). Not crash-atomic, like the rest of the no-rename mode.
    if (!rename) {
      await writingUnder(filePath, () =>
        withPathLock(filePath, async () => {
          await writeRawInPlaceLocked(id, filePath, stream, signal)
          // An abort observed here arrives after the in-place raw was committed (the previous version
          // is already gone): the store is complete and allowed to succeed, but the optional
          // compression is skipped — or torn down mid-flight via the signal — rather than doing
          // further expensive work for a cancelled request.
          if (!signal?.aborted) {
            let compressed = false
            try {
              compressed = await compressContentFile(filePath, logger, undefined, signal, components.fs)
            } catch (err) {
              // The compression failed (or was torn down): its own cleanup of the partial canonical
              // output is best-effort, so VERIFY none survived — in this mode the compression writes
              // to the canonical `.gzip` directly, reads prefer `.gzip`, and a surviving partial
              // would be served as corrupt content over the just-committed raw. Failures here are
              // post-commit storage errors, never abort-caused, so they must stay visible.
              try {
                await noFailUnlink(filePath + '.gzip')
                if (await existsForInvariant(filePath + '.gzip')) {
                  throw new Error(
                    `Compression of ${id} failed and its partial gzip output could not be removed; ` +
                      `reads would prefer the corrupt gzip over the committed raw.`
                  )
                }
              } catch (invariantErr) {
                throw markAsNonCancellationError(invariantErr)
              }
              if (signal?.aborted && isAbortError(err)) {
                // Provably abort-caused pipeline teardown of an optional post-commit compression:
                // not a failure of this (already completed) store — the raw stays primary.
              } else {
                // A real compression/storage failure that merely RACED the abort (ENOSPC, EACCES,
                // zlib errors, …): resolving would hide it as a successful store, and unmarked it
                // would be translated to the cancellation reason. Surface it as-is.
                throw markAsNonCancellationError(err)
              }
            }
            if (compressed) {
              // The in-place compression succeeded: the gzip exists at its canonical path and, under
              // the lock, the raw is provably still the bytes that were compressed.
              await noFailUnlink(filePath)
            }
          }
        })
      )
      return
    }
    // Fully staged: both the raw bytes and their gzip are produced in the operation-owned staging
    // area — the compression reads the PRIVATE staged raw, so no concurrent store/delete can
    // supersede or fail it — and the id transitions in ONE locked commit to either the gzip-only
    // or the raw-only representation of the new version. Until that commit the previous version
    // stays fully intact; a process killed at any point leaves only sweepable staged files.
    await writingUnder(filePath, () => storeCompressedStaged(id, filePath, stream, rename, signal))
  }

  /** The fully-staged compressed store. Separated so the directory-cache healing wraps it whole. */
  const storeCompressedStaged = async (
    id: string,
    filePath: string,
    stream: Readable,
    rename: (from: string, to: string) => Promise<void>,
    signal?: AbortSignal
  ): Promise<void> => {
    const stagedRawPath = journal.newTempPath()
    const stagedGzipPath = journal.newTempPath()
    // Set when a failed rename could not clear its intent: that exact staged path is the proof
    // the commit never landed and must survive the staging cleanup below.
    let preservedStagedPath: string | undefined
    try {
      await pipe(stream, components.fs.createWriteStream(stagedRawPath))
      // An abort observed once the source is consumed must still cancel the store: without these
      // checkpoints a cancelled request would keep paying for the compression and even commit the
      // object. Nothing has touched the canonical paths yet — the finally below removes the staged
      // residue and the previous version stays fully intact.
      signal?.throwIfAborted()
      // The signal also aborts the compression pipeline itself mid-flight (its partial staged
      // output is removed before the rejection propagates), so a cancelled request stops paying
      // CPU/disk immediately instead of only at the next checkpoint.
      let compressed: boolean
      try {
        compressed = await compressContentFile(stagedRawPath, logger, stagedGzipPath, signal, components.fs)
      } catch (err) {
        // This call site is the one place that hands a signal to an abortable pipeline, so it is
        // where an abort-shaped rejection is provably our own teardown rather than a coincidence:
        // convert it to the caller's reason here, which lets the generic translation stay strict
        // about abort shapes it cannot attribute. Any other failure surfaces as itself.
        if (signal?.aborted && isAbortError(err)) {
          signal.throwIfAborted()
        }
        throw err
      }
      signal?.throwIfAborted()
      await withPathLock(filePath, async () => {
        // Re-check INSIDE the lock: an abort landing while this store was queued on the path lock
        // (after the checkpoints above, with the source already consumed) must still cancel before
        // the irreversible commit below. Nothing has touched the canonical paths yet, so throwing
        // here is handled exactly like the pre-lock throws.
        signal?.throwIfAborted()
        try {
          // Intent-journaled: a crash between the commit rename and the counterpart cleanup is
          // reconciled at next construction, never leaving mixed versions for reads to prefer.
          if (compressed) {
            await journal.commitRepresentation('gzip', id, stagedGzipPath, filePath + '.gzip', filePath, rename, signal)
          } else {
            await journal.commitRepresentation('raw', id, stagedRawPath, filePath, filePath + '.gzip', rename, signal)
          }
        } finally {
          // Run even when the commit throws post-rename (failed counterpart cleanup).
          cache.forget(filePath)
          cache.invalidateInflight(filePath)
        }
      })
    } catch (err) {
      if (err instanceof UncommittedIntentSurvivedError) {
        preservedStagedPath = err.stagedPath
      }
      throw err
    } finally {
      // Whatever was not renamed into place is staging residue (the raw after a gzip-only commit,
      // both files after a failure) — except a preserved uncommitted-intent proof. The previous
      // canonical version is untouched on any error.
      if (stagedRawPath !== preservedStagedPath) {
        await noFailUnlink(stagedRawPath)
      }
      if (stagedGzipPath !== preservedStagedPath) {
        await noFailUnlink(stagedGzipPath)
      }
    }
  }

  await journal.reconcile()

  // Batch surfaces take an unbounded id list; see `mapWithConcurrency`. Sized so the two `stat`s each
  // `exist()` performs stay far below any conventional per-process file-descriptor limit.
  const BATCH_CONCURRENCY = 64

  return {
    async start(_startOptions: any) {
      // Idempotent: clear any existing timer first so a repeated start() doesn't leak intervals.
      if (evictionTimer) {
        clearInterval(evictionTimer)
      }
      // Track the in-flight eviction tick so stop() can await one that is already running; a tick
      // firing during a slow eviction receives that same in-flight promise from cache.evict().
      evictionTimer = setInterval(() => {
        evictionTick = cache.evict()
      }, CACHE_EVICTION_INTERVAL)
      evictionTimer.unref()
      // Detached best-effort cleanup of temp files orphaned by an interrupted write in a prior run.
      // Runs in the background so it never delays startup; `stop()` awaits it once, at shutdown.
      // Chained onto any previous sweep so a repeated start() cannot replace a still-running sweep
      // with a new promise (the older one would dangle past stop()) nor run two sweeps concurrently.
      tempFileSweep = tempFileSweep
        .then(() => journal.sweepOrphanedTempFiles())
        .then((removed) => {
          if (removed > 0) logger.info(`Removed ${removed} orphaned temp file(s) at startup`)
        })
        .catch((error) => logger.warn(`Orphaned temp-file sweep failed: ${error}`))
    },
    async stop() {
      if (evictionTimer) {
        clearInterval(evictionTimer)
        evictionTimer = undefined
      }
      // Wait for the startup temp-file sweep, an in-flight eviction tick and any inflight
      // decompressions before cleaning up
      await Promise.allSettled([tempFileSweep, evictionTick, ...cache.inflight()])
      // Evict all cached files on shutdown to prevent disk leaks across restarts
      await cache.evictAll()
    },
    storeStream: (id: string, stream: Readable, signal?: AbortSignal): Promise<void> =>
      runStoreWithSignal(stream, signal, () => doStoreStream(id, stream, signal)),
    retrieve,
    exist,
    storeStreamAndCompress: (id: string, stream: Readable, signal?: AbortSignal): Promise<void> =>
      runStoreWithSignal(stream, signal, () => doStoreStreamAndCompress(id, stream, signal)),
    async delete(ids: string[]): Promise<void> {
      for (const id of ids) {
        const filePath = await getFilePath(id)
        // Locked so an in-flight decompression can never resurrect the id by renaming its staged
        // bytes onto the canonical path after these unlinks.
        await withPathLock(filePath, async () => {
          // A pending intent (a failed counterpart cleanup earlier) must not outlive its id: an
          // orphaned journal whose id has neither a staged file nor any representation would refuse
          // the next construction even though this delete was intentional. Repair first (throws if
          // impossible), which discharges the journal; a crash mid-delete afterwards leaves at
          // worst a partial delete with NO journal, which construction accepts.
          await journal.repairPendingIntent(id)
          // Every removal below is verified: a delete that resolves while ANY representation
          // survives (cached raw, primary raw, or gzip) would leave the id readable after a
          // "successful" delete. Failures abort before touching the next representation, so a
          // failed delete always leaves a complete, readable version behind and rejects loudly.
          const wasCached = await cache.remove(filePath)
          if (!wasCached) {
            await noFailUnlink(filePath)
            if (await existsForInvariant(filePath)) {
              throw new Error(`Failed to delete ${id}: its raw representation could not be removed`)
            }
          }
          await noFailUnlink(filePath + '.gzip')
          if (await existsForInvariant(filePath + '.gzip')) {
            throw new Error(`Failed to delete ${id}: its gzip representation could not be removed`)
          }
          // Defensive: repairPendingIntent already discharged any journal; verify none remains.
          await journal.assertNoIntent(id, `Deleted ${id} but could not remove its intent journal`)
          cache.invalidateInflight(filePath)
        })
      }
    },
    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      return new Map(
        await mapWithConcurrency(
          cids,
          BATCH_CONCURRENCY,
          async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]
        )
      )
    },
    allFileIds: (prefix?: string) => allFileIdsRec(root, root, prefix),
    fileInfo,
    async fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
      return new Map(
        await mapWithConcurrency(
          cids,
          BATCH_CONCURRENCY,
          async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)]
        )
      )
    }
  }
}
