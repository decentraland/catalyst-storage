import { createHash } from 'crypto'
import path from 'path'
import { pipeline, Readable, Transform, Writable } from 'stream'
import { promisify } from 'util'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { assertStorableStream, SimpleContentItem, streamToBuffer } from './content-item'
import { isAbortError, runStoreWithSignal } from './cancellation'
import { forEachWithConcurrency, mapWithConcurrency } from './concurrency'
import { compressContentFile } from './extras/compression'
import { createFsInvariants } from './folder-based/fs-invariants'
import { createDecompressCache, InvalidationToken } from './folder-based/decompress-cache'
import { createIntentJournal, TEMP_DIR_NAME, UncommittedIntentSurvivedError } from './folder-based/intent-journal'
import { DecompressionLimitExceededError, PathNotContainedError } from './folder-based/errors'
import { assertStorableContentId, assertValidContentId, GZIP_EXTENSION, gzipPathOf } from './content-id'
import { destroyAllQuietly, destroyQuietly } from './stream-teardown'

const pipe = promisify(pipeline)

/**
 * How many entries of one directory `allFileIds()` will hold in memory to decide it from a SINGLE read.
 *
 * Above this it falls back to reading the directory twice — once for the compressed names, once to
 * yield — which streams and so bounds its memory by `opendir`'s own buffer instead.
 *
 * The two shapes this storage runs in sit on opposite sides of the line, which is the point:
 * - With hash prefixes (the default) a shard holds total/65,536 entries, so a root of 268 million ids
 *   still fits and every directory takes the one-read path. That halves the `getdents` traffic of a
 *   full walk — measured 34,510 `opendir` calls for 20,000 ids before, and it is the dominant cost of a
 *   GC or sync sweep over a sharded tree.
 * - In flat (`disablePrefixHash`) mode a single directory holds every id, so a large deployment
 *   overflows immediately and keeps the streaming behaviour it needs: buffering that listing retained
 *   ~300 bytes per entry, measured at 47MB before the first id came out for 200k ids and ~290MB for a
 *   million.
 *
 * 4,096 entries is ~160KB of names, which is nothing against the syscalls it saves.
 *
 * @public
 */
export const MAX_BUFFERED_DIRECTORY_ENTRIES = 4096

const ONE_HOUR_IN_MS = 60 * 60 * 1000
const FIVE_MINUTES_IN_MS = 5 * 60 * 1000
const FIVE_GB_IN_BYTES = 5 * 1024 * 1024 * 1024
const TWO_HUNDRED_FIFTY_SIX_MB_IN_BYTES = 256 * 1024 * 1024
/**
 * Concurrent gzip inflations allowed by default. Four keeps range-read throughput healthy while holding
 * the cache's worst-case overshoot to `4 × decompressMaxFileSize` (1 GB at the defaults) instead of
 * scaling with the caller's request concurrency. See `decompressMaxConcurrentInflations`.
 */
const DEFAULT_MAX_CONCURRENT_INFLATIONS = 4
/**
 * How long a ranged read keeps its decompressed cache file protected from LRU eviction.
 *
 * Covers the gap between `retrieve` returning and the consumer opening the lazy stream — microseconds in
 * any real consumer, so this is generous. Bounded rather than indefinite because a consumer is free to
 * drop the item without ever reading it, and a pin that outlived that would exempt the entry from the
 * size budget for good.
 */
const CACHE_PIN_GRACE_MS = 30_000

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
   * How many gzip range requests may inflate to disk at once. Default: 4.
   *
   * This is what bounds how far the decompress cache can exceed `decompressCacheMaxSize`, because
   * admission is only checked once an inflated file has been committed and the eviction it triggers
   * cannot be awaited (it needs the path lock the committing read still holds). The cache therefore
   * settles at up to `decompressMaxConcurrentInflations × decompressMaxFileSize` above the budget before
   * eviction catches up — 1 GB over at the defaults. Unbounded, the multiplier was the caller's own
   * request concurrency: 50 concurrent cold range reads measured 36x over budget.
   *
   * Excess range reads queue rather than fail. Raise it for more concurrent range throughput at the cost
   * of a larger transient overshoot; lower it to hold the budget tighter.
   */
  decompressMaxConcurrentInflations?: number
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
 * - **Every write is crash-atomic** — writes stage into a reserved directory and `rename` into place,
 *   so an interrupted write can never leave a partial file at a canonical path, and a reader
 *   concurrent with a write always observes one complete version. `rename` is therefore a REQUIRED
 *   capability of the filesystem component.
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

  // Normalized ONCE, here, so every path this storage builds from it can be assembled by
  // concatenation instead of being re-normalized per operation — `resolveFilePath` runs on every
  // single call, including every read. The trailing-separator strip runs afterwards because
  // `path.normalize` preserves a trailing separator, and a root that keeps one breaks the containment
  // check for every id ('/data/x/id'.startsWith('/data/x//') is false).
  root = path.normalize(root)
  // remove path separators / \ from the end of the folder
  while (root.endsWith(path.sep)) {
    root = root.slice(0, -1)
  }

  const USE_HASH_PREFIX = !(options?.disablePrefixHash ?? false)

  // ALL configuration validation happens before the first filesystem mutation, so an invalid
  // configuration fails without side effects (no root creation, no reserved dir, no marker write).
  const tempDirName = options?.tempDirectoryName ?? TEMP_DIR_NAME
  const tempDir = path.join(root, tempDirName)
  if (tempDirName === '' || tempDirName === '.' || tempDirName === '..' || /[/\\]/.test(tempDirName)) {
    throw new Error(`tempDirectoryName must be a single path segment, got: ${JSON.stringify(tempDirName)}`)
  }
  if (USE_HASH_PREFIX && /^[0-9a-f]{4}$/i.test(tempDirName)) {
    throw new Error(
      `tempDirectoryName must not look like a shard directory (4 hex characters) when hash prefixes are enabled, got: ${JSON.stringify(tempDirName)}`
    )
  }
  // NaN/Infinity/non-positive values would silently disable the decompression-bomb cap, or create
  // tight eviction loops and pathological cache behavior.
  for (const [optionName, value] of Object.entries({
    decompressCacheTTL: options?.decompressCacheTTL,
    decompressCacheMaxSize: options?.decompressCacheMaxSize,
    decompressCacheEvictionInterval: options?.decompressCacheEvictionInterval,
    decompressMaxFileSize: options?.decompressMaxFileSize,
    decompressMaxConcurrentInflations: options?.decompressMaxConcurrentInflations
  })) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
      throw new Error(`${optionName} must be a positive safe integer, got: ${String(value)}`)
    }
  }

  const { existsForInvariant, noFailUnlink } = createFsInvariants(components.fs)

  /**
   * The commit rename, called ON the component rather than detached off it.
   *
   * `const { rename } = components.fs` — which is what the three commit sites did — drops the receiver,
   * so an adapter whose methods rely on `this` (a class instance, the shape `IFileSystemComponent` being
   * `@public` invites) passed construction and every read and then failed with a `TypeError` at the
   * commit of EVERY write. `compressContentFile` documents this exact hazard as a rule; every other fs
   * call in this module already goes through the object. Defined once so the three sites cannot drift.
   */
  const rename = (from: string, to: string): Promise<void> => components.fs.rename(from, to)

  const CACHE_EVICTION_INTERVAL = options?.decompressCacheEvictionInterval ?? FIVE_MINUTES_IN_MS
  const MAX_DECOMPRESSED_SIZE = options?.decompressMaxFileSize ?? TWO_HUNDRED_FIFTY_SIX_MB_IN_BYTES

  const cache = createDecompressCache(
    { logger, fsInvariants: { existsForInvariant, noFailUnlink } },
    {
      ttl: options?.decompressCacheTTL ?? ONE_HOUR_IN_MS,
      maxSize: options?.decompressCacheMaxSize ?? FIVE_GB_IN_BYTES,
      maxConcurrentInflations: options?.decompressMaxConcurrentInflations ?? DEFAULT_MAX_CONCURRENT_INFLATIONS
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
  const MAX_KNOWN_DIRECTORIES = 100_000
  const knownDirectories = new Set<string>()

  function forgetDirectory(dirname: string): void {
    knownDirectories.delete(dirname)
  }

  /**
   * Records a directory this instance has observed to exist, so its later disappearance is
   * recognizable as damage rather than as an id that was never stored.
   *
   * Written by both the write path (which creates the directory) and the read path (which only
   * observes it) — "created or observed", as the cache is documented.
   */
  function rememberDirectory(dirname: string): void {
    if (knownDirectories.has(dirname)) return
    // Evict the OLDEST entry rather than clearing wholesale. The cost of a clear is not just the syscall
    // the previous comment described: membership here is the SOLE evidence `statForRead` uses to tell a
    // destroyed shard from one nothing was ever stored in, so a clear silently downgrades every cleared
    // shard's damage report to an ordinary "absent" — the "broken storage looks like an empty node"
    // answer this read contract exists to prevent. Sets iterate in insertion order, so the first key is
    // the oldest, and one eviction per insertion past the cap keeps the guarantee for everything else.
    // Unreachable in hash-prefix mode (16^4 shards < the cap) and bounded work in flat mode.
    while (knownDirectories.size >= MAX_KNOWN_DIRECTORIES) {
      const oldest = knownDirectories.values().next()
      if (oldest.done) break
      knownDirectories.delete(oldest.value)
    }
    knownDirectories.add(dirname)
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
      resolveFilePath: (id: string) => resolveFilePath(id)
    },
    { tempDir, tempDirName, useHashPrefix: USE_HASH_PREFIX }
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
    /**
     * Skips the parent-directory probe that classifies an absence as a miss rather than as damage.
     *
     * Set by a caller that probes BOTH of an id's representations and will therefore ask the question
     * again on its last probe: an id's two representations share a directory, so classifying on the
     * first miss spends a syscall whose answer the second probe supplies for free by succeeding. Every
     * read of raw-primary content — the majority, since images, models and GLB are stored uncompressed
     * — paid it: 3 stats where 2 suffice, measured at 13-20% of `exist`/`fileInfo`/`retrieve`.
     *
     * Safe because it only ever DEFERS: whenever every representation is absent, the last probe runs
     * the classification and a damaged shard still rejects instead of being reported empty.
     */
    deferAbsenceClassification = false
  ): Promise<{ size: number } | undefined> {
    try {
      const stat = await components.fs.stat(filePath)
      // Successfully statting a file PROVES its parent is an intact directory — nothing can be
      // statted inside a path that is not one. Recording that observation is what lets a LATER
      // disappearance of the same directory be classified as damage instead of as "nothing was ever
      // stored here". Load-bearing since reads stopped creating directories: `getFilePath` used to
      // populate this set on every read as a side effect of its `mkdir`, so an instance that only
      // ever READS would otherwise never learn which shards exist, and would answer a destroyed
      // shard with `false` for every id in it — the "broken storage looks like an empty node"
      // outcome this contract exists to prevent, on exactly the read-heavy node most likely to hit it.
      rememberDirectory(path.dirname(filePath))
      // A stat succeeding is NOT proof that content is here — only that SOMETHING is. A directory at a
      // content path made an id nothing was ever stored under into a phantom: `exist` answered true,
      // `fileInfo` reported the directory's own `stat.size` as a content length, and `retrieve` handed
      // back a ContentItem whose `asStream()` then died with EISDIR (or, at the `.gzip` path, made
      // `fileInfo`/`retrieve` reject outright — a storage-fault answer for a provably absent id). It is
      // reachable without any corruption, because nested ids are legal and legal: `storeStream('a/b')`
      // creates the directory `a`, and `a.gzip/b` creates `a.gzip`, which is `a`'s compressed path.
      // Worse, `delete` then rejected FOREVER ("its raw representation could not be removed"), taking
      // down every GC batch containing that id, and no store could ever fix it.
      //
      // `allFileIds` already agrees with this: it yields only regular files, so returning `undefined`
      // here is what makes enumeration and the point lookups answer the same question. Directories are
      // never removed to make room — destroying something this storage cannot prove it owns is exactly
      // what the reserved-namespace checks refuse to do.
      if (!stat.isFile()) return undefined
      return stat
    } catch (err: any) {
      // ENAMETOOLONG joins ENOENT/ENOTDIR as PROVABLY absent: no file of that name can exist, so it
      // is a miss rather than a storage fault. `exist()` answered `false` for it before switching to
      // this probe, and turning that into a throw failed whole `existMultiple` batches.
      if (err?.code !== 'ENOENT' && err?.code !== 'ENOTDIR' && err?.code !== 'ENAMETOOLONG') throw err
      // An id's two representations live in the SAME directory, so a caller probing both asks this
      // question at most once — on its LAST probe, once every representation has come back absent. An
      // earlier probe defers (see the parameter), which is what keeps a hit on the second
      // representation from paying for a classification the hit itself renders unnecessary. The window
      // this gives up — the directory being destroyed between two adjacent probes of the same
      // operation, downgrading a fault to a miss — is narrower than the races the read path already
      // tolerates.
      if (deferAbsenceClassification) return undefined
      // A missing-file error here has two very different meanings, and only one of them is a miss.
      // The parent directory decides which — and it must be proven to be a DIRECTORY, not merely
      // present: an access check passes for a regular file left at the shard path, while every stat
      // beneath it fails with ENOTDIR, so "present" alone would classify a corrupted tree as a miss.
      //
      // Costs one syscall, and only after a stat has already failed — hits, the hot path the
      // directory cache exists for, are untouched.
      const dirname = path.dirname(filePath)
      let parent: { isDirectory(): boolean } | undefined
      let parentProbeFailure: unknown
      try {
        parent = await components.fs.stat(dirname)
      } catch (probeErr: any) {
        parentProbeFailure = probeErr
      }

      // An intact directory that simply does not contain this file: the ordinary miss. Remembered
      // for the same reason as a successful stat above — it is proof this shard exists right now.
      if (parent?.isDirectory()) {
        rememberDirectory(dirname)
        return undefined
      }

      // Something is AT the shard path but is not a directory. Never a legitimate empty state — a
      // regular file there makes every id in the shard unreadable — so it is a fault whoever created
      // it. Invalidating the cache entry lets a write recreate the tree once whatever occupies the
      // path is gone (a foreign file is never removed here: destroying something this storage cannot
      // prove it owns is exactly what the reserved-namespace checks refuse to do).
      if (parent) {
        forgetDirectory(dirname)
        logger.warn(`Refusing to report ${filePath} as absent: its parent path exists but is not a directory`)
        throw err
      }

      // The parent could not be read at all. If the probe failed for a reason OTHER than the
      // directory being absent (EACCES, EIO), this storage cannot answer the question and must not
      // pretend the id is missing.
      // ENOTDIR here means an ANCESTOR of the shard is not a directory — a hard obstruction that
      // makes every id beneath it unreadable, never a legitimate empty state. It belongs with the
      // "parent exists but is not a directory" case above, not with "the shard was never created".
      const probeCode = (parentProbeFailure as { code?: string } | null)?.code
      if (probeCode !== 'ENOENT') {
        forgetDirectory(dirname)
        logger.warn(`Refusing to report ${filePath} as absent: its parent directory could not be read`)
        throw err
      }

      // The parent directory does not exist. Reads no longer create it (see `resolveFilePath`), so
      // this is the normal answer for a shard nothing was ever stored in — the id is absent. It is a
      // FAULT only when this instance created or observed that directory, which means the tree it
      // owns was destroyed underneath it, taking every id inside with it.
      if (!knownDirectories.has(dirname)) return undefined
      forgetDirectory(dirname)
      logger.warn(`Refusing to report ${filePath} as absent: its parent directory was removed underneath us`)
      throw err
    }
  }

  const tempDirLower = tempDir.toLowerCase()

  /**
   * Whether a path IS the reserved staging directory or lands inside it.
   *
   * The ONE definition of "reserved", used both to reject an id that resolves into the staging area and
   * to skip that area while enumerating. Those two must agree: while enumeration compared the directory
   * entry's name to `tempDirectoryName` exactly, a reserved directory whose on-disk casing differed
   * from the configured name — a deployment that changed `tempDirectoryName`'s casing, since `mkdir`
   * matches case-insensitively and leaves the original entry — was DESCENDED INTO. `allFileIds()` then
   * yielded staged files, intent journals and the ownership marker as content ids: with hash prefixes a
   * staged file came out as a bare id whose `delete()` resolved while removing nothing, and in flat mode
   * the ids came out reserved, so `delete()` rejected and took the whole GC batch down with it.
   *
   * Compared case-INSENSITIVELY for the same reason ids are: a case-folding filesystem (APFS, NTFS, an
   * SMB/CIFS mount) resolves `.TMP-WRITES/x` onto the reserved directory, so an exact-case check waves
   * the write through into the staging namespace, where it is invisible to `allFileIds()` and makes the
   * NEXT construction refuse to start over a file "this storage did not create".
   *
   * Anchored on the ROOT-relative path rather than on a bare name, so a same-named directory deeper in
   * the tree (reachable via a slash-containing id in flat mode) is still enumerated — the reservation
   * covers one directory, not a filename.
   *
   * Only the PREFIX is folded, so the cost is bounded by the reserved directory's own path length
   * rather than growing with the id.
   */
  function isInsideReservedTempDir(candidate: string): boolean {
    if (candidate.length < tempDir.length) return false
    if (candidate.slice(0, tempDir.length).toLowerCase() !== tempDirLower) return false
    return candidate.length === tempDir.length || candidate[tempDir.length] === path.sep
  }

  /**
   * Resolves an id to its canonical path, validating it, WITHOUT touching the filesystem.
   *
   * Read paths use this. `getFilePath` (which also creates the parent) is for writes only: creating
   * a directory is a side effect a read has no business having, and it was not free. With hash
   * prefixes it merely pre-created all 65,536 shards over time, but in flat mode ids nest, so every
   * probe of a NEVER-STORED nested id — `exist('a/b/c/missing')` — left `a/b/c/` behind permanently.
   * A caller passing through untrusted ids could grow the inode count without limit, and
   * `allFileIds()` then walked those empty trees on every enumeration, forever.
   */
  async function resolveFilePath(id: string): Promise<string> {
    // The id-shape rules shared with every other backend, so they cannot drift apart:
    // - an empty id resolves to the containment directory itself, which is a directory and not
    //   anyone's content (and is the one input the round-trip check below cannot reject on its own,
    //   because an empty id and an empty relative path are equal);
    // - `<id>.gzip` is this storage's own name for the compressed representation of `<id>`, so an id
    //   ending in it is not addressable: it occupies another id's second path. The damage is not
    //   hypothetical — storing `foo` and `foo.gzip` made `retrieve('foo')` serve `foo.gzip`'s bytes
    //   (inflating them, with a contentSize read out of the wrong file's last four bytes),
    //   `exist('foo.gzip')` answer false, and `allFileIds()` report a phantom `foo` twice while never
    //   listing `foo.gzip` — so a consumer syncing or GC-ing from it would delete real content;
    // - a NUL byte cannot be part of a filename; `fs` rejects it with ERR_INVALID_ARG_VALUE, which is
    //   not one of the "provably absent" codes, so it would surface from `exist()` as a storage fault.
    assertValidContentId(id)

    // We are sharding the files using the first 4 digits of its sha1 hash, because it generates collisions
    // for the file system to handle millions of files in the same directory.
    // This way, asuming that sha1 hash distribution is ~uniform we are reducing by 16^4 the max amount of files in a directory.
    const hash = createHash('sha1').update(id).digest('hex').substring(0, 4)

    // `root` is already normalized, and a 4-hex shard needs no normalizing, so this is a concatenation
    // rather than `path.normalize(path.join(...))` (measured 409 ns per call, on every operation).
    const directoryPath = USE_HASH_PREFIX ? root + path.sep + hash : root

    // What the id resolves to IF it needs no normalization, which is also the only shape an
    // addressable id may have.
    const unnormalized = directoryPath + path.sep + id
    const finalPath = path.normalize(unnormalized)

    // FAST PATH: normalizing changed nothing, so the id names exactly its own path directly under the
    // containment directory — both invariants below then hold BY CONSTRUCTION (the resolved path is
    // literally `directoryPath + sep + id`, so it round-trips and it is inside). This replaces a
    // `path.relative` round trip that cost 1204 ns of the 2543 ns this function spent per call, and it
    // is the same invariant stated as an equality instead of a comparison.
    //
    // Verified equivalent by fuzzing the two formulations against each other over 40,066 adversarial
    // ids in both shard modes: identical resolved paths and identical rejection messages.
    //
    // The trailing-separator exclusion is load-bearing and not decoration: `path.normalize` PRESERVES a
    // trailing separator, so `'x/'` is unchanged by it and would take the fast path — while
    // `path.relative` strips it, which is what makes `'x/'` an alias of `'x'`. Without this, `'x/'`
    // resolved onto another id's file instead of being rejected.
    if (finalPath !== unnormalized || id.endsWith(path.sep)) {
      // SLOW PATH, taken only by an id that is already invalid: run the original checks verbatim, in
      // their original order, so every rejection keeps its exact class, message and reason. The two are
      // orthogonal and the order is load-bearing — `../evil` round-trips cleanly (so it passes the
      // aliasing check) and is caught by containment, which is the accurate diagnosis for it.

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
    }

    // The temp-write namespace is reserved: an id resolving into it (reachable when
    // disablePrefixHash makes the root itself the containment dir, e.g. '.tmp-writes/foo') would be
    // hidden from allFileIds and could be deleted by the startup sweep.
    if (isInsideReservedTempDir(finalPath)) {
      throw new PathNotContainedError('Cannot manipulate files inside the reserved temp-write folder')
    }

    return finalPath
  }

  /**
   * Resolves an id AND ensures its parent directory exists. For WRITE paths only — see
   * `resolveFilePath` for why reads must not create anything.
   */
  async function getFilePath(id: string): Promise<string> {
    const finalPath = await resolveFilePath(id)
    // recursively creates the directory structure if needed
    const dirname = path.dirname(finalPath)

    if (!knownDirectories.has(dirname)) {
      if (!(await components.fs.existPath(dirname))) {
        await components.fs.mkdir(dirname, { recursive: true })
      }
      rememberDirectory(dirname)
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
    resolveContentSize = true,
    // Defers the absence classification to a later probe of the SAME id — see `statForRead`. Set when
    // this is the gzip probe of a `retrieve()` that will go on to probe the raw.
    deferAbsenceClassification = false,
    // Pre-resolved canonical raw path, so an operation that already resolved this id does not pay the
    // sha1 and the path validation again (2.5 µs of CPU per resolve).
    baseFilePath?: string
  ): Promise<ContentItem | undefined> => {
    const extension = encoding ? '.' + encoding : ''
    const filePath = (baseFilePath ?? (await resolveFilePath(id))) + extension

    const stat = await statForRead(filePath, deferAbsenceClassification)
    if (!stat) return undefined

    if (range) {
      const clampedEnd = clampRange(range, stat.size)
      // SNAPSHOT, not a lazy read of `range.start`. `size` and `clampedEnd` are computed now while the
      // stream is created later, so closing over the caller's object let a mutation in between decide
      // which bytes are served under an already-advertised length: `retrieve(id, r)` then `r.start = 2`
      // served 3 bytes as 5, and `r.start = 5` made `asStream()` throw ERR_OUT_OF_RANGE from inside
      // `createReadStream`. The in-memory backend slices eagerly and so was never exposed to this.
      const start = range.start
      return new SimpleContentItem(
        async () => components.fs.createReadStream(filePath, { start, end: clampedEnd }),
        clampedEnd - start + 1,
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

  /** Counts the bytes flowing through it, so a completed inflation knows its own output size. */
  function createByteCounter(onTotal: (total: number) => void): Transform {
    let total = 0
    return new Transform({
      transform(chunk: Buffer, _encoding, callback) {
        total += chunk.length
        callback(null, chunk)
      },
      flush(callback) {
        onTotal(total)
        callback()
      }
    })
  }

  /** Returns how many bytes were written, so callers need no `stat` to register the result. */
  async function inflateGzipItemInto(gzipItem: ContentItem, target: string): Promise<number> {
    // Both streams are created inside the try and torn down if anything fails before `pipe` takes
    // ownership: arguments evaluate left to right, so the source (and the file descriptor behind it)
    // already exists by the time the destination is constructed, and a custom adapter may throw
    // synchronously there where native fs would report asynchronously. Without this the source is
    // left paused mid-read, holding its descriptor for the life of the process. When `pipe` did run
    // it has already destroyed both, so the teardown here is a no-op in the common failure case.
    let source: Readable | undefined
    let destination: Writable | undefined
    let written = 0
    try {
      source = await gzipItem.asStream()
      destination = components.fs.createWriteStream(target)
      // Cap how much the gzip may inflate to so a decompression bomb cannot write an unbounded file
      // to disk. The gzip trailer's declared size is attacker-controllable, so the limit is enforced
      // on the actual inflated bytes.
      await pipe(
        source,
        createSizeLimitTransform(MAX_DECOMPRESSED_SIZE),
        createByteCounter((total) => (written = total)),
        destination
      )
      return written
    } catch (err) {
      destroyAllQuietly(source, destination)
      throw err
    }
  }

  async function materializeRangeCacheFromGzip(
    id: string,
    uncompressedPath: string,
    token: InvalidationToken
  ): Promise<void> {
    // `false`: only the stream is used here, so the trailer read that resolves the logical size
    // would be pure overhead on the decompression path. The canonical path is already resolved by the
    // caller, so it is threaded through rather than recomputed.
    const gzipItem = await retrieveWithEncoding(id, 'gzip', undefined, false, false, uncompressedPath)
    if (!gzipItem) {
      return
    }

    const gzipPath = gzipPathOf(uncompressedPath)
    const sourceVanished = () => gzipSourceVanishedForRead(gzipPath)
    {
      // Stage the inflation in the temp dir so a process killed mid-decompress can never leave a
      // partial file at the canonical uncompressed path — a later range request would silently serve
      // its truncated bytes as valid content. Publishing it needs the atomic rename: writing straight
      // onto the canonical path would let a concurrent reader be served its truncated bytes.
      const writePath = journal.newTempPath()
      try {
        const inflatedSize = await inflateGzipItemInto(gzipItem, writePath)
        // Commit under the path lock so this rename can never interleave with a store or delete on
        // the same canonical path; discard when the source gzip was replaced or the id deleted while
        // inflating.
        const committed = await withPathLock(uncompressedPath, async () => {
          if (token.invalidated) return false
          await rename(writePath, uncompressedPath)
          // Registered with the size the inflation itself counted, not a fresh `stat`: a stat that
          // failed AFTER the rename had landed skipped `record` entirely, leaving a decompressed copy
          // at the canonical path that eviction never knew about and `allFileIds` hides.
          cache.record(uncompressedPath, inflatedSize)
          return true
        })
        if (!committed) {
          await noFailUnlink(writePath)
        }
      } catch (err) {
        // Remove the partial staged file; the canonical path was never touched.
        await noFailUnlink(writePath)
        // The staged inflation lands in the same reserved directory as staged stores, and heals it
        // the same way if it vanished underneath a live instance.
        if ((err as { code?: string } | null)?.code === 'ENOENT') {
          await journal.ensureTempDir().catch(() => undefined)
        }
        // An invalidated token means the id was overwritten/deleted while inflating — the failure
        // belongs to the replaced gzip, not to the caller's request. Resolving lets the retry loop
        // observe the new representation instead of the error bubbling into a spurious undefined for
        // a valid id.
        if (token.invalidated || (await sourceVanished())) return
        throw err
      }
    }
  }

  /**
   * Pipes a source into a freshly created write stream at `target`.
   *
   * The destination is constructed inside a try because native `fs` reports an open failure
   * asynchronously but a custom adapter may THROW synchronously — and then `pipeline` never takes
   * ownership of the source, leaving the caller's stream paused and undestroyed with its descriptor
   * (or socket) held for the life of the process. `inflateGzipItemInto` and `compressContentFile`
   * already guard this window; the three store pipes did not.
   */
  async function pipeTo(source: Readable, target: string): Promise<void> {
    let destination: Writable
    try {
      destination = components.fs.createWriteStream(target)
    } catch (err) {
      destroyQuietly(source)
      throw err
    }
    await pipe(source, destination)
  }

  /**
   * A staged write, which additionally heals the reserved staging directory.
   *
   * That directory is created once, at construction. If something removes it while this instance is
   * live, EVERY store and every gzip range read failed at its staged write, permanently, because
   * nothing recreated it — and `writingUnder` responded by invalidating the SHARD directory, which
   * was never the problem. Shard directories already self-heal exactly this way.
   *
   * This store still fails: its source has already been consumed, so there is nothing left to
   * re-pipe. What is restored is the directory, so the next store succeeds instead of inheriting a
   * permanently broken instance.
   */
  async function pipeToStaged(source: Readable, target: string): Promise<void> {
    try {
      await pipeTo(source, target)
    } catch (err) {
      if ((err as { code?: string } | null)?.code === 'ENOENT') {
        await journal.ensureTempDir().catch(() => undefined)
      }
      throw err
    }
  }

  const doStoreStream = async (id: string, stream: Readable, signal?: AbortSignal): Promise<void> => {
    // ID BEFORE SOURCE, matching the other two backends. The order is observable: `storeStream('../evil',
    // consumedStream)` answered `PathNotContainedError` on the in-memory backend and
    // `ERR_STREAM_PREMATURE_CLOSE` here, so a caller branching on the typed error to choose between "reject
    // the request" and "retry with a fresh source" got a different answer per backend for one bad call. The
    // id is the caller's own bad argument and the cheaper thing to check, so it wins.
    //
    // An id no directory entry can hold is not storable, and this backend used to say so with a raw
    // ENAMETOOLONG from the commit rename rather than the typed error every other id rejection uses.
    assertStorableContentId(id)
    const filePath = await getFilePath(id)
    // A source that cannot supply content must be refused, not stored: piping an already-consumed
    // stream writes zero bytes and RESOLVES, so this committed an empty object under the id and
    // reported success. See `assertStorableStream`.
    assertStorableStream(stream)
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
        await pipeToStaged(stream, tempPath)
        // An abort observed once the source is consumed must still cancel the store before the
        // commit; the catch below removes the staged file and the canonical path stays untouched.
        signal?.throwIfAborted()
        await withPathLock(filePath, async () => {
          // Re-check INSIDE the lock: an abort landing while this store was queued on the path lock
          // (after the checkpoint above, with the source already consumed) must still cancel before
          // the irreversible commit below. Nothing has touched the canonical paths yet, so throwing
          // here is handled exactly like the pre-lock throw.
          signal?.throwIfAborted()
          let committed = false
          try {
            // The raw and its .gzip are one versioned object: a gzip left from a previous version
            // would be preferred by retrieve() and serve stale bytes over the content just stored
            // (intent-journaled so even a crash mid-cleanup cannot leave the stale gzip preferred).
            await journal.commitRepresentation(
              'raw',
              id,
              tempPath,
              filePath,
              gzipPathOf(filePath),
              rename,
              signal,
              () => (committed = true)
            )
          } finally {
            // Gated on the rename having LANDED, not on reaching this line. The commit still needs
            // this bookkeeping when it fails AFTER the rename (a failed counterpart cleanup), but it
            // can also fail on either side of that point, and running it for a pre-rename failure
            // was actively harmful: `forget` drops the tracking WITHOUT unlinking, so a decompressed
            // range-cache file at this path became invisible to eviction and to `evictAll()` — an
            // untracked copy that no longer counts against `decompressCacheMaxSize` and is never
            // reclaimed — while `invalidateInflight` discarded a concurrent inflation that was still
            // current, costing a redundant inflate and, twice over, a spurious `undefined` from a
            // ranged read of a present id. Before the rename the canonical paths are untouched, so
            // the cache state describing them is still accurate.
            if (committed) {
              cache.forget(filePath)
              cache.invalidateInflight(filePath)
            }
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
      // Resolved ONCE for the whole call and threaded through. Every resolve re-hashes the id and
      // re-validates the path (~2.5 µs of CPU), and this function used to do it twice for an ordinary
      // hit and four times for a cold ranged read — including once purely to name the path for
      // `cache.touch`.
      const baseFilePath = await resolveFilePath(id)
      let contentItem: ContentItem | undefined = undefined
      // The gzip probe defers its absence classification to the raw probe below, which asks the
      // question once for both representations.
      if (!range) contentItem = await retrieveWithEncoding(id, 'gzip', undefined, true, true, baseFilePath)
      if (!contentItem) {
        contentItem = await retrieveWithEncoding(id, null, range, true, false, baseFilePath)
        if (contentItem && range) {
          // Update last access if this file is in the cache
          cache.touch(baseFilePath)
        }
      }

      // If range was requested but uncompressed file doesn't exist, fall back to
      // decompressing the gzip file, writing it to disk as a cache, and serving the range.
      // Two attempts: a decompression can be invalidated by a concurrent overwrite committing while
      // it inflates (its stale output is correctly discarded), leaving this request with neither a
      // cached file nor its result — the second attempt re-reads the id's current representation
      // instead of returning a spurious undefined for a valid id.
      for (let attempt = 0; attempt < 2 && !contentItem && range; attempt++) {
        // Pinned across the inflation AND the probe that turns it into an item, so the size-eviction pass
        // this very inflation can trigger cannot unlink the file this read is about to open. Released as
        // soon as it is known that no item came out of the attempt; otherwise the pin's own expiry
        // releases it once the consumer has had its chance to open the returned stream.
        const releasePin = cache.pin(baseFilePath, CACHE_PIN_GRACE_MS)
        try {
          // Deduplicated across concurrent callers of the same path, and handed the invalidation token
          // that says whether the gzip this inflation started from is still the current version.
          await cache.deduplicateInflation(baseFilePath, (token) =>
            materializeRangeCacheFromGzip(id, baseFilePath, token)
          )

          // Serve range from the cached uncompressed file (undefined when the gzip didn't exist or
          // the decompression was discarded; the loop then retries once)
          contentItem = await retrieveWithEncoding(id, null, range, true, false, baseFilePath)
        } catch (err) {
          releasePin()
          throw err
        }
        if (!contentItem) {
          releasePin()
        } else {
          // Released the moment the consumer actually opens the stream, which is the event the pin exists
          // to wait for. Leaving it to the timer instead made the pin defeat the size budget it was added
          // alongside: a burst of range reads would hold every one of its entries un-evictable for the
          // whole grace period, measured at 37x over budget with the eviction pass unable to touch
          // anything. The timer stays as the backstop for an item that is never read at all.
          //
          // Wrapped over `asRawStream` with the inner item's own encoding (always `null` here — this is
          // the decompressed cache file, read through a byte range), so `asStream`'s decoding behaviour
          // and the item's advertised metadata are unchanged.
          const inner = contentItem
          contentItem = new SimpleContentItem(
            async () => {
              try {
                return await inner.asRawStream()
              } finally {
                releasePin()
              }
            },
            inner.size,
            inner.encoding,
            inner.contentSize
          )
        }
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
    const filePath = await resolveFilePath(id)
    // Probed with `statForRead`, the same rule the rest of the read path uses: only a file PROVABLY
    // gone is absent. `existPath` tests F_OK|R_OK, so a present-but-unreadable file (mode/ACL damage,
    // EIO) read as `false` — the "a broken store looks like an empty one" answer this storage's read
    // contract exists to remove, and one `fileInfo()` already refuses to give for the very same id.
    // The gzip probe DEFERS its absence classification to the raw probe below: both representations
    // share a directory, so a raw hit proves it intact for free and a full miss still classifies once.
    if ((await statForRead(gzipPathOf(filePath), true)) !== undefined) return true
    return (await statForRead(filePath)) !== undefined
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
  /**
   * Whether an id's compressed representation is STILL on disk, used to confirm a skip rather than
   * assume one. Absent only when provably so: any other fault means it may well be there, and the
   * caller's safe response to that is to treat it as present (see `allFileIdsRec`).
   */
  async function gzipCounterpartStillExists(gzipPath: string): Promise<boolean> {
    try {
      await components.fs.stat(gzipPath)
      return true
    } catch (err) {
      const code = (err as { code?: string } | null)?.code
      return code !== 'ENOENT' && code !== 'ENOTDIR'
    }
  }

  /**
   * Re-adopts decompressed range-cache files that a previous run left behind, so they count against the
   * size budget again and eviction can reclaim them.
   *
   * The tracker is in MEMORY only. A clean `stop()` evicts everything it knows about, but an unclean exit
   * (SIGKILL, OOM, a crash) leaves one decompressed copy per id that was range-read that boot, and the
   * next boot knows nothing about them: `reconcile()` and the temp-file sweep only inspect the reserved
   * directory, and `allFileIds()` deliberately hides a raw file that has a `.gzip` sibling. So they were
   * invisible to TTL and LRU eviction AND to `evictAll()`, stopped counting against
   * `decompressCacheMaxSize`, and were reclaimed only if that exact id happened to be stored or deleted
   * again — disk growing across restarts, up to the whole gzip-only working set, with the budget silently
   * unenforced.
   *
   * A raw file with a `.gzip` sibling IS such a copy: a committed version of an id is raw-only or
   * gzip-only, never both, so the pair can only mean "gzip is primary, raw is its derived cache".
   *
   * Two STREAMING passes per directory, the same shape `allFileIdsRec` uses for a large one: only gzip
   * names are held, never the whole listing. Best-effort and detached from startup — a failure here costs
   * the budget its accuracy, never correctness.
   */
  async function adoptOrphanedCacheFiles(folder: string): Promise<number> {
    let adopted = 0
    const gzipNames = new Set<string>()
    const subdirectories: string[] = []
    try {
      for await (const entry of await components.fs.opendir(folder, { bufferSize: 4000 })) {
        if (entry.isDirectory()) {
          const subdirectory = folder + path.sep + entry.name
          if (!isInsideReservedTempDir(subdirectory)) subdirectories.push(subdirectory)
        } else if (entry.name.endsWith(GZIP_EXTENSION)) {
          gzipNames.add(entry.name)
        }
      }
      // Nothing compressed here means nothing derived here; skip the second pass entirely, which is the
      // steady state for a root of raw-primary content.
      if (gzipNames.size > 0) {
        for await (const entry of await components.fs.opendir(folder, { bufferSize: 4000 })) {
          if (entry.isDirectory() || entry.name.endsWith(GZIP_EXTENSION)) continue
          if (!gzipNames.has(entry.name + GZIP_EXTENSION)) continue
          const filePath = folder + path.sep + entry.name
          if (cache.isTracked(filePath)) continue
          try {
            const stat = await components.fs.stat(filePath)
            if (!stat.isFile()) continue
            // Under the path lock, like every other admission: a store or delete for this id may be
            // committing right now, and re-checking inside settles the race in its favour.
            await withPathLock(filePath, async () => {
              if (cache.isTracked(filePath)) return
              if (!(await existsForInvariant(filePath))) return
              cache.record(filePath, stat.size)
              adopted++
            })
          } catch {
            // A file that vanished or cannot be statted is not adoptable; the next boot will try again.
          }
        }
      }
    } catch {
      // An unreadable directory costs only the adoption of whatever is inside it.
    }
    for (const subdirectory of subdirectories) {
      adopted += await adoptOrphanedCacheFiles(subdirectory)
    }
    return adopted
  }

  const allFileIdsRec = async function* (folder: string, idBase: string, prefix?: string): AsyncIterable<string> {
    // `idBase` is always an ancestor of `folder`, and an entry name never contains a separator or a
    // `.`/`..` segment, so an id is a plain slice of a path built by concatenation — no `path.resolve`
    // (354 ns) or `path.relative` (781 ns) per entry, together the largest single cost of a walk
    // (measured 6.5 -> 4.55 µs/id over a 200k-id root). Concatenation also keeps `folder` in the same
    // absolute-or-relative form as `root`, which `path.resolve` would not.
    const idOf = (entryPath: string): string => entryPath.slice(idBase.length + 1)

    // Which raw files have a compressed sibling: an id's two representations always live in the SAME
    // directory, so the listing already contains the answer and it does not need one `access(2)` per
    // raw file to ask it.
    const gzipNames = new Set<string>()
    // The directory itself, retained ONLY while it is small enough to be worth retaining — see
    // MAX_BUFFERED_DIRECTORY_ENTRIES. Dropped the moment it is not.
    let buffered: { name: string; isDirectory: boolean }[] | undefined = []

    for await (const entry of await components.fs.opendir(folder, { bufferSize: 4000 })) {
      const isDirectory = entry.isDirectory()
      if (!isDirectory && entry.name.endsWith(GZIP_EXTENSION)) gzipNames.add(entry.name)
      if (buffered) {
        if (buffered.length >= MAX_BUFFERED_DIRECTORY_ENTRIES) {
          // Too big to hold: release it and finish this read collecting only the gzip names, which is
          // exactly the streaming first pass the large-directory path below needs. Nothing is re-read.
          buffered = undefined
        } else {
          buffered.push({ name: entry.name, isDirectory })
        }
      }
    }

    /**
     * The id a file entry stands for, or `undefined` when the prefix filter excludes it.
     *
     * Deliberately does NOT apply the raw/gzip dedup: the two paths below decide that differently — one
     * from its single snapshot, the other with a confirming `stat` — and folding it in here silently
     * re-skipped the raw the confirmation had just decided to keep.
     */
    const idForEntry = (name: string): string | undefined => {
      const entryPath = folder + path.sep + name
      const isGzip = name.endsWith(GZIP_EXTENSION)
      const id = idOf(isGzip ? entryPath.slice(0, -GZIP_EXTENSION.length) : entryPath)
      return prefix && !id.startsWith(prefix) ? undefined : id
    }

    const subdirectories: string[] = []

    if (buffered) {
      // SMALL DIRECTORY — one `opendir`, and the strongest correctness this function can offer.
      //
      // Every decision comes from a SINGLE read, so a skip is justified by an entry from that same read
      // and therefore by one this loop provably also visits: an id can be yielded neither zero times
      // (skipping a raw means its gzip is in `buffered`) nor twice (a raw with a gzip sibling is always
      // skipped). No confirming `stat`, and no residual duplicate window — both of which the
      // two-pass path below has to live with.
      //
      // This is the DEFAULT shape. With hash prefixes a shard holds total/65,536 entries, so a root of
      // 268 million ids still sits under the cap and every directory takes this path.
      for (const entry of buffered) {
        if (entry.isDirectory) {
          if (isInsideReservedTempDir(folder + path.sep + entry.name)) continue
          subdirectories.push(entry.name)
          continue
        }
        // A raw whose `.gzip` is in this same snapshot is that gzip's decompressed cache, not a second
        // id — and the gzip entry, also in this snapshot, is the one that yields it.
        if (!entry.name.endsWith(GZIP_EXTENSION) && gzipNames.has(entry.name + GZIP_EXTENSION)) continue
        const id = idForEntry(entry.name)
        if (id !== undefined) yield id
      }
      buffered = undefined
    } else {
      // LARGE DIRECTORY — a second, STREAMING pass, which is what a flat-mode root with hundreds of
      // thousands of ids in one directory needs. Buffering the whole listing there retained ~300 bytes
      // per entry: measured at 47MB before the first id came out for 200k ids, ~290MB for a million.
      // Entries are yielded as they arrive, so nothing accumulates and the first id does not wait for
      // the listing to drain.
      for await (const entry of await components.fs.opendir(folder, { bufferSize: 4000 })) {
        if (entry.isDirectory()) {
          if (isInsideReservedTempDir(folder + path.sep + entry.name)) continue
          subdirectories.push(entry.name)
          continue
        }
        // CONFIRMED, not assumed — the price of deciding from a listing read BEFORE this one.
        // `gzipNames` describes the directory as the first pass saw it, and a representation transition
        // landing between the two made that answer stale in the direction that HIDES content: the first
        // pass recorded `<id>.gzip`, the commit replaced it with `<id>`, and this pass then skipped the
        // raw for a gzip that no longer existed — so an id holding a complete representation for the
        // whole enumeration was yielded by NEITHER pass (measured: `allFileIds()` yielded nothing for it
        // while `exist()` answered true). One `stat` closes it, and only for a raw that HAS a gzip
        // sibling — the decompression-cache case, not the steady state.
        //
        // The residual window is the opposite, benign one: a transition can still let this pass see both
        // representations and yield the id twice. Enumerating an id twice costs an idempotent repeat;
        // failing to enumerate one that is present under-reports the node's content. A fault on the
        // confirming `stat` is treated as "still there" for the same reason — the gzip is then in this
        // listing and gets yielded by its own entry.
        if (
          !entry.name.endsWith(GZIP_EXTENSION) &&
          gzipNames.has(entry.name + GZIP_EXTENSION) &&
          (await gzipCounterpartStillExists(folder + path.sep + entry.name + GZIP_EXTENSION))
        ) {
          continue
        }
        const id = idForEntry(entry.name)
        if (id !== undefined) yield id
      }
    }

    // Descended into after this directory's own entries are done, so its buffer is released first.
    for (const name of subdirectories) {
      const entryPath = folder + path.sep + name
      // With hash prefixes the SHARD is the id namespace root, so ids nested inside it are relative
      // to the shard rather than to the storage root.
      yield* allFileIdsRec(entryPath, USE_HASH_PREFIX && folder === root ? entryPath : idBase, prefix)
    }
  }

  /**
   * The gzip trailer's declared original size: a `number` when the format supplies one, `null` when it
   * cannot, or `undefined` when the file disappeared while being read (the caller then falls through
   * to the id's other representation). A storage failure rejects rather than answering any of these.
   *
   * TWO CASES WHERE THE NUMBER IS DECLARED BUT WRONG, both inherent to reading ISIZE rather than
   * inflating, and both therefore reported by `FileInfo.contentSize` as the display-only hint
   * `src/types.ts` documents it to be:
   *
   * - **Originals past 4 GiB.** ISIZE is the original size mod 2^32, so a 5 GiB original declares 1 GiB.
   *   Nothing in the trailer distinguishes that from a genuine 1 GiB original, so it cannot be detected
   *   here — an earlier version of this comment claimed `null` was returned for it, which was never true.
   * - **Multi-member gzips.** A concatenation of members (`cat a.gz b.gz`, which any migration or
   *   operator can produce, and which `asStream()` decodes IN FULL because zlib does) has one trailer per
   *   member, and this reads the LAST one — so a 50 KB object built from two members can declare 4 bytes.
   *
   * Detecting either requires inflating the whole member chain, which is exactly the O(n) CPU this O(1)
   * trailer read exists to avoid on a metadata call. Content this storage WROTE is always a single member
   * under 4 GiB, so both cases are limited to gzips placed under a shard by something else.
   */
  async function readGzipOriginalSize(filePath: string, gzipSize: number): Promise<number | null | undefined> {
    // The gzip format (RFC 1952) stores the original uncompressed size in its
    // trailer — the last 4 bytes (ISIZE field, uint32 little-endian).
    // Accurate only for a single-member original under 4GB; see the caveats above.
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

      // Re-stat and require the size to be UNCHANGED before trusting those bytes. The read opened
      // the path after the stat that produced `gzipSize`, so a concurrent overwrite in between makes
      // the offsets address the wrong file, in both directions:
      // - SHRANK: fewer than 4 bytes come back. Reading on would throw ERR_BUFFER_OUT_OF_BOUNDS, and
      //   the caller's probe cannot recognise the race either, because the path still EXISTS (it was
      //   replaced, not removed).
      // - GREW: exactly 4 bytes come back, but from the MIDDLE of the new file's compressed body —
      //   an arbitrary uint32 returned as if it were the trailer. This is the one case that produced
      //   a number this storage had guessed, which the contract promises never to do.
      //
      // Re-read against a FRESH stat rather than reporting a non-answer. Both non-answers are
      // actively harmful: `undefined` means "this representation is gone, try the other one", and
      // when the new version is gzip-primary there is no other one, so a present id was reported
      // ABSENT (~1.3% of reads under concurrent compressing writes); `null` means "size unknown", and
      // the consumer this field exists for bounds range requests with `contentSize ?? size`, so it
      // would silently substitute the COMPRESSED size and serve a truncated range.
      // Verification only: a blip on this extra syscall must not reject a read whose bytes are
      // already in hand, so a probe failure falls back to trusting them. Its job is to catch a
      // CONCURRENT OVERWRITE, and an overwrite does not make the file unstattable.
      let current: { size: number } | undefined
      try {
        current = await statForRead(filePath)
      } catch {
        return buffer.length === 4 ? buffer.readUInt32LE(0) : null
      }
      if (current === undefined) return undefined
      if (current.size === gzipSize) {
        // Nothing moved: these bytes really are this file's trailer.
        if (buffer.length === 4) return buffer.readUInt32LE(0)
        // Unreachable for an unchanged file — the range was computed from this very size — but a
        // custom adapter could still short-read, and inventing a number here is exactly what must
        // not happen.
        return null
      }
      if (current.size < 8) return null
      const retried = await streamToBuffer(
        components.fs.createReadStream(filePath, { start: current.size - 4, end: current.size - 1 })
      )
      // Only trust the retry if the file did not move AGAIN underneath it; otherwise these four
      // bytes come from the middle of yet another version's compressed body. A file being rewritten
      // repeatedly genuinely has no readable size, and `null` says exactly that rather than
      // inventing a number.
      const after = await statForRead(filePath).catch(() => undefined)
      if (retried.length !== 4 || after === undefined || after.size !== current.size) return null
      return retried.readUInt32LE(0)
    } catch (err) {
      // `null` is a legitimate answer — content whose size the format cannot express genuinely has no
      // declared size — so it must not double as "we could not read it". Callers cannot tell those
      // apart, and at least one uses `contentSize ?? size` to bound range requests, where a masked
      // failure silently substitutes the COMPRESSED size.
      //
      // Same rule as every other read, via the same probe: a file provably gone with its parent intact
      // is the documented mid-read race, and everything else — EIO, EACCES, a damaged shard (which
      // `statForRead` rejects on) — is a fault that surfaces.
      //
      // The probe is GUARDED, unlike its two siblings above which already were: `statForRead` throws on
      // a damaged shard, and letting that escape from here replaced `err` — the actual reason the
      // trailer could not be read, and the only one an operator can act on — with a secondary fault
      // discovered while classifying it.
      let provablyGone: boolean
      try {
        provablyGone = (await statForRead(filePath)) === undefined
      } catch {
        provablyGone = false
      }
      if (provablyGone) return undefined
      throw err
    }
  }

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    await assertNotQuarantined(id)
    const possibleEncondings = ['gzip', null]
    const baseFilePath = await resolveFilePath(id)

    // Both representations share a parent directory, so the absence classification happens at most
    // once — on the LAST representation probed, and only if every one of them was absent.
    for (const encoding of possibleEncondings) {
      const extension = encoding ? '.' + encoding : ''
      const filePath = baseFilePath + extension
      const stat = await statForRead(filePath, encoding !== null)
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
    // See `doStoreStream` for both checks and for why the id is validated before the source.
    assertStorableContentId(id)
    const filePath = await getFilePath(id)
    assertStorableStream(stream)
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
      await pipeToStaged(stream, stagedRawPath)
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
        let committed = false
        let rawPathReleased = false
        const onCommitted = () => (committed = true)
        try {
          // Intent-journaled: a crash between the commit rename and the counterpart cleanup is
          // reconciled at next construction, never leaving mixed versions for reads to prefer.
          if (compressed) {
            await journal.commitRepresentation(
              'gzip',
              id,
              stagedGzipPath,
              gzipPathOf(filePath),
              filePath,
              rename,
              signal,
              onCommitted,
              // For a GZIP commit the raw path is the COUNTERPART, removed by an unlink after the
              // rename rather than overwritten by it — so only its proven removal means the cache entry
              // describing it no longer describes a file.
              () => (rawPathReleased = true)
            )
          } else {
            await journal.commitRepresentation(
              'raw',
              id,
              stagedRawPath,
              filePath,
              gzipPathOf(filePath),
              rename,
              signal,
              onCommitted,
              // A RAW commit renames ONTO the raw path, so the rename itself is what invalidates the
              // entry; the counterpart here is the gzip, which the cache does not track.
              () => (rawPathReleased = true)
            )
          }
        } finally {
          // `forget` drops tracking WITHOUT unlinking, so it is only correct once the file it tracked is
          // gone. For a raw commit the rename replaced it; for a gzip commit the unlink had to remove it,
          // and gating both on the rename alone orphaned a decompressed cache file whose unlink FAILED —
          // untracked, unevictable, uncounted against the budget, and reclaimed only by a restart.
          if (committed && (rawPathReleased || !compressed)) {
            cache.forget(filePath)
          }
          // Independent of the above: once the rename lands, any inflation still in flight is producing
          // bytes for a version that is no longer current, whether or not the counterpart went away.
          if (committed) {
            cache.invalidateInflight(filePath)
          }
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
        // Chained onto the same tracked promise (rather than started in parallel) so `stop()` still awaits
        // exactly one thing and two starts cannot run two walks at once. Detached for the same reason as
        // the sweep above: it walks the content tree, which on a large root is real I/O that startup must
        // not wait for.
        .then(() => adoptOrphanedCacheFiles(root))
        .then((adopted) => {
          if (adopted > 0) {
            logger.info(
              `Re-adopted ${adopted} decompressed cache file(s) left by a previous run; they now count ` +
                `against the cache budget again`
            )
          }
        })
        .catch((error) => logger.warn(`Orphaned decompressed-cache adoption failed: ${error}`))
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
      // Bounded-concurrent like every other batch surface. Each id costs ~8 round-trips (repair
      // probe, cache removal or unlink, two verifications, the journal check) and they were all
      // serialized: a GC pass over 100k ids took minutes of pure latency on network-backed storage
      // for work that shares no state — different ids take different path locks.
      //
      // Failure semantics are preserved where they matter: the helper starts no new ids after the
      // first failure and reports that first error, so a broken storage still stops the batch. What
      // changes is only that ids already in flight run to completion rather than being abandoned —
      // `delete` is idempotent, so retrying the whole list remains the recovery.
      //
      // `forEach`, not `map`: nothing reads a result here, and the results array `map` allocates is
      // 8MB of `undefined` for a million-id sweep.
      await forEachWithConcurrency(ids, BATCH_CONCURRENCY, async (id) => {
        const filePath = await resolveFilePath(id)
        // Locked so an in-flight decompression can never resurrect the id by renaming its staged
        // bytes onto the canonical path after these unlinks.
        await withPathLock(filePath, async () => {
          // Invalidated FIRST, not last. This delete holds the path lock for its whole body, so an
          // inflation that started before it can only commit once this releases — and it re-checks its
          // token at that point. Invalidating at the END meant every step below (the repair, the two
          // verifications, the journal check) could throw first and leave the token VALID, so a failed
          // delete was followed by the in-flight inflation renaming its output onto the canonical path:
          // the id came back readable, as a cache-tracked file a later TTL pass would silently unlink,
          // so it flickered out of existence. Invalidating early has no cost — a delete that ends up
          // failing has nothing to gain from a decompression of what it was trying to remove.
          cache.invalidateInflight(filePath)
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
          await noFailUnlink(gzipPathOf(filePath))
          if (await existsForInvariant(gzipPathOf(filePath))) {
            throw new Error(`Failed to delete ${id}: its gzip representation could not be removed`)
          }
          // Defensive: repairPendingIntent already discharged any journal; verify none remains.
          await journal.assertNoIntent(id, `Deleted ${id} but could not remove its intent journal`)
        })
      })
    },
    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      return new Map(
        await mapWithConcurrency(cids, BATCH_CONCURRENCY, async (cid): Promise<[string, boolean]> => [
          cid,
          await exist(cid)
        ])
      )
    },
    allFileIds: (prefix?: string) => allFileIdsRec(root, root, prefix),
    fileInfo,
    async fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
      return new Map(
        await mapWithConcurrency(cids, BATCH_CONCURRENCY, async (cid): Promise<[string, FileInfo | undefined]> => [
          cid,
          await fileInfo(cid)
        ])
      )
    }
  }
}
