import { createHash, randomBytes } from 'crypto'
import path from 'path'
import { pipeline, Readable, Transform } from 'stream'
import { promisify } from 'util'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { compressContentFile } from './extras/compression'

const pipe = promisify(pipeline)

// Reserved directory (under the storage root) where an atomic `storeStream` stages its temp file
// before renaming it into place. Kept out of the content namespace — a shard is a 4-hex directory and
// content lives in files, never here — so a temp can never collide with, hide, or be mistaken for an
// addressable id. Skipped by `allFileIds` and cleaned at startup. Its name is therefore reserved.
const TEMP_DIR_NAME = '.tmp-writes'

// Matches exactly the names newTempPath generates (`<16-hex bootId>-<32-hex random>`). The startup
// sweep deletes ONLY files of this shape: anything else under the reserved dir is not ours to
// remove — in flat (disablePrefixHash) mode a deployment that predates the reservation may hold
// legitimate content there, and deleting unrecognized files would turn an upgrade into data loss.
const STAGED_FILE_NAME = /^[0-9a-f]{16}-[0-9a-f]{32}$/

// Matches the intent-journal files a representation-transition commit writes (`<40-hex
// sha1(id)>.intent`). An intent records which representation (raw|gzip) is the NEW primary for an
// id, so a crash between the commit rename and the counterpart cleanup is reconciled at the next
// construction instead of leaving mixed versions that reads could prefer. The path is a
// deterministic function of the id: at most one intent can ever exist per id (commits are
// serialized per path, and construction reconciles before any write), so reconciliation needs no
// ordering heuristics.
const INTENT_FILE_NAME = /^[0-9a-f]{40}\.intent$/

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
        callback(new Error(`Decompressed size exceeds the maximum allowed of ${maxBytes} bytes`))
        return
      }
      callback(null, chunk)
    }
  })
}

/**
 * Filesystem-backed content storage.
 *
 * Operational contract:
 * - **Exclusive root ownership** — a storage root must be owned by exactly one live storage
 *   instance. In-memory state (path locks, decompress-cache tracking, staged-write ownership) is
 *   per-instance; two instances over one root can delete each other's staged files and race their
 *   caches. Shared roots are not supported.
 * - **Crash-atomic writes require `fs.rename`** — when the filesystem component provides `rename`,
 *   every write stages into a reserved directory and renames into place, so an interrupted write
 *   can never leave a partial file at a canonical path. Without `rename` (legacy custom adapters)
 *   writes fall back to non-atomic direct writes; a warning is logged at construction.
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

  await components.fs.mkdir(root, { recursive: true })

  const USE_HASH_PREFIX = !(options?.disablePrefixHash ?? false)

  // Atomic-write support requires `rename` on the filesystem component. Without it (legacy custom
  // adapters) every write falls back to the in-place direct write and NONE of the staging machinery
  // applies — so the reserved temp namespace is neither created nor enforced: a legacy no-rename
  // deployment that stored ids under the default reserved name keeps working unchanged, it just
  // gets none of the crash-atomicity or reconciliation guarantees.
  const ATOMIC_MODE = !!components.fs.rename
  if (!ATOMIC_MODE) {
    logger.warn(
      'The filesystem component does not provide rename: writes will NOT be crash-atomic, and the reserved ' +
        'staging directory, orphan sweep and crash reconciliation are disabled (legacy direct-write mode).'
    )
  }

  // Created up front so storeStream can stage into it without a per-write mkdir.
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
    // A legacy flat-mode content id could live exactly AT the reserved path as a file; mkdir would
    // then fail with a low-level filesystem error. Detect it first and give the same actionable
    // guidance as the other reservation conflicts.
    if (await components.fs.existPath(tempDir)) {
      const tempDirStat = await components.fs.stat(tempDir)
      if (!tempDirStat.isDirectory()) {
        throw new Error(
          `Refusing to start: the reserved temp path '${tempDirName}' under the storage root exists as a file — ` +
            `likely a pre-existing content id. Migrate it out or configure a different tempDirectoryName.`
        )
      }
    }
    await components.fs.mkdir(tempDir, { recursive: true })
  }

  // Staged files are prefixed with a per-boot random id so the startup sweep can tell leftovers
  // from previous runs (any other prefix) apart from files this instance is writing right now —
  // a write racing the sweep can therefore never have its live staged file unlinked.
  const bootId = randomBytes(8).toString('hex')
  const newTempPath = (): string => path.join(tempDir, `${bootId}-${randomBytes(16).toString('hex')}`)

  // Journal for representation-transition commits: written before the commit rename whenever a
  // counterpart representation exists, removed once the counterpart is confirmed gone. A crash in
  // between leaves the intent behind, and the next construction resolves the mixed state in favor
  // of the representation the intent names (see reconcileIntents). The path is a deterministic
  // function of the id — at most one intent per id can ever exist, because commits are serialized
  // per path and construction reconciles before any write — so reconciliation needs no ordering
  // heuristics. Fresh-id writes — the overwhelmingly common case in content-addressed use — have no
  // counterpart and never pay for an intent.
  const intentPathFor = (id: string): string =>
    path.join(tempDir, `${createHash('sha1').update(id).digest('hex')}.intent`)

  async function writeIntent(op: 'raw' | 'gzip', id: string, stagedPath: string): Promise<string> {
    const intentPath = intentPathFor(id)
    // The staged BASENAME lets reconciliation prove whether the commit rename landed: renames
    // consume the staged file, so "staged still present" means the rename provably never happened.
    // Stored as a basename (not an absolute path) so a root remount cannot poison the journal.
    const body = JSON.stringify({ op, id, staged: path.basename(stagedPath) })
    await pipe(Readable.from([Buffer.from(body)]), components.fs.createWriteStream(intentPath))
    return intentPath
  }

  // Applies a pending intent, distinguishing the two crash windows by the staged file the intent
  // names (reconciliation runs before the sweep, so a crashed boot's staged file is still there):
  // - staged file present  → the rename never happened: discard the staged file and the intent; the
  //   previous representation(s) stay untouched. A pre-rename intent can therefore NEVER delete the
  //   previous primary (e.g. a valid gzip alongside its own decompressed raw cache).
  // - staged gone, primary present → the rename landed: remove the stale counterpart (throwing when
  //   it cannot be removed — live reads do not consult intents and would keep serving it), then
  //   discharge the intent.
  // - neither → nothing can be proven: refuse, manual repair required.
  // Used by construction-time reconciliation and by commits that find a pending intent for their id
  // (a failed cleanup earlier in this process), so an unapplied repair instruction is never
  // overwritten.
  async function applyPendingIntent(intentPath: string): Promise<void> {
    const body = await components.fs.readFile(intentPath, 'utf8')
    let op: string, id: string, staged: string
    try {
      ;({ op, id, staged } = JSON.parse(body))
    } catch {
      op = id = staged = ''
    }
    if ((op !== 'raw' && op !== 'gzip') || !id || !STAGED_FILE_NAME.test(staged ?? '')) {
      // A partial/malformed intent means its commit never started (intents are written before
      // renames): discard it; an orphaned staged file, if any, is handled by the sweep.
      await noFailUnlink(intentPath)
      return
    }
    const stagedPath = path.join(tempDir, staged)
    if (await components.fs.existPath(stagedPath)) {
      // Prepared but never renamed: the commit did not happen.
      await noFailUnlink(stagedPath)
      await noFailUnlink(intentPath)
      logger.info(`Discarded a prepared but uncommitted ${op} transition`, { id })
      return
    }
    const filePath = await getFilePath(id)
    const gzipPath = filePath + '.gzip'
    const primaryPath = op === 'raw' ? filePath : gzipPath
    const counterpartPath = op === 'raw' ? gzipPath : filePath
    if (!(await components.fs.existPath(primaryPath))) {
      throw new Error(
        `Cannot reconcile the interrupted ${op} commit for ${id}: neither its staged file nor its committed ` +
          `representation exists.`
      )
    }
    if (await components.fs.existPath(counterpartPath)) {
      await noFailUnlink(counterpartPath)
      if (await components.fs.existPath(counterpartPath)) {
        throw new Error(
          `Cannot repair the interrupted ${op} commit for ${id}: its stale ${
            op === 'raw' ? 'gzip' : 'raw'
          } representation could not be removed.`
        )
      }
      logger.info(`Reconciled an interrupted ${op} commit`, { id })
    }
    await noFailUnlink(intentPath)
  }

  // Construction invariant: the storage never runs in a state where the reserved staging namespace
  // could hide addressable content. With hash prefixes, ids can never resolve into the reserved dir
  // (containment sends them under a shard), so everything inside is ours by construction. In flat
  // (disablePrefixHash) mode the root IS the content namespace, so ownership must be proven, and
  // the marker's mere EXISTENCE is not proof: before the reservation, the marker path itself was a
  // valid content id, so a legacy deployment could hold an arbitrary file there. Ownership requires
  // ALL of: the marker exists, its bytes are exactly what this storage writes, and every other
  // entry matches the staged-name shape this storage generates. Anything else — an unmarked
  // non-empty directory, a marker with foreign content, an unrecognized sibling, or a failure to
  // establish ownership — REFUSES TO START instead of warning-and-hiding: pre-existing ids under
  // the reserved name would otherwise be silently unreachable (or, shape-matching ones, sweepable)
  // after an upgrade. A byte-exact forgery of the marker alongside exclusively staged-shaped
  // siblings is indistinguishable from genuine ownership by construction and is treated as opt-in.
  // This also means the orphan sweep never needs a runtime ownership check.
  const OWNERSHIP_MARKER = '.owned-by-catalyst-storage'
  const OWNERSHIP_MARKER_CONTENT = 'reserved by catalyst-storage for atomic write staging\n'
  if (ATOMIC_MODE && !USE_HASH_PREFIX) {
    const markerPath = path.join(tempDir, OWNERSHIP_MARKER)
    const refuseToStart = (reason: string): never => {
      throw new Error(
        `Refusing to start: ${reason} In disablePrefixHash mode the reserved temp directory '${tempDirName}' may hold ` +
          `pre-existing content ids that the reservation would hide from retrieval and enumeration. ` +
          `Migrate those files out of '${tempDirName}', configure a different tempDirectoryName, or restore the ` +
          `'${OWNERSHIP_MARKER}' marker (with its original content) if they are staging leftovers from a previous run.`
      )
    }
    if (await components.fs.existPath(markerPath)) {
      const markerBody = await components.fs.readFile(markerPath, 'utf8')
      if (markerBody !== OWNERSHIP_MARKER_CONTENT) {
        refuseToStart(
          `the ownership marker '${OWNERSHIP_MARKER}' exists but its content is not the one this storage writes, ` +
            `so it may be a pre-existing content id rather than a marker.`
        )
      }
      const foreign = (await components.fs.readdir(tempDir)).filter(
        (entry) => entry !== OWNERSHIP_MARKER && !STAGED_FILE_NAME.test(entry) && !INTENT_FILE_NAME.test(entry)
      )
      if (foreign.length > 0) {
        refuseToStart(
          `the reserved temp directory '${tempDirName}' contains ${foreign.length} file(s) that this storage ` +
            `did not create.`
        )
      }
    } else {
      const entries = await components.fs.readdir(tempDir)
      if (entries.length > 0) {
        refuseToStart(
          `the reserved temp directory '${tempDirName}' already contains ${entries.length} file(s) that this ` +
            `storage cannot prove it owns.`
        )
      }
      await pipe(Readable.from([Buffer.from(OWNERSHIP_MARKER_CONTENT)]), components.fs.createWriteStream(markerPath))
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
  const CACHE_TTL = options?.decompressCacheTTL ?? ONE_HOUR_IN_MS
  const CACHE_MAX_SIZE = options?.decompressCacheMaxSize ?? FIVE_GB_IN_BYTES
  const CACHE_EVICTION_INTERVAL = options?.decompressCacheEvictionInterval ?? FIVE_MINUTES_IN_MS
  const MAX_DECOMPRESSED_SIZE = options?.decompressMaxFileSize ?? TWO_HUNDRED_FIFTY_SIX_MB_IN_BYTES

  // LRU cache tracker for decompressed gzip files written to disk
  const decompressCache = new Map<string, { size: number; lastAccess: number }>()
  let totalCacheSize = 0

  // Concurrency guard: prevents multiple simultaneous decompressions of the same file
  const inflightDecompressions = new Map<string, Promise<void>>()

  // Serializes commits (rename/write/unlink) on a canonical path so a store, a delete and a
  // decompression can never interleave their final steps. Only the short commit sections take the
  // lock — long-running pipes stay outside — and the map entry is removed once its chain drains.
  const pathLocks = new Map<string, Promise<unknown>>()
  function withPathLock<T>(filePath: string, fn: () => Promise<T>): Promise<T> {
    const prev = pathLocks.get(filePath) ?? Promise.resolve()
    const run = prev.then(fn, fn)
    const guard = run.then(
      () => undefined,
      () => undefined
    )
    pathLocks.set(filePath, guard)
    void guard.then(() => {
      if (pathLocks.get(filePath) === guard) pathLocks.delete(filePath)
    })
    return run
  }

  // A decompression inflates whatever gzip existed when it started; if the id is overwritten or
  // deleted before the decompression commits, its output is stale and must be discarded instead of
  // clobbering the newer canonical file (or resurrecting a deleted one). The owner registers a token
  // before opening the gzip; writers/deleters mark it inside their locked commit. Bounded by
  // in-flight decompressions.
  const inflightDecompressionTokens = new Map<string, { invalidated: boolean }>()
  function invalidateInflightDecompression(filePath: string): void {
    const token = inflightDecompressionTokens.get(filePath)
    if (token) token.invalidated = true
  }

  // Commits a staged file onto its canonical primary path and removes the other representation.
  // The logical object spans two paths (raw and .gzip) but only the rename is atomic, so when a
  // counterpart exists an intent is journaled FIRST: if the process dies (or the unlink fails)
  // between the rename and the cleanup, the next construction reconciles the mixed state in favor
  // of the representation committed here, instead of reads preferring the stale counterpart. Must
  // be called while holding the path lock.
  async function commitRepresentation(
    op: 'raw' | 'gzip',
    id: string,
    stagedPath: string,
    primaryPath: string,
    counterpartPath: string,
    rename: (from: string, to: string) => Promise<void>
  ): Promise<void> {
    // A pending intent means a previous commit for this id failed its cleanup in this process:
    // repair first (throws if impossible), so the intent written below always describes a
    // transition from a consistent state and never overwrites an unapplied repair instruction.
    if (await components.fs.existPath(intentPathFor(id))) {
      await applyPendingIntent(intentPathFor(id))
    }
    const hadCounterpart = await components.fs.existPath(counterpartPath)
    const intentPath = hadCounterpart ? await writeIntent(op, id, stagedPath) : undefined
    try {
      await rename(stagedPath, primaryPath)
    } catch (err) {
      // The commit did not happen, so the intent must not survive it: a later repair would treat
      // the failed commit as successful and remove the counterpart — e.g. delete a valid gzip
      // primary in favor of its own decompressed raw cache.
      if (intentPath) {
        await noFailUnlink(intentPath)
        if (await components.fs.existPath(intentPath)) {
          // Double filesystem failure: the stale intent could later be applied as if the commit
          // succeeded. Escalate with explicit manual guidance instead of the bare rename error.
          throw new Error(
            `Failed to commit ${id} AND failed to clear its intent journal — remove '${intentPath}' manually ` +
              `before restarting, or the failed commit may be applied as if it had succeeded. ` +
              `Original error: ${err instanceof Error ? err.message : String(err)}`
          )
        }
      }
      throw err
    }
    if (hadCounterpart) {
      await noFailUnlink(counterpartPath)
      if (await components.fs.existPath(counterpartPath)) {
        // The new version is committed, but the stale counterpart could not be removed and reads in
        // THIS process would keep preferring it. Fail the store loudly instead of resolving with a
        // lie: the intent survives, so the next construction finishes the cleanup, and a retried
        // store re-attempts it immediately.
        throw new Error(
          `Stored ${id} but failed to remove its previous ${op === 'raw' ? 'gzip' : 'raw'} representation; ` +
            `reads may serve the previous version until a retry or restart completes the cleanup.`
        )
      }
      if (intentPath) {
        await noFailUnlink(intentPath)
      }
    }
  }

  // Drops the cache-tracking entry WITHOUT unlinking the file. Used when the canonical path stops
  // being a derived cache and becomes primary content (a store landed there): a stale entry would
  // let TTL/size eviction delete the only copy of the new content.
  function forgetCacheEntry(filePath: string): void {
    const entry = decompressCache.get(filePath)
    if (entry) {
      totalCacheSize -= entry.size
      decompressCache.delete(filePath)
    }
  }

  let evicting = false
  async function evictCache() {
    if (evicting) return
    evicting = true
    try {
      await runEviction()
    } finally {
      evicting = false
    }
  }

  // Unlinks an evicted cache file under the path lock, re-checking the entry is still current: a
  // store may have promoted the path to primary content (forgetting the entry) between the eviction
  // scan and this delete — unlinking then would destroy the only copy of the new content.
  async function evictCacheEntry(filePath: string, entry: { size: number; lastAccess: number }): Promise<void> {
    await withPathLock(filePath, async () => {
      if (decompressCache.get(filePath) !== entry) return
      await noFailUnlink(filePath)
      totalCacheSize -= entry.size
      decompressCache.delete(filePath)
    })
  }

  async function runEviction() {
    const now = Date.now()

    // TTL eviction
    for (const [filePath, entry] of decompressCache) {
      if (now - entry.lastAccess > CACHE_TTL) {
        await evictCacheEntry(filePath, entry)
      }
    }

    // Size eviction (LRU)
    if (totalCacheSize > CACHE_MAX_SIZE) {
      const sorted = [...decompressCache.entries()].sort((a, b) => a[1].lastAccess - b[1].lastAccess)
      for (const [filePath, entry] of sorted) {
        if (totalCacheSize <= CACHE_MAX_SIZE) break
        await evictCacheEntry(filePath, entry)
      }
    }
  }

  let evictionTimer: ReturnType<typeof setInterval> | undefined
  // Tracks the detached startup temp-file sweep so `stop()` can await it (rather than leaving a
  // promise dangling past shutdown).
  let tempFileSweep: Promise<void> = Promise.resolve()

  async function getFilePath(id: string): Promise<string> {
    // We are sharding the files using the first 4 digits of its sha1 hash, because it generates collisions
    // for the file system to handle millions of files in the same directory.
    // This way, asuming that sha1 hash distribution is ~uniform we are reducing by 16^4 the max amount of files in a directory.
    const hash = createHash('sha1').update(id).digest('hex').substring(0, 4)

    const directoryPath = path.normalize(USE_HASH_PREFIX ? path.join(root, hash) : root)

    const finalPath = path.normalize(path.join(directoryPath, id))

    // recursively creates the directory structure if needed
    const dirname = path.dirname(finalPath)

    // Containment check. We compare against `directoryPath + path.sep` (not a bare `startsWith`)
    // so a sibling directory that merely shares the prefix — e.g. id "../<root>-evil/x" resolving
    // to "<root>-evil" — cannot pass: "/data/contents-evil".startsWith("/data/contents") is true,
    // but it is outside "/data/contents/".
    if (finalPath !== directoryPath && !finalPath.startsWith(directoryPath + path.sep)) {
      throw new Error('Cannot manipulate files outside of the root storage folder')
    }

    // The temp-write namespace is reserved: an id resolving into it (reachable when
    // disablePrefixHash makes the root itself the containment dir, e.g. '.tmp-writes/foo') would be
    // hidden from allFileIds and could be deleted by the startup sweep.
    if (ATOMIC_MODE && (finalPath === tempDir || finalPath.startsWith(tempDir + path.sep))) {
      throw new Error('Cannot manipulate files inside the reserved temp-write folder')
    }

    if (!(await components.fs.existPath(dirname))) {
      await components.fs.mkdir(dirname, { recursive: true })
    }

    return finalPath
  }

  const retrieveWithEncoding = async (
    id: string,
    encoding: string | null,
    range?: { start: number; end: number }
  ): Promise<ContentItem | undefined> => {
    const extension = encoding ? '.' + encoding : ''
    const filePath = (await getFilePath(id)) + extension

    if (await components.fs.existPath(filePath)) {
      const stat = await components.fs.stat(filePath)

      if (range) {
        const clampedEnd = clampRange(range, stat.size)
        return new SimpleContentItem(
          async () => components.fs.createReadStream(filePath, { start: range.start, end: clampedEnd }),
          clampedEnd - range.start + 1,
          encoding
        )
      }

      return new SimpleContentItem(async () => components.fs.createReadStream(filePath), stat.size, encoding)
    }

    return undefined
  }

  const noFailUnlink = async (path: string): Promise<boolean> => {
    try {
      await components.fs.unlink(path)
      return true
    } catch (error) {
      return false
    }
  }

  const storeStream = async (id: string, stream: Readable): Promise<void> => {
    const filePath = await getFilePath(id)
    const { rename } = components.fs
    // A custom fs adapter that predates the optional `rename` falls back to the original direct
    // write. It isn't crash-atomic, but keeps the public IFileSystemComponent backward-compatible;
    // the bundled createFsComponent provides rename and so takes the atomic path below.
    if (!rename) {
      await withPathLock(filePath, async () => {
        try {
          await pipe(stream, components.fs.createWriteStream(filePath))
          // The raw and its .gzip are one versioned object: a gzip left from a previous version
          // would be preferred by retrieve() and serve stale bytes over the content just stored.
          await noFailUnlink(filePath + '.gzip')
          forgetCacheEntry(filePath)
          invalidateInflightDecompression(filePath)
        } catch (err) {
          // Clean up the partial output while still holding the lock: doing it after release could
          // delete a queued writer's freshly committed content for the same id.
          await noFailUnlink(filePath)
          throw err
        }
      })
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
    const tempPath = newTempPath()
    try {
      await pipe(stream, components.fs.createWriteStream(tempPath))
      await withPathLock(filePath, async () => {
        try {
          // The raw and its .gzip are one versioned object: a gzip left from a previous version
          // would be preferred by retrieve() and serve stale bytes over the content just stored
          // (intent-journaled so even a crash mid-cleanup cannot leave the stale gzip preferred).
          await commitRepresentation('raw', id, tempPath, filePath, filePath + '.gzip', rename)
        } finally {
          // Run the bookkeeping even when the commit throws (a failed counterpart cleanup reports
          // failure AFTER the rename landed): drop any stale decompress-cache tracking so eviction
          // can never delete the new content, and tell an in-flight decompression it is outdated.
          forgetCacheEntry(filePath)
          invalidateInflightDecompression(filePath)
        }
      })
    } catch (err) {
      // On a write error the temp file may be partial; on a rename error it still exists. Either way
      // remove it so a failed store never leaves a stray file behind (the final path is untouched).
      await noFailUnlink(tempPath)
      throw err
    }
  }

  async function removeCacheEntry(filePath: string): Promise<boolean> {
    const entry = decompressCache.get(filePath)
    if (entry) {
      await noFailUnlink(filePath)
      totalCacheSize -= entry.size
      decompressCache.delete(filePath)
      return true
    }
    return false
  }

  function touchCacheEntry(filePath: string) {
    const entry = decompressCache.get(filePath)
    if (entry) {
      entry.lastAccess = Date.now()
    }
  }

  // Concurrent-read contract: reads are deliberately NOT serialized against writes (locking the hot
  // read path would be far too costly). Every read observes some COMPLETE committed version of the
  // id — commits are atomic renames and a version's raw/gzip transition happens under the path
  // lock — but a read that overlaps a commit may still serve the previous version (e.g. its gzip,
  // which retrieve prefers, in the instant before the committing section unlinks it). Reads started
  // after a store/delete promise resolves observe that operation's outcome. The returned
  // ContentItem opens its stream LAZILY: a store/delete landing between retrieve() and asStream()
  // can unlink the observed file, making asStream() fail (typically ENOENT) — callers should treat
  // that as a retryable miss, exactly like retrieve() having returned undefined.
  const retrieve = async (id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> => {
    if (range) validateRange(range)
    try {
      let contentItem: ContentItem | undefined = undefined
      if (!range) contentItem = await retrieveWithEncoding(id, 'gzip')
      if (!contentItem) {
        contentItem = await retrieveWithEncoding(id, null, range)
        if (contentItem && range) {
          // Update last access if this file is in the cache
          touchCacheEntry(await getFilePath(id))
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

        // Deduplicate concurrent decompressions of the same file. The promise is created and
        // registered synchronously — there is no `await` between the `get` and the `set` — so
        // simultaneous callers share a single decompression. Otherwise both would pass the
        // "not in flight" check, write the same cache file concurrently (corrupting it) and
        // double-count its size against totalCacheSize.
        let decompressPromise = inflightDecompressions.get(uncompressedPath)
        const isOwner = !decompressPromise
        if (!decompressPromise) {
          decompressPromise = (async () => {
            // Register the invalidation token BEFORE opening the gzip: any store/delete committing
            // after this point marks it, so stale output is discarded; one committing before it means
            // the gzip opened below is already the newest version.
            const token = { invalidated: false }
            inflightDecompressionTokens.set(uncompressedPath, token)
            try {
              const gzipItem = await retrieveWithEncoding(id, 'gzip')
              if (!gzipItem) {
                return
              }
              const { rename } = components.fs
              if (rename) {
                // Stage the inflation in the temp dir so a process killed mid-decompress can never
                // leave a partial file at the canonical uncompressed path — a later range request
                // would silently serve its truncated bytes as valid content.
                const writePath = newTempPath()
                try {
                  // Cap how much the gzip may inflate to so a decompression bomb cannot write an
                  // unbounded file to disk. The gzip trailer's declared size is attacker-
                  // controllable, so the limit is enforced on the actual inflated bytes.
                  await pipe(
                    await gzipItem.asStream(),
                    createSizeLimitTransform(MAX_DECOMPRESSED_SIZE),
                    components.fs.createWriteStream(writePath)
                  )
                  // Commit under the path lock so this rename can never interleave with a store or
                  // delete on the same canonical path; discard when the source gzip was replaced or
                  // the id deleted while inflating.
                  const committed = await withPathLock(uncompressedPath, async () => {
                    if (token.invalidated) return false
                    await rename(writePath, uncompressedPath)
                    const stat = await components.fs.stat(uncompressedPath)
                    decompressCache.set(uncompressedPath, { size: stat.size, lastAccess: Date.now() })
                    totalCacheSize += stat.size
                    return true
                  })
                  if (!committed) {
                    await noFailUnlink(writePath)
                  }
                } catch (err) {
                  // Remove the partial staged file; the canonical path was never touched.
                  await noFailUnlink(writePath)
                  // An invalidated token means the id was overwritten/deleted while inflating —
                  // the failure belongs to the replaced gzip, not to the caller's request.
                  // Resolving lets the retry loop observe the new representation instead of the
                  // error bubbling into a spurious undefined for a valid id.
                  if (token.invalidated) return
                  throw err
                }
                return
              }

              // In-place (no rename) legacy path: there is no staging, so the ENTIRE inflate/
              // register sequence runs under the path lock and honors the invalidation token — a
              // concurrent store/delete completing first must not be overwritten by a stale
              // decompression, and the cleanup of a failed inflation must not race a newer writer.
              await withPathLock(uncompressedPath, async () => {
                if (token.invalidated) return
                try {
                  await pipe(
                    await gzipItem.asStream(),
                    createSizeLimitTransform(MAX_DECOMPRESSED_SIZE),
                    components.fs.createWriteStream(uncompressedPath)
                  )
                } catch (err) {
                  // Under the lock the partial file is provably ours to remove.
                  await noFailUnlink(uncompressedPath)
                  // Defensive symmetry with the staged branch: writers take this same lock, so the
                  // token cannot flip mid-section today.
                  if (token.invalidated) return
                  throw err
                }
                const stat = await components.fs.stat(uncompressedPath)
                decompressCache.set(uncompressedPath, { size: stat.size, lastAccess: Date.now() })
                totalCacheSize += stat.size
              })
            } finally {
              if (inflightDecompressionTokens.get(uncompressedPath) === token) {
                inflightDecompressionTokens.delete(uncompressedPath)
              }
            }
          })()
          inflightDecompressions.set(uncompressedPath, decompressPromise)
        }

        try {
          await decompressPromise
        } finally {
          if (isOwner) inflightDecompressions.delete(uncompressedPath)
        }

        // Serve range from the cached uncompressed file (undefined when the gzip didn't exist or
        // the decompression was discarded; the loop then retries once)
        contentItem = await retrieveWithEncoding(id, null, range)
      }

      return contentItem
    } catch (error: any) {
      if (error instanceof RangeError) throw error
      logger.error(error)
    }
    return undefined
  }

  async function exist(id: string): Promise<boolean> {
    const filePath = await getFilePath(id)
    return (await components.fs.existPath(filePath + '.gzip')) || (await components.fs.existPath(filePath))
  }

  const allFileIdsRec = async function* (folder: string, prefix?: string): AsyncIterable<string> {
    const dirEntries = await components.fs.opendir(folder, { bufferSize: 4000 })
    for await (const entry of dirEntries) {
      if (entry.isDirectory()) {
        // The reserved temp-write dir only exists directly under the storage root; skip it there and
        // only there, so a deeper same-named directory (reachable via a slash-containing id) is not
        // silently hidden from enumeration.
        if (ATOMIC_MODE && folder === root && entry.name === tempDirName) continue
        yield* allFileIdsRec(path.resolve(folder, entry.name), prefix)
      } else if (!prefix || entry.name.startsWith(prefix)) {
        const baseName = entry.name.replace(/\.gzip$/, '')
        // Skip cached uncompressed files when the .gzip version also exists
        if (baseName !== entry.name || !(await components.fs.existPath(path.resolve(folder, baseName + '.gzip')))) {
          yield baseName
        }
      }
    }
  }

  async function readGzipOriginalSize(filePath: string, gzipSize: number): Promise<number | null> {
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
      return buffer.readUInt32LE(0)
    } catch {
      return null
    }
  }

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    const possibleEncondings = ['gzip', null]
    const baseFilePath = await getFilePath(id)

    for (const encoding of possibleEncondings) {
      const extension = encoding ? '.' + encoding : ''
      const filePath = baseFilePath + extension
      if (await components.fs.existPath(filePath)) {
        const stat = await components.fs.stat(filePath)
        if (encoding === 'gzip') {
          const contentSize = await readGzipOriginalSize(filePath, stat.size)
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

  // Removes temp files left behind by writes interrupted in a previous run. Staged filenames carry
  // this boot's random prefix, so a staged-shape file with a different prefix is by construction a
  // leftover of an earlier process — a write racing this sweep stages under the current bootId and
  // is never touched. Best-effort: a missing dir or a failed unlink is ignored.
  const sweepOrphanedTempFiles = async (): Promise<number> => {
    // No staging happens without atomic-write support, so there is nothing of ours to sweep — and
    // the directory (if it exists at all) is not ours to touch.
    if (!ATOMIC_MODE) return 0
    // Ownership of the reserved dir is a construction invariant (see the OWNERSHIP_MARKER logic in
    // the factory): if this storage is running, everything staged-shaped in there is ours.
    let entries: string[]
    try {
      entries = await components.fs.readdir(tempDir)
    } catch {
      return 0
    }
    let removed = 0
    for (const entry of entries) {
      if (!STAGED_FILE_NAME.test(entry) || entry.startsWith(`${bootId}-`)) continue
      if (await noFailUnlink(path.join(tempDir, entry))) removed++
    }
    return removed
  }

  // Resolves mixed raw/gzip states left by a crash between a commit rename and its counterpart
  // cleanup. Each surviving intent names the representation that was being committed for an id;
  // when both representations exist, the counterpart is removed so reads can never prefer the stale
  // one. Only the NEWEST intent per id is applied (a crash can leave several from re-attempts). An
  // intent whose id is in a consistent state is simply discharged. Runs at construction, before any
  // operation can observe the storage.
  async function reconcileIntents(): Promise<void> {
    // No intents are ever written without atomic-write support.
    if (!ATOMIC_MODE) return
    let entries: string[]
    try {
      entries = await components.fs.readdir(tempDir)
    } catch {
      return
    }
    // Intent paths are a deterministic function of the id, so there is at most one per id and no
    // ordering to resolve. A repair that cannot be completed FAILS CONSTRUCTION: live reads do not
    // consult intents, so a usable instance over an unreconciled mixed state would keep serving the
    // stale representation for its whole lifetime.
    for (const name of entries.filter((entry) => INTENT_FILE_NAME.test(entry)).sort()) {
      try {
        await applyPendingIntent(path.join(tempDir, name))
      } catch (err: any) {
        throw new Error(
          `Refusing to start: ${err instanceof Error ? err.message : String(err)} ` +
            `The intent journal '${name}' under '${tempDirName}' was kept; fix the underlying filesystem issue ` +
            `(permissions, immutability) and restart.`
        )
      }
    }
  }

  await reconcileIntents()

  return {
    async start(_startOptions: any) {
      // Idempotent: clear any existing timer first so a repeated start() doesn't leak intervals.
      if (evictionTimer) {
        clearInterval(evictionTimer)
      }
      evictionTimer = setInterval(evictCache, CACHE_EVICTION_INTERVAL)
      evictionTimer.unref()
      // Detached best-effort cleanup of temp files orphaned by an interrupted write in a prior run.
      // Runs in the background so it never delays startup; `stop()` awaits it once, at shutdown.
      tempFileSweep = sweepOrphanedTempFiles()
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
      // Wait for the startup temp-file sweep and any inflight decompressions before cleaning up
      await Promise.allSettled([tempFileSweep, ...inflightDecompressions.values()])
      // Evict all cached files on shutdown to prevent disk leaks across restarts
      for (const [filePath, entry] of decompressCache) {
        await evictCacheEntry(filePath, entry)
      }
    },
    storeStream,
    retrieve,
    exist,
    async storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
      const filePath = await getFilePath(id)
      const { rename } = components.fs
      // Without rename (legacy custom fs adapter) everything is necessarily in place, so the whole
      // sequence runs under the path lock: no concurrent store/delete can interleave between the
      // raw write, the compression and the raw cleanup (which would otherwise be able to delete a
      // newer writer's file). Not crash-atomic, like the rest of the no-rename mode.
      if (!rename) {
        await withPathLock(filePath, async () => {
          try {
            await pipe(stream, components.fs.createWriteStream(filePath))
            await noFailUnlink(filePath + '.gzip')
            forgetCacheEntry(filePath)
            invalidateInflightDecompression(filePath)
          } catch (err) {
            // Clean up the partial output while still holding the lock (see storeStream).
            await noFailUnlink(filePath)
            throw err
          }
          if (await compressContentFile(filePath, logger)) {
            // The in-place compression succeeded: the gzip exists at its canonical path and, under
            // the lock, the raw is provably still the bytes that were compressed.
            await noFailUnlink(filePath)
          }
        })
        return
      }
      // Fully staged: both the raw bytes and their gzip are produced in the operation-owned staging
      // area — the compression reads the PRIVATE staged raw, so no concurrent store/delete can
      // supersede or fail it — and the id transitions in ONE locked commit to either the gzip-only
      // or the raw-only representation of the new version. Until that commit the previous version
      // stays fully intact; a process killed at any point leaves only sweepable staged files.
      const stagedRawPath = newTempPath()
      const stagedGzipPath = newTempPath()
      try {
        await pipe(stream, components.fs.createWriteStream(stagedRawPath))
        const compressed = await compressContentFile(stagedRawPath, logger, stagedGzipPath)
        await withPathLock(filePath, async () => {
          try {
            // Intent-journaled: a crash between the commit rename and the counterpart cleanup is
            // reconciled at next construction, never leaving mixed versions for reads to prefer.
            if (compressed) {
              await commitRepresentation('gzip', id, stagedGzipPath, filePath + '.gzip', filePath, rename)
            } else {
              await commitRepresentation('raw', id, stagedRawPath, filePath, filePath + '.gzip', rename)
            }
          } finally {
            // Run even when the commit throws post-rename (failed counterpart cleanup).
            forgetCacheEntry(filePath)
            invalidateInflightDecompression(filePath)
          }
        })
      } finally {
        // Whatever was not renamed into place is staging residue (the raw after a gzip-only commit,
        // both files after a failure). The previous canonical version is untouched on any error.
        await noFailUnlink(stagedRawPath)
        await noFailUnlink(stagedGzipPath)
      }
    },
    async delete(ids: string[]): Promise<void> {
      for (const id of ids) {
        const filePath = await getFilePath(id)
        // Locked so an in-flight decompression can never resurrect the id by renaming its staged
        // bytes onto the canonical path after these unlinks.
        await withPathLock(filePath, async () => {
          const wasCached = await removeCacheEntry(filePath)
          if (!wasCached) {
            await noFailUnlink(filePath)
          }
          await noFailUnlink(filePath + '.gzip')
          invalidateInflightDecompression(filePath)
        })
      }
    },
    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      const entries = await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]))
      return new Map(entries)
    },
    allFileIds: (prefix?: string) => allFileIdsRec(root, prefix),
    fileInfo,
    async fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
      return new Map(
        await Promise.all(cids.map(async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)]))
      )
    }
  }
}
