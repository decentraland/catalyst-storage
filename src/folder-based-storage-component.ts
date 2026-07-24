import { createHash, randomBytes } from 'crypto'
import path from 'path'
import { pipeline, Readable, Transform } from 'stream'
import { promisify } from 'util'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem, streamToBuffer } from './content-item'
import { isAbortError, markAsNonCancellationError, runStoreWithSignal } from './cancellation'
import { compressContentFile } from './extras/compression'

const pipe = promisify(pipeline)

// Thrown when a commit rename failed AND its pre-rename intent could not be cleared. The staged
// file it names is then the only PROOF that the rename never landed: callers must preserve that
// exact path (instead of their usual staging cleanup), so the next construction can discard the
// intent as pre-rename instead of applying the failed commit as a completed transition.
class UncommittedIntentSurvivedError extends Error {
  constructor(
    readonly stagedPath: string,
    message: string
  ) {
    super(message)
  }
}

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
 * - **Atomicity covers process crashes, NOT power-loss durability** — staged data is deliberately
 *   not `fsync`'d before the commit rename. A power loss / kernel panic between write and flush may
 *   lose the file entirely (never a partial/mixed state); content is content-addressed and
 *   re-downloadable, so durability past process death is intentionally out of contract.
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

  await components.fs.mkdir(root, { recursive: true })

  if (ATOMIC_MODE) {
    // stat() follows symlinks, so a pre-existing symlink at the reserved path would pass the
    // directory check below and route staged writes and the startup sweep OUTSIDE the storage
    // root. Refuse it when the fs component can detect it (lstat is optional for custom adapters;
    // without it, the documented exclusive-root operational model is the guarantee).
    if (components.fs.lstat) {
      let linkStat
      try {
        linkStat = await components.fs.lstat(tempDir)
      } catch {
        linkStat = undefined
      }
      if (linkStat?.isSymbolicLink()) {
        throw new Error(
          `Refusing to start: the reserved temp path '${tempDirName}' is a symbolic link; staged writes and the ` +
            `startup sweep would operate outside the storage root. Replace it with a real directory or configure ` +
            `a different tempDirectoryName.`
        )
      }
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
    // Created up front so storeStream can stage into it without a per-write mkdir.
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

  // Existence check for recovery invariants. existPath() tests F_OK|R_OK, so a file left behind by
  // a failed unlink in an UNREADABLE state (mode/ACL damage, transient permission problem) would
  // read as absent — letting a must-succeed cleanup be falsely considered complete, and the mixed
  // state resurface later with no repair signal. Here only ENOENT/ENOTDIR mean absent; any other
  // error fails the repair/commit path loudly.
  async function existsForInvariant(target: string): Promise<boolean> {
    try {
      await components.fs.stat(target)
      return true
    } catch (err: any) {
      if (err?.code === 'ENOENT' || err?.code === 'ENOTDIR') return false
      throw err
    }
  }

  // Ids whose post-rename counterpart cleanup failed in THIS process: the on-disk state is mixed
  // (new primary + stale counterpart) with the intent preserved, and live reads must not serve it —
  // non-range reads would prefer the stale counterpart while range reads see the new bytes. Reads
  // check this set (an O(1) lookup, no syscalls — the hot path is untouched when it is empty) and,
  // for a quarantined id, repair under the path lock or report the id unavailable. Entries clear on
  // any successful repair (read-triggered, retried store, delete) and do not survive restarts,
  // where construction-time reconciliation takes over.
  const unreconciledIds = new Set<string>()

  // Repair gate for reads of a quarantined id: applies the pending intent under the path lock and
  // reports whether the id is safe to serve. Never throws — an unrepairable id stays quarantined
  // and the caller reports no result rather than exposing a known-mixed state.
  async function ensureReconciled(id: string): Promise<boolean> {
    if (!unreconciledIds.has(id)) return true
    const filePath = await getFilePath(id)
    return withPathLock(filePath, async () => {
      if (!unreconciledIds.has(id)) return true
      try {
        const intentPath = intentPathFor(id)
        if (await existsForInvariant(intentPath)) {
          await applyPendingIntent(intentPath)
        } else {
          // No journal left: the mixed state was repaired elsewhere (retried store, delete).
          unreconciledIds.delete(id)
        }
        return !unreconciledIds.has(id)
      } catch {
        return false
      }
    })
  }

  // After a FAILED commit rename, the pre-rename intent must be PROVABLY gone before the staged
  // proof may be discarded. Both "the journal survived" and "the journal cannot be proven gone"
  // (the verification stat itself fails) preserve the proof via the typed error — otherwise a raw
  // EACCES would escape untyped, the callers' staging cleanup would destroy the staged file, and a
  // later restart could misapply the surviving pre-rename intent as a committed transition.
  async function clearIntentOrThrowPreservingProof(
    intentPath: string,
    stagedPath: string,
    id: string,
    originalError: unknown
  ): Promise<void> {
    await noFailUnlink(intentPath)
    try {
      if (!(await existsForInvariant(intentPath))) return
    } catch {
      // Could not prove the dangerous journal is gone; fall through and preserve the staged proof.
    }
    throw new UncommittedIntentSurvivedError(
      stagedPath,
      `Failed to commit ${id} AND failed to prove its intent journal was removed; the staged file is preserved ` +
        `as proof the commit never landed, so a restart repairs this once the filesystem issue is fixed. ` +
        `Original error: ${originalError instanceof Error ? originalError.message : String(originalError)}`
    )
  }

  // Removing an intent journal is semantically must-succeed wherever it is called: a journal that
  // outlives its purpose is later interpreted as a pending repair instruction. Centralized so the
  // invariant cannot be accidentally weakened back into a best-effort unlink.
  async function removeIntentOrThrow(intentPath: string, context: string): Promise<void> {
    await noFailUnlink(intentPath)
    if (await existsForInvariant(intentPath)) {
      throw new Error(`${context}: the intent journal '${intentPath}' could not be removed.`)
    }
  }

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
    if (
      (op !== 'raw' && op !== 'gzip') ||
      !id ||
      !STAGED_FILE_NAME.test(staged ?? '') ||
      // The intent path is a deterministic function of the id: a body whose id does not hash to
      // this filename is corruption or operator error, and applying it would reconcile the WRONG
      // id. Treat it as malformed.
      intentPathFor(id) !== intentPath
    ) {
      // A partial/malformed intent means its commit never started (intents are written before
      // renames): discard it; an orphaned staged file, if any, is handled by the sweep.
      await removeIntentOrThrow(intentPath, 'Discarding a malformed intent journal failed')
      return
    }
    const stagedPath = path.join(tempDir, staged)
    if (await existsForInvariant(stagedPath)) {
      // Prepared but never renamed: the commit did not happen. The staged file is the PROOF of
      // that, and the intent is the dangerous artifact — remove the journal first (must succeed),
      // and only then the inert staged garbage. The reverse order could destroy the proof while the
      // journal survives, letting the next construction reinterpret this pre-rename intent as a
      // committed transition and delete the valid counterpart.
      await removeIntentOrThrow(intentPath, `Cannot discard the uncommitted ${op} intent for ${id}`)
      await noFailUnlink(stagedPath)
      unreconciledIds.delete(id)
      logger.info(`Discarded a prepared but uncommitted ${op} transition`, { id })
      return
    }
    const filePath = await getFilePath(id)
    const gzipPath = filePath + '.gzip'
    const primaryPath = op === 'raw' ? filePath : gzipPath
    const counterpartPath = op === 'raw' ? gzipPath : filePath
    if (!(await existsForInvariant(primaryPath))) {
      throw new Error(
        `Cannot reconcile the interrupted ${op} commit for ${id}: neither its staged file nor its committed ` +
          `representation exists.`
      )
    }
    if (await existsForInvariant(counterpartPath)) {
      await noFailUnlink(counterpartPath)
      if (await existsForInvariant(counterpartPath)) {
        throw new Error(
          `Cannot repair the interrupted ${op} commit for ${id}: its stale ${
            op === 'raw' ? 'gzip' : 'raw'
          } representation could not be removed.`
        )
      }
      logger.info(`Reconciled an interrupted ${op} commit`, { id })
    }
    await removeIntentOrThrow(intentPath, `Reconciled ${id} but could not discharge its intent journal`)
    unreconciledIds.delete(id)
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
    rename: (from: string, to: string) => Promise<void>,
    signal?: AbortSignal
  ): Promise<void> {
    try {
      await doCommitRepresentation(op, id, stagedPath, primaryPath, counterpartPath, rename, signal)
    } catch (err) {
      // The commit phase's own cancellation checkpoints throw the caller's abort reason and nothing
      // else, so identity is the whole test: pass that through untouched (it IS a cancellation, and
      // marking would tag a caller-owned object with this module's internal symbol). Every other
      // failure here — pending-intent repair, journal write, rename, counterpart cleanup, quarantine
      // — cannot be caused by abort teardown (the source is fully consumed before any commit begins
      // and this backend has no abort hook), so it is a real storage error that cancellation
      // translation must never mask, whatever shape it happens to have.
      if (signal?.aborted && err === signal.reason) {
        throw err
      }
      throw markAsNonCancellationError(err)
    }
  }

  async function doCommitRepresentation(
    op: 'raw' | 'gzip',
    id: string,
    stagedPath: string,
    primaryPath: string,
    counterpartPath: string,
    rename: (from: string, to: string) => Promise<void>,
    signal?: AbortSignal
  ): Promise<void> {
    // A pending intent means a previous commit for this id failed its cleanup in this process:
    // repair first (throws if impossible), so the intent written below always describes a
    // transition from a consistent state and never overwrites an unapplied repair instruction.
    if (await existsForInvariant(intentPathFor(id))) {
      await applyPendingIntent(intentPathFor(id))
    }
    // The pre-rename phase awaits repair, existence checks and the journal write: an abort landing
    // during any of them (with the source long consumed) must still cancel before the irreversible
    // rename. Here no commit artifact exists yet, so a plain throw suffices; a completed repair
    // above is idempotent state that needs no undoing.
    signal?.throwIfAborted()
    const hadCounterpart = await existsForInvariant(counterpartPath)
    const intentPath = hadCounterpart ? await writeIntent(op, id, stagedPath) : undefined
    if (intentPath && signal?.aborted) {
      // Cancelled after the intent was journaled but before the rename: the commit never happened,
      // so the journal must not survive (a later repair would apply it as a completed transition).
      // Clearing it is must-succeed — if it cannot be cleared, the staged file is preserved as the
      // proof the rename never landed, exactly like a failed rename.
      await clearIntentOrThrowPreservingProof(intentPath, stagedPath, id, signal.reason)
    }
    // Unconditional last checkpoint before the irreversible rename: without it, an abort landing
    // during the awaited counterpart check on a fresh id (no counterpart → no intent → the block
    // above skipped) would proceed to commit. No journal exists past this line unless it was just
    // cleared, so a plain throw is safe.
    signal?.throwIfAborted()
    try {
      await rename(stagedPath, primaryPath)
    } catch (err) {
      // The commit did not happen, so the intent must not survive it: a later repair would treat
      // the failed commit as successful and remove the counterpart — e.g. delete a valid gzip
      // primary in favor of its own decompressed raw cache.
      if (intentPath) {
        // A surviving (or unprovably-removed) intent could later be applied as if the commit
        // succeeded. The staged file is the proof it did not — the typed error tells callers to
        // preserve it, which also makes this state self-healing: the next construction sees the
        // staged file, classifies the intent as pre-rename and discards both, previous
        // representations untouched.
        await clearIntentOrThrowPreservingProof(intentPath, stagedPath, id, err)
      }
      throw err
    }
    if (hadCounterpart) {
      await noFailUnlink(counterpartPath)
      let counterpartGone: boolean
      try {
        counterpartGone = !(await existsForInvariant(counterpartPath))
      } catch (verifyErr) {
        // Possibly mixed and unprovable: quarantine so reads repair-or-refuse instead of serving it.
        unreconciledIds.add(id)
        throw verifyErr
      }
      if (!counterpartGone) {
        // The new version is committed, but the stale counterpart could not be removed. The
        // on-disk state is MIXED: without a guard, non-range reads would keep preferring the stale
        // counterpart while range reads see the new bytes — two versions of one id from the same
        // process. Quarantine the id (reads repair-or-refuse, see ensureReconciled) and fail the
        // store loudly; the intent survives, so a retried store, a read-triggered repair or the
        // next construction finishes the cleanup.
        unreconciledIds.add(id)
        throw new Error(
          `Stored ${id} but failed to remove its previous ${op === 'raw' ? 'gzip' : 'raw'} representation; ` +
            `the id is quarantined from reads until a retry, read-triggered repair or restart completes the cleanup.`
        )
      }
      if (intentPath) {
        await removeIntentOrThrow(intentPath, `Committed ${id} but could not discharge its intent journal`)
      }
      unreconciledIds.delete(id)
    }
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
      forgetCacheEntry(filePath)
      invalidateInflightDecompression(filePath)
    } catch (err) {
      // Clean up the partial output while still holding the lock: doing it after release could
      // delete a queued writer's freshly committed content for the same id.
      await noFailUnlink(filePath)
      throw err
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

  // Returns the CURRENT in-flight eviction when one is already running, instead of a resolved
  // no-op: the interval callback assigns this to the tracked tick, so a tick firing during a slow
  // eviction must hand back the real promise — otherwise stop() would await the no-op and could
  // resolve while the actual eviction is still unlinking files.
  let inflightEviction: Promise<void> | undefined
  function evictCache(): Promise<void> {
    if (inflightEviction) return inflightEviction
    inflightEviction = runEviction()
      .catch((error) => logger.warn(`Cache eviction failed: ${error}`))
      .finally(() => {
        inflightEviction = undefined
      })
    return inflightEviction
  }

  // Unlinks an evicted cache file under the path lock, re-checking the entry is still current: a
  // store may have promoted the path to primary content (forgetting the entry) between the eviction
  // scan and this delete — unlinking then would destroy the only copy of the new content.
  async function evictCacheEntry(filePath: string, entry: { size: number; lastAccess: number }): Promise<void> {
    await withPathLock(filePath, async () => {
      if (decompressCache.get(filePath) !== entry) return
      await noFailUnlink(filePath)
      // Keep the tracking when the file survives the unlink, so the next eviction tick retries it
      // instead of leaving an untracked (unaccounted, never-retried) cache file on disk.
      if (await existsForInvariant(filePath)) return
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
  // Tracks the in-flight eviction tick so `stop()` can await one that is already running.
  let evictionTick: Promise<void> = Promise.resolve()
  // Tracks the detached startup temp-file sweep so `stop()` can await it (rather than leaving a
  // promise dangling past shutdown). Repeated start() calls CHAIN onto it instead of replacing it.
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

  const doStoreStream = async (id: string, stream: Readable, signal?: AbortSignal): Promise<void> => {
    const filePath = await getFilePath(id)
    const { rename } = components.fs
    // A custom fs adapter that predates the optional `rename` falls back to the original direct
    // write. It isn't crash-atomic, but keeps the public IFileSystemComponent backward-compatible;
    // the bundled createFsComponent provides rename and so takes the atomic path below.
    if (!rename) {
      await withPathLock(filePath, () => writeRawInPlaceLocked(id, filePath, stream, signal))
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
          await commitRepresentation('raw', id, tempPath, filePath, filePath + '.gzip', rename, signal)
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
      // remove it so a failed store never leaves a stray file behind (the final path is untouched) —
      // EXCEPT when the temp file is the preserved proof of an uncommitted intent that could not be
      // cleared: destroying it would let the next reconciliation apply the failed commit.
      if (!(err instanceof UncommittedIntentSurvivedError && err.stagedPath === tempPath)) {
        await noFailUnlink(tempPath)
      }
      throw err
    }
  }

  async function removeCacheEntry(filePath: string): Promise<boolean> {
    const entry = decompressCache.get(filePath)
    if (!entry) return false
    await noFailUnlink(filePath)
    // Verify before dropping the tracking: reporting the cached raw as handled while it survives
    // would let delete() remove the gzip and resolve — resurrecting the untracked cache file as
    // readable primary content after a "successful" delete.
    if (await existsForInvariant(filePath)) {
      throw new Error(`Failed to remove the cached decompressed content at ${filePath}`)
    }
    totalCacheSize -= entry.size
    decompressCache.delete(filePath)
    return true
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
  // that as a retryable miss, exactly like retrieve() having returned undefined. Ids quarantined by
  // a failed post-rename cleanup are repaired before serving or reported as absent — a read never
  // exposes a known-mixed state (see unreconciledIds).
  const retrieve = async (id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> => {
    if (range) validateRange(range)
    if (unreconciledIds.has(id) && !(await ensureReconciled(id))) {
      logger.warn(`Refusing to serve a quarantined mixed-state id`, { id })
      return undefined
    }
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
    if (unreconciledIds.has(id) && !(await ensureReconciled(id))) return false
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
    if (unreconciledIds.has(id) && !(await ensureReconciled(id))) return undefined
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

  const doStoreStreamAndCompress = async (id: string, stream: Readable, signal?: AbortSignal): Promise<void> => {
    const filePath = await getFilePath(id)
    const { rename } = components.fs
    // Without rename (legacy custom fs adapter) everything is necessarily in place, so the whole
    // sequence runs under the path lock: no concurrent store/delete can interleave between the
    // raw write, the compression and the raw cleanup (which would otherwise be able to delete a
    // newer writer's file). Not crash-atomic, like the rest of the no-rename mode.
    if (!rename) {
      await withPathLock(filePath, async () => {
        await writeRawInPlaceLocked(id, filePath, stream, signal)
        // An abort observed here arrives after the in-place raw was committed (the previous version
        // is already gone): the store is complete and allowed to succeed, but the optional
        // compression is skipped — or torn down mid-flight via the signal — rather than doing
        // further expensive work for a cancelled request.
        if (!signal?.aborted) {
          let compressed = false
          try {
            compressed = await compressContentFile(filePath, logger, undefined, signal)
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
      return
    }
    // Fully staged: both the raw bytes and their gzip are produced in the operation-owned staging
    // area — the compression reads the PRIVATE staged raw, so no concurrent store/delete can
    // supersede or fail it — and the id transitions in ONE locked commit to either the gzip-only
    // or the raw-only representation of the new version. Until that commit the previous version
    // stays fully intact; a process killed at any point leaves only sweepable staged files.
    const stagedRawPath = newTempPath()
    const stagedGzipPath = newTempPath()
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
        compressed = await compressContentFile(stagedRawPath, logger, stagedGzipPath, signal)
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
            await commitRepresentation('gzip', id, stagedGzipPath, filePath + '.gzip', filePath, rename, signal)
          } else {
            await commitRepresentation('raw', id, stagedRawPath, filePath, filePath + '.gzip', rename, signal)
          }
        } finally {
          // Run even when the commit throws post-rename (failed counterpart cleanup).
          forgetCacheEntry(filePath)
          invalidateInflightDecompression(filePath)
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

  await reconcileIntents()

  return {
    async start(_startOptions: any) {
      // Idempotent: clear any existing timer first so a repeated start() doesn't leak intervals.
      if (evictionTimer) {
        clearInterval(evictionTimer)
      }
      // Track the in-flight eviction tick so stop() can await one that is already running; a tick
      // firing during a slow eviction receives that same in-flight promise from evictCache().
      evictionTimer = setInterval(() => {
        evictionTick = evictCache()
      }, CACHE_EVICTION_INTERVAL)
      evictionTimer.unref()
      // Detached best-effort cleanup of temp files orphaned by an interrupted write in a prior run.
      // Runs in the background so it never delays startup; `stop()` awaits it once, at shutdown.
      // Chained onto any previous sweep so a repeated start() cannot replace a still-running sweep
      // with a new promise (the older one would dangle past stop()) nor run two sweeps concurrently.
      tempFileSweep = tempFileSweep
        .then(() => sweepOrphanedTempFiles())
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
      await Promise.allSettled([tempFileSweep, evictionTick, ...inflightDecompressions.values()])
      // Evict all cached files on shutdown to prevent disk leaks across restarts
      for (const [filePath, entry] of decompressCache) {
        await evictCacheEntry(filePath, entry)
      }
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
          const pendingIntentPath = intentPathFor(id)
          if (ATOMIC_MODE && (await existsForInvariant(pendingIntentPath))) {
            await applyPendingIntent(pendingIntentPath)
          }
          // Every removal below is verified: a delete that resolves while ANY representation
          // survives (cached raw, primary raw, or gzip) would leave the id readable after a
          // "successful" delete. Failures abort before touching the next representation, so a
          // failed delete always leaves a complete, readable version behind and rejects loudly.
          const wasCached = await removeCacheEntry(filePath)
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
          if (ATOMIC_MODE) {
            // Defensive: applyPendingIntent already discharged any journal; verify none remains.
            await removeIntentOrThrow(pendingIntentPath, `Deleted ${id} but could not remove its intent journal`)
          }
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
