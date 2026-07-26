import { ILoggerComponent } from '@well-known-components/interfaces'
import { createHash, randomBytes } from 'crypto'
import path from 'path'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import { markAsNonCancellationError } from '../cancellation'
import { IFileSystemComponent } from '../fs/types'
import { FsInvariants } from './fs-invariants'

const pipe = promisify(pipeline)

/**
 * Thrown when a commit rename failed AND its pre-rename intent could not be cleared. The staged
 * file it names is then the only PROOF that the rename never landed: callers must preserve that
 * exact path (instead of their usual staging cleanup), so the next construction can discard the
 * intent as pre-rename instead of applying the failed commit as a completed transition.
 */
export class UncommittedIntentSurvivedError extends Error {
  constructor(
    readonly stagedPath: string,
    message: string
  ) {
    super(message)
  }
}

/**
 * Reserved directory (under the storage root) where an atomic `storeStream` stages its temp file
 * before renaming it into place. Kept out of the content namespace — a shard is a 4-hex directory and
 * content lives in files, never here — so a temp can never collide with, hide, or be mistaken for an
 * addressable id. Skipped by `allFileIds` and cleaned at startup. Its name is therefore reserved.
 */
export const TEMP_DIR_NAME = '.tmp-writes'

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

const OWNERSHIP_MARKER = '.owned-by-catalyst-storage'
const OWNERSHIP_MARKER_CONTENT = 'reserved by catalyst-storage for atomic write staging\n'

/** Which of an id's two representations a commit is making primary. */
export type Representation = 'raw' | 'gzip'

/**
 * The staging area and its crash-recovery journal.
 *
 * A logical object spans two canonical paths (`<id>` and `<id>.gzip`) but only one rename can be
 * atomic, so a representation transition is journaled: an intent naming the new primary is written
 * before the commit rename and discharged once the counterpart is provably gone. A crash (or a
 * failed cleanup) in between leaves the journal behind, and the next construction resolves the
 * mixed state in favor of the representation the intent names — instead of reads silently preferring
 * the stale counterpart.
 *
 * Every method here is a no-op or a refusal in non-atomic mode: without `fs.rename` nothing stages,
 * so there is nothing to journal, sweep or reconcile.
 */
export type IntentJournal = {
  /** A fresh staged path under the reserved directory, tagged with this instance's boot id. */
  newTempPath(): string
  /**
   * Commits a staged file onto its canonical primary path and removes the other representation.
   *
   * Must be called while holding the id's path lock. That lock is always taken on the RAW path
   * (`<id>`), for a gzip commit as much as a raw one — every operation that can touch either
   * representation (stores, deletes, decompressions, cache eviction) locks that one path, which is
   * what makes them mutually exclusive across BOTH of an id's paths.
   */
  commitRepresentation(
    op: Representation,
    id: string,
    stagedPath: string,
    primaryPath: string,
    counterpartPath: string,
    rename: (from: string, to: string) => Promise<void>,
    signal?: AbortSignal
  ): Promise<void>
  /**
   * Applies a pending intent for this id if one exists, so a caller never overwrites an unapplied
   * repair instruction. Throws when the repair cannot be completed.
   */
  repairPendingIntent(id: string): Promise<void>
  /** Verifies no journal is left for this id, throwing with `context` when one survives. */
  assertNoIntent(id: string, context: string): Promise<void>
  /**
   * Whether reads must not serve this id: a post-rename counterpart cleanup failed in THIS process,
   * so the on-disk state is mixed. An O(1) lookup with no syscalls — the read path is untouched
   * while nothing is quarantined.
   */
  isQuarantined(id: string): boolean
  /**
   * Repair gate for reads of a quarantined id: applies the pending intent under the path lock and
   * reports whether the id is safe to serve. Never throws — an unrepairable id stays quarantined and
   * the caller reports no result rather than exposing a known-mixed state.
   */
  ensureReconciled(id: string): Promise<boolean>
  /**
   * Resolves mixed raw/gzip states left by a crash between a commit rename and its counterpart
   * cleanup. Runs at construction, before any operation can observe the storage.
   */
  reconcile(): Promise<void>
  /** Removes staged files left behind by writes interrupted in a previous run. Best-effort. */
  sweepOrphanedTempFiles(): Promise<number>
}

export type IntentJournalOptions = {
  /** The reserved staging directory, already validated as a single segment under the root. */
  tempDir: string
  /** Its basename, used verbatim in the operator-facing refusal messages. */
  tempDirName: string
  /** False in legacy no-rename mode, where none of the staging machinery applies. */
  atomic: boolean
  /** False in `disablePrefixHash` mode, where the root itself is the content namespace. */
  useHashPrefix: boolean
}

/**
 * Prepares the reserved staging directory (refusing to start if it cannot be proven safe to use) and
 * returns the journal over it. Performs filesystem mutations, so callers must validate all
 * configuration first.
 */
export async function createIntentJournal(
  components: {
    fs: IFileSystemComponent
    logger: ILoggerComponent.ILogger
    fsInvariants: FsInvariants
    /** Serializes commits on a canonical path; supplied by the decompress cache. */
    withPathLock: <T>(filePath: string, fn: () => Promise<T>) => Promise<T>
    /** Maps an id to its canonical raw path. Called lazily, never during construction. */
    resolveFilePath: (id: string) => Promise<string>
  },
  options: IntentJournalOptions
): Promise<IntentJournal> {
  const { fs, logger, withPathLock, resolveFilePath } = components
  const { existsForInvariant, noFailUnlink } = components.fsInvariants
  const { tempDir, tempDirName, atomic, useHashPrefix } = options

  if (atomic) {
    // stat() follows symlinks, so a pre-existing symlink at the reserved path would pass the
    // directory check below and route staged writes and the startup sweep OUTSIDE the storage
    // root. Refuse it when the fs component can detect it (lstat is optional for custom adapters;
    // without it, the documented exclusive-root operational model is the guarantee).
    if (fs.lstat) {
      let linkStat
      try {
        linkStat = await fs.lstat(tempDir)
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
    if (await fs.existPath(tempDir)) {
      const tempDirStat = await fs.stat(tempDir)
      if (!tempDirStat.isDirectory()) {
        throw new Error(
          `Refusing to start: the reserved temp path '${tempDirName}' under the storage root exists as a file — ` +
            `likely a pre-existing content id. Migrate it out or configure a different tempDirectoryName.`
        )
      }
    }
    // Created up front so storeStream can stage into it without a per-write mkdir.
    await fs.mkdir(tempDir, { recursive: true })
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
  if (atomic && !useHashPrefix) {
    const markerPath = path.join(tempDir, OWNERSHIP_MARKER)
    const refuseToStart = (reason: string): never => {
      throw new Error(
        `Refusing to start: ${reason} In disablePrefixHash mode the reserved temp directory '${tempDirName}' may hold ` +
          `pre-existing content ids that the reservation would hide from retrieval and enumeration. ` +
          `Migrate those files out of '${tempDirName}', configure a different tempDirectoryName, or restore the ` +
          `'${OWNERSHIP_MARKER}' marker (with its original content) if they are staging leftovers from a previous run.`
      )
    }
    if (await fs.existPath(markerPath)) {
      const markerBody = await fs.readFile(markerPath, 'utf8')
      if (markerBody !== OWNERSHIP_MARKER_CONTENT) {
        refuseToStart(
          `the ownership marker '${OWNERSHIP_MARKER}' exists but its content is not the one this storage writes, ` +
            `so it may be a pre-existing content id rather than a marker.`
        )
      }
      const foreign = (await fs.readdir(tempDir)).filter(
        (entry) => entry !== OWNERSHIP_MARKER && !STAGED_FILE_NAME.test(entry) && !INTENT_FILE_NAME.test(entry)
      )
      if (foreign.length > 0) {
        refuseToStart(
          `the reserved temp directory '${tempDirName}' contains ${foreign.length} file(s) that this storage ` +
            `did not create.`
        )
      }
    } else {
      const entries = await fs.readdir(tempDir)
      if (entries.length > 0) {
        refuseToStart(
          `the reserved temp directory '${tempDirName}' already contains ${entries.length} file(s) that this ` +
            `storage cannot prove it owns.`
        )
      }
      await pipe(Readable.from([Buffer.from(OWNERSHIP_MARKER_CONTENT)]), fs.createWriteStream(markerPath))
    }
  }

  // Staged files are prefixed with a per-boot random id so the startup sweep can tell leftovers
  // from previous runs (any other prefix) apart from files this instance is writing right now —
  // a write racing the sweep can therefore never have its live staged file unlinked.
  const bootId = randomBytes(8).toString('hex')
  const newTempPath = (): string => path.join(tempDir, `${bootId}-${randomBytes(16).toString('hex')}`)

  // Journal for representation-transition commits: written before the commit rename whenever a
  // counterpart representation exists, removed once the counterpart is confirmed gone. A crash in
  // between leaves the intent behind, and the next construction resolves the mixed state in favor
  // of the representation the intent names (see reconcile). The path is a deterministic function of
  // the id — at most one intent per id can ever exist, because commits are serialized per path and
  // construction reconciles before any write — so reconciliation needs no ordering heuristics.
  // Fresh-id writes — the overwhelmingly common case in content-addressed use — have no counterpart
  // and never pay for an intent.
  const intentPathFor = (id: string): string =>
    path.join(tempDir, `${createHash('sha1').update(id).digest('hex')}.intent`)

  // Ids whose post-rename counterpart cleanup failed in THIS process: the on-disk state is mixed
  // (new primary + stale counterpart) with the intent preserved, and live reads must not serve it —
  // non-range reads would prefer the stale counterpart while range reads see the new bytes. Reads
  // check this set and, for a quarantined id, repair under the path lock or report the id
  // unavailable. Entries clear on any successful repair (read-triggered, retried store, delete) and
  // do not survive restarts, where construction-time reconciliation takes over.
  const unreconciledIds = new Set<string>()

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

  async function writeIntent(op: Representation, id: string, stagedPath: string): Promise<string> {
    const intentPath = intentPathFor(id)
    // The staged BASENAME lets reconciliation prove whether the commit rename landed: renames
    // consume the staged file, so "staged still present" means the rename provably never happened.
    // Stored as a basename (not an absolute path) so a root remount cannot poison the journal.
    const body = JSON.stringify({ op, id, staged: path.basename(stagedPath) })
    await pipe(Readable.from([Buffer.from(body)]), fs.createWriteStream(intentPath))
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
    const body = await fs.readFile(intentPath, 'utf8')
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
    const filePath = await resolveFilePath(id)
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

  async function repairPendingIntent(id: string): Promise<void> {
    if (!atomic) return
    const intentPath = intentPathFor(id)
    if (await existsForInvariant(intentPath)) {
      await applyPendingIntent(intentPath)
    }
  }

  async function doCommitRepresentation(
    op: Representation,
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
    await repairPendingIntent(id)
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

  return {
    newTempPath,
    repairPendingIntent,
    isQuarantined: (id: string) => unreconciledIds.has(id),

    // Commits a staged file onto its canonical primary path and removes the other representation.
    // The logical object spans two paths (raw and .gzip) but only the rename is atomic, so when a
    // counterpart exists an intent is journaled FIRST: if the process dies (or the unlink fails)
    // between the rename and the cleanup, the next construction reconciles the mixed state in favor
    // of the representation committed here, instead of reads preferring the stale counterpart. Must
    // be called while holding the path lock.
    async commitRepresentation(op, id, stagedPath, primaryPath, counterpartPath, rename, signal): Promise<void> {
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
    },

    async assertNoIntent(id: string, context: string): Promise<void> {
      if (!atomic) return
      await removeIntentOrThrow(intentPathFor(id), context)
    },

    async ensureReconciled(id: string): Promise<boolean> {
      if (!unreconciledIds.has(id)) return true
      const filePath = await resolveFilePath(id)
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
    },

    async reconcile(): Promise<void> {
      // No intents are ever written without atomic-write support.
      if (!atomic) return
      let entries: string[]
      try {
        entries = await fs.readdir(tempDir)
      } catch (err: any) {
        // ENOENT is the only tolerable answer — nothing was ever staged here, so there are no intents
        // to apply. Swallowing anything else (EACCES, EIO) meant construction could not know whether
        // a pending repair existed and started regardless, which is exactly the "usable instance over
        // an unreconciled mixed state" the loop below refuses to allow: reads never consult intents,
        // so the stale representation would be served for the whole process lifetime.
        if (err?.code === 'ENOENT') return
        throw new Error(
          `Refusing to start: the reserved temp directory '${tempDirName}' could not be read, so pending intent ` +
            `journals cannot be reconciled. Fix the underlying filesystem issue (permissions, mount) and restart. ` +
            `Original error: ${err instanceof Error ? err.message : String(err)}`
        )
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
    },

    // Removes temp files left behind by writes interrupted in a previous run. Staged filenames carry
    // this boot's random prefix, so a staged-shape file with a different prefix is by construction a
    // leftover of an earlier process — a write racing this sweep stages under the current bootId and
    // is never touched. Best-effort: a missing dir or a failed unlink is ignored.
    async sweepOrphanedTempFiles(): Promise<number> {
      // No staging happens without atomic-write support, so there is nothing of ours to sweep — and
      // the directory (if it exists at all) is not ours to touch.
      if (!atomic) return 0
      // Ownership of the reserved dir is a construction invariant (see the OWNERSHIP_MARKER logic
      // above): if this storage is running, everything staged-shaped in there is ours.
      let entries: string[]
      try {
        entries = await fs.readdir(tempDir)
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
  }
}
