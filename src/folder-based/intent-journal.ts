import { ILoggerComponent } from '@well-known-components/interfaces'
import { createHash, randomBytes } from 'crypto'
import path from 'path'
import { pipeline, Readable } from 'stream'
import { promisify } from 'util'
import { markAsNonCancellationError } from '../cancellation'
import { IFileSystemComponent } from '../fs/types'
import { FsInvariants } from './fs-invariants'
import { mapWithConcurrency } from '../concurrency'
// Declared with the other caller-facing errors, since this one reaches callers through `storeStream`.
// Re-exported here so existing deep imports of this module keep resolving it.
import { UncommittedIntentSurvivedError } from './errors'
import { gzipPathOf } from '../content-id'

export { UncommittedIntentSurvivedError }

const pipe = promisify(pipeline)

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

// Matches the intent-journal files a representation-transition commit writes (`<64-hex
// sha256(id)>.intent`). An intent records which representation (raw|gzip) is the NEW primary for an
// id, so a crash between the commit rename and the counterpart cleanup is reconciled at the next
// construction instead of leaving mixed versions that reads could prefer. The path is a
// deterministic function of the id: at most one intent can ever exist per id (commits are
// serialized per path, and construction reconciles before any write), so reconciliation needs no
// ordering heuristics.
//
// 40-hex is still matched so an intent written by a PREVIOUS version (which named these files with
// sha1) is recognized and reconciled after an upgrade. Discarding it instead would leave the mixed
// state it describes — new primary plus stale counterpart — in place, with reads preferring the
// stale one. `isIntentPathFor` accepts either name for the same id, so such an intent reconciles
// normally; only new ones are written under the sha256 name.
const INTENT_FILE_NAME = /^([0-9a-f]{64}|[0-9a-f]{40})\.intent$/

/** How many orphaned staged files the startup sweep unlinks at once. */
const SWEEP_CONCURRENCY = 32

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
    signal?: AbortSignal,
    /**
     * Invoked exactly once, immediately after the commit rename lands — that is, at the precise
     * point the canonical path stops holding the previous version.
     *
     * Callers need this because the commit can fail on EITHER side of that line and the two demand
     * opposite bookkeeping. Everything before the rename (a pending-intent repair, the abort
     * checkpoints, the counterpart probe, the journal write) leaves the canonical paths untouched,
     * so cache state describing them is still accurate; only once the rename has landed does that
     * state describe a version that no longer exists.
     */
    onCommitted?: () => void,
    /**
     * Invoked exactly once, once the counterpart representation is PROVEN gone (including when there
     * was none to remove).
     *
     * Distinct from `onCommitted` because for a GZIP commit the counterpart is the raw path — the very
     * path the caller's decompress-cache tracks. Gating that bookkeeping on the rename alone dropped
     * the cache's record of a file the failed unlink had LEFT ON DISK: untracked, so invisible to TTL
     * and LRU eviction and to `evictAll()` on stop, and no longer counted against the cache budget.
     * Only a counterpart that is provably gone justifies forgetting the entry that described it.
     */
    onCounterpartRemoved?: () => void
  ): Promise<void>
  /**
   * Applies a pending intent for this id if one exists, so a caller never overwrites an unapplied
   * repair instruction. Throws when the repair cannot be completed.
   */
  repairPendingIntent(id: string): Promise<void>
  /**
   * Removes any journal left for this id and verifies it is gone, throwing with `context` otherwise.
   *
   * Named for the invariant it establishes, but it is NOT a read-only assertion: it unlinks first.
   * Callers reach it after a repair has already discharged the journal, so the unlink is normally a
   * no-op — it exists so a journal can never outlive the id it describes.
   */
  assertNoIntent(id: string, context: string): Promise<void>
  /** Recreates the reserved staging directory if it disappeared while this instance was running. */
  ensureTempDir(): Promise<void>
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
  const { tempDir, tempDirName, useHashPrefix } = options

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
  // Probed with `stat` alone rather than gated on `existPath` first. `existPath` tests F_OK|R_OK, so a
  // present-but-UNREADABLE entry at the reserved path (mode or ACL damage, exactly the state a legacy
  // content id can be left in) read as absent — and `mkdir` then failed with a bare `EEXIST` instead of the
  // actionable message below, which is the one thing this check exists to produce. `existsForInvariant` is
  // not the right probe either: a DIRECTORY here is the normal, expected state.
  let tempDirStat: { isDirectory(): boolean } | undefined
  try {
    tempDirStat = await fs.stat(tempDir)
  } catch (err: any) {
    if (err?.code !== 'ENOENT' && err?.code !== 'ENOTDIR') throw err
  }
  if (tempDirStat && !tempDirStat.isDirectory()) {
    throw new Error(
      `Refusing to start: the reserved temp path '${tempDirName}' under the storage root exists as a file — ` +
        `likely a pre-existing content id. Migrate it out or configure a different tempDirectoryName.`
    )
  }
  // Created up front so storeStream can stage into it without a per-write mkdir.
  await fs.mkdir(tempDir, { recursive: true })

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
  if (!useHashPrefix) {
    const markerPath = path.join(tempDir, OWNERSHIP_MARKER)
    const refuseToStart = (reason: string): never => {
      throw new Error(
        `Refusing to start: ${reason} In disablePrefixHash mode the reserved temp directory '${tempDirName}' may hold ` +
          `pre-existing content ids that the reservation would hide from retrieval and enumeration. ` +
          `Migrate those files out of '${tempDirName}', configure a different tempDirectoryName, or restore the ` +
          `'${OWNERSHIP_MARKER}' marker (with its original content) if they are staging leftovers from a previous run.`
      )
    }
    // `existsForInvariant`, not `existPath`: an unreadable marker read as absent took the `else` branch,
    // where `readdir` sees the marker itself and refuses to start with a remedy telling the operator to
    // restore a marker that is already there — a dead end. This way the unreadable marker reaches the
    // `readFile` below, whose failure names the real problem.
    if (await existsForInvariant(markerPath)) {
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
  // sha256, not sha1: ids can be caller-supplied, and two ids sharing an intent filename would let a
  // commit for one clobber or clear the other's live journal — they take DIFFERENT path locks, so
  // nothing serializes them — leaving the second id's mixed state unrepaired. Chosen-prefix SHA-1
  // collisions are purchasable; this costs one hash per commit that has a counterpart.
  const intentPathFor = (id: string): string =>
    path.join(tempDir, `${createHash('sha256').update(id).digest('hex')}.intent`)

  /**
   * Intent journals this process has written and not yet PROVEN removed.
   *
   * A journal only ever exists after a counterpart cleanup failed, and `reconcile()` guarantees the disk
   * holds none once construction returns — it applies every journal it finds or refuses to start. So in
   * the steady state this set is empty, and "does this id have a pending intent?" is answerable without
   * touching the filesystem. It used to cost, per operation: one `stat` on every store (the pre-commit
   * repair probe) and one `stat` plus one `unlink` plus one more `stat` on every DELETE — measured at
   * 56.2 -> 38.5 µs per deleted id at the storage's own concurrency, ~17.7 s per million ids, to ask a
   * question whose answer was provably "no".
   *
   * Entries are added BEFORE the journal write and removed only once its absence is proven, so the set
   * is never optimistic in the dangerous direction: a rejected write that still landed a valid journal
   * (the NFS/FUSE report-at-close case) stays tracked. Like `hasLegacyIntents`, `knownDirectories` and
   * the staged-file ownership model, this relies on the documented exclusive-root ownership — a second
   * live instance over one root is out of contract.
   */
  const liveIntentPaths = new Set<string>()

  /**
   * Whether this root still holds journals named by the pre-sha256 scheme.
   *
   * Set once, by `reconcile()`, which already lists the staging directory at construction. Every
   * legacy lookup is gated on it, so a deployment that has never seen an older version — which is
   * every deployment after its first clean boot — pays nothing: without the gate, `findIntentPath`
   * cost a second `stat` on every commit and every delete, and `assertNoIntent` a second unlink AND
   * stat, measured at +43% syscalls on the delete path this library explicitly optimises.
   */
  let hasLegacyIntents = false

  /** The name a pre-sha256 version of this library would have used. Read, never written. */
  const legacyIntentPathFor = (id: string): string =>
    path.join(tempDir, `${createHash('sha1').update(id).digest('hex')}.intent`)

  /** Whether `intentPath` is the journal slot of `id`, under either the current or the legacy name. */
  const isIntentPathFor = (id: string, intentPath: string): boolean =>
    intentPath === intentPathFor(id) || (hasLegacyIntents && intentPath === legacyIntentPathFor(id))

  /**
   * The journal actually on disk for this id, if any — checking the legacy name too, so an intent
   * left by an older version is still found by a repair, a delete or a retried store rather than
   * surviving as an orphan that fails the next construction.
   */
  async function findIntentPath(id: string): Promise<string | undefined> {
    const candidates = hasLegacyIntents ? [intentPathFor(id), legacyIntentPathFor(id)] : [intentPathFor(id)]
    for (const candidate of candidates) {
      if (await existsForInvariant(candidate)) return candidate
    }
    return undefined
  }

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
      if (!(await existsForInvariant(intentPath))) {
        liveIntentPaths.delete(intentPath)
        return
      }
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
    try {
      await fs.unlink(intentPath)
      // Removed by us. Still verified below: this invariant is must-succeed, and an adapter that
      // resolves an unlink without removing anything is exactly what the verification is for. Rare —
      // a journal only exists after a counterpart cleanup failed.
    } catch (err: any) {
      const code = err?.code
      // The unlink ITSELF proved the journal is absent, so the follow-up `stat` would be asking a
      // question already answered. This is the ordinary case — no journal exists — and it is on the
      // delete path, which a GC sweep runs per id.
      if (code === 'ENOENT' || code === 'ENOTDIR') {
        liveIntentPaths.delete(intentPath)
        return
      }
      // Any other failure decides nothing; the verification below does.
    }
    let survived: boolean
    try {
      survived = await existsForInvariant(intentPath)
    } catch (err: any) {
      // An unprovable removal is as dangerous as a survivor — the journal may still be there — but a
      // raw EACCES escaping here loses the `context` every call site constructs, which is the only
      // thing telling an operator WHICH invariant broke. Re-raise as the same failure, with the cause.
      throw new Error(
        `${context}: the intent journal '${intentPath}' could not be proven removed. ` +
          `Original error: ${err instanceof Error ? err.message : String(err)}`
      )
    }
    if (survived) {
      throw new Error(`${context}: the intent journal '${intentPath}' could not be removed.`)
    }
    liveIntentPaths.delete(intentPath)
  }

  /**
   * Recreates the reserved staging directory if it disappeared while this instance was running.
   *
   * Hoisted out of the returned object so the journal write can heal itself the same way a staged write
   * does — both land in this directory, so both have to survive it going away.
   */
  async function ensureTempDir(): Promise<void> {
    await fs.mkdir(tempDir, { recursive: true })
    // The marker is not a check, it is the EVIDENCE the next construction consumes: without it, a
    // flat-mode root whose staging directory was healed refuses to start as soon as a crash leaves
    // one staged file behind ("contains files this storage cannot prove it owns"). Healing would
    // otherwise turn a transient outage into a permanent, operator-only one.
    if (!useHashPrefix && !(await existsForInvariant(path.join(tempDir, OWNERSHIP_MARKER)))) {
      await pipe(
        Readable.from([Buffer.from(OWNERSHIP_MARKER_CONTENT)]),
        fs.createWriteStream(path.join(tempDir, OWNERSHIP_MARKER))
      )
    }
  }

  async function writeIntent(op: Representation, id: string, stagedPath: string): Promise<string> {
    const intentPath = intentPathFor(id)
    // The staged BASENAME lets reconciliation prove whether the commit rename landed: renames
    // consume the staged file, so "staged still present" means the rename provably never happened.
    // Stored as a basename (not an absolute path) so a root remount cannot poison the journal.
    const body = JSON.stringify({ op, id, staged: path.basename(stagedPath) })
    // Tracked BEFORE the write, not after: a write that REJECTS can still have landed a complete journal
    // (a filesystem that reports its error at close), and an untracked journal on disk is one a later
    // repair would never look for.
    liveIntentPaths.add(intentPath)
    try {
      await pipe(Readable.from([Buffer.from(body)]), fs.createWriteStream(intentPath))
    } catch (err) {
      // The reserved directory disappearing under a live instance is healed here for the same reason
      // `pipeToStaged` heals it: without this the caller's `writingUnder` responds to the ENOENT by
      // invalidating the SHARD directory cache entry, which was never the problem, so the
      // misattribution this journal write was the last place still making. This store still fails (its
      // source is consumed); what is restored is the directory, so the next one does not inherit a
      // permanently broken instance.
      if ((err as { code?: string } | null)?.code === 'ENOENT') {
        await ensureTempDir().catch(() => undefined)
      }
      throw err
    }
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
      // TYPE, not just truthiness. `!id` passes any truthy non-string, and the very next check hashes
      // it: `createHash('sha256').update(12345)` throws a TypeError, which `reconcile()` wraps into
      // `Refusing to start: The "data" argument must be of type string…` while KEEPING the journal — so
      // every subsequent boot failed the same way, permanently, over a body this branch was written to
      // discard. `staged` is typed too: it survived only because the regex coerced it.
      typeof id !== 'string' ||
      typeof staged !== 'string' ||
      !id ||
      !STAGED_FILE_NAME.test(staged) ||
      // The intent path is a deterministic function of the id: a body whose id does not hash to
      // this filename is corruption or operator error, and applying it would reconcile the WRONG
      // id. Treat it as malformed. Either hash name is accepted so an intent written before the
      // sha256 change still reconciles.
      !isIntentPathFor(id, intentPath)
    ) {
      // A partial/malformed intent means its commit never started (intents are written before
      // renames): discard it; an orphaned staged file, if any, is handled by the sweep.
      //
      // LOGGED, unlike before: this was the one reconciliation outcome that left no trace, and the
      // assumption behind discarding silently — that a malformed body can only mean the write never got
      // going — holds against process death but not against power loss, where the commit rename's
      // metadata can be journaled while the intent file's data blocks are not. A zero-length intent
      // beside a genuinely mixed on-disk state then reads as "never started" and the id keeps serving
      // two versions, one per read path, with nothing anywhere saying so.
      // Logged AFTER the removal is proven, not before: `removeIntentOrThrow` can throw, and announcing the
      // discard first produced a log line saying the journal WAS discarded immediately followed by a
      // `Refusing to start` saying it was kept. `byteLength`, not `body.length`, because `body` is a decoded
      // utf8 string and code units are the wrong unit precisely for the torn/zero-length write this
      // diagnoses.
      await removeIntentOrThrow(intentPath, 'Discarding a malformed intent journal failed')
      logger.warn(`Discarded a malformed intent journal; if content for this id reads inconsistently, repair it`, {
        intent: path.basename(intentPath),
        bytes: Buffer.byteLength(body, 'utf8')
      })
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
    const gzipPath = gzipPathOf(filePath)
    const primaryPath = op === 'raw' ? filePath : gzipPath
    const counterpartPath = op === 'raw' ? gzipPath : filePath
    if (!(await existsForInvariant(primaryPath))) {
      throw new Error(
        `Cannot reconcile the interrupted ${op} commit for ${id}: neither its staged file nor its committed ` +
          `representation exists.`
      )
    }
    // Reconciliation resolves in favour of the COMMITTED representation, and deliberately does not
    // try to validate it first. A power loss can leave the renamed primary zero-length or truncated,
    // but the counterpart is exposed to exactly the same failure and cannot be validated at all — raw
    // content has no format to check — so preferring it is never provably safer. An earlier attempt
    // here rejected a gzip primary under 20 bytes and kept the raw instead; measured, that replaced a
    // loud `Z_DATA_ERROR` on read with a silently served 0-byte body, and contradicted the documented
    // rule. Content is content-addressed: a consumer must detect and discard unreadable content, and
    // a corrupt primary that fails loudly is the outcome that lets it.
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
    // Nothing is on disk to repair unless this process wrote a journal it could not clear — see
    // `liveIntentPaths`. Construction has already applied (or refused to start over) every journal a
    // previous process left, so the ordinary store and delete pay no syscall for this question.
    if (liveIntentPaths.size === 0) return
    const intentPath = await findIntentPath(id)
    if (intentPath) {
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
    signal?: AbortSignal,
    onCommitted?: () => void,
    onCounterpartRemoved?: () => void
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
    let intentPath: string | undefined
    if (hadCounterpart) {
      try {
        intentPath = await writeIntent(op, id, stagedPath)
      } catch (err) {
        // A REJECTED journal write can still have left a complete, valid journal on disk: the body is
        // one small buffer, so a filesystem that reports its write error at close (NFS, FUSE, a custom
        // adapter) fails the pipe after the bytes have landed. Nothing else here catches this, so the
        // intent survived while the caller's ordinary staging cleanup destroyed the staged file that
        // proves the rename never happened — and the next construction, finding a journal whose staged
        // file AND whose primary are both absent, REFUSED TO START, permanently, over content that was
        // never damaged. Clear it on the same must-succeed terms as a failed rename.
        await clearIntentOrThrowPreservingProof(intentPathFor(id), stagedPath, id, err)
        throw err
      }
    }
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
    let renamed = false
    try {
      await rename(stagedPath, primaryPath)
      renamed = true
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
    // Outside the try: a throwing callback must not be mistaken for a failed rename, which would
    // discharge the journal over a commit that DID land and leave a permanent mixed state with
    // nothing left to reconcile it.
    if (renamed) onCommitted?.()
    if (!hadCounterpart) {
      // Nothing to remove, so the counterpart is trivially gone. Reported so a caller can use one rule
      // for both shapes rather than special-casing the fresh-id commit.
      onCounterpartRemoved?.()
    }
    if (hadCounterpart) {
      const unlinked = await noFailUnlink(counterpartPath)
      let counterpartGone: boolean
      try {
        counterpartGone = !(await existsForInvariant(counterpartPath))
      } catch (verifyErr) {
        // Possibly mixed and unprovable: quarantine so reads repair-or-refuse instead of serving it.
        unreconciledIds.add(id)
        // The unlink may well have SUCCEEDED and only the verification failed (an EIO/EACCES on the shard),
        // in which case the counterpart really is gone and the caller's bookkeeping for it is stale. Without
        // this the decompress cache kept counting a deleted file's bytes against its budget until an
        // eviction pass happened to retry that path — and if the same shard fault kept failing, forever.
        if (unlinked) onCounterpartRemoved?.()
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
      // Proven gone: past the `counterpartGone` guard above, which throws otherwise. Reported before
      // the journal discharge, which can fail on its own without resurrecting the counterpart.
      onCounterpartRemoved?.()
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

    // The staging directory is created once, at construction. If something removes it while this
    // instance is live, EVERY store and every gzip range read fails at its staged write — forever,
    // because nothing recreated it. Shard directories already self-heal exactly this way; the
    // reserved directory was the one place that did not. Only the directory is recreated: the
    // ownership marker and its checks belong to construction, which already proved this root is ours.
    ensureTempDir,

    // Commits a staged file onto its canonical primary path and removes the other representation.
    // The logical object spans two paths (raw and .gzip) but only the rename is atomic, so when a
    // counterpart exists an intent is journaled FIRST: if the process dies (or the unlink fails)
    // between the rename and the cleanup, the next construction reconciles the mixed state in favor
    // of the representation committed here, instead of reads preferring the stale counterpart. Must
    // be called while holding the path lock.
    async commitRepresentation(
      op,
      id,
      stagedPath,
      primaryPath,
      counterpartPath,
      rename,
      signal,
      onCommitted,
      onCounterpartRemoved
    ): Promise<void> {
      try {
        await doCommitRepresentation(
          op,
          id,
          stagedPath,
          primaryPath,
          counterpartPath,
          rename,
          signal,
          onCommitted,
          onCounterpartRemoved
        )
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
      // Deliberately NOT gated on `liveIntentPaths`, unlike `repairPendingIntent`. This is the
      // defensive check that a journal never outlives its id, so making it conditional on the
      // in-memory view of which journals exist would make it vacuous — it could then only ever fire in
      // the cases the view already knows about, which are exactly the ones `repairPendingIntent`
      // handled. `removeIntentOrThrow` is what was made cheap instead: an `unlink` answering ENOENT is
      // itself the proof of absence, so the ordinary case costs one syscall rather than two.
      await removeIntentOrThrow(intentPathFor(id), context)
      // Only when this root actually carries pre-sha256 journals: such an intent must not survive a
      // delete of its id, or the next construction refuses to start over a journal whose id no longer
      // has either representation. Gated so the ordinary path keeps its single unlink+stat.
      if (hasLegacyIntents) {
        await removeIntentOrThrow(legacyIntentPathFor(id), context)
      }
    },

    async ensureReconciled(id: string): Promise<boolean> {
      if (!unreconciledIds.has(id)) return true
      // Inside the try that makes this method's documented "never throws" true.
      //
      // Belt and braces rather than a reachable path: `unreconciledIds` never survives a restart, so an id
      // is only ever quarantined by a commit in THIS process, which already resolved it — and
      // `resolveFilePath` is deterministic for a live instance's configuration. Kept because the guarantee
      // this method advertises should not depend on that reasoning staying true, and because if it ever did
      // throw, the escape turned the repair gate's boolean answer into a rejection out of
      // `assertNotQuarantined`.
      let filePath: string
      try {
        filePath = await resolveFilePath(id)
      } catch (err) {
        logger.warn(`Read-triggered repair of the mixed state for ${id} failed; reads stay refused`, {
          error: err instanceof Error ? err.message : String(err)
        })
        return false
      }
      return withPathLock(filePath, async () => {
        if (!unreconciledIds.has(id)) return true
        try {
          const intentPath = await findIntentPath(id)
          if (intentPath) {
            await applyPendingIntent(intentPath)
          } else {
            // No journal left: the mixed state was repaired elsewhere (retried store, delete).
            unreconciledIds.delete(id)
          }
          return !unreconciledIds.has(id)
        } catch (err) {
          // The caller reports the id as unreadable; without this the CAUSE appeared nowhere at all,
          // leaving an operator with "could not be repaired" and no way to find out why.
          logger.warn(`Read-triggered repair of the mixed state for ${id} failed; reads stay refused`, {
            error: err instanceof Error ? err.message : String(err)
          })
          return false
        }
      })
    },

    async reconcile(): Promise<void> {
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
      const intents = entries.filter((entry) => INTENT_FILE_NAME.test(entry)).sort()
      // Decided before anything is applied, and never re-armed: after this boot discharges them, no
      // legacy-named journal can exist again (only `intentPathFor` is ever written).
      hasLegacyIntents = intents.some((name) => name.length === '.intent'.length + 40)
      for (const name of intents) {
        try {
          await applyPendingIntent(path.join(tempDir, name))
        } catch (err: any) {
          // The remedy has to name what an operator can actually DO. The previous wording said "fix the
          // underlying filesystem issue (permissions, immutability)", which is right for a journal that
          // could not be removed but wrong for the other way this fails: an intent whose id has neither a
          // staged file nor a committed representation is unrepairable no matter how healthy the
          // filesystem is, and the only way forward is to delete the journal — a file the message never
          // named, in a directory documented as reserved.
          throw new Error(
            `Refusing to start: ${err instanceof Error ? err.message : String(err)} ` +
              `The intent journal '${name}' under '${tempDirName}' was kept, so this will fail identically on every ` +
              `restart until it is resolved. If the cause is a filesystem fault (permissions, immutability, a ` +
              `read-only mount), fix that and restart. If the journal is unrepairable — its id has neither a staged ` +
              `file nor a committed representation, so there is nothing left to reconcile — remove ` +
              `'${path.join(tempDir, name)}' to discard the repair instruction, then restart; the id's content is ` +
              `re-fetchable because it is content-addressed.`
          )
        }
      }
    },

    // Removes temp files left behind by writes interrupted in a previous run. Staged filenames carry
    // this boot's random prefix, so a staged-shape file with a different prefix is by construction a
    // leftover of an earlier process — a write racing this sweep stages under the current bootId and
    // is never touched. Best-effort: a missing dir or a failed unlink is ignored.
    async sweepOrphanedTempFiles(): Promise<number> {
      // Ownership of the reserved dir is a construction invariant (see the OWNERSHIP_MARKER logic
      // above): if this storage is running, everything staged-shaped in there is ours.
      let entries: string[]
      try {
        entries = await fs.readdir(tempDir)
      } catch {
        return 0
      }
      // A staged file NAMED BY a surviving intent is that intent's proof the rename never landed —
      // never sweepable garbage. Reconciliation runs before this sweep within one process, so the
      // set is normally empty; it matters when a root is shared (out of contract, but the sweep is
      // where one instance could otherwise erase another's proof and make its next reconciliation
      // misread a pre-rename intent as a completed commit, deleting a valid representation).
      const claimedByAnIntent = new Set<string>()
      for (const name of entries.filter((entry) => INTENT_FILE_NAME.test(entry))) {
        try {
          const { staged } = JSON.parse(await fs.readFile(path.join(tempDir, name), 'utf8'))
          if (typeof staged === 'string') claimedByAnIntent.add(staged)
        } catch {
          // A malformed or unreadable intent claims nothing; reconciliation discards it separately.
        }
      }
      const sweepable = entries.filter(
        (entry) => STAGED_FILE_NAME.test(entry) && !entry.startsWith(`${bootId}-`) && !claimedByAnIntent.has(entry)
      )
      // Bounded-concurrent rather than one at a time: after a crash that left thousands of staged
      // files this ran as thousands of serialized unlinks, and it runs at startup.
      const outcomes = await mapWithConcurrency(sweepable, SWEEP_CONCURRENCY, (entry) =>
        noFailUnlink(path.join(tempDir, entry))
      )
      return outcomes.filter(Boolean).length
    }
  }
}
