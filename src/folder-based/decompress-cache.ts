import { ILoggerComponent } from '@well-known-components/interfaces'
import { FsInvariants } from './fs-invariants'

/**
 * Marks whether the decompression that owns it is still relevant. A decompression inflates whatever
 * gzip existed when it started; if the id is overwritten or deleted before the decompression
 * commits, its output is stale and must be discarded instead of clobbering the newer canonical file
 * (or resurrecting a deleted one).
 */
export type InvalidationToken = { invalidated: boolean }

/**
 * Tracking for the decompressed copies this storage writes to disk when serving a range request for
 * gzip-only content, plus the concurrency control that keeps those writes from racing each other or
 * the canonical paths they land on.
 *
 * The cache and the locking live together because they are one concern: an entry is only ever
 * created, promoted or evicted while holding the lock for its path, which is what makes "is this
 * file a derived cache or primary content?" a decidable question at every point.
 */
export type DecompressCache = {
  /**
   * Serializes commits (rename/write/unlink) on a canonical path so a store, a delete and a
   * decompression can never interleave their final steps. Only the short commit sections take the
   * lock — long-running pipes stay outside — and the map entry is removed once its chain drains.
   */
  withPathLock<T>(filePath: string, fn: () => Promise<T>): Promise<T>
  /**
   * Runs `inflate` at most once per path across concurrent callers, and gives it the invalidation
   * token for that path. The in-flight promise is registered synchronously — no `await` between the
   * lookup and the store — so simultaneous callers share a single decompression; otherwise both
   * would pass the "not in flight" check, write the same cache file concurrently (corrupting it) and
   * double-count its size against the cache total.
   *
   * The token is registered BEFORE `inflate` runs: any store/delete committing after that point
   * marks it, so stale output is discarded, while one committing before it means the gzip the
   * inflation opens is already the newest version.
   */
  deduplicateInflation(filePath: string, inflate: (token: InvalidationToken) => Promise<void>): Promise<void>
  /** Marks an in-flight decompression for this path stale. Called by writers inside their locked commit. */
  invalidateInflight(filePath: string): void
  /** Every in-flight decompression, so shutdown can await them. */
  inflight(): Iterable<Promise<void>>
  /** Starts tracking a freshly committed decompressed file. Must be called under the path lock. */
  record(filePath: string, size: number): void
  /**
   * Protects `filePath` from LRU eviction while a read still needs it, returning the release function.
   *
   * `retrieve` hands back a LAZY ContentItem, so between committing a decompressed file and the consumer
   * opening it there is a window in which LRU could unlink it and turn present content into an ENOENT.
   * `graceMs` bounds the pin so an item that is never consumed cannot exempt its entry indefinitely.
   * TTL eviction is not affected: an entry past its TTL is stale regardless of who is holding it.
   *
   * Bound to the entry tracked at the time of the call: a LATER cache file at the same path is a different
   * thing and is not protected by this pin. A path with no tracked entry has nothing to protect, so the
   * returned release is a no-op.
   */
  pin(filePath: string, graceMs: number): () => void
  /**
   * Drops the tracking entry WITHOUT unlinking the file. Used when the canonical path stops being a
   * derived cache and becomes primary content (a store landed there): a stale entry would let
   * TTL/size eviction delete the only copy of the new content.
   */
  forget(filePath: string): void
  /**
   * Removes a tracked cache file and its entry, reporting whether the path was cached at all.
   * Throws when the file survives: reporting the cached raw as handled while it survives would let
   * `delete()` remove the gzip and resolve — resurrecting the untracked cache file as readable
   * primary content after a "successful" delete.
   */
  remove(filePath: string): Promise<boolean>
  /** Refreshes the last-access time used by TTL and LRU eviction. */
  touch(filePath: string): void
  /** Whether this path is currently tracked as a derived cache file. */
  isTracked(filePath: string): boolean
  /** Runs one eviction pass (TTL, then LRU down to the size budget), deduplicated while in flight. */
  evict(): Promise<void>
  /** Evicts every tracked file, to prevent disk leaks across restarts. */
  evictAll(): Promise<void>
}

export type DecompressCacheOptions = {
  /** TTL in milliseconds after which an untouched cached file is evicted. */
  ttl: number
  /** Total size budget in bytes; the least recently used entries are evicted past it. */
  maxSize: number
  /**
   * How many inflations may run at once. Admission is checked only AFTER an inflated file has been
   * committed, and the eviction it triggers is deliberately not awaited (it needs the path lock the
   * caller still holds), so this is what actually bounds the overshoot: worst case
   * `maxConcurrentInflations × decompressMaxFileSize` above `maxSize`. Unbounded, it was
   * `request concurrency × decompressMaxFileSize` — measured at 36x over budget with 50 concurrent cold
   * range reads, and ~12.8 GB of derived files at the shipped defaults.
   *
   * Excess inflations QUEUE rather than fail: a range read of present content must still be served.
   *
   * Optional so a caller that does not care about the overshoot bound (the tests that exercise eviction
   * directly) need not state one; the storage component always passes its configured value.
   */
  maxConcurrentInflations?: number
}

/** Used when `maxConcurrentInflations` is not given. The storage component supplies its own default. */
const DEFAULT_MAX_CONCURRENT_INFLATIONS_FALLBACK = 4

/**
 * How long a stalled eviction waits before an admission may retry it.
 *
 * A stall means the last pass freed nothing while over budget — either a failing unlink, or nothing
 * evictable but protected entries. The first is worth retrying rarely (it needs the underlying fault to be
 * fixed), the second as soon as a new candidate exists. One second serves both: it keeps a degraded mount
 * from re-running a full failing pass per inflation, and is far below the eviction interval, so a burst
 * still converges without waiting for the timer.
 */
const STALL_RETRY_INTERVAL_MS = 1_000

export function createDecompressCache(
  components: { logger: ILoggerComponent.ILogger; fsInvariants: FsInvariants },
  options: DecompressCacheOptions
): DecompressCache {
  const { logger } = components
  const { existsForInvariant, noFailUnlink } = components.fsInvariants

  // LRU cache tracker for decompressed gzip files written to disk.
  //
  // The Map's INSERTION ORDER is maintained as its access order: `record` inserts at the back and
  // `touch` re-inserts, so iterating from the front visits least-recently-used first. That is what lets
  // eviction skip sorting. The alternative — sorting the whole tracker on every pass — is not a small
  // cost at realistic sizes, because the entry count is `maxSize / average inflated file`: a 5GB budget
  // over 50KB files is 100k entries, measured at 78ms of BLOCKING event-loop time per pass (3.9ms at
  // 5k, 17ms at 20k) versus 0.055ms for the ordered walk. `record()` can start a pass from inside a
  // range read, so that was latency on the read path.
  const entries = new Map<string, { size: number; lastAccess: number; generation: number }>()
  let totalCacheSize = 0
  /**
   * Distinguishes successive cache files at the SAME path, so a pin cannot outlive the file it was taken for.
   * Monotonic and never reused; see `pin`.
   */
  let nextGeneration = 0

  // Concurrency guard: prevents multiple simultaneous decompressions of the same file
  const inflightDecompressions = new Map<string, Promise<void>>()

  // Bounded by in-flight decompressions.
  const inflightTokens = new Map<string, InvalidationToken>()

  /**
   * Admission gate for inflations, bounding how far the cache can run over budget.
   *
   * FIFO so a burst cannot starve its earliest arrival. Held across the whole inflation — including the
   * commit rename and `record` — so `maxConcurrentInflations` really is the number of inflated files that
   * can be in flight at once, which is what makes the overshoot bound in `DecompressCacheOptions` true.
   */
  /**
   * Paths a range read is currently depending on, by reference count.
   *
   * Held from just before an inflation until the read that caused it has produced its item, so LRU cannot
   * unlink the file that read is about to open. Every pin carries a timer that releases it, because the
   * item `retrieve` returns is LAZY and a consumer is free never to open it — without the expiry a
   * never-consumed item would pin its entry for the life of the process and quietly exempt it from the
   * size budget, which is the same class of leak as the untracked orphan this cache already guards.
   */
  const pins = new Map<string, Map<number, number>>()

  function pin(filePath: string, graceMs: number): () => void {
    // Bound to the tracked entry's GENERATION, not just to the path. A path can hold a succession of
    // different cache files — `forget()` when a store promotes it to primary content, `remove()` on a delete,
    // then a later inflation recording a new one — and a path-keyed pin outlived its own file, so a pin taken
    // for one generation went on protecting whatever landed at that path next. That silently exempted an
    // entry nobody was reading from the size budget for the rest of the grace window.
    //
    // No entry means nothing to protect, so the pin is a no-op rather than a promise about a path: eviction
    // only ever walks tracked entries, and the caller (an ordinary raw-primary read) pays nothing.
    const generation = entries.get(filePath)?.generation
    if (generation === undefined) return () => undefined

    const forPath = pins.get(filePath) ?? new Map<number, number>()
    forPath.set(generation, (forPath.get(generation) ?? 0) + 1)
    pins.set(filePath, forPath)

    let released = false
    const release = (): void => {
      if (released) return
      released = true
      const held = pins.get(filePath)
      if (held) {
        const remaining = (held.get(generation) ?? 1) - 1
        if (remaining > 0) held.set(generation, remaining)
        else held.delete(generation)
        if (held.size === 0) pins.delete(filePath)
      }
      // RELEASING A PIN IS AN EVICTION TRIGGER. Passes were driven only by admissions and the interval, so
      // the pass that ran on the LAST admission of a burst saw every entry still pinned, freed nothing, and
      // nothing re-ran it: a burst of range reads left every derived file on disk until the next timer tick,
      // measured at 31x over budget with no tick due for a minute. A release is the moment a candidate
      // becomes evictable, so it is exactly when the budget is worth re-checking. `evict()` deduplicates, so
      // a wave of releases schedules one pass; the stall is cleared because the state it described (nothing
      // evictable) is precisely what just changed.
      if (totalCacheSize > options.maxSize) {
        if (!stalledOnEvictionFailure) evictionStalled = false
        if (inflightEviction) evictionRequestedAgain = true
        else if (!evictionStalled) void evict()
      }
    }
    const expiry = setTimeout(release, graceMs)
    // Never keep the process alive for a pin; the cache is a disk-space optimisation, not work in flight.
    expiry.unref?.()

    // DELIBERATELY NOT CAPPED. Force-releasing the oldest pin at a cap was tried and reverted: it hands the
    // budget back its accuracy by re-exposing a reader to the ENOENT its pin exists to prevent, and failing
    // a read of content that is present — a 5xx, per the read contract — is strictly worse than holding more
    // disk than the budget for a few seconds. What bounds the exposure instead is how long a pin can live
    // (`graceMs`), which is why that window is short. See `pin`'s caller for the resulting bound.
    return () => {
      clearTimeout(expiry)
      release()
    }
  }

  let activeInflations = 0
  const inflationQueue: Array<() => void> = []
  const maxConcurrentInflations = options.maxConcurrentInflations ?? DEFAULT_MAX_CONCURRENT_INFLATIONS_FALLBACK

  async function acquireInflationSlot(): Promise<void> {
    if (activeInflations < maxConcurrentInflations) {
      activeInflations++
      return
    }
    // The slot is TRANSFERRED, so the waiter does not increment on resume — see `releaseInflationSlot`.
    await new Promise<void>((resolve) => inflationQueue.push(resolve))
  }

  function releaseInflationSlot(): void {
    // HAND THE SLOT OVER without dropping it, rather than decrement-then-resolve. A waiter's continuation
    // runs a microtask after its resolve, so decrementing first left `activeInflations` below the limit for
    // that gap and a caller arriving inside it took the slot too — measured at 2 concurrent inflations with
    // a limit of 1. Keeping the count unchanged while the queue is non-empty means the limit holds across
    // the handoff, and only a release with nobody waiting actually frees a slot.
    const next = inflationQueue.shift()
    if (next) {
      next()
      return
    }
    activeInflations--
  }

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

  function forget(filePath: string): void {
    const entry = entries.get(filePath)
    if (entry) {
      totalCacheSize -= entry.size
      entries.delete(filePath)
    }
  }

  // Unlinks an evicted cache file under the path lock, re-checking the entry is still current: a
  // store may have promoted the path to primary content (forgetting the entry) between the eviction
  // scan and this delete — unlinking then would destroy the only copy of the new content.
  async function evictEntry(
    filePath: string,
    entry: { size: number; lastAccess: number; generation: number },
    /**
     * The entry's `lastAccess` when the pass selected it. A pass takes its snapshot and then awaits
     * an unlink per entry, so by the time this runs the entry may have been TOUCHED by a read that
     * started afterwards — and `touch` mutates the same object, so the identity check above still
     * matches and the entry would be evicted anyway. LRU then does the opposite of what it promises:
     * the most recently used file is the one deleted, and the reader that touched it finds its
     * lazily-opened stream gone. Admission-triggered passes made this window wide, because eviction
     * now begins while the caller is still working rather than only on the timer.
     *
     * Omitted by `evictAll`, which is shutdown and must not be deferred by concurrent activity.
     */
    expectedLastAccess?: number
  ): Promise<boolean> {
    return await withPathLock(filePath, async () => {
      if (entries.get(filePath) !== entry) return false
      if (expectedLastAccess !== undefined && entry.lastAccess !== expectedLastAccess) return false
      await noFailUnlink(filePath)
      // Keep the tracking when the file survives the unlink, so the next eviction tick retries it
      // instead of leaving an untracked (unaccounted, never-retried) cache file on disk.
      if (await existsForInvariant(filePath)) return false
      totalCacheSize -= entry.size
      entries.delete(filePath)
      return true
    })
  }

  /**
   * Evicts one entry without letting its failure end the pass.
   *
   * `existsForInvariant` THROWS on anything that is not ENOENT/ENOTDIR — that is its purpose — so a
   * single cache file on a damaged mount (EIO, EACCES) used to abort the whole loop. Map iteration is
   * insertion-ordered, so every entry behind it was skipped and the size-eviction block below was
   * never reached; the poisoned entry is never removed, so EVERY subsequent tick died in the same
   * place and `maxSize` stopped being enforced for the life of the process. The same throw also
   * rejected `stop()` through `evictAll`, leaving every tracked file on disk.
   *
   * The entry stays tracked, so the next tick retries it — recovering on its own once the underlying
   * fault is fixed.
   */
  async function evictEntrySafely(
    filePath: string,
    entry: { size: number; lastAccess: number; generation: number },
    expectedLastAccess?: number
  ): Promise<boolean> {
    try {
      return await evictEntry(filePath, entry, expectedLastAccess)
    } catch (error) {
      logger.warn(`Could not evict the cached decompressed file at ${filePath}; it stays tracked for a later retry`, {
        error: error instanceof Error ? error.message : String(error)
      })
      return false
    }
  }

  /** Set when an admission crossed the budget while a pass was already running. */
  let evictionRequestedAgain = false
  /** Set when a pass ends over budget having freed nothing, so retries wait for the timer. */
  let evictionStalled = false
  /**
   * Admissions counted since this cache was created. MONOTONIC — never decreases.
   *
   * The stall latch needs to distinguish "retrying could now make progress" from "retrying will fail the
   * same way", and it must not be fooled by the tracker SHRINKING. Comparing `entries.size` did both
   * wrongly: `forget`/`remove` (an ordinary store or delete) lower it below the value recorded at stall
   * time, after which `entries.size > stalledAtEntryCount` is permanently false and admission-triggered
   * eviction never runs again — measured at 400 bytes retained against a 10-byte budget with four
   * evictable entries and a working unlink.
   */
  let admissionsSeen = 0
  /** `admissionsSeen` when the stall was recorded, so a NEW admission can be recognised. */
  let stalledAtAdmission = 0
  /** When the stall was recorded, for the backoff that applies to a FAILING stall. */
  let stalledAt = 0
  /**
   * Whether the stalled pass actually TRIED to evict something and failed, as opposed to finding nothing
   * evictable at all.
   *
   * The two causes need opposite retry policies, and conflating them broke the latch in both directions.
   * "Tried and failed" is a damaged mount: retrying per admission re-runs a whole failing pass, and a
   * warning per entry, for every inflation that lands — the spin the latch exists to prevent. "Nothing to
   * try" is the ordinary burst, where every candidate was protected or pinned: the next admission IS a new
   * candidate, retrying is cheap and productive, and rate-limiting it instead let a burst of range reads
   * settle far over budget because no pass could run until the backoff expired.
   */
  let stalledOnEvictionFailure = false

  /**
   * Runs eviction passes until the budget is met, nothing is left to try, or a pass makes no progress.
   *
   * ITERATIVE rather than tail-recursive. Each pass re-runs only when the previous one actually freed
   * something, so the depth was bounded by the entry count — but the module's own sizing note puts that at
   * ~100k entries, and an async self-call suspends a frame per level rather than reusing one. A loop makes
   * the bound structural instead of something to reason about, and `now` is recomputed per pass either way.
   */
  async function runEviction() {
    for (;;) {
      const now = Date.now()
      // Whether any entry was handed to `evictEntrySafely` this pass. See `stalledOnEvictionFailure`.
      let attemptedAnEviction = false
      // How many entries this pass actually REMOVED.
      //
      // Counted, not inferred from the total. `freedSomething = totalCacheSize < before` compared against a
      // snapshot taken at pass start, and every admission landing DURING the pass raises the total past it — so
      // a pass that had evicted perfectly well concluded it freed nothing, stalled, and stopped enforcing the
      // budget. That is what made a burst settle 34x over: 50 admissions arrive while the first pass is on its
      // first `await`, and the pass then measured 14.7 MB against a 600 KB snapshot and gave up.
      let evictedThisPass = 0

      // TTL eviction. Entries are visited oldest-first, so the first one still inside its TTL means
      // every entry behind it is too — no need to walk the rest of the tracker.
      for (const [filePath, entry] of entries) {
        if (now - entry.lastAccess <= options.ttl) break
        // PINNED entries are skipped here as well as in the size pass. "Past its TTL" and "no reader needs it"
        // are different claims, and at a default TTL of an hour against a pin measured in seconds they never
        // disagree — but an aggressively short `decompressCacheTTL` makes them collide, and the whole point of
        // the pin is that a file survives until the read holding it has its descriptor. `continue`, not
        // `break`: a pinned entry must not stop the older ones behind it from being reclaimed. The next pass
        // takes it once the pin is gone; `evictAll` on shutdown ignores pins by design.
        if (pins.get(filePath)?.has(entry.generation)) continue
        attemptedAnEviction = true
        if (await evictEntrySafely(filePath, entry, entry.lastAccess)) evictedThisPass++
      }

      // Size eviction (LRU)
      if (totalCacheSize > options.maxSize) {
        // Walked from the front, which IS least-recently-used first (see `entries`), instead of sorting.
        //
        // The most recently used entry is never evicted here, so a budget smaller than a single
        // decompressed file cannot delete the copy the request that just created it is about to open —
        // `retrieve` builds a LAZY ContentItem, so the file has to outlive the call. With the access
        // ordering that is simply "stop before the last entry". This is only a guarantee for the
        // SINGLE-entry case: with concurrent inflations of different ids the just-recorded entry may not
        // be the most recent by the time a pass runs, and its lazily-opened stream can still fail ENOENT.
        // Callers already have to treat that as a retryable miss (see the read contract). The cache holds
        // at most `maxSize` plus one file; TTL reclaims the survivor.
        //
        // `lastAccess` is read as each entry is CHOSEN, so an entry touched while an earlier unlink was
        // in flight is no longer a valid LRU victim and is skipped by `evictEntry`'s own check.
        let evictable = entries.size - 1
        for (const [filePath, entry] of entries) {
          if (evictable-- <= 0) break
          if (totalCacheSize <= options.maxSize) break
          // A PINNED entry belongs to a range read that is still between committing this file and opening
          // it. "Stop before the last entry" protects exactly one, which is a guarantee for one reader and
          // no more: with concurrent inflations of distinct ids an entry stops being the most recent as
          // soon as another lands, and 20 concurrent reads of present gzip-only ids produced a spurious
          // ENOENT — content that was never missing, surfaced to the caller as a 5xx by the read contract.
          // `continue`, not `break`: a pinned entry must not shield the older ones behind it from eviction.
          if (pins.get(filePath)?.has(entry.generation)) continue
          attemptedAnEviction = true
          if (await evictEntrySafely(filePath, entry, entry.lastAccess)) evictedThisPass++
        }
      }

      // Entries admitted DURING this pass are missing from the snapshot above, and `evict()` turned
      // every `record()` in that window into a no-op by handing back the in-flight promise. Without a
      // re-arm a burst of range requests settled ~9x over budget and stayed there until the next timer
      // tick. Only re-run while progress is being made: a pass that frees nothing (an unlinkable file,
      // or nothing evictable left besides the protected MRU entry) would otherwise spin on every
      // admission, retrying a failing unlink and logging each time.
      const freedSomething = evictedThisPass > 0
      if (totalCacheSize > options.maxSize && !freedSomething) {
        evictionStalled = true
        stalledAtAdmission = admissionsSeen
        stalledAt = Date.now()
        stalledOnEvictionFailure = attemptedAnEviction
        return
      }
      evictionStalled = false
      if ((evictionRequestedAgain || totalCacheSize > options.maxSize) && freedSomething) {
        evictionRequestedAgain = false
        continue
      }
      return
    }
  }

  // Returns the CURRENT in-flight eviction when one is already running, instead of a resolved
  // no-op: the interval callback assigns this to the tracked tick, so a tick firing during a slow
  // eviction must hand back the real promise — otherwise stop() would await the no-op and could
  // resolve while the actual eviction is still unlinking files.
  let inflightEviction: Promise<void> | undefined
  function evict(): Promise<void> {
    // A timer tick always clears the stall: whatever made the last pass unproductive may be gone.
    evictionStalled = false
    if (inflightEviction) {
      evictionRequestedAgain = true
      return inflightEviction
    }
    inflightEviction = runEviction()
      .catch((error) => logger.warn(`Cache eviction failed: ${error}`))
      .finally(() => {
        inflightEviction = undefined
        // CONSUME a request that arrived while this pass was running. `runEviction`'s own re-arm sits after
        // the stall check, so a pass that took the stall return dropped `evictionRequestedAgain` on the floor
        // — and a pass can complete SYNCHRONOUSLY (an unproductive one hits no await), which means the
        // admissions right behind it all find `inflightEviction` still set and can only set that flag. The
        // result was a burst that scheduled no pass at all: nothing was ever reclaimed.
        //
        // Gated on the stall having been cleared, which the admission and pin-release paths only do when
        // something genuinely changed — so a damaged mount still backs off instead of spinning here.
        if (evictionRequestedAgain && totalCacheSize > options.maxSize && !evictionStalled) {
          evictionRequestedAgain = false
          void evict()
        }
      })
    return inflightEviction
  }

  return {
    withPathLock,
    forget,
    evict,
    pin,
    inflight: () => inflightDecompressions.values(),
    invalidateInflight(filePath: string): void {
      const token = inflightTokens.get(filePath)
      if (token) token.invalidated = true
    },
    deduplicateInflation(filePath: string, inflate: (token: InvalidationToken) => Promise<void>): Promise<void> {
      let pending = inflightDecompressions.get(filePath)
      const isOwner = !pending
      if (!pending) {
        pending = (async () => {
          // Queued BEFORE the token is registered and before any inflating starts, so a waiting caller
          // holds no invalidation state and costs nothing but the promise it is parked on. Joiners of an
          // already-in-flight inflation never queue — they share this one.
          await acquireInflationSlot()
          const token: InvalidationToken = { invalidated: false }
          inflightTokens.set(filePath, token)
          try {
            await inflate(token)
          } finally {
            if (inflightTokens.get(filePath) === token) {
              inflightTokens.delete(filePath)
            }
            releaseInflationSlot()
          }
        })()
        inflightDecompressions.set(filePath, pending)
      }
      return pending.finally(() => {
        if (isOwner) inflightDecompressions.delete(filePath)
      })
    },
    record(filePath: string, size: number): void {
      // Drop any existing entry first: `totalCacheSize` is a running sum, so overwriting an entry
      // without subtracting its old size would inflate the total permanently and make the size
      // budget evict content that is not actually over it.
      forget(filePath)
      entries.set(filePath, { size, lastAccess: Date.now(), generation: nextGeneration++ })
      totalCacheSize += size
      // The budget was previously consulted ONLY by the periodic sweep, so it bounded nothing between
      // ticks: a burst of range requests over distinct gzip-only ids (a sync or backfill pass is
      // exactly that shape) could each inflate up to `decompressMaxFileSize` and write hundreds of
      // gigabytes against a 5GB budget before the first tick fired. Crossing the limit now triggers
      // an eviction immediately. `evict()` deduplicates itself, so a burst schedules one pass, not
      // one per entry; it is deliberately not awaited — `record` runs inside a commit holding this
      // path's lock, and the eviction it starts needs that same lock.
      admissionsSeen++
      // A stall recorded before this admission is no longer good evidence that a pass would be
      // unproductive: this admission is itself a fresh eviction candidate (whatever was protected as
      // most-recent or pinned no longer is). Whether that justifies retrying NOW depends on why the pass
      // freed nothing — see `stalledOnEvictionFailure`. A pass that found nothing to try retries
      // immediately, so a burst converges; one whose unlinks failed backs off, so a damaged mount does not
      // re-run a whole failing pass per inflation.
      if (
        evictionStalled &&
        admissionsSeen > stalledAtAdmission &&
        (!stalledOnEvictionFailure || Date.now() - stalledAt >= STALL_RETRY_INTERVAL_MS)
      ) {
        evictionStalled = false
      }
      if (totalCacheSize > options.maxSize && !evictionStalled) {
        if (inflightEviction) {
          evictionRequestedAgain = true
        } else {
          void evict()
        }
      }
    },
    async remove(filePath: string): Promise<boolean> {
      const entry = entries.get(filePath)
      if (!entry) return false
      await noFailUnlink(filePath)
      // Verify before dropping the tracking: reporting the cached raw as handled while it survives
      // would let delete() remove the gzip and resolve — resurrecting the untracked cache file as
      // readable primary content after a "successful" delete.
      if (await existsForInvariant(filePath)) {
        throw new Error(`Failed to remove the cached decompressed content at ${filePath}`)
      }
      totalCacheSize -= entry.size
      entries.delete(filePath)
      return true
    },
    isTracked: (filePath: string) => entries.has(filePath),
    touch(filePath: string): void {
      const entry = entries.get(filePath)
      if (entry) {
        entry.lastAccess = Date.now()
        // Re-inserted at the back so the Map's insertion order stays the access order eviction walks.
        // Mutating `lastAccess` alone would leave the tracker unordered and force a sort per pass.
        entries.delete(filePath)
        entries.set(filePath, entry)
      }
    },
    async evictAll(): Promise<void> {
      // Isolated per entry for the same reason as the periodic pass: one unreadable file must not
      // reject stop() and strand every other cached file on disk.
      for (const [filePath, entry] of entries) {
        await evictEntrySafely(filePath, entry)
      }
    }
  }
}
