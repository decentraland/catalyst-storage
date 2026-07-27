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
  const entries = new Map<string, { size: number; lastAccess: number }>()
  let totalCacheSize = 0

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
  const pins = new Map<string, number>()

  function pin(filePath: string, graceMs: number): () => void {
    pins.set(filePath, (pins.get(filePath) ?? 0) + 1)
    let released = false
    const release = (): void => {
      if (released) return
      released = true
      const remaining = (pins.get(filePath) ?? 1) - 1
      if (remaining > 0) pins.set(filePath, remaining)
      else pins.delete(filePath)
    }
    const expiry = setTimeout(release, graceMs)
    // Never keep the process alive for a pin; the cache is a disk-space optimisation, not work in flight.
    expiry.unref?.()
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
    await new Promise<void>((resolve) => inflationQueue.push(resolve))
    activeInflations++
  }

  function releaseInflationSlot(): void {
    activeInflations--
    inflationQueue.shift()?.()
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
    entry: { size: number; lastAccess: number },
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
  ): Promise<void> {
    await withPathLock(filePath, async () => {
      if (entries.get(filePath) !== entry) return
      if (expectedLastAccess !== undefined && entry.lastAccess !== expectedLastAccess) return
      await noFailUnlink(filePath)
      // Keep the tracking when the file survives the unlink, so the next eviction tick retries it
      // instead of leaving an untracked (unaccounted, never-retried) cache file on disk.
      if (await existsForInvariant(filePath)) return
      totalCacheSize -= entry.size
      entries.delete(filePath)
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
    entry: { size: number; lastAccess: number },
    expectedLastAccess?: number
  ): Promise<void> {
    try {
      await evictEntry(filePath, entry, expectedLastAccess)
    } catch (error) {
      logger.warn(`Could not evict the cached decompressed file at ${filePath}; it stays tracked for a later retry`, {
        error: error instanceof Error ? error.message : String(error)
      })
    }
  }

  /** Set when an admission crossed the budget while a pass was already running. */
  let evictionRequestedAgain = false
  /** Set when a pass ends over budget having freed nothing, so retries wait for the timer. */
  let evictionStalled = false
  /**
   * How many entries were tracked when the stall was recorded.
   *
   * The stall exists to stop admission-triggered passes from spinning on a failing unlink, but it was
   * cleared ONLY by `evict()`, which admission never reaches while stalled — so the only thing that could
   * clear it was the interval installed by `start()`, and a storage used without `start()` lost on-disk
   * bounding permanently (measured: 5 of 5 decompressed copies retained against a 10-byte budget, versus
   * 1 of 5 with the timer running). The FIRST pass is routinely unproductive — a single entry over budget
   * has nothing evictable but the protected MRU entry — so this was reachable on an ordinary config.
   *
   * Comparing the entry count is what distinguishes the two stall causes without conflating them: a new
   * admission means a new eviction candidate exists (the entry protected as most-recent no longer is), so
   * a retry can now make progress; a failing unlink with no new entries still waits for the timer.
   */
  let stalledAtEntryCount = 0

  async function runEviction() {
    const now = Date.now()
    const before = totalCacheSize

    // TTL eviction. Entries are visited oldest-first, so the first one still inside its TTL means
    // every entry behind it is too — no need to walk the rest of the tracker.
    for (const [filePath, entry] of entries) {
      if (now - entry.lastAccess <= options.ttl) break
      await evictEntrySafely(filePath, entry, entry.lastAccess)
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
        if (pins.has(filePath)) continue
        await evictEntrySafely(filePath, entry, entry.lastAccess)
      }
    }

    // Entries admitted DURING this pass are missing from the snapshot above, and `evict()` turned
    // every `record()` in that window into a no-op by handing back the in-flight promise. Without a
    // re-arm a burst of range requests settled ~9x over budget and stayed there until the next timer
    // tick. Only re-run while progress is being made: a pass that frees nothing (an unlinkable file,
    // or nothing evictable left besides the protected MRU entry) would otherwise spin on every
    // admission, retrying a failing unlink and logging each time.
    const freedSomething = totalCacheSize < before
    if (totalCacheSize > options.maxSize && !freedSomething) {
      evictionStalled = true
      stalledAtEntryCount = entries.size
      return
    }
    evictionStalled = false
    if ((evictionRequestedAgain || totalCacheSize > options.maxSize) && freedSomething) {
      evictionRequestedAgain = false
      await runEviction()
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
      entries.set(filePath, { size, lastAccess: Date.now() })
      totalCacheSize += size
      // The budget was previously consulted ONLY by the periodic sweep, so it bounded nothing between
      // ticks: a burst of range requests over distinct gzip-only ids (a sync or backfill pass is
      // exactly that shape) could each inflate up to `decompressMaxFileSize` and write hundreds of
      // gigabytes against a 5GB budget before the first tick fired. Crossing the limit now triggers
      // an eviction immediately. `evict()` deduplicates itself, so a burst schedules one pass, not
      // one per entry; it is deliberately not awaited — `record` runs inside a commit holding this
      // path's lock, and the eviction it starts needs that same lock.
      // A stall that was recorded with FEWER entries than are tracked now is no longer good evidence that
      // a pass would be unproductive: this admission is itself a fresh candidate. See `stalledAtEntryCount`.
      if (evictionStalled && entries.size > stalledAtEntryCount) {
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
