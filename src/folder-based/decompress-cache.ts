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
   * Starts tracking a decompressed file this instance did NOT write — one left on disk by a previous
   * run that never reached `stop()`. Returns whether it was adopted; an already-tracked path is
   * ignored, so adoption can never double-count against the size budget. Must be called under the
   * path lock, with the caller having just confirmed the path is still a derived cache copy.
   */
  adopt(filePath: string, size: number): boolean
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
}

export function createDecompressCache(
  components: { logger: ILoggerComponent.ILogger; fsInvariants: FsInvariants },
  options: DecompressCacheOptions
): DecompressCache {
  const { logger } = components
  const { existsForInvariant, noFailUnlink } = components.fsInvariants

  // LRU cache tracker for decompressed gzip files written to disk
  const entries = new Map<string, { size: number; lastAccess: number }>()
  let totalCacheSize = 0

  // Concurrency guard: prevents multiple simultaneous decompressions of the same file
  const inflightDecompressions = new Map<string, Promise<void>>()

  // Bounded by in-flight decompressions.
  const inflightTokens = new Map<string, InvalidationToken>()

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
  async function evictEntry(filePath: string, entry: { size: number; lastAccess: number }): Promise<void> {
    await withPathLock(filePath, async () => {
      if (entries.get(filePath) !== entry) return
      await noFailUnlink(filePath)
      // Keep the tracking when the file survives the unlink, so the next eviction tick retries it
      // instead of leaving an untracked (unaccounted, never-retried) cache file on disk.
      if (await existsForInvariant(filePath)) return
      totalCacheSize -= entry.size
      entries.delete(filePath)
    })
  }

  async function runEviction() {
    const now = Date.now()

    // TTL eviction
    for (const [filePath, entry] of entries) {
      if (now - entry.lastAccess > options.ttl) {
        await evictEntry(filePath, entry)
      }
    }

    // Size eviction (LRU)
    if (totalCacheSize > options.maxSize) {
      const sorted = [...entries.entries()].sort((a, b) => a[1].lastAccess - b[1].lastAccess)
      for (const [filePath, entry] of sorted) {
        if (totalCacheSize <= options.maxSize) break
        await evictEntry(filePath, entry)
      }
    }
  }

  // Returns the CURRENT in-flight eviction when one is already running, instead of a resolved
  // no-op: the interval callback assigns this to the tracked tick, so a tick firing during a slow
  // eviction must hand back the real promise — otherwise stop() would await the no-op and could
  // resolve while the actual eviction is still unlinking files.
  let inflightEviction: Promise<void> | undefined
  function evict(): Promise<void> {
    if (inflightEviction) return inflightEviction
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
          const token: InvalidationToken = { invalidated: false }
          inflightTokens.set(filePath, token)
          try {
            await inflate(token)
          } finally {
            if (inflightTokens.get(filePath) === token) {
              inflightTokens.delete(filePath)
            }
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
    },
    adopt(filePath: string, size: number): boolean {
      if (entries.has(filePath)) return false
      entries.set(filePath, { size, lastAccess: Date.now() })
      totalCacheSize += size
      return true
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
    touch(filePath: string): void {
      const entry = entries.get(filePath)
      if (entry) {
        entry.lastAccess = Date.now()
      }
    },
    async evictAll(): Promise<void> {
      for (const [filePath, entry] of entries) {
        await evictEntry(filePath, entry)
      }
    }
  }
}
