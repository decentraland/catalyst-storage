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
   * segment. Default: '.tmp-writes'.
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

  // Created up front so storeStream can stage into it without a per-write mkdir.
  const tempDirName = options?.tempDirectoryName ?? TEMP_DIR_NAME
  if (tempDirName === '' || tempDirName === '.' || tempDirName === '..' || /[/\\]/.test(tempDirName)) {
    throw new Error(`tempDirectoryName must be a single path segment, got: ${JSON.stringify(tempDirName)}`)
  }
  if (USE_HASH_PREFIX && /^[0-9a-f]{4}$/i.test(tempDirName)) {
    throw new Error(
      `tempDirectoryName must not look like a shard directory (4 hex characters) when hash prefixes are enabled, got: ${JSON.stringify(tempDirName)}`
    )
  }
  const tempDir = path.join(root, tempDirName)
  await components.fs.mkdir(tempDir, { recursive: true })

  // Staged files are prefixed with a per-boot random id so the startup sweep can tell leftovers
  // from previous runs (any other prefix) apart from files this instance is writing right now —
  // a write racing the sweep can therefore never have its live staged file unlinked.
  const bootId = randomBytes(8).toString('hex')
  const newTempPath = (): string => path.join(tempDir, `${bootId}-${randomBytes(16).toString('hex')}`)

  // The sweep may only delete files in a temp directory this storage provably owns. With hash
  // prefixes, ids can never resolve into the reserved dir (containment sends them under a shard),
  // so everything inside is ours. In flat mode the dir may predate the reservation and hold
  // legitimate content ids — even ones that coincidentally match the staged-name shape — so
  // ownership must be established explicitly: a marker written by us, or a directory found empty
  // (claimed by writing the marker). A pre-existing non-empty, unmarked flat-mode dir is never
  // swept; its files are preserved and surfaced via the warning below.
  const OWNERSHIP_MARKER = '.owned-by-catalyst-storage'
  let sweepAllowed = USE_HASH_PREFIX
  if (!USE_HASH_PREFIX) {
    const markerPath = path.join(tempDir, OWNERSHIP_MARKER)
    if (await components.fs.existPath(markerPath)) {
      sweepAllowed = true
    } else {
      try {
        const entries = await components.fs.readdir(tempDir)
        if (entries.length === 0) {
          await pipe(
            Readable.from([Buffer.from('reserved by catalyst-storage for atomic write staging\n')]),
            components.fs.createWriteStream(markerPath)
          )
          sweepAllowed = true
        } else {
          // In flat mode the root is the content namespace, so a deployment that predates the
          // reservation may hold content ids under the reserved directory. They are preserved (the
          // sweep is disabled without ownership) but are not addressable while the reservation
          // holds — surface them loudly so the operator can migrate them or pick a different
          // tempDirectoryName.
          logger.warn(
            `Found ${entries.length} pre-existing file(s) under the reserved temp directory '${tempDirName}'. ` +
              `They are preserved on disk (the orphan sweep stays disabled) but are not addressable as content ids; ` +
              `migrate them out or configure a different tempDirectoryName.`
          )
        }
      } catch {
        // best-effort: ownership could not be established, so the sweep stays disabled
      }
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

  // A compression stage (storeStreamAndCompress) produces a gzip of the raw bytes it committed; if
  // another store or a delete lands on the same path before the gzip commit, that gzip belongs to
  // replaced bytes and must be discarded — and the raw file must not be unlinked, since it may now
  // be someone else's newer primary content. Registered at raw-commit time, marked by any later
  // committer, unregistered when the compression stage ends. Bounded by in-flight compressions.
  const inflightCompressionTokens = new Map<string, Set<{ stale: boolean }>>()
  function registerCompressionToken(filePath: string): { stale: boolean } {
    const token = { stale: false }
    const tokens = inflightCompressionTokens.get(filePath) ?? new Set()
    tokens.add(token)
    inflightCompressionTokens.set(filePath, tokens)
    return token
  }
  function unregisterCompressionToken(filePath: string, token: { stale: boolean }): void {
    const tokens = inflightCompressionTokens.get(filePath)
    if (!tokens) return
    tokens.delete(token)
    if (tokens.size === 0) inflightCompressionTokens.delete(filePath)
  }
  function markCompressionsStale(filePath: string): void {
    for (const token of inflightCompressionTokens.get(filePath) ?? []) {
      token.stale = true
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
    if (finalPath === tempDir || finalPath.startsWith(tempDir + path.sep)) {
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
          markCompressionsStale(filePath)
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
        await rename(tempPath, filePath)
        // The raw and its .gzip are one versioned object: a gzip left from a previous version would
        // be preferred by retrieve() and serve stale bytes over the content just stored.
        await noFailUnlink(filePath + '.gzip')
        // The canonical path now holds primary content: drop any stale decompress-cache tracking so
        // eviction can never delete it, tell an in-flight decompression its output is outdated, and
        // tell an in-flight compression its staged gzip no longer matches the canonical bytes.
        forgetCacheEntry(filePath)
        invalidateInflightDecompression(filePath)
        markCompressionsStale(filePath)
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
              // Stage the inflation in the temp dir when rename is available, so a process killed
              // mid-decompress can never leave a partial file at the canonical uncompressed path —
              // a later range request would silently serve its truncated bytes as valid content.
              // Without rename (legacy custom fs adapter) fall back to writing in place.
              const { rename } = components.fs
              const writePath = rename ? newTempPath() : uncompressedPath
              try {
                // Cap how much the gzip may inflate to so a decompression bomb cannot write an
                // unbounded file to disk. The gzip trailer's declared size is attacker-controllable,
                // so the limit is enforced on the actual inflated bytes, not a declared value.
                await pipe(
                  await gzipItem.asStream(),
                  createSizeLimitTransform(MAX_DECOMPRESSED_SIZE),
                  components.fs.createWriteStream(writePath)
                )
                if (rename) {
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
                  return
                }
              } catch (err) {
                // Remove partial file to prevent serving corrupt data (or a partially-written bomb)
                await noFailUnlink(writePath)
                throw err
              }

              // In-place (no rename) legacy path: register the cache entry as before.
              const stat = await components.fs.stat(uncompressedPath)
              decompressCache.set(uncompressedPath, { size: stat.size, lastAccess: Date.now() })
              totalCacheSize += stat.size
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
        if (folder === root && entry.name === tempDirName) continue
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
    // Never delete anything in a directory whose ownership was not established (see the
    // OWNERSHIP_MARKER logic above): flat-mode legacy content could coincidentally match the
    // staged-name shape.
    if (!sweepAllowed) return 0
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
      // Without rename (legacy custom fs adapter) fall back to the original in-place behavior: the
      // in-place compression truncates/removes the old gzip itself, and none of it is crash-atomic.
      if (!rename) {
        await storeStream(id, stream)
        if (await compressContentFile(filePath, logger)) {
          // try to remove original file if present
          const contentItem = await retrieve(id)
          if (contentItem?.encoding) {
            await noFailUnlink(filePath)
          }
        }
        return
      }
      // The raw file and its .gzip are one versioned object, and retrieve() prefers the gzip. The
      // overwrite therefore commits in two locked steps:
      //   1. raw commit — rename the staged raw into place AND remove the previous version's gzip in
      //      the same locked section, so no reader can ever pair the new raw with the old gzip and
      //      no decompression of the old gzip can commit past this point.
      //   2. gzip commit — after compressing (outside any lock), re-take the lock and, only if no
      //      other store/delete landed in between (compression token still fresh), rename the staged
      //      gzip into place and remove the now-redundant raw. If the token went stale, the staged
      //      gzip belongs to replaced bytes: discard it and leave the newer content untouched.
      // A process killed between the steps leaves the raw as the (fully valid) primary
      // representation — never a partial file at a canonical path.
      const tempPath = newTempPath()
      let token: { stale: boolean }
      try {
        await pipe(stream, components.fs.createWriteStream(tempPath))
        token = await withPathLock(filePath, async () => {
          await rename(tempPath, filePath)
          await noFailUnlink(filePath + '.gzip')
          forgetCacheEntry(filePath)
          invalidateInflightDecompression(filePath)
          markCompressionsStale(filePath)
          return registerCompressionToken(filePath)
        })
      } catch (err) {
        await noFailUnlink(tempPath)
        throw err
      }
      try {
        const stagedGzipPath = newTempPath()
        try {
          const compressed = await compressContentFile(filePath, logger, stagedGzipPath)
          await withPathLock(filePath, async () => {
            if (token.stale) {
              // Another store or delete landed after our raw commit: the staged gzip compresses
              // bytes that are no longer canonical. Discard it; the newer committer owns the path.
              if (compressed) await noFailUnlink(stagedGzipPath)
              return
            }
            if (!compressed) {
              // Not beneficial: the raw stays primary; the old gzip was already removed at raw
              // commit and compressContentFile removed its own staged output.
              return
            }
            await rename(stagedGzipPath, filePath + '.gzip')
            // The gzip is now the primary representation; the raw becomes redundant. Unlinking it
            // under the same lock and token check guarantees it is still the exact version this
            // gzip was produced from.
            await noFailUnlink(filePath)
          })
        } catch (err) {
          // compressContentFile already removed its own (possibly partial) staged output on error;
          // this covers a failed rename, whose staged file would otherwise linger until the sweep.
          await noFailUnlink(stagedGzipPath)
          throw err
        }
      } finally {
        unregisterCompressionToken(filePath, token)
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
          markCompressionsStale(filePath)
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
