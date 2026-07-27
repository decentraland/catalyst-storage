import { mkdtempSync, promises as nodeFs, readFileSync, rmSync, writeFileSync } from 'fs'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import { createLogComponent } from '@well-known-components/logger'
import {
  bufferToStream,
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  createInMemoryStorage,
  IContentStorageComponent,
  IFileSystemComponent,
  PathNotContainedError,
  SimpleContentItem,
  streamToBuffer
} from '../src'
import { assertStorableStream } from '../src'
import { createDecompressCache, DecompressCache } from '../src/folder-based/decompress-cache'

/**
 * Regression tests for the defects found in the multi-agent bug review. Each block names the failure it
 * pins and the observable symptom it produced, so a change that reintroduces one fails here.
 */
describe('bug review fixes', () => {
  const logs = async () => createLogComponent({})
  const realFs = (): IFileSystemComponent => createFsComponent()
  /** This library's suffix for the compressed representation, spelled once. */
  const GZIP_SUFFIX = '.gzip'
  /**
   * Resolves to `'never settled'` if `work` has not settled within the budget.
   *
   * The timer is CLEARED once the race is decided, so a decided test leaves no live handle behind (each use
   * otherwise kept one for the full budget, past the end of its own test).
   */
  /**
   * Waits until `condition` holds, or gives up after `timeoutMs`.
   *
   * Returns rather than throwing, so the caller's own `expect` produces the failure message. Polling keeps a
   * test that SHOULD pass fast and a test that should fail deterministic, which a fixed sleep does not: a
   * sleep that is long enough on an idle machine can be too short on a loaded one, and too short a sleep can
   * make a genuinely broken build look green.
   */
  const waitUntil = async (condition: () => boolean, timeoutMs = 1_000): Promise<void> => {
    const deadline = Date.now() + timeoutMs
    while (!condition() && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
  }
  const settleOrHang = async <T>(work: Promise<T>): Promise<T | 'never settled'> => {
    let timer: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        work,
        new Promise<'never settled'>((resolve) => {
          timer = setTimeout(() => resolve('never settled'), 2_000)
        })
      ])
    } finally {
      if (timer) clearTimeout(timer)
    }
  }

  describe('when a source has been paused before being handed to a store', () => {
    let source: Readable
    let storage: IContentStorageComponent

    beforeEach(() => {
      // `on('data')` only auto-resumes while `flowing !== false`, and an explicit pause sets it to exactly
      // that — so nothing flowed, no terminal event ever fired, and the store never settled. A service that
      // paused an incoming body while it did async work held that request for the life of the process.
      storage = createInMemoryStorage()
      source = Readable.from([Buffer.from('paused-but-untouched')])
      source.pause()
    })

    it('should be accepted as storable, because nothing has been read from it', () => {
      expect(() => assertStorableStream(source)).not.toThrow()
    })

    it('should settle rather than hang forever', async () => {
      await expect(settleOrHang(storage.storeStream('an-id', source))).resolves.not.toBe('never settled')
    })

    it('should store the whole body', async () => {
      await storage.storeStream('an-id', source)
      const item = await storage.retrieve('an-id')

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('paused-but-untouched')
    })
  })

  describe('when a source already has a competing readable listener', () => {
    let source: Readable

    beforeEach(() => {
      // Nothing has been READ, so `readableDidRead` is false and the source looks pristine — but that
      // listener takes precedence over both `pipe` and `resume()`, so it can never flow. Refused rather
      // than hung, and refused on its own merits too: the two consumers would race for the same bytes.
      source = Readable.from([Buffer.from('contended')])
      source.on('readable', () => undefined)
    })

    it('should be refused as unstorable', () => {
      expect(() => assertStorableStream(source)).toThrow(/'readable' listener/)
    })

    it('should reject the buffering helper rather than never settle', async () => {
      await expect(settleOrHang(streamToBuffer(source).catch((error: Error) => error.message))).resolves.toMatch(
        /'readable' listener/
      )
    })
  })

  describe('when content is stored under an encoding that names an Object.prototype member', () => {
    let opened: number
    let itemWith: (encoding: string) => SimpleContentItem

    beforeEach(() => {
      // Lowercasing the coding makes `constructor` and `__proto__` the two that reach `Object.prototype`.
      // For `constructor` the inherited `Object` is truthy, so the refusal was skipped, the source was
      // OPENED, and `pipeline` then threw from outside any try/catch — leaking it once per read attempt.
      opened = 0
      itemWith = (encoding: string) =>
        new SimpleContentItem(
          async () => {
            opened++
            return Readable.from([Buffer.from('body')])
          },
          4,
          encoding
        )
    })

    it('should refuse a constructor encoding by naming it unsupported', async () => {
      await expect(itemWith('constructor').asStream()).rejects.toThrow(/unsupported encoding/)
    })

    it('should refuse an upper-cased constructor encoding too', async () => {
      await expect(itemWith('CONSTRUCTOR').asStream()).rejects.toThrow(/unsupported encoding/)
    })

    it('should refuse a __proto__ encoding by naming it unsupported', async () => {
      await expect(itemWith('__proto__').asStream()).rejects.toThrow(/unsupported encoding/)
    })

    it('should not open the source when refusing a constructor encoding', async () => {
      await itemWith('constructor')
        .asStream()
        .catch(() => undefined)

      expect(opened).toBe(0)
    })
  })

  describe('when the folder-based backend is asked about an id whose parent path is also over-long', () => {
    let storage: IContentStorageComponent
    let memory: IContentStorageComponent
    let root: string
    let deepId: string
    let ordinaryId: string
    let batch: string[]
    let body: Buffer

    beforeEach(async () => {
      // ENAMETOOLONG was allowed as "provably absent" for the file stat, but the absence CLASSIFICATION
      // then stat'd the parent — whose path is over-long too — and re-threw. Reads rejected with a raw
      // errno for a provably absent id, and one such id failed a whole `existMultiple` batch.
      root = mkdtempSync(path.join(os.tmpdir(), 'too-long-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root)
      memory = createInMemoryStorage()
      deepId = Array.from({ length: 30 }, (_, index) => `s${index}`.padEnd(240, 'x')).join('/')
      ordinaryId = 'an-ordinary-id'
      batch = [ordinaryId, deepId]
      body = Buffer.from('x')
      await storage.storeStream(ordinaryId, bufferToStream(Buffer.from('o')))
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should report it absent rather than reject', async () => {
      await expect(storage.exist(deepId)).resolves.toBe(false)
    })

    it('should report absence from fileInfo as well', async () => {
      await expect(storage.fileInfo(deepId)).resolves.toBeUndefined()
    })

    it('should keep the answers for the other ids in a batch', async () => {
      const answers = await storage.existMultiple(batch)

      expect(answers.get(ordinaryId)).toBe(true)
    })

    it('should refuse to store it with a typed error rather than a raw errno', async () => {
      await expect(storage.storeStream(deepId, bufferToStream(body))).rejects.toBeInstanceOf(PathNotContainedError)
    })

    it('should refuse to store it on the in-memory backend too', async () => {
      await expect(memory.storeStream(deepId, bufferToStream(body))).rejects.toBeInstanceOf(PathNotContainedError)
    })
  })

  describe('when one id is a path prefix of another in flat mode', () => {
    let storage: IContentStorageComponent
    let root: string
    let storeOutcome: unknown

    beforeEach(async () => {
      // A filesystem cannot hold a file and a directory at one path. `existPath` passes for the file, so
      // mkdir was skipped and the failure surfaced several awaits later as a bare ENOTDIR from the commit
      // rename. The fix is at the STORE, which now refuses the id up front with the typed error every other
      // unstorable-name rule uses. Reads of it deliberately still REJECT: reporting a non-directory in the
      // storage's own tree as "absent" would let a broken store look like an empty one, and the reasoning
      // that the obstruction must be another id's content does not hold in the default hash mode, where the
      // shard is a hash of the whole id.
      root = mkdtempSync(path.join(os.tmpdir(), 'prefix-id-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.storeStream('a', bufferToStream(Buffer.from('A')))
      storeOutcome = await storage.storeStream('a/b', bufferToStream(Buffer.from('B'))).catch((error) => error)
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should refuse the second store with a typed error rather than a bare errno', () => {
      expect(storeOutcome).toBeInstanceOf(PathNotContainedError)
    })

    it('should name the occupied parent path in the rejection', () => {
      expect((storeOutcome as Error).message).toMatch(/already occupied by another id's content/)
    })

    it('should leave the id that was stored first intact', async () => {
      const item = await storage.retrieve('a')

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('A')
    })
  })

  describe('when a foreign file named exactly like the compressed suffix sits in a shard', () => {
    let storage: IContentStorageComponent
    let root: string
    let enumerated: string[]

    beforeEach(async () => {
      // The suffix was stripped unconditionally, so the remainder was the empty id — which `exist` and
      // `delete` both reject, meaning a GC sweep that enumerated and deleted failed its whole batch on
      // every retry, forever.
      root = mkdtempSync(path.join(os.tmpdir(), 'bare-gzip-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.storeStream('an-id', bufferToStream(Buffer.from('content')))
      writeFileSync(path.join(root, '.gzip'), 'foreign')
      enumerated = []
      for await (const id of storage.allFileIds()) enumerated.push(id)
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should not enumerate an empty id for it', () => {
      expect(enumerated).toEqual(['an-id'])
    })

    it('should let a delete of everything it enumerated succeed', async () => {
      await expect(storage.delete(enumerated)).resolves.toBeUndefined()
    })
  })

  describe('when a delete batch contains an unaddressable id', () => {
    let storage: IContentStorageComponent
    let root: string
    let badIdFirst: string[]
    let badIdLast: string[]

    beforeEach(async () => {
      // The removals are bounded-concurrent, so a bad id anywhere in the list used to let up to 64 ids
      // complete first — a rejected batch had deleted a nondeterministic prefix of the ids behind it,
      // while the in-memory backend deleted none of them.
      root = mkdtempSync(path.join(os.tmpdir(), 'delete-atomic-'))
      badIdFirst = ['../evil', 'victim']
      badIdLast = ['victim', '../evil']
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root)
      await storage.storeStream('victim', bufferToStream(Buffer.from('v')))
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should reject with a typed error', async () => {
      await expect(storage.delete(badIdFirst)).rejects.toBeInstanceOf(PathNotContainedError)
    })

    it('should leave an id following the invalid one in place', async () => {
      await storage.delete(badIdFirst).catch(() => undefined)

      await expect(storage.exist('victim')).resolves.toBe(true)
    })

    it('should leave an id preceding the invalid one in place', async () => {
      await storage.delete(badIdLast).catch(() => undefined)

      await expect(storage.exist('victim')).resolves.toBe(true)
    })
  })

  describe('when a range read of a gzip-only id finds a directory at its uncompressed path', () => {
    let storage: IContentStorageComponent
    let root: string

    beforeEach(async () => {
      // Reads report a directory as absent, which is exactly what routes a RANGE request into the
      // inflation — whose commit then renamed onto that directory and failed with a bare EISDIR on every
      // call, forever, for an id whose whole-file reads succeed.
      root = mkdtempSync(path.join(os.tmpdir(), 'range-eisdir-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))
      await storage.storeStreamAndCompress('a', bufferToStream(Buffer.from('x'.repeat(5000))))
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should still serve the whole file', async () => {
      const item = await storage.retrieve('a')

      expect((await streamToBuffer(await item!.asStream())).length).toBe(5000)
    })

    it('should reject a range read with a message naming the occupied path', async () => {
      await expect(storage.retrieve('a', { start: 0, end: 9 })).rejects.toThrow(/occupied by a directory/)
    })
  })

  describe('when a store commits while an inflation of the same id waits for a slot', () => {
    let storage: IContentStorageComponent
    let root: string
    let gatedFs: IFileSystemComponent
    let releaseRename: () => void
    let failUnlinkSuffix: string | undefined
    let range: { start: number; end: number }

    beforeEach(async () => {
      // The invalidation token used to be registered only AFTER the slot wait, so a read parked on a full
      // queue held no token for a writer to mark: it woke with a FRESH one, inflated the superseded gzip and
      // renamed that stale output over the primary the store had just committed — a silently lost write that
      // outlived a restart, because reconcile then dropped the counterpart and left the OLD bytes as the id's
      // only representation.
      //
      // THE FAILING COUNTERPART UNLINK IS LOAD-BEARING, not incidental colour. A raw commit removes
      // `<id>.gzip` as its counterpart, so with a working unlink the woken inflation finds no source and bails
      // — the "protected only by accident" path the fix's own comment describes. Only a commit that KEEPS the
      // gzip exercises the bug, and an earlier version of this test omitted the injection and therefore passed
      // against the unfixed code. Injecting EPERM reproduces the real-world shape: a cleanup that could not
      // run, which quarantines the id and leaves its gzip on disk.
      root = mkdtempSync(path.join(os.tmpdir(), 'slot-race-'))
      range = { start: 0, end: 9 }
      failUnlinkSuffix = undefined
      releaseRename = () => undefined
      const base = createFsComponent()
      const gate = new Promise<void>((resolve) => {
        releaseRename = resolve
      })
      gatedFs = Object.create(base)
      gatedFs.rename = async (from: Parameters<IFileSystemComponent['rename']>[0], to: typeof from) => {
        // Parks the decoy's commit, which holds the one inflation slot so the target's read has to queue.
        if (String(to).endsWith(`${path.sep}decoy`)) await gate
        return base.rename(from, to)
      }
      gatedFs.unlink = async (target: Parameters<IFileSystemComponent['unlink']>[0]) => {
        if (failUnlinkSuffix !== undefined && String(target).endsWith(failUnlinkSuffix)) {
          throw Object.assign(new Error(`EPERM: operation not permitted, unlink '${String(target)}'`), {
            code: 'EPERM'
          })
        }
        return base.unlink(target)
      }
      storage = await createFolderBasedFileSystemContentStorage({ fs: gatedFs, logs: await logs() }, root, {
        disablePrefixHash: true,
        decompressMaxConcurrentInflations: 1,
        decompressCacheMaxSize: 50_000_000
      })
      await storage.storeStreamAndCompress('decoy', bufferToStream(Buffer.from('D'.repeat(6000))))
      await storage.storeStreamAndCompress('target', bufferToStream(Buffer.from('O'.repeat(6000))))
    })

    afterEach(async () => {
      releaseRename()
      failUnlinkSuffix = undefined
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    const runRace = async (): Promise<void> => {
      const decoyRead = storage.retrieve('decoy', range).catch(() => undefined)
      await new Promise((resolve) => setTimeout(resolve, 100))
      const parkedRead = storage.retrieve('target', range).catch(() => undefined)
      await new Promise((resolve) => setTimeout(resolve, 100))

      failUnlinkSuffix = `target${GZIP_SUFFIX}`
      await storage.storeStream('target', bufferToStream(Buffer.from('N'.repeat(6000)))).catch(() => undefined)
      releaseRename()
      await Promise.all([decoyRead, parkedRead])
      await new Promise((resolve) => setTimeout(resolve, 200))
      failUnlinkSuffix = undefined
    }

    it('should leave the newly committed bytes at the canonical path', async () => {
      await runRace()

      expect(readFileSync(path.join(root, 'target'), 'utf8')[0]).toBe('N')
    })

    it('should still serve the new content after a restart has reconciled the id', async () => {
      await runRace()
      await storage.stop?.()
      storage = await createFolderBasedFileSystemContentStorage({ fs: createFsComponent(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      const item = await storage.retrieve('target')

      expect((await streamToBuffer(await item!.asStream())).toString()[0]).toBe('N')
    })
  })

  describe('when the storage is started again after being stopped', () => {
    let storage: IContentStorageComponent
    let root: string
    let range: { start: number; end: number }

    beforeEach(async () => {
      // `stop()` closes the cache so a late inflation cannot commit a derived file behind `evictAll`. Nothing
      // reopened it, and `start()` is documented as re-callable — so after one cycle every inflation was born
      // pre-invalidated and discarded its output: range reads of gzip-only content answered `undefined`
      // forever while `exist()` reported the id present, after paying for two full inflations per request.
      root = mkdtempSync(path.join(os.tmpdir(), 'reopen-'))
      range = { start: 0, end: 9 }
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true,
        decompressCacheMaxSize: 50_000_000
      })
      await storage.storeStreamAndCompress('gz-id', bufferToStream(Buffer.from('q'.repeat(6000))))
      await storage.start?.({} as never)
      await storage.stop?.()
      await storage.start?.({} as never)
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should serve a byte range of gzip-only content again', async () => {
      const item = await storage.retrieve('gz-id', range)

      expect(item?.size).toBe(10)
    })

    it('should agree with exist about the id being present', async () => {
      await expect(storage.exist('gz-id')).resolves.toBe(true)
    })
  })

  describe('when a range read has not registered its inflation before stop is called', () => {
    let storage: IContentStorageComponent
    let root: string
    let range: { start: number; end: number }

    beforeEach(async () => {
      // `inflight()` only reports inflations that have already registered, so a read still in its
      // pre-inflation stats committed its derived file behind `evictAll` — and the next boot deliberately
      // never adopts one, so it leaked for good on a CLEAN shutdown.
      root = mkdtempSync(path.join(os.tmpdir(), 'stop-leak-'))
      range = { start: 0, end: 9 }
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true,
        decompressCacheMaxSize: 50_000_000
      })
      await storage.storeStreamAndCompress('an-id', bufferToStream(Buffer.from('z'.repeat(30_000))))
    })

    afterEach(async () => {
      // Stopped here as well as inside the test: a `beforeEach` failure, or an assertion that throws before
      // the test's own `stop()`, would otherwise leave a live eviction interval and intent journal behind.
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should leave no derived copy on disk', async () => {
      const read = storage.retrieve('an-id', range).catch(() => undefined)
      await storage.stop?.()
      await read
      await new Promise((resolve) => setTimeout(resolve, 100))

      await expect(nodeFs.stat(path.join(root, 'an-id'))).rejects.toMatchObject({ code: 'ENOENT' })
    })
  })

  describe('when a reader pins a cache entry after an eviction pass has already selected it', () => {
    let cache: DecompressCache
    let unlinked: string[]
    let releaseLock: () => void
    let pinAfterSelection: () => () => void

    beforeEach(() => {
      // The pass checks pins when it SELECTS a victim and then awaits the victim's PATH LOCK, so a pin taken
      // inside that window was invisible to it and the file was unlinked underneath the reader — whose
      // lazily-opened stream then failed ENOENT for content that was present at `retrieve()` time.
      // `lastAccess` cannot stand in for the check, because `pin` does not touch it.
      //
      // The window is reproduced by holding that lock, which is what a concurrent commit or delete does (the
      // folder-based `delete` holds it across ~8 filesystem round trips).
      unlinked = []
      releaseLock = () => undefined
      cache = createDecompressCache(
        {
          logger: { log() {}, info() {}, debug() {}, warn() {}, error() {} } as never,
          fsInvariants: {
            existsForInvariant: async () => false,
            noFailUnlink: async (target: string) => {
              unlinked.push(target)
              return true
            }
          }
        },
        { ttl: 3_600_000, maxSize: 100 }
      )
      // The lock is taken BEFORE the entries are recorded, because `record` itself starts a pass the moment
      // the budget is crossed — so without this the admission-triggered pass would evict `/victim` here, with
      // no pin yet in existence, and the test would be measuring the wrong pass.
      const gate = new Promise<void>((resolve) => {
        releaseLock = resolve
      })
      void cache.withPathLock('/victim', () => gate)
      // Two entries over the budget, oldest first, so the LRU walk selects `/victim` and protects `/filler`
      // as the most recent.
      cache.record('/victim', 90)
      cache.record('/filler', 90)
      pinAfterSelection = () => cache.pin('/victim', 5_000)
    })

    afterEach(() => {
      releaseLock()
    })

    it('should not unlink the pinned entry', async () => {
      pinAfterSelection()
      releaseLock()
      await cache.evict()

      expect(unlinked).not.toContain('/victim')
    })

    it('should keep the pinned entry tracked so a later pass can reclaim it', async () => {
      pinAfterSelection()
      releaseLock()
      await cache.evict()

      expect(cache.isTracked('/victim')).toBe(true)
    })

    it('should reclaim it once the pin is released, rather than treating the block as a mount failure', async () => {
      const release = pinAfterSelection()
      releaseLock()
      await cache.evict()
      // A pin-blocked pass must not arm the damaged-mount back-off: that flag is precisely what gates OFF the
      // pin-release eviction trigger, so the one stall a release is meant to clear became the one it could not.
      release()
      // POLLED rather than slept on: the reclaim is driven by the release, so a fixed sleep only decides how
      // long the test takes when it is going to pass, while making it timing-sensitive when it should fail.
      await waitUntil(() => unlinked.includes('/victim'))

      expect(unlinked).toContain('/victim')
    })
  })

  describe('when an inflation asks for its slot more than once', () => {
    let cache: DecompressCache

    beforeEach(() => {
      // `acquireSlot` documented itself as idempotent but guarded on a boolean set only AFTER the await, so
      // two concurrent calls both queued and the second waited on a slot that could never be released — the
      // release happens once `inflate` settles, and `inflate` was blocked on that very call.
      cache = createDecompressCache(
        {
          logger: { log() {}, info() {}, debug() {}, warn() {}, error() {} } as never,
          fsInvariants: { existsForInvariant: async () => false, noFailUnlink: async () => true }
        },
        { ttl: 3_600_000, maxSize: 1_000, maxConcurrentInflations: 1 }
      )
    })

    it('should settle when the calls are concurrent', async () => {
      const inflation = cache.deduplicateInflation('/path', async (_token, acquireSlot) => {
        await Promise.all([acquireSlot(), acquireSlot()])
      })

      await expect(settleOrHang(inflation)).resolves.not.toBe('never settled')
    })

    it('should release the slot so a later inflation can still run', async () => {
      await cache.deduplicateInflation('/first', async (_token, acquireSlot) => {
        await Promise.all([acquireSlot(), acquireSlot()])
      })
      const second = cache.deduplicateInflation('/second', async (_token, acquireSlot) => acquireSlot())

      await expect(settleOrHang(second)).resolves.not.toBe('never settled')
    })
  })
})
