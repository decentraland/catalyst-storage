import { mkdtempSync, promises as nodeFs, readFileSync, rmSync, writeFileSync } from 'fs'
import os from 'os'
import path from 'path'
import { PassThrough, Readable } from 'stream'
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
import { createS3BasedFileSystemContentStorage } from '../src/s3-based-storage-component'
import { createFakeS3Client } from './fake-s3-client'

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
      //
      // The directory is created DIRECTLY, not by storing a nested id, because the store side now refuses to
      // create this state at all (see the prefix-collision block above): a compressed store whose raw path is
      // a directory would produce an id that can never serve a range. So this state is only reachable as
      // pre-existing on-disk residue — an older version of this library, or an operator — which is precisely
      // why the read path still has to answer for it rather than assume it away.
      root = mkdtempSync(path.join(os.tmpdir(), 'range-eisdir-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.storeStreamAndCompress('a', bufferToStream(Buffer.from('x'.repeat(5000))))
      await nodeFs.mkdir(path.join(root, 'a'))
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

  describe('when a range read is waiting for an inflation slot as shutdown begins', () => {
    // The pre-inflation check cannot see a `close()` that lands while the request sits in the slot QUEUE, and
    // that wait is unbounded in the number of inflations ahead of it. So a request registered while the cache
    // was open went on to inflate after shutdown had closed it — one inflation slot, gzip CPU and up to
    // `decompressMaxFileSize` of staging write, for output the commit then discards. Measured with one slot and
    // a parked 24 MB gzip: a full inflation began after `stop()` had already closed the cache.
    let storage: IContentStorageComponent
    let root: string
    let closed: boolean
    let stagingWritesAfterClose: string[]
    let releaseFirstInflation: () => void

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'slot-queue-shutdown-'))
      closed = false
      stagingWritesAfterClose = []
      let parkedHasProbed: () => void = () => {}
      const probed = new Promise<void>((resolve) => {
        parkedHasProbed = resolve
      })
      let slotHolderIsInflating: () => void = () => {}
      const inflating = new Promise<void>((resolve) => {
        slotHolderIsInflating = resolve
      })
      // ARMED ONLY FOR THE READ PHASE. A compressed store checks BOTH of its commit targets, so the gzip path is
      // stated during setup too — signalling on that made the wait below return immediately and left the timer as
      // the only synchronisation, which held when this test ran alone and not under full-suite load: the parked
      // request then registered AFTER `close()`, so its token was born invalidated and the top-of-function check
      // absorbed it. The test passed while the guard it exists for was neutralised.
      let armed = false
      const base = createFsComponent()
      const gatedFs: IFileSystemComponent = {
        ...base,
        // Holds the ONLY inflation slot open until the test releases it, so the second request is genuinely
        // parked in the queue rather than merely slower.
        createReadStream: ((probedPath: any, options: any) => {
          if (!String(probedPath).endsWith(`first${GZIP_SUFFIX}`)) return base.createReadStream(probedPath, options)
          const gate = new PassThrough()
          releaseFirstInflation = () => {
            base.createReadStream(probedPath, options).pipe(gate)
          }
          // Reached only AFTER this request has taken the slot, so it is proof of which one holds it.
          slotHolderIsInflating()
          return gate as any
        }) as any,
        createWriteStream: ((written: any, options: any) => {
          if (closed) stagingWritesAfterClose.push(path.basename(String(written)))
          return base.createWriteStream(written, options)
        }) as any,
        stat: (async (probedPath: any, ...rest: any[]) => {
          // The parked request's own gzip probe: it happens after its token is registered and with nothing but
          // microtasks left before it queues for a slot, so this proves the window is genuinely open.
          if (armed && String(probedPath).endsWith(`second${GZIP_SUFFIX}`)) parkedHasProbed()
          return base.stat(probedPath, ...rest)
        }) as any
      }
      storage = await createFolderBasedFileSystemContentStorage({ fs: gatedFs, logs: await logs() }, root, {
        disablePrefixHash: true,
        decompressMaxConcurrentInflations: 1,
        decompressCacheMaxSize: 50_000_000
      })
      for (const id of ['first', 'second']) {
        await storage.storeStreamAndCompress(id, bufferToStream(Buffer.from('z'.repeat(200_000))))
      }

      armed = true
      // STRICTLY SEQUENCED, because with one slot the winner of a free-for-all is whichever request happens to
      // probe faster — and if the second one wins it, the first parks somewhere this gate cannot reach.
      const holdsTheSlot = storage.retrieve('first', { start: 0, end: 9 }).catch(() => undefined)
      await inflating
      const parked = storage.retrieve('second', { start: 0, end: 9 }).catch(() => undefined)
      await probed
      // A macrotask, which drains every pending microtask: the parked request has nothing but those left to do
      // before it queues, so after this it is provably IN the queue rather than merely likely to be.
      await new Promise((resolve) => setImmediate(resolve))
      // `stop()` closes the cache synchronously and only then awaits the inflights, so the parked request is
      // still queued at the moment the cache closes — releasing the slot holder is what lets it through.
      closed = true
      const stopped = storage.stop!()
      releaseFirstInflation()
      await Promise.all([stopped, holdsTheSlot, parked])
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should not stage an inflation that shutdown has already made pointless', () => {
      expect(stagingWritesAfterClose).toEqual([])
    })

    it('should leave no derived copy of the parked id on disk', async () => {
      await expect(nodeFs.stat(path.join(root, 'second'))).rejects.toMatchObject({ code: 'ENOENT' })
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

  describe('when a source is handed over with a competing reader already consuming it', () => {
    let storage: IContentStorageComponent
    let s3: IContentStorageComponent
    let memory: IContentStorageComponent
    let root: string
    let stolen: Buffer[]
    let contendedSource: () => Readable

    beforeEach(async () => {
      // A listener that has not read yet leaves `readableDidRead` false, so the source looks pristine. On a
      // backend that consumes by EXPLICIT READ the listener is not blocked — and that is what makes it
      // dangerous rather than harmless: the two race, and every byte the listener wins is a byte the upload
      // never sees. S3 was briefly exempted from the refusal on the reasoning that it "stored such sources
      // correctly", which holds only while the listener is idle. With one that actually reads, S3 committed 0
      // of 2000 bytes and `storeStream` RESOLVED — silent corruption under a content-addressed id.
      root = mkdtempSync(path.join(os.tmpdir(), 'contended-'))
      stolen = []
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      s3 = await createS3BasedFileSystemContentStorage({ logs: await logs() }, createFakeS3Client(), {
        Bucket: 'a-bucket',
        fileTypeLoader: async () => ({ fileTypeFromBuffer: async () => undefined }) as never
      })
      memory = createInMemoryStorage()
      // Multi-chunk and slow, so the competing reader has real opportunities to win a chunk. A single
      // synchronous chunk does not reproduce it.
      contendedSource = () => {
        const source = Readable.from(
          (async function* () {
            for (let index = 0; index < 20; index++) {
              await new Promise((resolve) => setTimeout(resolve, 5))
              yield Buffer.alloc(100, 'z')
            }
          })()
        )
        source.on('readable', () => {
          let chunk: Buffer | null
          while ((chunk = source.read() as Buffer | null) !== null) stolen.push(chunk)
        })
        return source
      }
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should be refused by the S3 backend rather than committing the bytes it won the race for', async () => {
      await expect(s3.storeStream('an-id', contendedSource())).rejects.toThrow(/'readable' listener/)
    })

    it('should leave nothing stored on the S3 backend', async () => {
      await s3.storeStream('an-id', contendedSource()).catch(() => undefined)

      await expect(s3.exist('an-id')).resolves.toBe(false)
    })

    it('should be refused by the folder-based backend', async () => {
      await expect(storage.storeStream('an-id', contendedSource())).rejects.toThrow(/'readable' listener/)
    })

    it('should be refused by the in-memory backend', async () => {
      await expect(memory.storeStream('an-id', contendedSource())).rejects.toThrow(/'readable' listener/)
    })
  })

  describe('when an id is stored after another id has nested itself under that path', () => {
    let storage: IContentStorageComponent
    let root: string
    let rawOutcome: unknown
    let compressedOutcome: unknown

    beforeEach(async () => {
      // The mirror of the parent-occupied case: `a/b` stored first creates the directory `a`, so a later store
      // of `a` has a perfectly good parent and only failed at the COMMIT, with a bare EISDIR from the rename
      // several awaits after the id was accepted. A compressed store of `a` was worse than untyped — it
      // SUCCEEDED, leaving an id whose whole reads work and whose byte ranges can never be served, because the
      // range path has to publish its decompressed copy at exactly the occupied path.
      root = mkdtempSync(path.join(os.tmpdir(), 'reverse-prefix-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))
      rawOutcome = await storage.storeStream('a', bufferToStream(Buffer.from('A'))).catch((error) => error)
      compressedOutcome = await storage
        .storeStreamAndCompress('a', bufferToStream(Buffer.from('x'.repeat(5000))))
        .catch((error) => error)
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should refuse the raw store with a typed error rather than a bare errno', () => {
      expect(rawOutcome).toBeInstanceOf(PathNotContainedError)
    })

    it('should refuse the compressed store too, rather than creating an id that cannot serve ranges', () => {
      expect(compressedOutcome).toBeInstanceOf(PathNotContainedError)
    })

    it('should leave the nested id readable', async () => {
      const item = await storage.retrieve('a/b')

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('nested')
    })
  })

  describe('when a delete batch contains an id inside the reserved staging directory', () => {
    let storage: IContentStorageComponent
    let root: string
    let batch: string[]

    beforeEach(async () => {
      // The pre-validation pass originally applied only the SHARED id rules, which have no notion of the
      // reserved directory — that rejection lives in `resolveFilePath`, inside the concurrent removal loop. So
      // this batch passed pre-validation, removed `victim`, and only then rejected: the partial application the
      // pass exists to rule out. Reachable in flat mode, where the root itself is the id namespace.
      root = mkdtempSync(path.join(os.tmpdir(), 'reserved-delete-'))
      batch = ['victim', '.tmp-writes/x']
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.storeStream('victim', bufferToStream(Buffer.from('v')))
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should reject the batch', async () => {
      await expect(storage.delete(batch)).rejects.toBeInstanceOf(PathNotContainedError)
    })

    it('should not have deleted the valid id ahead of it', async () => {
      await storage.delete(batch).catch(() => undefined)

      await expect(storage.exist('victim')).resolves.toBe(true)
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

  describe('when a range read runs while the storage is shutting down', () => {
    let storage: IContentStorageComponent
    let root: string
    let range: { start: number; end: number }
    let writesOpened: string[]

    beforeEach(async () => {
      // A closed cache refuses to publish a derived file, so a range read of gzip-only content cannot be
      // served and REJECTS — "cannot be read right now", not "not here", because the content is on disk.
      // The guard implementing that asserted the premise instead of checking it.
      root = mkdtempSync(path.join(os.tmpdir(), 'shutdown-range-'))
      range = { start: 0, end: 9 }
      writesOpened = []
      const base = createFsComponent()
      const observed: IFileSystemComponent = {
        ...base,
        createWriteStream: ((target: any, ...rest: any[]) => {
          writesOpened.push(String(target))
          return (base.createWriteStream as any)(target, ...rest)
        }) as IFileSystemComponent['createWriteStream']
      }
      storage = await createFolderBasedFileSystemContentStorage({ fs: observed, logs: await logs() }, root, {
        disablePrefixHash: true
      })
      await storage.start?.({} as never)
      await storage.storeStreamAndCompress('gz-id', bufferToStream(Buffer.from('z'.repeat(30_000))))
      await storage.stop?.()
      writesOpened.length = 0
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should report an absent id as absent rather than as a storage fault', async () => {
      // The guard's premise is "the id's gzip is present", which is FALSE here: an id with no gzip returns
      // from the inflation early, having discarded nothing. Every range read of an unknown id answered 5xx
      // for as long as the cache stayed closed, with a message claiming the content was intact — for content
      // `exist()` simultaneously reported absent.
      await expect(storage.retrieve('never-stored', range)).resolves.toBeUndefined()
    })

    it('should agree with exist about the absent id', async () => {
      expect(await storage.exist('never-stored')).toBe(false)
    })

    it('should still reject for gzip-only content that IS present', async () => {
      // The other direction, which must not regress: this content is on disk and simply cannot be served as
      // a range while shutting down. Answering `undefined` here would hand the caller a 404 for content
      // `exist()` reports present.
      await expect(storage.retrieve('gz-id', range)).rejects.toThrow(/shutting down/)
    })

    it('should still serve a range of raw-primary content', async () => {
      await storage.storeStream('raw-id', bufferToStream(Buffer.from('r'.repeat(100))))

      expect((await storage.retrieve('raw-id', range))?.size).toBe(10)
    })

    it('should not inflate anything it is going to discard', async () => {
      // The token is born invalidated once the cache is closed, so the commit would unlink whatever this
      // produced — after paying an inflation slot, the gzip CPU and up to `decompressMaxFileSize` of staged
      // write, twice, since `retrieve` retries. No staged file should be opened at all.
      await storage.retrieve('gz-id', range).catch(() => undefined)

      expect(writesOpened).toEqual([])
    })
  })
})
