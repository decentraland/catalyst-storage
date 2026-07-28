import { spawnSync } from 'child_process'
import { createHash } from 'crypto'
import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { Readable, Writable } from 'stream'
import { gzipSync } from 'zlib'
import { createLogComponent } from '@well-known-components/logger'
import {
  bufferToStream,
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  createInMemoryStorage,
  IContentStorageComponent,
  IFileSystemComponent,
  SimpleContentItem,
  streamToBuffer
} from '../src'
import { mapWithConcurrency } from '../src/concurrency'
import { assertAddressableContentId } from '../src/content-id'
import { PathNotContainedError } from '../src/folder-based/errors'
import { intentNameFor } from './file-system-utils'
import { createDecompressCache } from '../src/folder-based/decompress-cache'
import { MAX_BUFFERED_DIRECTORY_ENTRIES } from '../src/folder-based-storage-component'

/**
 * Regression tests for the defects found in the deep review. Each block names the failure it pins,
 * so a future change that reintroduces one fails here rather than in production.
 */
describe('review regressions', () => {
  describe('when a store is handed an already-aborted signal and a source that fails to open', () => {
    let storage: IContentStorageComponent
    let uncaught: unknown[]
    let listeners: NodeJS.UncaughtExceptionListener[]

    beforeEach(() => {
      // `stream.destroy()` on an fs.ReadStream whose open(2) is still in flight still emits 'error'.
      // On the pre-aborted path `operation()` never runs, so nothing else attaches a listener and the
      // emit became an uncaught exception — which terminates the process by default.
      storage = createInMemoryStorage()
      uncaught = []
      listeners = process.listeners('uncaughtException')
      process.removeAllListeners('uncaughtException')
      process.on('uncaughtException', (error) => uncaught.push(error))
    })

    afterEach(() => {
      process.removeAllListeners('uncaughtException')
      listeners.forEach((listener) => process.on('uncaughtException', listener))
    })

    it('should reject with the abort reason without letting the source error escape the process', async () => {
      const controller = new AbortController()
      const reason = new Error('client disconnected before the store began')
      controller.abort(reason)
      const source = createFsComponent().createReadStream(path.join(os.tmpdir(), 'definitely-absent-source'))

      await expect(storage.storeStream('an-id', source, controller.signal)).rejects.toBe(reason)
      await new Promise((resolve) => setTimeout(resolve, 50))

      expect(uncaught).toEqual([])
    })
  })

  describe('when streamToBuffer is given a stream that has already settled', () => {
    const settle = async (build: () => Promise<Readable>): Promise<string> => {
      const stream = await build()
      return Promise.race([
        streamToBuffer(stream).then(
          () => 'resolved',
          (error: Error) => `rejected: ${error.message}`
        ),
        new Promise<string>((resolve) => setTimeout(() => resolve('never settled'), 250))
      ])
    }

    describe('and the stream was fully consumed by someone else', () => {
      it('should reject rather than hang forever', async () => {
        const outcome = await settle(async () => {
          const stream = Readable.from([Buffer.from('hello')])
          for await (const _chunk of stream) {
            // drain
          }
          return stream
        })

        expect(outcome).toBe('rejected: Stream closed before it ended.')
      })
    })

    describe('and the stream was destroyed before being read', () => {
      it('should reject rather than hang forever', async () => {
        const outcome = await settle(async () => {
          const stream = Readable.from([Buffer.from('hello')])
          stream.destroy()
          return stream
        })

        expect(outcome).toBe('rejected: Stream closed before it ended.')
      })
    })

    describe('and the stream already errored', () => {
      it('should reject with the original error', async () => {
        const outcome = await settle(async () => {
          const stream = Readable.from([Buffer.from('hello')])
          // Absorbing the emit is the fixture's job: without a listener, destroying with an error is
          // an unhandled 'error' event from the test itself, before streamToBuffer is ever called.
          stream.on('error', () => undefined)
          stream.destroy(new Error('upstream exploded'))
          await new Promise((resolve) => setImmediate(resolve))
          return stream
        })

        expect(outcome).toBe('rejected: upstream exploded')
      })
    })
  })

  describe('when a ContentItem carries an encoding other than gzip', () => {
    describe('and the encoding is identity', () => {
      let item: SimpleContentItem

      beforeEach(() => {
        item = new SimpleContentItem(async () => bufferToStream(Buffer.from('plain')), 5, 'identity')
      })

      it('should normalize the encoding to null', () => {
        expect(item.encoding).toBeNull()
      })

      it('should report the known content size instead of unknown', () => {
        expect(item.contentSize).toBe(5)
      })
    })

    describe('and the encoding is an unsupported coding', () => {
      let item: SimpleContentItem
      let source: Readable
      let opened: number

      beforeEach(() => {
        opened = 0
        source = bufferToStream(Buffer.from('compressed somehow'))
        item = new SimpleContentItem(
          async () => {
            opened++
            return source
          },
          18,
          'x-unknown-coding'
        )
      })

      it('should reject rather than hand back encoded bytes labelled as decoded', async () => {
        await expect(item.asStream()).rejects.toThrow(/unsupported encoding/)
      })

      it('should not open the source at all, rather than opening and releasing it', async () => {
        // Stronger than the leak check this replaces: the coding is settled before `streamCreator` runs, so an
        // undecodable representation costs no S3 GetObject and no file open — and an open that fails on its own
        // can no longer replace the clearer statement that the representation cannot be decoded.
        await item.asStream().catch(() => undefined)

        expect(opened).toBe(0)
      })

      it('should leave the source undestroyed, because it was never taken', async () => {
        await item.asStream().catch(() => undefined)

        expect(source.destroyed).toBe(false)
      })
    })
  })

  describe('when mapWithConcurrency is given a NaN limit', () => {
    let peak: number
    let inFlight: number

    beforeEach(async () => {
      // NaN carries no intent — unlike Infinity, which is a deliberate "no limit" — so it must fall
      // back to the safe end rather than the unbounded fan-out this helper exists to prevent.
      peak = 0
      inFlight = 0
      await mapWithConcurrency(
        Array.from({ length: 40 }, (_, index) => index),
        Number.NaN,
        async () => {
          inFlight++
          peak = Math.max(peak, inFlight)
          await new Promise((resolve) => setTimeout(resolve, 1))
          inFlight--
        }
      )
    })

    it('should serialize the work instead of running every item at once', () => {
      expect(peak).toBe(1)
    })
  })

  describe('when the in-memory backend is given an id the folder-based backend rejects', () => {
    let storage: IContentStorageComponent

    beforeEach(() => {
      storage = createInMemoryStorage()
    })

    it.each([[''], ['foo.gzip'], ['../evil'], ['./x']])(
      'should reject a store of %p with PathNotContainedError',
      async (id: string) => {
        await expect(storage.storeStream(id, bufferToStream(Buffer.from('x')))).rejects.toThrow(PathNotContainedError)
      }
    )

    it('should reject exist for the same ids', async () => {
      await expect(storage.exist('../evil')).rejects.toThrow(PathNotContainedError)
    })

    it('should report retrieve as nothing to serve, matching the folder-based read contract', async () => {
      await expect(storage.retrieve('../evil')).resolves.toBeUndefined()
    })
  })

  describe('when assertAddressableContentId is given an id that names its own path', () => {
    it('should accept it', () => {
      expect(() => assertAddressableContentId('QmSomeContentHash')).not.toThrow()
    })
  })
})

describe('folder-based review regressions', () => {
  const id = 'some-id'
  let root: string
  let storage: IContentStorageComponent

  const logs = async () => createLogComponent({})
  const shardOf = (base: string): string => path.join(base, '9584')
  const realFs = (): IFileSystemComponent => createFsComponent()

  afterEach(async () => {
    await storage?.stop?.()
    if (root) rmSync(root, { recursive: true, force: true })
  })

  describe('when reading ids that were never stored', () => {
    let nested: IContentStorageComponent

    beforeEach(async () => {
      // `getFilePath` used to mkdir on EVERY call, including reads. With hash prefixes that merely
      // pre-created shards, but in flat mode ids nest, so probing untrusted ids grew the inode count
      // without limit and left `allFileIds` walking empty trees forever.
      root = mkdtempSync(path.join(os.tmpdir(), 'read-side-effects-'))
      nested = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root, {
        disablePrefixHash: true
      })
      storage = nested
      await nested.exist('a/b/c/missing')
      await nested.retrieve('d/e/missing')
      await nested.fileInfo('f/g/missing')
    })

    it('should not create any directory for them', async () => {
      const entries = await nodeFs.readdir(root)

      expect(entries.filter((entry) => entry !== '.tmp-writes')).toEqual([])
    })

    it('should still report them as absent', async () => {
      await expect(nested.exist('a/b/c/missing')).resolves.toBe(false)
    })
  })

  describe('when the reserved staging directory is removed while the storage is live', () => {
    beforeEach(async () => {
      // It was created once, at construction, so nothing recreated it: every store failed ENOENT
      // forever, and `writingUnder` invalidated the shard directory, which was never the problem.
      root = mkdtempSync(path.join(os.tmpdir(), 'temp-dir-healing-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root)
      await storage.storeStream(id, bufferToStream(Buffer.from('first')))
      rmSync(path.join(root, '.tmp-writes'), { recursive: true, force: true })
    })

    it('should fail the store that raced the removal', async () => {
      await expect(storage.storeStream(id, bufferToStream(Buffer.from('second')))).rejects.toThrow()
    })

    it('should recreate it so the next store succeeds instead of failing forever', async () => {
      await storage.storeStream(id, bufferToStream(Buffer.from('second'))).catch(() => undefined)

      await expect(storage.storeStream(id, bufferToStream(Buffer.from('third')))).resolves.toBeUndefined()
    })
  })

  describe('when a commit fails before its rename', () => {
    let cachedRawPath: string

    beforeEach(async () => {
      // The bookkeeping in the commit's `finally` ran for pre-rename failures too. `cache.forget`
      // drops tracking WITHOUT unlinking, so the decompressed range-cache copy at the canonical path
      // became invisible to eviction and to evictAll() — leaked, and no longer counted against the
      // size budget.
      root = mkdtempSync(path.join(os.tmpdir(), 'pre-rename-failure-'))
      const base = realFs()
      const failingIntentWrite: IFileSystemComponent = {
        ...base,
        createWriteStream: ((target: any, options?: any) => {
          if (String(target).endsWith('.intent')) {
            return new Writable({
              write(_chunk, _encoding, callback) {
                callback(Object.assign(new Error('EIO on write'), { code: 'EIO' }))
              }
            })
          }
          return base.createWriteStream(target, options)
        }) as IFileSystemComponent['createWriteStream']
      }
      storage = await createFolderBasedFileSystemContentStorage({ fs: failingIntentWrite, logs: await logs() }, root, {
        decompressCacheTTL: 1,
        decompressCacheEvictionInterval: 40
      })
      await storage.start?.({} as any)
      cachedRawPath = path.join(shardOf(root), id)

      await storage.storeStreamAndCompress(id, bufferToStream(Buffer.from('a'.repeat(4096))))
      await storage.retrieve(id, { start: 0, end: 9 })
      await storage.storeStream(id, bufferToStream(Buffer.from('b'))).catch(() => undefined)
    })

    it('should keep the decompressed copy tracked so eviction still reclaims it', async () => {
      await new Promise((resolve) => setTimeout(resolve, 150))
      await storage.stop?.()

      expect(await realFs().existPath(cachedRawPath)).toBe(false)
    })
  })

  describe('when an intent journal write fails after its body reached the disk', () => {
    let restartError: unknown

    beforeEach(async () => {
      // A filesystem that reports its write error at close (NFS, FUSE, a custom adapter) leaves a
      // COMPLETE, valid journal behind while the caller's staging cleanup destroys the staged file
      // that proves the rename never landed. The next construction then found a journal whose staged
      // file and whose primary were both absent, and refused to start — permanently, over content
      // that was never damaged.
      root = mkdtempSync(path.join(os.tmpdir(), 'orphaned-intent-'))
      const base = realFs()
      const deferredCloseError: IFileSystemComponent = {
        ...base,
        createWriteStream: ((target: any, options?: any) => {
          if (String(target).endsWith('.intent')) {
            const chunks: Buffer[] = []
            return new Writable({
              write(chunk, _encoding, callback) {
                chunks.push(Buffer.from(chunk))
                callback()
              },
              final(callback) {
                void nodeFs
                  .writeFile(String(target), Buffer.concat(chunks))
                  .then(() => callback(Object.assign(new Error('EIO on close'), { code: 'EIO' })))
              }
            })
          }
          return base.createWriteStream(target, options)
        }) as IFileSystemComponent['createWriteStream']
      }
      storage = await createFolderBasedFileSystemContentStorage({ fs: deferredCloseError, logs: await logs() }, root)
      await storage.storeStreamAndCompress(id, bufferToStream(Buffer.from('a'.repeat(4096))))
      await storage.storeStream(id, bufferToStream(Buffer.from('b'))).catch(() => undefined)

      restartError = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root).then(
        (created) => {
          storage = created
          return undefined
        },
        (error: unknown) => error
      )
    })

    it('should leave no journal behind for the failed commit', async () => {
      const entries = await nodeFs.readdir(path.join(root, '.tmp-writes'))

      expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
    })

    it('should let a later instance start over the same root', () => {
      expect(restartError).toBeUndefined()
    })

    it('should keep the previous version readable', async () => {
      await expect(storage.exist(id)).resolves.toBe(true)
    })
  })

  describe('when many ids are deleted at once', () => {
    let ids: string[]

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'concurrent-delete-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs: realFs(), logs: await logs() }, root)
      ids = Array.from({ length: 200 }, (_, index) => `bulk-${index}`)
      for (const each of ids) {
        await storage.storeStream(each, bufferToStream(Buffer.from('x')))
      }
      await storage.delete(ids)
    })

    it('should remove every one of them', async () => {
      const survivors = await storage.existMultiple(ids)

      expect([...survivors.values()].filter(Boolean)).toEqual([])
    })
  })

  describe('when the eviction pass meets a cached file it cannot stat', () => {
    let cachedRawPath: string
    let failStat: boolean

    beforeEach(async () => {
      // `existsForInvariant` throws on anything but ENOENT/ENOTDIR, and the loop did not guard it —
      // so one damaged path aborted the whole pass, skipped every entry behind it, and died in the
      // same place on every subsequent tick. It also rejected stop().
      root = mkdtempSync(path.join(os.tmpdir(), 'eviction-poisoning-'))
      failStat = false
      const base = realFs()
      const flaky: IFileSystemComponent = {
        ...base,
        stat: (async (target: any, ...rest: any[]) => {
          if (failStat && String(target).endsWith('poisoned')) {
            throw Object.assign(new Error('EIO: i/o error'), { code: 'EIO' })
          }
          return (base.stat as any)(target, ...rest)
        }) as IFileSystemComponent['stat'],
        unlink: (async (target: any) => {
          if (failStat && String(target).endsWith('poisoned')) {
            throw Object.assign(new Error('EIO: i/o error'), { code: 'EIO' })
          }
          return base.unlink(target)
        }) as IFileSystemComponent['unlink']
      }
      storage = await createFolderBasedFileSystemContentStorage({ fs: flaky, logs: await logs() }, root, {
        decompressCacheTTL: 1
      })
      for (const each of ['poisoned', 'healthy']) {
        await storage.storeStreamAndCompress(each, bufferToStream(Buffer.from('a'.repeat(4096))))
        await storage.retrieve(each, { start: 0, end: 9 })
      }
      cachedRawPath = path.join(root, createHash('sha1').update('healthy').digest('hex').substring(0, 4), 'healthy')
      failStat = true
    })

    it('should still evict the healthy entries behind it', async () => {
      await storage.stop?.()

      expect(await realFs().existPath(cachedRawPath)).toBe(false)
    })

    it('should not reject stop', async () => {
      await expect(storage.stop?.()).resolves.toBeUndefined()
    })
  })
})

describe('review regressions: remaining branches', () => {
  describe('when a ContentItem is stored with a deflate encoding', () => {
    it('should inflate it', async () => {
      const { deflateSync } = await import('zlib')
      const item = new SimpleContentItem(
        async () => bufferToStream(deflateSync(Buffer.from('deflated body'))),
        20,
        'deflate'
      )

      expect((await streamToBuffer(await item.asStream())).toString()).toBe('deflated body')
    })
  })

  describe('when a ContentItem is stored with a brotli encoding', () => {
    it('should decompress it', async () => {
      const { brotliCompressSync } = await import('zlib')
      const item = new SimpleContentItem(
        async () => bufferToStream(brotliCompressSync(Buffer.from('brotli body'))),
        20,
        'br'
      )

      expect((await streamToBuffer(await item.asStream())).toString()).toBe('brotli body')
    })
  })

  describe('when the filesystem component throws synchronously while opening a write stream', () => {
    let root: string
    let storage: IContentStorageComponent
    let source: Readable
    let failure: unknown

    beforeEach(async () => {
      // `pipeline` never takes ownership, so without the guard the caller's source is left paused
      // and undestroyed with its descriptor held for the life of the process.
      root = mkdtempSync(path.join(os.tmpdir(), 'sync-open-throw-'))
      const base = createFsComponent()
      const throwing: IFileSystemComponent = {
        ...base,
        createWriteStream: ((target: any, options?: any) => {
          if (String(target).includes('.tmp-writes')) {
            throw new Error('adapter refused to open the staged path')
          }
          return base.createWriteStream(target, options)
        }) as IFileSystemComponent['createWriteStream']
      }
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: throwing, logs: await createLogComponent({}) },
        root
      )
      source = bufferToStream(Buffer.from('content'))
      failure = await storage.storeStream('an-id', source).then(
        () => undefined,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should surface the adapter failure', () => {
      expect((failure as Error).message).toMatch(/adapter refused/)
    })

    it('should destroy the caller source rather than leak it', () => {
      expect(source.destroyed).toBe(true)
    })
  })

  describe('when an intent journal removal cannot even be verified', () => {
    let root: string
    let failure: unknown

    beforeEach(async () => {
      // A raw EACCES escaping loses the `context` every call site constructs, which is the only thing
      // telling an operator WHICH invariant broke.
      root = mkdtempSync(path.join(os.tmpdir(), 'unverifiable-removal-'))
      const base = createFsComponent()
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs: base, logs: await createLogComponent({}) },
        root
      )
      await storage.storeStream('some-id', bufferToStream(Buffer.from('x')))
      await storage.stop?.()

      let statShouldFail = false
      const flaky: IFileSystemComponent = {
        ...base,
        // The repair probe must still work; only the VERIFICATION that follows the removal fails, so
        // the failure lands in `removeIntentOrThrow` rather than earlier.
        unlink: (async (target: any) => {
          if (String(target).endsWith('.intent')) {
            statShouldFail = true
            return undefined
          }
          return base.unlink(target)
        }) as IFileSystemComponent['unlink'],
        stat: (async (target: any, ...rest: any[]) => {
          if (statShouldFail && String(target).endsWith('.intent')) {
            throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' })
          }
          return (base.stat as any)(target, ...rest)
        }) as IFileSystemComponent['stat']
      }
      const reopened = await createFolderBasedFileSystemContentStorage(
        { fs: flaky, logs: await createLogComponent({}) },
        root
      )
      failure = await reopened.delete(['some-id']).then(
        () => undefined,
        (error: unknown) => error
      )
      await reopened.stop?.()
    })

    afterEach(() => {
      rmSync(root, { recursive: true, force: true })
    })

    it('should name the invariant that broke rather than surfacing a bare errno', () => {
      expect((failure as Error).message).toMatch(/could not be proven removed/)
    })
  })

  describe('when the startup sweep meets a staged file claimed by a surviving intent', () => {
    let root: string
    let storage: IContentStorageComponent
    let stagedName: string

    beforeEach(async () => {
      // The staged file is the intent's PROOF the rename never landed. Sweeping it away lets a later
      // reconciliation misread a pre-rename intent as a completed commit and delete a valid
      // representation.
      root = mkdtempSync(path.join(os.tmpdir(), 'sweep-claimed-'))
      const shard = path.join(root, '9584')
      await nodeFs.mkdir(shard, { recursive: true })
      await nodeFs.writeFile(path.join(shard, 'some-id'), 'raw')
      await nodeFs.mkdir(path.join(root, '.tmp-writes'), { recursive: true })
      stagedName = 'aaaabbbbccccdddd-00000000000000000000000000000001'
      await nodeFs.writeFile(path.join(root, '.tmp-writes', stagedName), 'staged bytes')
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', intentNameFor('other-id')),
        JSON.stringify({ op: 'raw', id: 'other-id', staged: stagedName })
      )
      // A second, unclaimed staged file from an earlier boot: this one IS sweepable.
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', 'aaaabbbbccccdddd-00000000000000000000000000000002'),
        'orphan'
      )
    })

    afterEach(async () => {
      await storage?.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should sweep the orphan but keep the claimed proof', async () => {
      // Construction reconciles first: the claimed intent is discarded as pre-rename (its staged file
      // is present), which also removes the staged file. Re-create both to exercise the sweep alone.
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        root
      )
      await nodeFs.writeFile(path.join(root, '.tmp-writes', stagedName), 'staged bytes')
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', intentNameFor('other-id')),
        JSON.stringify({ op: 'raw', id: 'other-id', staged: stagedName })
      )
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', 'aaaabbbbccccdddd-00000000000000000000000000000002'),
        'orphan'
      )
      await storage.start?.({} as any)
      await new Promise((resolve) => setTimeout(resolve, 100))

      const entries = await nodeFs.readdir(path.join(root, '.tmp-writes'))
      expect(entries).toContain(stagedName)
      expect(entries).not.toContain('aaaabbbbccccdddd-00000000000000000000000000000002')
    })
  })
})

describe('when a shard directory is destroyed underneath a running instance', () => {
  let root: string
  let storage: IContentStorageComponent
  let shard: string

  beforeEach(async () => {
    // The fault must be detected by an instance that has only ever READ the shard. Reads stopped
    // creating directories, so nothing registered them any more and a restarted read-mostly replica
    // answered `false` for every id in a wiped shard — the "broken store looks like an empty node"
    // outcome the read contract exists to remove.
    root = mkdtempSync(path.join(os.tmpdir(), 'destroyed-shard-'))
    shard = path.join(root, '9584')
    const seed = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root
    )
    await seed.storeStream('some-id', bufferToStream(Buffer.from('content')))
    await seed.stop?.()

    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root
    )
    await storage.exist('some-id')
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  describe('and the instance has only ever read from it', () => {
    beforeEach(() => {
      rmSync(shard, { recursive: true, force: true })
    })

    it('should reject exist rather than report the ids as absent', async () => {
      await expect(storage.exist('some-id')).rejects.toMatchObject({ code: 'ENOENT' })
    })

    it('should reject retrieve for the same reason', async () => {
      await expect(storage.retrieve('some-id')).rejects.toMatchObject({ code: 'ENOENT' })
    })
  })

  describe('and the shard path is replaced by a regular file', () => {
    beforeEach(async () => {
      rmSync(shard, { recursive: true, force: true })
      await nodeFs.writeFile(shard, 'not a directory')
    })

    it('should reject rather than report the ids as absent', async () => {
      await expect(storage.exist('some-id')).rejects.toBeDefined()
    })
  })

  describe('and the STORAGE ROOT itself is destroyed before any operation records a shard', () => {
    // The root is created by construction, and that `mkdir` is an observation — but it was never recorded,
    // so at the one boundary nothing else registers there was no observed ancestor to attribute damage to.
    // A root removed or replaced underneath a live instance therefore classified every read under an
    // uncreated shard as an ordinary miss: `exist()` answered `false` for a storage root that was GONE,
    // which is the "a broken store looks like an empty node" outcome this read contract exists to refuse.
    //
    // Each case constructs and then destroys WITHOUT performing an operation first, because an operation
    // records the shard and would supply the observed ancestor by a different route.
    let parent: string
    let rootPath: string
    let orphaned: IContentStorageComponent

    const build = async (useHashPrefix: boolean): Promise<IContentStorageComponent> => {
      parent = mkdtempSync(path.join(os.tmpdir(), 'root-gone-'))
      rootPath = path.join(parent, 'storage-root')
      return createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        rootPath,
        { disablePrefixHash: !useHashPrefix }
      )
    }

    afterEach(async () => {
      await orphaned?.stop?.().catch(() => undefined)
      if (parent) rmSync(parent, { recursive: true, force: true })
    })

    describe('and hash prefixes are enabled', () => {
      beforeEach(async () => {
        orphaned = await build(true)
        rmSync(rootPath, { recursive: true, force: true })
      })

      it('should reject rather than report the id as absent', async () => {
        await expect(orphaned.exist('some-id')).rejects.toMatchObject({ code: 'ENOENT' })
      })
    })

    describe('and the root is replaced by a regular file', () => {
      beforeEach(async () => {
        orphaned = await build(true)
        rmSync(rootPath, { recursive: true, force: true })
        await nodeFs.writeFile(rootPath, 'not a directory')
      })

      it('should reject rather than report the id as absent', async () => {
        await expect(orphaned.exist('some-id')).rejects.toBeDefined()
      })
    })

    describe('and hash prefixes are disabled, so the root IS the namespace directory', () => {
      beforeEach(async () => {
        orphaned = await build(false)
        rmSync(rootPath, { recursive: true, force: true })
      })

      it('should reject rather than report the id as absent', async () => {
        await expect(orphaned.exist('some-id')).rejects.toMatchObject({ code: 'ENOENT' })
      })

      it('should reject retrieve for the same reason', async () => {
        await expect(orphaned.retrieve('some-id')).rejects.toMatchObject({ code: 'ENOENT' })
      })
    })
  })

  describe('and a store later claims the path a damaged directory occupied', () => {
    // Damage evidence and a legitimately occupied path are different states, and the first has to stop
    // applying once the second is true. It did not: after `<root>/a2` had been observed as a directory and
    // then destroyed, a SUCCESSFUL store of the id `a2` left the damage entry in place, so reads of `a2/b`
    // kept rejecting — while the store side refuses that id as an ordinary prefix collision and `delete`
    // resolves for it. Those three have to agree, and under a legitimately occupied prefix the answer is
    // "absent", which is the case this whole change exists to report correctly.
    let repaired: IContentStorageComponent
    let flatRoot: string

    /**
     * Observes a directory under `a2`, destroys `<root>/a2`, reports the damage once — which is what moves
     * the observation into the damaged set — then stores the id `a2` so it owns the prefix.
     *
     * `nestedId` sets HOW DEEP the observed directory is, and that is the point of parameterising it: damage
     * is recorded against the store's own `dirname`, so `a2/b` files it under `<root>/a2` — the same path the
     * later store commits — while `a2/b/c` files it under `<root>/a2/b`, a DESCENDANT of that path. Clearing
     * only the committed path fixed the first and left the second rejecting.
     */
    const damageThenStore = async (compressed: boolean, nestedId: string): Promise<void> => {
      await repaired.storeStream(nestedId, bufferToStream(Buffer.from('nested')))
      rmSync(path.join(flatRoot, 'a2'), { recursive: true, force: true })
      await expect(repaired.exist(nestedId)).rejects.toBeDefined()
      // The id `a2` now legitimately owns that path — as a file for a raw store, or as `a2.gzip` for a
      // compressed one. Both shapes must clear the evidence: the id exists either way.
      const body = compressed ? Buffer.alloc(3000, 'A') : Buffer.from('content of a2')
      if (compressed) await repaired.storeStreamAndCompress('a2', bufferToStream(body))
      else await repaired.storeStream('a2', bufferToStream(body))
    }

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'repaired-prefix-'))
      repaired = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
    })

    afterEach(async () => {
      await repaired.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    describe('and the store committed the raw representation', () => {
      beforeEach(async () => {
        await damageThenStore(false, 'a2/b')
      })

      it('should report a read under the occupied prefix as absent rather than a fault', async () => {
        expect(await repaired.exist('a2/b')).toBe(false)
      })

      it('should agree with retrieve', async () => {
        expect(await repaired.retrieve('a2/b')).toBeUndefined()
      })

      it('should still serve the id that now owns the path', async () => {
        expect(await repaired.exist('a2')).toBe(true)
      })
    })

    describe('and the damage was recorded against a DESCENDANT of the committed path', () => {
      // The store's own `dirname` is what gets recorded, so a deeper id files the damage below the path the
      // later store commits: `a2/b/c` records `<root>/a2/b` while the store of `a2` owns `<root>/a2`. Clearing
      // only the committed path left this rejecting — the same disagreement one level down.
      beforeEach(async () => {
        await damageThenStore(false, 'a2/b/c')
      })

      it('should report the deeper read as absent rather than a fault', async () => {
        expect(await repaired.exist('a2/b/c')).toBe(false)
      })

      it('should agree with retrieve', async () => {
        expect(await repaired.retrieve('a2/b/c')).toBeUndefined()
      })

      it('should report the intermediate path as absent too', async () => {
        expect(await repaired.exist('a2/b')).toBe(false)
      })

      it('should still serve the id that now owns the prefix', async () => {
        expect(await repaired.exist('a2')).toBe(true)
      })

      it('should still refuse to STORE the shadowed id, with the typed error', async () => {
        // The asymmetry the read answer is derived from: nothing can be created under a path a file occupies,
        // so the store is a caller error while the read has a provable answer.
        await expect(repaired.storeStream('a2/b/c', bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })
    })

    describe('and a COMPRESSED commit claims the prefix instead', () => {
      // A gzip-only commit writes `<root>/a2.gzip` and leaves `<root>/a2` unoccupied, so the filesystem raises
      // no objection to a directory being created there — but a byte RANGE of `a2` has to publish its
      // decompressed copy at exactly that path, so allowing it would leave `a2` able to serve whole reads and
      // never a range, with no way back: the path cannot be freed while the nested id lives there, and neither
      // re-storing nor deleting `a2` recovers. So the nested store is refused, and the read follows it — an id
      // that can never be created reads as absent, which is what keeps store, read and delete agreeing.
      //
      // Both commit shapes therefore answer the SAME way, by different mechanisms: the filesystem cannot hold a
      // file and a directory at one path, and this storage will not put a directory where a range must publish.
      beforeEach(async () => {
        await damageThenStore(true, 'a2/b/c')
      })

      it('should refuse a store of the nested id, with the typed error', async () => {
        await expect(repaired.storeStream('a2/b/c', bufferToStream(Buffer.from('again')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })

      it('should report the nested id as absent, since it can never be created again', async () => {
        expect(await repaired.exist('a2/b/c')).toBe(false)
      })

      it('should agree with retrieve', async () => {
        expect(await repaired.retrieve('a2/b/c')).toBeUndefined()
      })

      it('should let delete resolve for it, as the third surface of the same answer', async () => {
        await expect(repaired.delete(['a2/b/c'])).resolves.toBeUndefined()
      })

      it('should still serve the compressed id that owns the prefix', async () => {
        expect(await repaired.exist('a2')).toBe(true)
      })

      it('should still serve BYTE RANGES of it, which is what the refusal protects', async () => {
        const item = await repaired.retrieve('a2', { start: 0, end: 9 })
        await expect(streamToBuffer(await item!.asStream())).resolves.toHaveLength(10)
      })
    })

    describe('and an UNRELATED directory is damaged when the store commits', () => {
      // The other half of the rule: a store supersedes the damage it actually owns, and nothing else. Clearing
      // the whole set on any successful store would be simpler and wrong — one write anywhere in the root
      // would silence every outstanding damage report, so a destroyed directory elsewhere would go back to
      // reading as an ordinary miss, which is the failure this evidence exists to prevent.
      beforeEach(async () => {
        await repaired.storeStream('x1/b', bufferToStream(Buffer.from('nested')))
        rmSync(path.join(flatRoot, 'x1'), { recursive: true, force: true })
        await expect(repaired.exist('x1/b')).rejects.toBeDefined()
        // Commits somewhere else entirely; it owns nothing under `<root>/x1`.
        await repaired.storeStream('y1', bufferToStream(Buffer.from('unrelated')))
      })

      it('should keep rejecting reads under the still-damaged directory', async () => {
        await expect(repaired.exist('x1/b')).rejects.toBeDefined()
      })
    })

    describe('and a damaged sibling merely SHARES a string prefix with the committed path', () => {
      // `<root>/a2extra` starts with `<root>/a2` as a string but is not beneath it as a path, so a store of
      // `a2` must not supersede its damage. Matching on the bare prefix instead of a separator boundary is
      // the same class of mistake the containment check in `resolveFilePath` guards against.
      beforeEach(async () => {
        await repaired.storeStream('a2extra/b', bufferToStream(Buffer.from('nested')))
        rmSync(path.join(flatRoot, 'a2extra'), { recursive: true, force: true })
        await expect(repaired.exist('a2extra/b')).rejects.toBeDefined()
        await repaired.storeStream('a2', bufferToStream(Buffer.from('content of a2')))
      })

      it('should keep rejecting reads under the damaged sibling', async () => {
        await expect(repaired.exist('a2extra/b')).rejects.toBeDefined()
      })
    })

    describe('and the store committed the compressed representation', () => {
      beforeEach(async () => {
        await damageThenStore(true, 'a2/b')
      })

      it('should report the destroyed nested id as absent, since the compressed id owns the prefix', async () => {
        // Same reasoning as the deeper compressed case above, one level up: `<root>/a2` is where `a2`'s
        // decompressed copy has to be published, so `a2/b` can never be stored and absence is the answer the
        // store and delete surfaces already give.
        expect(await repaired.exist('a2/b')).toBe(false)
      })

      it('should still serve the id it committed', async () => {
        expect(await repaired.exist('a2')).toBe(true)
      })
    })
  })

  describe('when an ancestor cannot be read while an absence is being classified', () => {
    // The classifier walks up statting ancestors, and either probe can fail for a reason that proves nothing
    // — EACCES, EIO. Both fail CLOSED, because the alternative is reporting content absent on no evidence.
    let storage: IContentStorageComponent
    let storageRoot: string
    let unreadable: string

    /** A filesystem that answers normally except for one path, which fails as if permissions were damaged. */
    const failingStatFor = (target: () => string): IFileSystemComponent => {
      const base = createFsComponent()
      return {
        ...base,
        stat: (async (probed: any, ...rest: any[]) => {
          if (String(probed) === target()) {
            throw Object.assign(new Error(`EACCES: permission denied, stat '${probed}'`), { code: 'EACCES' })
          }
          return (base.stat as any)(probed, ...rest)
        }) as IFileSystemComponent['stat']
      }
    }

    afterEach(async () => {
      await storage?.stop?.()
      if (storageRoot) rmSync(storageRoot, { recursive: true, force: true })
    })

    describe('and the walk reaches it above a path that is merely missing', () => {
      beforeEach(async () => {
        storageRoot = mkdtempSync(path.join(os.tmpdir(), 'unreadable-ancestor-'))
        unreadable = ''
        storage = await createFolderBasedFileSystemContentStorage(
          { fs: failingStatFor(() => unreadable), logs: await createLogComponent({}) },
          storageRoot,
          { disablePrefixHash: true }
        )
        // `<root>/x` becomes a real directory, and `<root>/x/y` is simply not there — so the walk steps past the
        // missing level and asks about `<root>/x`, which is the level that cannot be read.
        await storage.storeStream('x/other', bufferToStream(Buffer.from('x')))
        unreadable = path.join(storageRoot, 'x')
      })

      it('should reject rather than report the id absent', async () => {
        await expect(storage.exist('x/y/z')).rejects.toBeDefined()
      })
    })

    describe('and it is a path the walk reaches ABOVE a broken observed directory', () => {
      beforeEach(async () => {
        storageRoot = mkdtempSync(path.join(os.tmpdir(), 'unreadable-walk-'))
        unreadable = ''
        storage = await createFolderBasedFileSystemContentStorage(
          { fs: failingStatFor(() => unreadable), logs: await createLogComponent({}) },
          storageRoot,
          { disablePrefixHash: true }
        )
        await storage.storeStream('a2/b', bufferToStream(Buffer.from('x')))
        rmSync(path.join(storageRoot, 'a2'), { recursive: true, force: true })
        // The walk starts at the broken `<root>/a2` and steps up to the root, which now cannot be read.
        unreadable = storageRoot
      })

      it('should reject rather than report the id absent', async () => {
        await expect(storage.exist('a2/b')).rejects.toBeDefined()
      })
    })
  })

  describe('when a shard holds a file whose name spells no addressable id', () => {
    let storage: IContentStorageComponent
    let storageRoot: string

    beforeEach(async () => {
      // `x.gzip.gzip` would be the compressed representation of `x.gzip`, and no id may end in that suffix,
      // so the derived id is one `resolveFilePath` refuses — the hash-mode half of that rejection.
      storageRoot = mkdtempSync(path.join(os.tmpdir(), 'shard-reserved-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        storageRoot
      )
      await storage.storeStream('real', bufferToStream(Buffer.from('x')))
      const shard = createHash('sha1').update('real').digest('hex').substring(0, 4)
      await nodeFs.writeFile(path.join(storageRoot, shard, 'x.gzip.gzip'), 'foreign')
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(storageRoot, { recursive: true, force: true })
    })

    it('should not enumerate it', async () => {
      const listed: string[] = []
      for await (const id of storage.allFileIds()) listed.push(id)

      expect(listed).toEqual(['real'])
    })
  })

  describe('when a regular file occupies a SHARD path in hash-prefix mode', () => {
    // A file where a shard belongs is foreign to the layout: content lives at `<root>/<shard>/<id>`, so nothing
    // resolves to `<root>/<shard>` and this storage would never put anything there. Treating it as another id's
    // content made every id in that shard — 1/65,536 of the keyspace — read as an empty node, and told a store
    // its id was bad when the id was fine and the tree was not.
    let storage: IContentStorageComponent
    let storageRoot: string
    const idInShard = 'some-id'

    beforeEach(async () => {
      storageRoot = mkdtempSync(path.join(os.tmpdir(), 'file-at-shard-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        storageRoot
      )
      const shard = createHash('sha1').update(idInShard).digest('hex').substring(0, 4)
      await nodeFs.writeFile(path.join(storageRoot, shard), 'foreign file where a shard belongs')
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(storageRoot, { recursive: true, force: true })
    })

    it('should reject exist rather than report the id absent', async () => {
      await expect(storage.exist(idInShard)).rejects.toBeDefined()
    })

    it('should reject fileInfo for the same reason', async () => {
      await expect(storage.fileInfo(idInShard)).rejects.toBeDefined()
    })

    it('should reject retrieve for the same reason', async () => {
      await expect(storage.retrieve(idInShard)).rejects.toBeDefined()
    })

    it('should refuse a store as a storage fault, NOT as a bad content id', async () => {
      // The class matters: a service maps the typed error to 400 and stops retrying, which is wrong when the
      // id is valid and an operator has to clear the obstruction.
      const failure = await storage.storeStream(idInShard, bufferToStream(Buffer.from('x'))).then(
        () => undefined,
        (error) => error
      )

      expect(failure).toBeDefined()
      expect(failure).not.toBeInstanceOf(PathNotContainedError)
    })

    it('should say what is wrong with the tree', async () => {
      await expect(storage.storeStream(idInShard, bufferToStream(Buffer.from('x')))).rejects.toThrow(
        /no content id resolves to/
      )
    })
  })

  describe('when a NESTED id needs a directory under a foreign file', () => {
    // The immediate-parent case and the recursive-mkdir case are different branches, and only the first was
    // classified. `statOccupant` collapses an ancestor ENOTDIR to "absent", so a nested id took the
    // `mkdir(..., { recursive: true })` path, failed ENOTDIR there, and was reported as a bad content id —
    // sending a service to 400 over a valid id and hiding the operator action that would fix the tree.
    const shardOfId = (id: string): string => createHash('sha1').update(id).digest('hex').substring(0, 4)
    let storage: IContentStorageComponent
    let storageRoot: string
    const nestedId = 'some/child'

    beforeEach(async () => {
      storageRoot = mkdtempSync(path.join(os.tmpdir(), 'nested-foreign-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        storageRoot
      )
      // A foreign file where the SHARD belongs, so the obstruction is two levels above the id's own parent.
      await nodeFs.writeFile(path.join(storageRoot, shardOfId(nestedId)), 'foreign')
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(storageRoot, { recursive: true, force: true })
    })

    it('should refuse the store as a storage fault, not as a bad content id', async () => {
      const failure = await storage.storeStream(nestedId, bufferToStream(Buffer.from('x'))).then(
        () => undefined,
        (error) => error
      )

      expect(failure).toBeDefined()
      expect(failure).not.toBeInstanceOf(PathNotContainedError)
    })

    it('should name the obstructing path', async () => {
      await expect(storage.storeStream(nestedId, bufferToStream(Buffer.from('x')))).rejects.toThrow(
        /is not a directory and is not content any id resolves to/
      )
    })
  })

  describe('when a nested id needs a directory under ANOTHER ID content', () => {
    // The counterpart, so the recursive-mkdir branch keeps the typed class where the obstruction really is a
    // prefix collision: `<root>/a` is exactly where the id `a` lives, and `a/b/c` asks for a directory there.
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'nested-content-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      await storage.storeStream('a', bufferToStream(Buffer.from('content')))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should refuse the store with the typed error', async () => {
      await expect(storage.storeStream('a/b/c', bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    })
  })

  describe('when a foreign file is named like the reserved directory plus the compressed suffix', () => {
    // `<root>/.tmp-writes.gzip` is not inside the reserved directory, so it is enumerable — but the id it
    // would stand for is `.tmp-writes`, whose RAW path IS the reserved directory, which every point lookup
    // refuses. Checking only the `.gzip` path for containment accepted it and enumeration reported an id
    // nothing could serve.
    let storage: IContentStorageComponent
    let flatRoot: string
    let listed: string[]

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'reserved-gzip-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      await nodeFs.writeFile(path.join(flatRoot, '.tmp-writes.gzip'), 'foreign')
      await storage.storeStream('real', bufferToStream(Buffer.from('x')))
      listed = []
      for await (const id of storage.allFileIds()) listed.push(id)
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should not enumerate the reserved id', () => {
      expect(listed).not.toContain('.tmp-writes')
    })

    it('should still enumerate the real content', () => {
      expect(listed).toEqual(['real'])
    })

    it('should agree with the point lookup, which refuses that id', async () => {
      await expect(storage.exist('.tmp-writes')).rejects.toBeInstanceOf(PathNotContainedError)
    })
  })

  describe('when a regular file occupies a path that IS another id content', () => {
    // The other side of the same test, so the two classes cannot be conflated: in flat mode `<root>/a` is
    // exactly where the id `a` lives, so `a/b` really is a prefix collision — absent on a read, and a typed
    // rejection on a store.
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'file-at-content-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      await storage.storeStream('a', bufferToStream(Buffer.from('content')))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should report the nested id as absent', async () => {
      expect(await storage.exist('a/b')).toBe(false)
    })

    it('should refuse a store with the typed error', async () => {
      await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    })
  })

  describe('when something that is neither a file nor a directory occupies a path', () => {
    // A fifo, socket or device node is FOREIGN to this storage: no id can create one, so it is a storage
    // fault rather than a bad request — the distinction the typed error draws everywhere else.
    let storage: IContentStorageComponent
    let flatRoot: string
    let madeFifo: boolean

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'foreign-node-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      madeFifo = spawnSync('mkfifo', [path.join(flatRoot, 'pipe')]).status === 0
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should refuse to store an id whose PARENT path is one, as a fault not a bad request', async () => {
      if (!madeFifo) return
      await expect(storage.storeStream('pipe/child', bufferToStream(Buffer.from('x')))).rejects.toThrow(
        /neither a directory nor a regular file/
      )
    })

    it('should refuse to store an id whose own COMMIT TARGET is one', async () => {
      if (!madeFifo) return
      await expect(storage.storeStream('pipe', bufferToStream(Buffer.from('x')))).rejects.toThrow(
        /neither a regular file\s*\n?\s*nor a directory|neither a regular file nor a directory/
      )
    })

    // The direct content path used to be the one place this rule was not applied: every non-file occupant was
    // collapsed into the directory case and answered as a clean miss, so a fifo or socket sitting exactly where
    // an id's content belongs made `exist` false, `fileInfo`/`retrieve` undefined and `existMultiple` carry on —
    // a provably-absent answer for a path that is not empty. Serving it would have been worse still: opening a
    // fifo for reading blocks until a writer appears, so the request would hang instead of failing.
    it('should reject exist rather than reporting absence', async () => {
      if (!madeFifo) return
      await expect(storage.exist('pipe')).rejects.toBeDefined()
    })

    it('should reject fileInfo rather than reporting absence', async () => {
      if (!madeFifo) return
      await expect(storage.fileInfo('pipe')).rejects.toBeDefined()
    })

    it('should reject retrieve rather than reporting absence', async () => {
      if (!madeFifo) return
      await expect(storage.retrieve('pipe')).rejects.toBeDefined()
    })

    it('should reject a byte-range read rather than reporting absence', async () => {
      if (!madeFifo) return
      await expect(storage.retrieve('pipe', { start: 0, end: 4 })).rejects.toBeDefined()
    })

    it('should fail an existMultiple batch containing it rather than answering false for it', async () => {
      if (!madeFifo) return
      await expect(storage.existMultiple(['pipe', 'other'])).rejects.toBeDefined()
    })

    it('should not enumerate it as an id, which no read would accept', async () => {
      if (!madeFifo) return
      // Enumeration and the point lookups have to answer the same question: yielding an id whose `exist()`
      // throws hands a GC sweep a batch that fails on every retry, forever.
      const ids: string[] = []
      for await (const id of storage.allFileIds()) ids.push(id)
      expect(ids).toEqual([])
    })

    it('should reject a read NESTED under it rather than reporting absence', async () => {
      if (!madeFifo) return
      // Nothing can be created beneath it and nothing serves it, so — like a file no id resolves to — it is
      // foreign state in this storage's own tree rather than a prefix another id legitimately owns.
      await expect(storage.exist('pipe/child')).rejects.toBeDefined()
    })
  })

  describe('when an id stored COMPRESSED would have a directory created beside it', () => {
    // `storeStreamAndCompress('a')` leaves only `a.gzip`, so the raw path `a` is free and a nested store used to
    // create a directory there unopposed. Whole reads of `a` kept working and ranges of it then rejected
    // FOREVER — the decompressed copy a range needs has to be published at exactly that path. Nothing could
    // recover it: re-storing `a` raw or compressed is refused (its commit target is now a directory), and
    // deleting `a` leaves the directory behind. The same end state reached the other way round — directory
    // first, then a compressed store — was already refused for precisely this reason, so the verdict depended
    // on arrival order rather than on the state.
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'compressed-prefix-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      await storage.storeStreamAndCompress('a', bufferToStream(Buffer.alloc(3000, 'A')))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should refuse the nested store with the typed error', async () => {
      await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    })

    it('should refuse it however many levels separate the two', async () => {
      // The collision can sit any number of ancestors above the directory being created.
      await expect(storage.storeStream('a/b/c', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    })

    it('should refuse a nested COMPRESSED store too, which commits to the same directory', async () => {
      await expect(
        storage.storeStreamAndCompress('a/b', bufferToStream(Buffer.alloc(3000, 'B')))
      ).rejects.toBeInstanceOf(PathNotContainedError)
    })

    it('should leave NOTHING on disk, since an empty directory would be the breakage itself', async () => {
      // Checked before the mkdir rather than undone after it: this component exposes no `rmdir`, and a directory
      // left behind by the rejection would break ranges exactly as the store would have.
      await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeDefined()

      // The reserved staging directory is this storage's own and always present; nothing else may be.
      const entries = (await nodeFs.readdir(flatRoot)).filter((name) => !name.startsWith('.tmp'))
      expect(entries).toEqual(['a.gzip'])
    })

    it('should keep byte ranges of the compressed id servable, which is the point', async () => {
      await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeDefined()

      const item = await storage.retrieve('a', { start: 0, end: 9 })
      await expect(streamToBuffer(await item!.asStream())).resolves.toEqual(Buffer.alloc(10, 'A'))
    })

    describe('and the raw prefix directory ALREADY exists', () => {
      // The rule is about the state, so it cannot depend on whether this call had to create the directory. A
      // tree written by an older version has exactly this shape — the nested store used to be allowed — as does
      // one where a version that created directories on read, or an operator, got there first. Gated on the
      // mkdir, the check let a store commit content below the very path `a`'s range cache must publish at.
      beforeEach(async () => {
        await nodeFs.mkdir(path.join(flatRoot, 'a'))
      })

      it('should refuse the nested store just the same', async () => {
        await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })

      it('should refuse it when the pre-existing directory is deeper than the collision', async () => {
        await nodeFs.mkdir(path.join(flatRoot, 'a', 'b'))

        await expect(storage.storeStream('a/b/c', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })

      it('should add nothing under it, so the tree gains no unservable content', async () => {
        await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeDefined()

        await expect(nodeFs.readdir(path.join(flatRoot, 'a'))).resolves.toEqual([])
      })

      describe('and something has already put that directory in the mkdir-skip cache', () => {
        // That cache means "this is a directory RIGHT NOW", which a read, an enumeration, the absence classifier
        // and the commit-target probe all establish — and none of them runs this check. Gating on it let the
        // guard lapse the moment anything looked at the tree: enumerating the root was enough.
        it('should still refuse after allFileIds() has warmed it', async () => {
          for await (const _unusedId of storage.allFileIds()) {
            // Drained for the side effect: the walk records every directory it opens.
          }

          await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
            PathNotContainedError
          )
        })

        it('should still refuse after a read of the nested id has warmed it', async () => {
          await storage.exist('a/b').catch(() => undefined)

          await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
            PathNotContainedError
          )
        })

        it('should still refuse after a read of another file inside it has warmed it', async () => {
          await nodeFs.writeFile(path.join(flatRoot, 'a', 'pre'), 'x')
          await storage.exist('a/pre').catch(() => undefined)

          await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
            PathNotContainedError
          )
        })
      })
    })

    describe('and repeated stores go into a directory the store path has already checked', () => {
      // The check is per DIRECTORY, not per store: once the store path has asked, repeat stores skip it and the
      // write path costs exactly what it did before. Only PASSES are cached, so a refusal is re-decided every
      // time and deleting the compressed id lifts it at once.
      let countingRoot: string
      let countingStorage: IContentStorageComponent
      let twinProbes: number

      beforeEach(async () => {
        countingRoot = mkdtempSync(path.join(os.tmpdir(), 'prefix-verify-once-'))
        twinProbes = 0
        const base = createFsComponent()
        const countingFs: IFileSystemComponent = {
          ...base,
          stat: (async (probed: any, ...rest: any[]) => {
            if (String(probed) === path.join(countingRoot, 'd.gzip')) twinProbes++
            return base.stat(probed, ...rest)
          }) as any
        }
        countingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: countingFs, logs: await createLogComponent({}) },
          countingRoot,
          { disablePrefixHash: true }
        )
        for (let i = 0; i < 5; i++) {
          await countingStorage.storeStream(`d/x${i}`, bufferToStream(Buffer.from('x')))
        }
      })

      afterEach(async () => {
        await countingStorage.stop?.()
        rmSync(countingRoot, { recursive: true, force: true })
      })

      it('should probe the compressed twin once, not once per store', () => {
        expect(twinProbes).toBe(1)
      })

      it('should have stored every id', async () => {
        await expect(countingStorage.exist('d/x4')).resolves.toBe(true)
      })
    })

    describe('and the directory vanishes before a compressed store takes the freed name', () => {
      // The one sequence that can falsify a cached pass, and it needs the directory to be removed from under a
      // live instance: the compressed store is legal once the raw path is free, and a store nesting under it
      // afterwards finds both cache entries stale. It cannot create the collision — the rename has no directory
      // to land in — but the first attempt reports that as a bare ENOENT rather than the typed refusal, and only
      // the retry (past the invalidated cache entry) gives the right class. Pinned as the known edge it is.
      let staleRoot: string
      let staleStorage: IContentStorageComponent

      beforeEach(async () => {
        staleRoot = mkdtempSync(path.join(os.tmpdir(), 'prefix-vanished-'))
        staleStorage = await createFolderBasedFileSystemContentStorage(
          { fs: createFsComponent(), logs: await createLogComponent({}) },
          staleRoot,
          { disablePrefixHash: true }
        )
        await staleStorage.storeStream('a/b', bufferToStream(Buffer.from('nested')))
        await staleStorage.delete(['a/b'])
        await nodeFs.rmdir(path.join(staleRoot, 'a'))
        await staleStorage.storeStreamAndCompress('a', bufferToStream(Buffer.alloc(3000, 'A')))
      })

      afterEach(async () => {
        await staleStorage.stop?.()
        rmSync(staleRoot, { recursive: true, force: true })
      })

      it('should not commit content below the compressed id raw path', async () => {
        await expect(staleStorage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeDefined()

        await expect(nodeFs.stat(path.join(staleRoot, 'a', 'b'))).rejects.toMatchObject({ code: 'ENOENT' })
      })

      it('should give the typed refusal once the stale cache entry is gone', async () => {
        await staleStorage.storeStream('a/b', bufferToStream(Buffer.from('nested'))).catch(() => undefined)

        await expect(staleStorage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })

      it('should keep byte ranges of the compressed id servable throughout', async () => {
        await staleStorage.storeStream('a/b', bufferToStream(Buffer.from('nested'))).catch(() => undefined)

        const item = await staleStorage.retrieve('a', { start: 0, end: 9 })
        await expect(streamToBuffer(await item!.asStream())).resolves.toEqual(Buffer.alloc(10, 'A'))
      })
    })

    it('should still allow a nested store once the compressed id is deleted', async () => {
      // The rule is about the STATE, so clearing it lifts the refusal.
      await storage.delete(['a'])

      await expect(storage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).resolves.toBeUndefined()
    })

    it('should still allow an unrelated nested store beside it', async () => {
      await expect(storage.storeStream('other/b', bufferToStream(Buffer.from('nested')))).resolves.toBeUndefined()
    })

    describe('and hash prefixes put the two ids in different shards', () => {
      let hashRoot: string
      let hashStorage: IContentStorageComponent

      beforeEach(async () => {
        hashRoot = mkdtempSync(path.join(os.tmpdir(), 'compressed-prefix-hash-'))
        hashStorage = await createFolderBasedFileSystemContentStorage(
          { fs: createFsComponent(), logs: await createLogComponent({}) },
          hashRoot
        )
        await hashStorage.storeStreamAndCompress('a', bufferToStream(Buffer.alloc(3000, 'A')))
      })

      afterEach(async () => {
        await hashStorage.stop?.()
        rmSync(hashRoot, { recursive: true, force: true })
      })

      it('should allow the nested store, since the two never share a directory', async () => {
        await expect(hashStorage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).resolves.toBeUndefined()
      })

      it('should keep byte ranges of the compressed id servable', async () => {
        await hashStorage.storeStream('a/b', bufferToStream(Buffer.from('nested')))

        const item = await hashStorage.retrieve('a', { start: 0, end: 9 })
        await expect(streamToBuffer(await item!.asStream())).resolves.toEqual(Buffer.alloc(10, 'A'))
      })

      it('should not refuse for a compressed file no id resolves to', async () => {
        // A `.gzip` in the WRONG shard is foreign state, not another id's content: `a`'s compressed form lives
        // under `sha1('a')`, so a file of that name beside `a/b`'s own shard protects no id at all, and refusing
        // for it would send a service to 400 over a valid id it can never correct. The canonicality test is what
        // separates the two, and it is also what keeps this check off the hot path — a shard name never spells
        // its own id, so hash-mode stores settle it without a single stat.
        const nestedShard = path.join(hashRoot, createHash('sha1').update('a/b').digest('hex').substring(0, 4))
        await nodeFs.mkdir(nestedShard, { recursive: true })
        await nodeFs.writeFile(path.join(nestedShard, 'a.gzip'), 'foreign')

        await expect(hashStorage.storeStream('a/b', bufferToStream(Buffer.from('nested')))).resolves.toBeUndefined()
      })
    })
  })

  describe('when a foreign node occupies ONE representation of an id', () => {
    // A fifo at `<id>.gzip` says nothing about the raw file beside it. Faulting on the first probe would answer
    // "cannot be read" for content this storage can read — the same bug as reporting a fault as absence, only
    // pointing the other way — so the fault is deferred and raised only if no representation serves the id.
    let storage: IContentStorageComponent
    let flatRoot: string
    let madeFifo: boolean

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'foreign-representation-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    describe('and it is the COMPRESSED path while the raw content is intact', () => {
      beforeEach(async () => {
        await storage.storeStream('a', bufferToStream(Buffer.from('REAL')))
        madeFifo = spawnSync('mkfifo', [path.join(flatRoot, 'a.gzip')]).status === 0
      })

      it('should still report the id present', async () => {
        if (!madeFifo) return
        expect(await storage.exist('a')).toBe(true)
      })

      it('should still serve the raw bytes', async () => {
        if (!madeFifo) return
        const item = await storage.retrieve('a')
        await expect(streamToBuffer(await item!.asStream())).resolves.toEqual(Buffer.from('REAL'))
      })

      it('should still enumerate the id exactly once', async () => {
        if (!madeFifo) return
        // The gzip-name set must not count it either: a fifo named `a.gzip` is not a compressed
        // representation, so suppressing the raw entry for it would leave the id yielded by NEITHER.
        const ids: string[] = []
        for await (const id of storage.allFileIds()) ids.push(id)
        expect(ids).toEqual(['a'])
      })
    })

    describe('and it is the RAW path while the compressed content is intact', () => {
      beforeEach(async () => {
        await storage.storeStreamAndCompress('g', bufferToStream(Buffer.alloc(3000, 'A')))
        madeFifo = spawnSync('mkfifo', [path.join(flatRoot, 'g')]).status === 0
      })

      it('should still report the id present', async () => {
        if (!madeFifo) return
        expect(await storage.exist('g')).toBe(true)
      })

      it('should still enumerate the id, from its compressed entry', async () => {
        if (!madeFifo) return
        const ids: string[] = []
        for await (const id of storage.allFileIds()) ids.push(id)
        expect(ids).toEqual(['g'])
      })
    })

    describe('and NEITHER representation holds content', () => {
      beforeEach(() => {
        madeFifo = spawnSync('mkfifo', [path.join(flatRoot, 'a.gzip')]).status === 0
      })

      it('should reject the read, since the deferred fault is all that is left', async () => {
        if (!madeFifo) return
        await expect(storage.exist('a')).rejects.toBeDefined()
      })

      it('should not enumerate the id', async () => {
        if (!madeFifo) return
        const ids: string[] = []
        for await (const id of storage.allFileIds()) ids.push(id)
        expect(ids).toEqual([])
      })
    })

    describe('and the directory holding it is too large to buffer', () => {
      beforeEach(async () => {
        for (let i = 0; i < MAX_BUFFERED_DIRECTORY_ENTRIES + 4; i++) {
          await nodeFs.writeFile(path.join(flatRoot, `f${String(i).padStart(6, '0')}`), 'x')
        }
        madeFifo = spawnSync('mkfifo', [path.join(flatRoot, 'zzpipe')]).status === 0
      })

      it('should skip it on the streaming pass too, yielding every real id', async () => {
        if (!madeFifo) return
        const ids: string[] = []
        for await (const id of storage.allFileIds()) ids.push(id)
        expect(ids).toEqual(expect.not.arrayContaining(['zzpipe']))
      })

      it('should still yield all of the real entries', async () => {
        if (!madeFifo) return
        const ids: string[] = []
        for await (const id of storage.allFileIds()) ids.push(id)
        expect(ids).toHaveLength(MAX_BUFFERED_DIRECTORY_ENTRIES + 4)
      })
    })
  })

  describe('when a compressed representation is too small to hold a trailer', () => {
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      // The gzip format keeps the original size in the last four bytes, so a file under eight bytes cannot
      // carry one — the logical size is genuinely unknown rather than guessable.
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'tiny-gzip-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      await nodeFs.writeFile(path.join(flatRoot, 'tiny.gzip'), Buffer.from([0x1f, 0x8b, 0x08, 0x00]))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should report the content size as unknown rather than inventing one', async () => {
      expect((await storage.fileInfo('tiny'))?.contentSize).toBeNull()
    })

    it('should still report the stored size', async () => {
      expect((await storage.fileInfo('tiny'))?.size).toBe(4)
    })
  })

  describe('when an id has an over-long segment that is not its last', () => {
    let storage: IContentStorageComponent

    beforeEach(() => {
      // The budget differs by position: a FINAL segment must also leave room for `.gzip`, since that is the
      // id's compressed representation, while an intermediate one gets all of NAME_MAX. The rejection says
      // which rule it applied.
      storage = createInMemoryStorage()
    })

    it('should reject naming the plain NAME_MAX budget', async () => {
      const overLong = `${'x'.repeat(300)}/tail`

      await expect(storage.storeStream(overLong, bufferToStream(Buffer.from('x')))).rejects.toThrow(
        /past the 255 this storage can address/
      )
    })

    it('should reject a final segment against the smaller budget instead', async () => {
      const overLongTail = `head/${'x'.repeat(300)}`

      await expect(storage.storeStream(overLongTail, bufferToStream(Buffer.from('x')))).rejects.toThrow(
        /less \.gzip for the compressed representation/
      )
    })
  })

  describe('when the storage root is given with a trailing separator', () => {
    let storage: IContentStorageComponent
    let storageRoot: string

    beforeEach(async () => {
      // Stripped at construction, because a root that keeps one breaks the containment comparison for every
      // id: `'/data/x/id'.startsWith('/data/x//')` is false.
      storageRoot = mkdtempSync(path.join(os.tmpdir(), 'trailing-sep-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        storageRoot + path.sep
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(storageRoot, { recursive: true, force: true })
    })

    it('should store and read back through the normalized root', async () => {
      await storage.storeStream('an-id', bufferToStream(Buffer.from('content')))

      expect(await storage.exist('an-id')).toBe(true)
    })

    it('should still refuse an id that escapes the root', async () => {
      await expect(storage.exist('../evil')).rejects.toBeInstanceOf(PathNotContainedError)
    })
  })

  describe('when enumeration recovers an id from a file path', () => {
    // Slicing the path is only the inverse of storing when the file is where this storage would have put it.
    // A foreign file in the wrong shard yielded an id whose own shard is elsewhere, so `exist()` answered
    // false for something enumeration had just reported — and a GC consumer acting on the pair would delete
    // the REAL id from its own shard while leaving the foreign file behind.
    const shardOfId = (id: string): string => createHash('sha1').update(id).digest('hex').substring(0, 4)
    let storage: IContentStorageComponent
    let storageRoot: string

    afterEach(async () => {
      await storage?.stop?.()
      if (storageRoot) rmSync(storageRoot, { recursive: true, force: true })
    })

    describe('and the file sits in a shard that is not the one its id hashes to', () => {
      let listed: string[]

      beforeEach(async () => {
        storageRoot = mkdtempSync(path.join(os.tmpdir(), 'enum-noncanonical-'))
        storage = await createFolderBasedFileSystemContentStorage(
          { fs: createFsComponent(), logs: await createLogComponent({}) },
          storageRoot
        )
        await storage.storeStream('a/b', bufferToStream(Buffer.from('x')))
        const observed = path.join(storageRoot, shardOfId('a/b'), 'a')
        rmSync(observed, { recursive: true, force: true })
        await nodeFs.writeFile(observed, 'foreign')
        listed = []
        for await (const id of storage.allFileIds()) listed.push(id)
      })

      it('should not yield the derived id', async () => {
        expect(listed).not.toContain('a')
      })

      it('should agree with the point lookup, which cannot serve it', async () => {
        // The round-trip contract: enumeration must only ever yield ids the point lookups accept.
        expect(await storage.exist('a')).toBe(false)
      })
    })

    describe('and the file spells a name no id can have', () => {
      let listed: string[]

      beforeEach(async () => {
        // `x.gzip.gzip` is the compressed representation of `x.gzip`, and no id may end in that suffix — so
        // this is a name nothing storable produces. It used to be yielded as `x.gzip`, whose `exist()` then
        // THREW `PathNotContainedError`, the same shape as the empty-id case this walk already guards.
        storageRoot = mkdtempSync(path.join(os.tmpdir(), 'enum-reserved-'))
        storage = await createFolderBasedFileSystemContentStorage(
          { fs: createFsComponent(), logs: await createLogComponent({}) },
          storageRoot,
          { disablePrefixHash: true }
        )
        await nodeFs.writeFile(path.join(storageRoot, 'x.gzip.gzip'), 'foreign')
        await storage.storeStream('real', bufferToStream(Buffer.from('x')))
        listed = []
        for await (const id of storage.allFileIds()) listed.push(id)
      })

      it('should not yield it', () => {
        expect(listed).not.toContain('x.gzip')
      })

      it('should still yield the ids that are real', () => {
        expect(listed).toEqual(['real'])
      })
    })

    describe('and the root is heavy with SUBDIRECTORIES rather than files', () => {
      let listed: string[]
      let expected: string[]

      beforeEach(async () => {
        // Past the buffered cap the walk streams, and it used to collect every subdirectory NAME before
        // descending — one string per top-level directory, which is the same unbounded shape as holding one
        // per entry. It now descends inline. This pins the behaviour that change had to preserve: every id
        // still comes out exactly once.
        storageRoot = mkdtempSync(path.join(os.tmpdir(), 'enum-dirheavy-'))
        storage = await createFolderBasedFileSystemContentStorage(
          { fs: createFsComponent(), logs: await createLogComponent({}) },
          storageRoot,
          { disablePrefixHash: true }
        )
        expected = []
        for (let i = 0; i < MAX_BUFFERED_DIRECTORY_ENTRIES + 20; i++) {
          const id = `d${String(i).padStart(6, '0')}/x`
          await nodeFs.mkdir(path.join(storageRoot, path.dirname(id)), { recursive: true })
          await nodeFs.writeFile(path.join(storageRoot, id), 'x')
          expected.push(id)
        }
        listed = []
        for await (const id of storage.allFileIds()) listed.push(id)
      })

      it('should yield every id exactly once', () => {
        expect(listed.slice().sort()).toEqual(expected.slice().sort())
      })
    })
  })

  describe('and an observed directory is replaced by a file in HASH-PREFIX mode', () => {
    // A depth test is enough in flat mode and wrong here: an id's shard is `sha1(the whole id)`, so a file at
    // `<root>/<shard>/a` is content for the id `a` only when that shard is `sha1('a')`. With `a/b` stored, the
    // observed directory sits in `sha1('a/b')`'s shard, and a file put there is reachable as NO id at all —
    // `exist('a')` looks in a different shard — so it is foreign state where a directory used to be, which is
    // damage rather than a legitimately claimed prefix.
    const shardOfId = (id: string): string => createHash('sha1').update(id).digest('hex').substring(0, 4)
    let storage: IContentStorageComponent
    let hashRoot: string

    beforeEach(async () => {
      hashRoot = mkdtempSync(path.join(os.tmpdir(), 'hash-noncanonical-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        hashRoot
      )
      await storage.storeStream('a/b', bufferToStream(Buffer.from('x')))
      const observed = path.join(hashRoot, shardOfId('a/b'), 'a')
      rmSync(observed, { recursive: true, force: true })
      await nodeFs.writeFile(observed, 'foreign, and not any id this storage serves')
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(hashRoot, { recursive: true, force: true })
    })

    it('should reject exist rather than report the id absent', async () => {
      await expect(storage.exist('a/b')).rejects.toBeDefined()
    })

    it('should reject fileInfo for the same reason', async () => {
      await expect(storage.fileInfo('a/b')).rejects.toBeDefined()
    })

    it('should reject retrieve for the same reason', async () => {
      await expect(storage.retrieve('a/b')).rejects.toBeDefined()
    })

    it('should not serve that file as the id its name spells, since it is in the wrong shard', async () => {
      // The reason absence would be wrong: nothing resolves to that path, so calling it "content" credits an
      // id this storage cannot actually serve.
      expect(await storage.exist('a')).toBe(false)
    })
  })

  describe('and the file replacing it IS the canonical path for its id', () => {
    // The other side, which must stay absence. It needs `p` and `p/child` to land in one shard, so the id is
    // searched for rather than hard-coded — deterministic, since it only depends on sha1.
    const shardOfId = (id: string): string => createHash('sha1').update(id).digest('hex').substring(0, 4)
    let storage: IContentStorageComponent
    let hashRoot: string
    let collidingId: string

    beforeEach(async () => {
      collidingId = ''
      for (let i = 0; i < 400_000 && !collidingId; i++) {
        const candidate = `k${i}`
        if (shardOfId(candidate) === shardOfId(`${candidate}/child`)) collidingId = candidate
      }
      expect(collidingId).not.toBe('')

      hashRoot = mkdtempSync(path.join(os.tmpdir(), 'hash-canonical-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        hashRoot
      )
      // Observes the directory, has it destroyed, then legitimately stores the id that owns that exact path.
      await storage.storeStream(`${collidingId}/child`, bufferToStream(Buffer.from('x')))
      rmSync(path.join(hashRoot, shardOfId(collidingId), collidingId), { recursive: true, force: true })
      await storage.storeStream(collidingId, bufferToStream(Buffer.from('legitimate content')))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(hashRoot, { recursive: true, force: true })
    })

    it('should report the nested id as absent, because the prefix is real content', async () => {
      expect(await storage.exist(`${collidingId}/child`)).toBe(false)
    })

    it('should serve the id that owns the path', async () => {
      expect(await storage.exist(collidingId)).toBe(true)
    })
  })

  describe('and the file replacing it is the canonical COMPRESSED path for its id', () => {
    // `<id>.gzip` is this storage's own name for the compressed representation, so it counts as content too —
    // stripping the suffix before resolving is what makes that work, since no id may end in it.
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'gzip-canonical-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      // `g.gzip/inner` is a legal id — only an id ENDING in `.gzip` is reserved — so this observes the
      // directory `<root>/g.gzip`, which is also where `g`'s compressed representation belongs.
      await storage.storeStream('g.gzip/inner', bufferToStream(Buffer.from('x')))
      rmSync(path.join(flatRoot, 'g.gzip'), { recursive: true, force: true })
      await storage.storeStreamAndCompress('g', bufferToStream(Buffer.alloc(3000, 'A')))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should report the nested id as absent, because the compressed representation owns the path', async () => {
      expect(await storage.exist('g.gzip/inner')).toBe(false)
    })

    it('should serve the compressed id itself', async () => {
      expect(await storage.exist('g')).toBe(true)
    })
  })

  describe('and the directory was observed by a probe that then REFUSED the operation', () => {
    // The commit-target check proves a directory is there and then refuses to overwrite it. It records it
    // anyway: otherwise an observation depends on whether the operation that made it SUCCEEDED, and a later
    // removal of that directory reads as an ordinary miss for ids nested under it.
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'refused-observation-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should remember a directory the COMMIT-TARGET check refused to overwrite', async () => {
      // `<root>/a` is a directory this instance did not create, holding another id's nested content, so the
      // store of `a` is refused at its commit target — and that refusal is the only thing that observes it.
      await nodeFs.mkdir(path.join(flatRoot, 'a'))
      await nodeFs.writeFile(path.join(flatRoot, 'a', 'inner'), 'x')
      await expect(storage.storeStream('a', bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )

      rmSync(path.join(flatRoot, 'a'), { recursive: true, force: true })

      await expect(storage.exist('a/inner')).rejects.toBeDefined()
    })

    it('should report ids under a directory blocking gzip-only content as absent, not damaged', async () => {
      // This shape can now only be reached by FOREIGN action — a store that would create the directory beside
      // `g.gzip` is refused — and once it is reached, ids nested under it are unstorable, so their reads answer
      // absence however many times this instance saw the directory. The observation is still recorded (the
      // commit-target case above covers that); what this asserts is that being unstorable wins over it.
      await storage.storeStreamAndCompress('g', bufferToStream(Buffer.alloc(3000, 'A')))
      await nodeFs.mkdir(path.join(flatRoot, 'g'))
      await nodeFs.writeFile(path.join(flatRoot, 'g', 'inner'), 'x')
      await expect(storage.retrieve('g', { start: 0, end: 9 })).rejects.toBeDefined()

      rmSync(path.join(flatRoot, 'g'), { recursive: true, force: true })

      expect(await storage.exist('g/inner')).toBe(false)
    })
  })

  describe('and the directory was observed by CLIMBING to it from a deeper miss', () => {
    // The classifier walks up when the id's own parent is absent, and an intact ancestor it stats there is an
    // observation exactly like one the immediate-parent probe makes. It was not recorded, so the SAME directory
    // was observed or not depending only on the depth of the id that touched it first: `exist('d/x')` went
    // through the parent probe and recorded `<root>/d`, while `exist('d/e/f')` climbed to it and did not — so a
    // later removal rejected for one id and answered `false` for the other.
    let storage: IContentStorageComponent
    let flatRoot: string

    /** Reads `id`, then destroys `<root>/d` and reports how a read under it answers afterwards. */
    const observeThenRemove = async (id: string): Promise<'rejected' | 'absent'> => {
      expect(await storage.exist(id)).toBe(false)
      rmSync(path.join(flatRoot, 'd'), { recursive: true, force: true })
      return storage.exist('d/e/f').then(
        () => 'absent' as const,
        () => 'rejected' as const
      )
    }

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'climbed-observation-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      // A directory this instance did not create, so only a read can put it in the observation log.
      await nodeFs.mkdir(path.join(flatRoot, 'd'))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should reject after removal when the observation came from the immediate parent', async () => {
      expect(await observeThenRemove('d/x')).toBe('rejected')
    })

    it('should reject after removal when the observation came from a DEEPER miss', async () => {
      expect(await observeThenRemove('d/e/f')).toBe('rejected')
    })

    it('should still report an ordinary miss under a directory it never saw', async () => {
      // The other side: climbing must not invent observations for paths that were never there.
      expect(await storage.exist('never/seen/here')).toBe(false)
    })
  })

  describe('and the directory was observed some way other than by storing its own id', () => {
    // The contract is "a directory this instance created or observed", and three ways of learning one existed
    // did not record it: a recursive `mkdir` recorded only the leaf it was asked for, a lookup that found a
    // directory at a content path discarded the fact, and the enumeration walk opened directories without
    // noting them. Each left a directory this instance provably saw outside the evidence the classifier reads,
    // so its later removal was classified as an ordinary miss.
    let storage: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'observed-ways-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    describe('and it is an INTERMEDIATE directory a recursive mkdir created', () => {
      beforeEach(async () => {
        // Creates `<root>/a` and `<root>/a/b`; only the second is the dirname the store asked for.
        await storage.storeStream('a/b/c', bufferToStream(Buffer.from('x')))
        rmSync(path.join(flatRoot, 'a'), { recursive: true, force: true })
      })

      it('should reject a read under the intermediate directory it created', async () => {
        // `a/other` was never stored, but the directory that would have held the answer is gone, so absence
        // cannot be proven — and this instance created that directory, which is what makes it damage.
        await expect(storage.exist('a/other')).rejects.toBeDefined()
      })

      it('should still reject the read whose own parent was recorded', async () => {
        await expect(storage.exist('a/b/c')).rejects.toBeDefined()
      })
    })

    describe('and it was seen by a LOOKUP that found a directory at a content path', () => {
      beforeEach(async () => {
        await nodeFs.mkdir(path.join(flatRoot, 'd'), { recursive: true })
        await nodeFs.writeFile(path.join(flatRoot, 'd', 'inner'), 'x')
        // Reports `false` — a directory is not content — but the instance has now seen `<root>/d` be one.
        expect(await storage.exist('d')).toBe(false)
        rmSync(path.join(flatRoot, 'd'), { recursive: true, force: true })
      })

      it('should reject a later read nested under it', async () => {
        await expect(storage.exist('d/inner')).rejects.toBeDefined()
      })
    })

    describe('and it was seen by ENUMERATION', () => {
      beforeEach(async () => {
        await nodeFs.mkdir(path.join(flatRoot, 'e'), { recursive: true })
        await nodeFs.writeFile(path.join(flatRoot, 'e', 'inner'), 'x')
        const listed: string[] = []
        for await (const id of storage.allFileIds()) listed.push(id)
        expect(listed).toEqual(['e/inner'])
        rmSync(path.join(flatRoot, 'e'), { recursive: true, force: true })
      })

      it('should reject a later read nested under the walked directory', async () => {
        await expect(storage.exist('e/inner')).rejects.toBeDefined()
      })
    })

    describe('and nothing was ever observed on the path', () => {
      it('should still report an ordinary miss', async () => {
        // The other side of the rule: recording more observations must not turn never-created trees into
        // faults, or every unknown id in a fresh root would reject.
        expect(await storage.exist('untouched/child')).toBe(false)
      })
    })
  })

  describe('and the root observation has been dropped from the bounded cache', () => {
    // The root is seeded into `knownDirectories`, but that set is FIFO-bounded and the root is inserted
    // FIRST, so it is the first entry evicted once a flat-mode deployment observes more than
    // MAX_KNOWN_DIRECTORIES directories — which nested ids reach in ordinary operation. Filling that cache
    // in a test would mean creating 100,000 directories, so this drives the SAME loss through
    // `forgetDirectory(root)`, which a write failing ENOTDIR under a file-occupied root reaches immediately.
    // Either way the observation is gone from the set, which is the state that must not decide the answer.
    let orphaned: IContentStorageComponent
    let parentDir: string
    let rootPath: string

    beforeEach(async () => {
      parentDir = mkdtempSync(path.join(os.tmpdir(), 'root-evicted-'))
      rootPath = path.join(parentDir, 'storage-root')
      orphaned = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        rootPath,
        { disablePrefixHash: true }
      )
      rmSync(rootPath, { recursive: true, force: true })
      await nodeFs.writeFile(rootPath, 'not a directory')
      // Fails ENOTDIR at its staged write, and `writingUnder` responds by forgetting the root entry.
      await expect(orphaned.storeStream('some-id', bufferToStream(Buffer.from('x')))).rejects.toBeDefined()
    })

    afterEach(async () => {
      await orphaned?.stop?.().catch(() => undefined)
      rmSync(parentDir, { recursive: true, force: true })
    })

    it('should still reject rather than report the id as absent', async () => {
      await expect(orphaned.exist('some-id')).rejects.toBeDefined()
    })

    it('should still reject retrieve', async () => {
      await expect(orphaned.retrieve('some-id')).rejects.toBeDefined()
    })
  })

  describe('and an ancestor of an id path is another id content file', () => {
    let obstructed: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      // Flat mode, so ids share one namespace directory and `a/b/c/d` really does sit under `a/b`.
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'obstructed-'))
      obstructed = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      // Creates the directory `a` and the FILE `a/b`, so `a/b/c` can never be a directory.
      await obstructed.storeStream('a/b', bufferToStream(Buffer.from('content')))
    })

    afterEach(async () => {
      await obstructed.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    // THIS EXPECTATION WAS FLIPPED DELIBERATELY. It used to assert a bare ENOTDIR rejection, on the
    // reasoning that an obstruction above a shard is a hard fault. The rejection was the wrong answer for
    // THIS shape: a filesystem cannot hold a file and a directory at one path, so no file can exist at
    // `a/b/c/d` while `a/b` is a regular file — the id is provably absent, exactly as an over-long name
    // is. Rejecting made the three surfaces contradict each other (see the assertions below) and let one
    // such id destroy a whole batch. What still rejects is damage this instance can PROVE: a directory it
    // observed that stops being one, which the sibling describes above cover.
    it('should report the nested id as absent rather than rejecting', async () => {
      expect(await obstructed.exist('a/b/c/d')).toBe(false)
      expect(await obstructed.retrieve('a/b/c/d')).toBeUndefined()
      expect(await obstructed.fileInfo('a/b/c/d')).toBeUndefined()
    })

    it('should agree with delete, which already resolved for such an id', async () => {
      await expect(obstructed.delete(['a/b/c/d'])).resolves.toBeUndefined()
      expect(await obstructed.exist('a/b/c/d')).toBe(false)
    })

    it('should not lose the answers for every other id in a batch', async () => {
      // The whole batch used to reject on the obstructed id, so a GC or availability sweep learned
      // nothing about the ids around it — and got a 5xx for a question with a provable answer.
      const answers = await obstructed.existMultiple(['a/b', 'a/b/c/d'])
      expect(answers.get('a/b')).toBe(true)
      expect(answers.get('a/b/c/d')).toBe(false)
    })

    it('should still refuse to STORE the id, with the typed error', async () => {
      // A read asks a question whose true answer is "nothing"; a store asks to CREATE something no
      // filesystem can hold. Only the latter is a caller error, so only it rejects.
      await expect(obstructed.storeStream('a/b/c/d', bufferToStream(Buffer.from('nope')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    })

    it('should report absence even for a prefix this instance had observed as a directory', async () => {
      // What OCCUPIES the path decides, not what this instance saw there before. A file at a content path is
      // served as an id, so ids under it are unstorable — `store` refuses them, `delete` resolves for them —
      // and reads answering absence is what makes those three agree. Deciding from the observation instead
      // would have two nodes with identical disks answer differently, and would leave reads as the only
      // surface reporting a fault for an id the storage itself declares unstorable. The transition is still
      // logged, since it destroyed whatever was under that path.
      await obstructed.storeStream('a2/b', bufferToStream(Buffer.from('content')))
      rmSync(path.join(flatRoot, 'a2'), { recursive: true, force: true })
      await nodeFs.writeFile(path.join(flatRoot, 'a2'), 'not a directory')

      expect(await obstructed.exist('a2/b')).toBe(false)
    })

    it('should serve the file now occupying that path as its own id', async () => {
      // The reason absence is honest rather than silent: the node is not claiming to hold nothing, it is
      // claiming to hold `a2` and not `a2/b`, which is exactly the on-disk truth.
      await obstructed.storeStream('a2/b', bufferToStream(Buffer.from('content')))
      rmSync(path.join(flatRoot, 'a2'), { recursive: true, force: true })
      await nodeFs.writeFile(path.join(flatRoot, 'a2'), 'not a directory')

      expect(await obstructed.exist('a2')).toBe(true)
    })
  })

  describe('and an observed directory is replaced by content, at every depth', () => {
    // A file at a content path is served as an id, so every id nested under it is unstorable. Reads report
    // that as absence, which is what makes them agree with the store (a typed rejection) and with `delete`
    // (resolves) — and the answer is the same at every depth, which is the property that kept breaking while
    // it was derived from remembered damage keyed on one path.
    let damaged: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'damaged-deep-'))
      damaged = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      // Records `<root>/a2` as a directory this instance created...
      await damaged.storeStream('a2/b', bufferToStream(Buffer.from('content')))
      // ...which is then destroyed and replaced by a regular file, i.e. by something servable as the id `a2`.
      rmSync(path.join(flatRoot, 'a2'), { recursive: true, force: true })
      await nodeFs.writeFile(path.join(flatRoot, 'a2'), 'not a directory')
    })

    afterEach(async () => {
      await damaged.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it.each([['a2/b'], ['a2/b/c'], ['a2/b/c/d/e']])('should report %s as absent', async (id) => {
      expect(await damaged.exist(id)).toBe(false)
    })

    it('should agree with retrieve at depth', async () => {
      expect(await damaged.retrieve('a2/b/c/d/e')).toBeUndefined()
    })

    it('should refuse to STORE the nested id, with the typed error', async () => {
      // The asymmetry that makes absence the right read answer: nothing can be created under a file, so the
      // store is a caller error while the read has a provable answer.
      //
      // The read comes first deliberately. `ensureDirectoryFor` skips its occupant probe for a directory the
      // mkdir-skip cache still believes in, and this instance created `<root>/a2` before it was replaced — so
      // with a warm entry the store fails at its commit rename with a bare errno instead. The read is what
      // drops that entry (the classifier forgets every path it proves is not a directory), which is also the
      // realistic order: a service reads before it decides to store.
      expect(await damaged.exist('a2/b')).toBe(false)

      await expect(damaged.storeStream('a2/b', bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    })

    it('should resolve a delete of the nested id, as it already did', async () => {
      await expect(damaged.delete(['a2/b'])).resolves.toBeUndefined()
    })

    it('should give the same answer every time the id is asked about', async () => {
      // Derived from the tree on each call, so repetition cannot change it. This is what the previous
      // remembered-damage design could not hold: the first read rejected and every later one answered
      // `false` over an unchanged disk, because the report consumed its own evidence.
      const answers: boolean[] = []
      for (let attempt = 0; attempt < 3; attempt++) answers.push(await damaged.exist('a2/b'))

      expect(answers).toEqual([false, false, false])
    })
  })

  describe('and an observed directory deeper in the tree is REMOVED', () => {
    let removed: IContentStorageComponent
    let flatRoot: string

    beforeEach(async () => {
      // The same hole in the same shape, one branch over: the ENOENT parent probe also decided damage from
      // `knownDirectories.has(dirname)` alone, so a removed observed directory stayed loud for its immediate
      // child and went silent for every deeper descendant. Removal and replacement-by-a-file are one
      // question — "did a directory this instance observed stop being usable?" — so they must answer alike.
      flatRoot = mkdtempSync(path.join(os.tmpdir(), 'removed-deep-'))
      removed = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      // Records `<root>/a2` as a directory this instance created...
      await removed.storeStream('a2/b', bufferToStream(Buffer.from('content')))
      // ...which is then removed outright, so the parent probe fails ENOENT rather than ENOTDIR.
      rmSync(path.join(flatRoot, 'a2'), { recursive: true, force: true })
    })

    afterEach(async () => {
      await removed.stop?.()
      rmSync(flatRoot, { recursive: true, force: true })
    })

    it('should reject for the immediate child of the removed directory', async () => {
      await expect(removed.exist('a2/b')).rejects.toMatchObject({ code: 'ENOENT' })
    })

    // Each of these is the FIRST read this instance performs, so no earlier call can have invalidated the
    // entry the report is derived from — the reviewed defect is reachable from a cold instance.
    it('should reject when a deeper descendant is the first id read', async () => {
      await expect(removed.exist('a2/b/c')).rejects.toMatchObject({ code: 'ENOENT' })
    })

    it('should reject when a descendant several levels below is the first id read', async () => {
      await expect(removed.exist('a2/b/c/d/e')).rejects.toMatchObject({ code: 'ENOENT' })
    })

    it('should reject retrieve of a deeper descendant as the first read', async () => {
      await expect(removed.retrieve('a2/b/c/d/e')).rejects.toMatchObject({ code: 'ENOENT' })
    })

    it('should reject fileInfo of a deeper descendant as the first read', async () => {
      await expect(removed.fileInfo('a2/b/c/d/e')).rejects.toMatchObject({ code: 'ENOENT' })
    })

    it('should give the same answer every time the id is asked about', async () => {
      const answers: string[] = []
      for (let attempt = 0; attempt < 3; attempt++) {
        answers.push(
          await removed
            .exist('a2/b/c')
            .then(() => 'resolved')
            .catch(() => 'rejected')
        )
      }

      expect(answers).toEqual(['rejected', 'rejected', 'rejected'])
    })

    it('should let the FIRST write after the damage report recreate the directory', async () => {
      // The two contracts that pull in opposite directions once a directory breaks, and the reason the
      // observation is held in a second set rather than dropped from the mkdir-skip cache: a read has to keep
      // rejecting (evidence must survive) while a write has to recreate the tree on its next attempt (the
      // mkdir must not be skipped). Serving both from one flag meant breaking one of them.
      await expect(removed.exist('a2/b')).rejects.toMatchObject({ code: 'ENOENT' })

      await expect(removed.storeStream('a2/b', bufferToStream(Buffer.from('again')))).resolves.toBeUndefined()
    })

    it('should let a write heal the directory after a DEEPER read reported the damage', async () => {
      // The report can name an ANCESTOR of the id's own parent, and it is THAT path which has to leave the
      // mkdir-skip cache. Reading the immediate child cannot show this, because there the damaged path and
      // `dirname` coincide and the plain `forgetDirectory(dirname)` covers it either way; reading `a2/b/c`
      // makes them differ, so only moving the named path out of the cache lets the write recreate the tree.
      await expect(removed.exist('a2/b/c')).rejects.toMatchObject({ code: 'ENOENT' })

      await expect(removed.storeStream('a2/b', bufferToStream(Buffer.from('again')))).resolves.toBeUndefined()
    })

    it('should serve the id again once a write has repaired the directory, clearing the damage', async () => {
      await expect(removed.exist('a2/b')).rejects.toMatchObject({ code: 'ENOENT' })
      await removed.storeStream('a2/b', bufferToStream(Buffer.from('again')))

      expect(await removed.exist('a2/b')).toBe(true)
    })

    it('should still report an id under a shard that was never created as absent', async () => {
      // The ordinary miss, which must stay a miss: nothing on `<root>/never/...` was ever observed, so there
      // is no damage to report even though a removed observed directory exists elsewhere in this root.
      expect(await removed.exist('never/stored/here')).toBe(false)
    })
  })

  describe('and the parent directory cannot be probed at all', () => {
    let unreadable: IContentStorageComponent

    beforeEach(async () => {
      // The file stat says ENOENT but the parent probe fails EACCES: this storage cannot tell whether
      // the id is missing or its tree is broken, so it must not answer "missing".
      const failRoot = mkdtempSync(path.join(os.tmpdir(), 'unprobeable-'))
      const base = createFsComponent()
      const flaky: IFileSystemComponent = {
        ...base,
        stat: (async (target: any, ...rest: any[]) => {
          if (String(target).endsWith('9584')) {
            throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' })
          }
          return (base.stat as any)(target, ...rest)
        }) as IFileSystemComponent['stat']
      }
      unreadable = await createFolderBasedFileSystemContentStorage(
        { fs: flaky, logs: await createLogComponent({}) },
        failRoot
      )
    })

    afterEach(async () => {
      await unreadable.stop?.()
    })

    it('should reject instead of reporting the id as absent', async () => {
      await expect(unreadable.exist('some-id')).rejects.toMatchObject({ code: 'ENOENT' })
    })
  })
})

describe('when a root carries an intent journal written by a pre-sha256 version', () => {
  let root: string
  let storage: IContentStorageComponent
  let staleRawPath: string

  beforeEach(async () => {
    // Upgrade path: the old code named journals sha1(id).intent. Failing to recognize one leaves the
    // mixed state it describes in place, with reads preferring the stale counterpart forever.
    root = mkdtempSync(path.join(os.tmpdir(), 'legacy-intent-'))
    const shard = path.join(root, '9584')
    await nodeFs.mkdir(shard, { recursive: true })
    staleRawPath = path.join(shard, 'some-id')
    await nodeFs.writeFile(staleRawPath, 'stale raw')
    await nodeFs.writeFile(path.join(shard, 'some-id.gzip'), gzipSync(Buffer.from('committed gzip')))
    await nodeFs.mkdir(path.join(root, '.tmp-writes'), { recursive: true })
    await nodeFs.writeFile(
      path.join(root, '.tmp-writes', `${createHash('sha1').update('some-id').digest('hex')}.intent`),
      JSON.stringify({ op: 'gzip', id: 'some-id', staged: 'deadbeefdeadbeef-00000000000000000000000000000000' })
    )
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should reconcile it and remove the stale counterpart', async () => {
    expect(await createFsComponent().existPath(staleRawPath)).toBe(false)
  })

  it('should discharge the legacy journal', async () => {
    const entries = await nodeFs.readdir(path.join(root, '.tmp-writes'))

    expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
  })
})

describe('when the staging directory is healed in flat mode', () => {
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    // Healing used to drop the ownership marker, so the next start refused over its own staged file.
    root = mkdtempSync(path.join(os.tmpdir(), 'marker-healing-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await storage.storeStream('some-id', bufferToStream(Buffer.from('first')))
    rmSync(path.join(root, '.tmp-writes'), { recursive: true, force: true })
    await storage.storeStream('some-id', bufferToStream(Buffer.from('second'))).catch(() => undefined)
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should restore the ownership marker', async () => {
    expect(await createFsComponent().existPath(path.join(root, '.tmp-writes', '.owned-by-catalyst-storage'))).toBe(true)
  })

  it('should let a later instance start over a root that still holds a staged file', async () => {
    await nodeFs.writeFile(
      path.join(root, '.tmp-writes', 'aaaabbbbccccdddd-00000000000000000000000000000009'),
      'staged'
    )

    const reopened = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await reopened.stop?.()

    expect(reopened).toBeDefined()
  })
})

describe('when SimpleContentItem is constructed the way a JavaScript caller can', () => {
  // TypeScript requires `encoding`, but this ships as CommonJS and `SimpleContentItem` is @public, so
  // a JS consumer can pass fewer arguments. Doing so used to crash the constructor with a message
  // naming neither the class nor the argument. The cast models that caller.
  let construct: (...args: unknown[]) => SimpleContentItem

  beforeEach(() => {
    const Loose = SimpleContentItem as unknown as new (...args: unknown[]) => SimpleContentItem
    construct = (...args: unknown[]) => new Loose(...args)
  })

  it('should not throw when the encoding argument is omitted', () => {
    expect(() => construct(async () => bufferToStream(Buffer.from('x')), 1)).not.toThrow()
  })

  it('should treat an omitted encoding as unencoded', () => {
    expect(construct(async () => bufferToStream(Buffer.from('x')), 1).encoding).toBeNull()
  })
})

describe('when a ContentItem carries a multi-value content encoding', () => {
  let payload: Buffer

  beforeEach(() => {
    // S3 writes `aws-chunked` itself on flexible-checksum uploads, commonly alongside a real coding.
    payload = Buffer.from('the real content')
  })

  it('should decode the real coding and ignore the transfer coding', async () => {
    const item = new SimpleContentItem(async () => bufferToStream(gzipSync(payload)), 40, 'gzip, aws-chunked')

    expect((await streamToBuffer(await item.asStream())).toString()).toBe('the real content')
  })

  it('should treat a bare transfer coding as unencoded', async () => {
    const item = new SimpleContentItem(async () => bufferToStream(payload), payload.length, 'aws-chunked')

    expect((await streamToBuffer(await item.asStream())).toString()).toBe('the real content')
  })
})

describe('when enumerating a directory through allFileIds', () => {
  /** A directory listing this test controls, yielded lazily so consumption can be observed. */
  const dirOf = (names: string[], onEntryRead: () => void) => ({
    async *[Symbol.asyncIterator]() {
      for (const name of names) {
        onEntryRead()
        yield { name, isDirectory: () => false }
      }
    }
  })

  describe('and a compressed sibling is listed after the raw file it belongs to', () => {
    let listed: string[]

    beforeEach(async () => {
      // Directory order must not decide the answer: the raw is reached before its `.gzip` is seen,
      // and both are one id.
      const root = mkdtempSync(path.join(os.tmpdir(), 'sibling-order-'))
      // Both representations are written for real, so the forced listing DESCRIBES the directory
      // rather than contradicting it. The skip is confirmed against the filesystem (a `gzipNames`
      // entry from the first pass can be stale by the second), so a listing announcing a `.gzip` that
      // was never created is not the state this test means to pin — it is the id-hiding bug the
      // confirmation exists to catch.
      await nodeFs.writeFile(path.join(root, 'some-id'), 'the decompressed cache')
      await nodeFs.writeFile(path.join(root, 'some-id.gzip'), gzipSync(Buffer.from('the content')))
      const base = createFsComponent()
      const ordered: IFileSystemComponent = {
        ...base,
        opendir: (async () => dirOf(['some-id', 'some-id.gzip'], () => undefined)) as any
      }
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs: ordered, logs: await createLogComponent({}) },
        root,
        { disablePrefixHash: true }
      )
      listed = []
      for await (const each of storage.allFileIds()) listed.push(each)
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should yield the id exactly once', () => {
      expect(listed).toEqual(['some-id'])
    })
  })

  describe('and the directory holds far more entries than the yielding pass needs', () => {
    let readWhenFirstYielded: number
    let total: number

    beforeEach(async () => {
      // The pass that YIELDS must stream. Buffering it retained ~300 bytes per entry — hundreds of
      // megabytes for a large flat-mode root, allocated before a single id came out. Counting how
      // much of the second listing has been consumed at the first yield pins that directly, with no
      // dependence on heap measurement.
      total = 5000
      const names = Array.from({ length: total }, (_, index) => `id-${String(index).padStart(6, '0')}`)
      const root = mkdtempSync(path.join(os.tmpdir(), 'lazy-walk-'))
      const base = createFsComponent()
      let pass = 0
      let readThisPass = 0
      const counting: IFileSystemComponent = {
        ...base,
        opendir: (async () => {
          pass++
          readThisPass = 0
          return dirOf(names, () => {
            readThisPass++
          })
        }) as any
      }
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs: counting, logs: await createLogComponent({}) },
        root,
        { disablePrefixHash: true }
      )
      readWhenFirstYielded = total
      for await (const _id of storage.allFileIds()) {
        if (pass === 2) readWhenFirstYielded = readThisPass
        break
      }
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should yield the first id without draining the listing', () => {
      expect(readWhenFirstYielded).toBeLessThan(total)
    })

    it('should yield it after reading only the entries it needed', () => {
      expect(readWhenFirstYielded).toBe(1)
    })
  })
})

describe('when a cached entry is touched while a size-eviction pass is already running', () => {
  let unlinked: string[]

  beforeEach(async () => {
    // The TTL loop re-reads `lastAccess` per entry, so it already skips an entry touched mid-pass.
    // The SIZE loop cannot: it works from a snapshot sorted by `lastAccess`, so a victim chosen
    // before the touch was still evicted afterwards — LRU deleting a file that had just become the
    // most recently used, out from under the reader that touched it. Admission-triggered passes made
    // that window wide, because eviction now starts while callers are still working.
    unlinked = []
    let firstUnlinkSeen: () => void = () => undefined
    let releaseFirstUnlink: () => void = () => undefined
    const firstUnlink = new Promise<void>((resolve) => (firstUnlinkSeen = resolve))
    const gate = new Promise<void>((resolve) => (releaseFirstUnlink = resolve))

    const cache = createDecompressCache(
      {
        logger: { log() {}, info() {}, debug() {}, warn() {}, error() {} } as any,
        fsInvariants: {
          existsForInvariant: async () => false,
          noFailUnlink: async (target: string) => {
            unlinked.push(target)
            if (unlinked.length === 1) {
              firstUnlinkSeen()
              await gate
            }
            return true
          }
        }
      },
      { ttl: 3_600_000, maxSize: 100 }
    )

    // Three small entries, oldest first, then one large enough to cross the budget. Recording the
    // large one is what starts the pass, whose snapshot therefore ranks '/a' before '/b' before '/c'.
    for (const name of ['/a', '/b', '/c']) {
      cache.record(name, 10)
      await new Promise((resolve) => setTimeout(resolve, 2))
    }
    cache.record('/big', 100)

    await firstUnlink
    // '/b' is used after the pass has already chosen it as a victim.
    await new Promise((resolve) => setTimeout(resolve, 5))
    cache.touch('/b')
    releaseFirstUnlink()
    await cache.evict()
  })

  it('should evict the oldest entry it selected', () => {
    expect(unlinked).toContain('/a')
  })

  it('should not evict the entry that was touched after the pass chose it', () => {
    expect(unlinked).not.toContain('/b')
  })
})
