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

    describe('and a compressed commit leaves the raw path FREE', () => {
      // A gzip-only commit writes `<root>/a2.gzip` and leaves `<root>/a2` unoccupied, so — unlike the raw
      // commit above — it does NOT claim the prefix: `store('a2/b')` would succeed, creating the directory
      // alongside the gzip. The nested id is therefore not a prefix collision, it is a genuinely destroyed
      // one, and a fault is the answer this storage has always given for a directory it observed and lost.
      //
      // So the two commit shapes answer DIFFERENTLY here, and that is the on-disk truth rather than a
      // caprice: the store side diverges the same way, succeeding for one and refusing for the other.
      beforeEach(async () => {
        await damageThenStore(true, 'a2/b/c')
      })

      it('should report the destroyed nested id as a fault, since the prefix is not occupied', async () => {
        await expect(repaired.exist('a2/b/c')).rejects.toBeDefined()
      })

      it('should let a store of that nested id succeed, which is why absence would be wrong', async () => {
        await expect(repaired.storeStream('a2/b/c', bufferToStream(Buffer.from('again')))).resolves.toBeUndefined()
      })

      it('should serve it again once that store has repaired the tree', async () => {
        await repaired.storeStream('a2/b/c', bufferToStream(Buffer.from('again')))

        expect(await repaired.exist('a2/b/c')).toBe(true)
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

      it('should report the destroyed nested id as a fault, since the raw path stays free', async () => {
        // Same reasoning as the deeper compressed case above: nothing occupies `<root>/a2`, so `a2/b` is
        // storable and its content was destroyed rather than shadowed.
        await expect(repaired.exist('a2/b')).rejects.toBeDefined()
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

    describe('and it is the deepest observed ancestor, above the id own parent', () => {
      beforeEach(async () => {
        storageRoot = mkdtempSync(path.join(os.tmpdir(), 'unreadable-ancestor-'))
        unreadable = ''
        storage = await createFolderBasedFileSystemContentStorage(
          { fs: failingStatFor(() => unreadable), logs: await createLogComponent({}) },
          storageRoot,
          { disablePrefixHash: true }
        )
        // Observes `<root>/a`, so a read of `a/b/c` looks there rather than at its own parent `<root>/a/b`.
        await storage.storeStream('a/b', bufferToStream(Buffer.from('x')))
        unreadable = path.join(storageRoot, 'a')
      })

      it('should reject rather than report the id absent', async () => {
        await expect(storage.exist('a/b/c')).rejects.toBeDefined()
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

    it('should not report it as content on a read', async () => {
      if (!madeFifo) return
      expect(await storage.exist('pipe')).toBe(false)
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
