import { createHash } from 'crypto'
import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { Readable, Writable } from 'stream'
import { gzipSync } from 'zlib'
import { createLogComponent } from '@well-known-components/logger'
import {
  assertAddressableContentId,
  bufferToStream,
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  createInMemoryStorage,
  IContentStorageComponent,
  IFileSystemComponent,
  mapWithConcurrency,
  PathNotContainedError,
  SimpleContentItem,
  streamToBuffer
} from '../src'
import { intentNameFor } from './file-system-utils'
import { createDecompressCache } from '../src/folder-based/decompress-cache'

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

      beforeEach(() => {
        source = bufferToStream(Buffer.from('compressed somehow'))
        item = new SimpleContentItem(async () => source, 18, 'x-unknown-coding')
      })

      it('should reject rather than hand back encoded bytes labelled as decoded', async () => {
        await expect(item.asStream()).rejects.toThrow(/unsupported encoding/)
      })

      it('should release the opened source instead of leaking it', async () => {
        await item.asStream().catch(() => undefined)

        expect(source.destroyed).toBe(true)
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

  describe('when a range is requested on gzip-only content without rename support', () => {
    let noRename: IContentStorageComponent
    let payload: Buffer

    beforeEach(async () => {
      // Without rename there is nowhere to publish a decompressed copy atomically, so the inflation
      // used to stream straight onto the canonical path — where a concurrent reader could stat the
      // half-written file and be served truncated bytes as valid content.
      root = mkdtempSync(path.join(os.tmpdir(), 'no-rename-range-'))
      payload = Buffer.from(Array.from({ length: 5000 }, (_, index) => String.fromCharCode(97 + (index % 26))).join(''))
      const base = realFs()
      const withoutRename = { ...base }
      delete (withoutRename as { rename?: unknown }).rename
      const shard = shardOf(root)
      await nodeFs.mkdir(shard, { recursive: true })
      await nodeFs.writeFile(path.join(shard, `${id}.gzip`), gzipSync(payload))
      noRename = await createFolderBasedFileSystemContentStorage(
        { fs: withoutRename as IFileSystemComponent, logs: await logs() },
        root
      )
      storage = noRename
    })

    it('should serve exactly the requested slice of the decompressed content', async () => {
      const item = await noRename.retrieve(id, { start: 100, end: 199 })

      expect((await streamToBuffer(await item!.asStream())).equals(payload.subarray(100, 200))).toBe(true)
    })

    it('should report an unknown length rather than one derived from the gzip trailer', async () => {
      // The trailer is stored, attacker-controlled data that is only accurate mod 2^32 and describes
      // just the last member of a multi-member gzip, so it must not bound a read.
      const item = await noRename.retrieve(id, { start: 100, end: 199 })

      expect(item!.size).toBeNull()
    })

    it('should not publish a decompressed copy at the canonical path', async () => {
      await noRename.retrieve(id, { start: 0, end: 9 })

      expect(await realFs().existPath(path.join(shardOf(root), id))).toBe(false)
    })

    it('should clamp a range that runs past the end of the content', async () => {
      const item = await noRename.retrieve(id, { start: 4990, end: 99999 })

      expect((await streamToBuffer(await item!.asStream())).equals(payload.subarray(4990))).toBe(true)
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

  describe('when a legacy in-place store cannot remove the previous gzip and cannot roll back', () => {
    let root: string
    let storage: IContentStorageComponent
    let failure: unknown

    beforeEach(async () => {
      // The old rollback was unverified and unconditional: it claimed "rolled back" while the raw
      // survived, and — when the gzip removal had already succeeded and only its verification threw —
      // it removed the raw too, destroying both representations.
      root = mkdtempSync(path.join(os.tmpdir(), 'inplace-rollback-'))
      const base = createFsComponent()
      const noRename: IFileSystemComponent = {
        ...base,
        unlink: (async (_target: any) => {
          // Neither the gzip nor the rollback of the raw can be removed.
          throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
        }) as IFileSystemComponent['unlink']
      }
      delete (noRename as { rename?: unknown }).rename
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: noRename, logs: await createLogComponent({}) },
        root
      )
      const shard = path.join(root, '9584')
      await nodeFs.mkdir(shard, { recursive: true })
      await nodeFs.writeFile(path.join(shard, 'some-id.gzip'), gzipSync(Buffer.from('previous version')))

      failure = await storage.storeStream('some-id', bufferToStream(Buffer.from('new content'))).then(
        () => undefined,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should report that both representations are on disk instead of claiming a rollback', () => {
      expect((failure as Error).message).toMatch(/both its previous gzip and the newly written raw/)
    })
  })

  describe('when a legacy in-place store cannot verify whether the previous gzip survived', () => {
    let root: string
    let storage: IContentStorageComponent
    let failure: unknown

    beforeEach(async () => {
      // The gzip unlink SUCCEEDS and only its verification throws: removing the raw as well would
      // leave the id with no representation at all.
      root = mkdtempSync(path.join(os.tmpdir(), 'inplace-kept-raw-'))
      const base = createFsComponent()
      let statShouldFail = false
      const noRename: IFileSystemComponent = {
        ...base,
        stat: (async (target: any, ...rest: any[]) => {
          if (statShouldFail && String(target).endsWith('.gzip')) {
            throw Object.assign(new Error('EIO: i/o error'), { code: 'EIO' })
          }
          return (base.stat as any)(target, ...rest)
        }) as IFileSystemComponent['stat'],
        unlink: (async (target: any) => {
          const result = await base.unlink(target)
          if (String(target).endsWith('.gzip')) statShouldFail = true
          return result
        }) as IFileSystemComponent['unlink']
      }
      delete (noRename as { rename?: unknown }).rename
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: noRename, logs: await createLogComponent({}) },
        root
      )
      const shard = path.join(root, '9584')
      await nodeFs.mkdir(shard, { recursive: true })
      await nodeFs.writeFile(path.join(shard, 'some-id.gzip'), gzipSync(Buffer.from('previous version')))

      failure = await storage.storeStream('some-id', bufferToStream(Buffer.from('new content'))).then(
        () => undefined,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should report that the gzip state could not be determined instead of guessing', () => {
      expect((failure as Error).message).toMatch(/could not determine whether its previous gzip/)
    })

    it('should leave the newly written raw on disk', async () => {
      expect(await createFsComponent().existPath(path.join(root, '9584', 'some-id'))).toBe(true)
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

  describe('and an ancestor of the shard stops being a directory', () => {
    let obstructed: IContentStorageComponent

    beforeEach(async () => {
      // ENOTDIR from the PARENT probe means a hard obstruction above the shard; it used to be
      // lumped in with "this shard was never created" and answered as a miss.
      const flatRoot = mkdtempSync(path.join(os.tmpdir(), 'obstructed-'))
      obstructed = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        flatRoot,
        { disablePrefixHash: true }
      )
      await obstructed.storeStream('a/b', bufferToStream(Buffer.from('content')))
    })

    afterEach(async () => {
      await obstructed.stop?.()
    })

    it('should reject a read nested under the obstruction', async () => {
      await expect(obstructed.exist('a/b/c/d')).rejects.toMatchObject({ code: 'ENOTDIR' })
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

describe('when a legacy-mode range spans many chunks of the inflated stream', () => {
  let root: string
  let storage: IContentStorageComponent
  let payload: Buffer

  beforeEach(async () => {
    // A payload far larger than one stream chunk, so the slice transform's "entirely before" and
    // "entirely after" branches are actually taken rather than the whole body arriving at once.
    root = mkdtempSync(path.join(os.tmpdir(), 'multi-chunk-range-'))
    payload = Buffer.alloc(600_000)
    for (let index = 0; index < payload.length; index++) payload[index] = index % 251
    const base = createFsComponent()
    const withoutRename = { ...base }
    delete (withoutRename as { rename?: unknown }).rename
    const shard = path.join(root, '9584')
    await nodeFs.mkdir(shard, { recursive: true })
    await nodeFs.writeFile(path.join(shard, 'some-id.gzip'), gzipSync(payload))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: withoutRename as IFileSystemComponent, logs: await createLogComponent({}) },
      root
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should serve exactly the requested bytes from the middle of the content', async () => {
    const item = await storage.retrieve('some-id', { start: 300_000, end: 300_099 })

    expect((await streamToBuffer(await item!.asStream())).equals(payload.subarray(300_000, 300_100))).toBe(true)
  })

  it('should serve a range that starts inside one chunk and ends inside a later one', async () => {
    const item = await storage.retrieve('some-id', { start: 65_000, end: 200_000 })

    expect((await streamToBuffer(await item!.asStream())).equals(payload.subarray(65_000, 200_001))).toBe(true)
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
