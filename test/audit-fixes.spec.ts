import { createHash } from 'crypto'
import { mkdtempSync, rmSync, createReadStream, promises as nodeFsPromises } from 'fs'
import os from 'os'
import path from 'path'
import { Readable, Writable } from 'stream'
import { gzipSync } from 'zlib'
import { createLogComponent } from '@well-known-components/logger'
import {
  assertStorableStream,
  bufferToStream,
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  createInMemoryStorage,
  createS3BasedFileSystemContentStorage,
  streamToBuffer,
  FileTypeLoader,
  IContentStorageComponent,
  IFileSystemComponent,
  PathNotContainedError
} from '../src'
import { createFakeS3Client } from './fake-s3-client'

/** The real detector is ESM-only and reached through an import Jest's registry does not own. */
const undetectingLoader: FileTypeLoader = async () => ({ fileTypeFromBuffer: async () => undefined })

describe('when a store is handed a source something has already read from', () => {
  // The four state flags the guard used to rely on describe a FINISHED stream and all flip a tick after
  // the read that made the source unusable, so a caller that hashed, sniffed or measured a body first
  // walked straight through — and the store RESOLVED, three different ways.
  let root: string
  let payload: string
  let folderStorage: IContentStorageComponent
  let memoryStorage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'partially-consumed-'))
    payload = path.join(root, 'payload.bin')
    await nodeFsPromises.writeFile(payload, 'AAAABBBBCCCC')
    folderStorage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      path.join(root, 'store'),
      { disablePrefixHash: true }
    )
    memoryStorage = createInMemoryStorage()
  })

  afterEach(async () => {
    await folderStorage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  describe('and the source is a file stream a caller peeked four bytes from', () => {
    let source: Readable

    beforeEach(async () => {
      source = createReadStream(payload)
      await new Promise((resolve) => source.once('readable', resolve))
      source.read(4)
    })

    it('should reject rather than storing the body without its first four bytes', async () => {
      await expect(folderStorage.storeStream('peeked', source)).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })

    it('should leave nothing stored under the id', async () => {
      await folderStorage.storeStream('peeked', source).catch(() => undefined)

      expect(await folderStorage.exist('peeked')).toBe(false)
    })
  })

  describe('and the source is a push-based stream a caller pulled one chunk from', () => {
    let source: Readable

    beforeEach(async () => {
      source = new Readable({ read() {} })
      source.push(Buffer.from('AAAA'))
      source.push(Buffer.from('BBBB'))
      source.push(null)
      await source[Symbol.asyncIterator]().next()
    })

    it('should reject rather than committing a zero-byte object', async () => {
      await expect(folderStorage.storeStream('drained', source)).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })
  })

  describe('and the source is a Readable.from whose iterator was left suspended', () => {
    let source: Readable

    beforeEach(async () => {
      source = Readable.from([Buffer.from('AAAA'), Buffer.from('BBBB'), Buffer.from('CCCC')])
      await source[Symbol.asyncIterator]().next()
    })

    it('should reject rather than never settling', async () => {
      // The suspended iterator's own 'readable' listener takes precedence over `pipe`, so nothing ever
      // flowed and this call used to hang forever — there is no timeout anywhere in the library.
      await expect(folderStorage.storeStream('suspended', source)).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })
  })

  describe('and the same source shape is handed to the in-memory backend', () => {
    let source: Readable

    beforeEach(async () => {
      source = createReadStream(payload)
      await new Promise((resolve) => source.once('readable', resolve))
      source.read(4)
    })

    it('should reject there too, so the backends agree', async () => {
      await expect(memoryStorage.storeStream('peeked', source)).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })
  })

  describe('and the source has not been read from at all', () => {
    let source: Readable

    beforeEach(() => {
      source = createReadStream(payload)
    })

    it('should store the complete body', async () => {
      await folderStorage.storeStream('untouched', source)
      const item = await folderStorage.retrieve('untouched')

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('AAAABBBBCCCC')
    })
  })

  describe('and the source is live but empty', () => {
    let source: Readable

    beforeEach(() => {
      source = Readable.from([])
    })

    it('should still be accepted, because empty content is storable', async () => {
      await expect(folderStorage.storeStream('empty', source)).resolves.toBeUndefined()
    })
  })
})

describe('when a caller checks a source with the exported guard before storing it', () => {
  let source: Readable

  beforeEach(async () => {
    source = Readable.from([Buffer.from('AAAA'), Buffer.from('BBBB')])
    await source[Symbol.asyncIterator]().next()
  })

  it('should throw for a partially consumed source, so the caller need not learn it from a failed store', () => {
    expect(() => assertStorableStream(source)).toThrow('Stream closed before it ended.')
  })
})

describe('when a store rejects before it has consumed the source', () => {
  // Nothing had piped the stream, so nothing destroyed it, and an fs.ReadStream has no finalizer: a
  // service passing untrusted ids leaked one descriptor (or one undrained request socket) per rejected
  // call until EMFILE took down all storage.
  let root: string
  let payload: string
  let folderStorage: IContentStorageComponent
  let memoryStorage: IContentStorageComponent
  let source: Readable

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'store-leak-'))
    payload = path.join(root, 'payload.bin')
    await nodeFsPromises.writeFile(payload, 'CONTENT')
    folderStorage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      path.join(root, 'store'),
      { disablePrefixHash: true }
    )
    memoryStorage = createInMemoryStorage()
    source = createReadStream(payload)
  })

  afterEach(async () => {
    await folderStorage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  describe('and the id traverses out of the namespace', () => {
    it('should destroy the caller source on the folder-based backend', async () => {
      await expect(folderStorage.storeStream('../escape', source)).rejects.toBeInstanceOf(PathNotContainedError)

      expect(source.destroyed).toBe(true)
    })

    it('should destroy the caller source on the in-memory backend', async () => {
      await expect(memoryStorage.storeStream('../escape', source)).rejects.toBeInstanceOf(PathNotContainedError)

      expect(source.destroyed).toBe(true)
    })
  })

  describe('and a cancellation signal is also supplied', () => {
    let controller: AbortController

    beforeEach(() => {
      controller = new AbortController()
    })

    it('should still destroy the caller source', async () => {
      await expect(folderStorage.storeStream('../escape', source, controller.signal)).rejects.toBeInstanceOf(
        PathNotContainedError
      )

      expect(source.destroyed).toBe(true)
    })
  })
})

describe('when content is deleted between retrieve and the consumer opening the stream', () => {
  // The unencoded path returned the source itself with no 'error' listener, so this documented race
  // emitted an unhandled 'error' — which terminates the process by default. The gzip path was safe only
  // incidentally, because `pipeline` keeps a listener on the source.
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'read-delete-race-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await storage.storeStream('plain', bufferToStream(Buffer.from('hello world')))
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should not raise an unhandled error event when the stream is piped without an error handler', async () => {
    const item = await storage.retrieve('plain')
    await storage.delete(['plain'])
    const stream = await item!.asStream()
    await new Promise((resolve) => setImmediate(resolve))

    const uncaught: unknown[] = []
    const onUncaught = (error: unknown): void => {
      uncaught.push(error)
    }
    process.on('uncaughtException', onUncaught)
    try {
      stream.pipe(new Writable({ write: (_chunk, _encoding, callback) => callback() }))
      await new Promise((resolve) => setTimeout(resolve, 100))
    } finally {
      process.off('uncaughtException', onUncaught)
    }

    expect(uncaught).toEqual([])
  })

  it('should still deliver the error to a consumer that does listen for it', async () => {
    const item = await storage.retrieve('plain')
    await storage.delete(['plain'])
    const stream = await item!.asStream()

    await expect(new Promise((_resolve, reject) => stream.on('error', reject).resume())).rejects.toMatchObject({
      code: 'ENOENT'
    })
  })
})

describe('when a directory sits at the path a never-stored id resolves to', () => {
  // Nested ids are legal, so `storeStream('a/b')` creates the directory `a` and `a.gzip/b` creates `a`'s
  // compressed path. Statting one succeeded, so `a` became a phantom: present to `exist`, sized by the
  // directory's own stat, and `delete` rejected FOREVER, poisoning every GC batch containing it.
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'phantom-dir-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  describe('and the directory is at the raw content path', () => {
    beforeEach(async () => {
      await storage.storeStream('a/b', bufferToStream(Buffer.from('nested content')))
    })

    it('should report the id as absent', async () => {
      expect(await storage.exist('a')).toBe(false)
    })

    it('should report no file info for it', async () => {
      expect(await storage.fileInfo('a')).toBeUndefined()
    })

    it('should serve nothing for it', async () => {
      expect(await storage.retrieve('a')).toBeUndefined()
    })

    it('should resolve its delete rather than failing the batch forever', async () => {
      await expect(storage.delete(['a'])).resolves.toBeUndefined()
    })

    it('should agree with enumeration, which never listed it', async () => {
      const ids: string[] = []
      for await (const id of storage.allFileIds()) ids.push(id)

      expect(ids).toEqual(['a/b'])
    })
  })

  describe('and the directory is at the gzip content path', () => {
    beforeEach(async () => {
      await storage.storeStream('a.gzip/b', bufferToStream(Buffer.from('nested content')))
    })

    it('should report the id as absent rather than rejecting with EISDIR', async () => {
      expect(await storage.exist('a')).toBe(false)
    })

    it('should report no file info rather than rejecting with EISDIR', async () => {
      expect(await storage.fileInfo('a')).toBeUndefined()
    })

    it('should resolve its delete rather than failing the batch forever', async () => {
      await expect(storage.delete(['a'])).resolves.toBeUndefined()
    })
  })
})

describe('when an id has a path segment longer than a directory entry can hold', () => {
  // The in-memory backend accepted it while the folder-based one could not store it at all, and said so
  // with a bare ENAMETOOLONG rather than the typed error every other id rejection uses.
  let root: string
  let overLongId: string
  let folderStorage: IContentStorageComponent
  let memoryStorage: IContentStorageComponent
  let s3Storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'long-id-'))
    overLongId = 'z'.repeat(300)
    folderStorage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    memoryStorage = createInMemoryStorage()
    s3Storage = await createS3BasedFileSystemContentStorage(
      { logs: await createLogComponent({}) },
      createFakeS3Client(),
      { Bucket: 'a-bucket', fileTypeLoader: undetectingLoader }
    )
  })

  afterEach(async () => {
    await folderStorage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should reject the store with the typed error on the folder-based backend', async () => {
    await expect(folderStorage.storeStream(overLongId, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })

  it('should reject the store with the typed error on the in-memory backend', async () => {
    await expect(memoryStorage.storeStream(overLongId, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })

  it('should reject the store with the typed error on the S3 backend', async () => {
    await expect(s3Storage.storeStream(overLongId, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })

  it('should still report it absent from the read path, which no file of that name can occupy', async () => {
    expect(await folderStorage.exist(overLongId)).toBe(false)
  })

  it('should still resolve its delete rather than failing the batch', async () => {
    await expect(folderStorage.delete([overLongId])).resolves.toBeUndefined()
  })

  it('should accept an id that fits, including a multi-segment one', async () => {
    await expect(
      folderStorage.storeStream(`${'a'.repeat(200)}/${'b'.repeat(200)}`, bufferToStream(Buffer.from('x')))
    ).resolves.toBeUndefined()
  })
})

describe('when an id ends in the reserved gzip suffix followed by characters a filesystem strips', () => {
  // Win32 semantics (NTFS, any SMB/CIFS mount) discard trailing dots and spaces, so these resolve onto
  // `foo.gzip` — the compressed representation of `foo` — reproducing the damage the case-insensitive
  // check was added to prevent, through the spellings it did not cover.
  let storage: IContentStorageComponent

  beforeEach(() => {
    storage = createInMemoryStorage()
  })

  it.each(['foo.gzip', 'foo.GZIP', 'foo.gzip ', 'foo.gzip.', 'foo.gzip\t', 'foo.gzip  ..'])(
    'should reject %j',
    async (id: string) => {
      await expect(storage.storeStream(id, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    }
  )

  it('should still accept an id that merely contains the suffix without ending in it', async () => {
    await expect(storage.storeStream('foo.gzip.txt', bufferToStream(Buffer.from('x')))).resolves.toBeUndefined()
  })
})

describe('when a source emits string chunks rather than buffers', () => {
  // A stream in encoding mode is piped straight into an fs.WriteStream by the folder-based backend and
  // Buffer.from-ed by S3's head peek, so both STORE it — only the in-memory backend refused, which meant
  // a service exercised against it in tests behaved differently in production.
  let root: string
  let folderStorage: IContentStorageComponent
  let memoryStorage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'string-chunks-'))
    folderStorage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    memoryStorage = createInMemoryStorage()
  })

  afterEach(async () => {
    await folderStorage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should store and read back the same content on the in-memory backend', async () => {
    await memoryStorage.storeStream('from-string', Readable.from('hello world'))
    const item = await memoryStorage.retrieve('from-string')

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('hello world')
  })

  it('should agree with the folder-based backend, which already accepted it', async () => {
    await folderStorage.storeStream('from-string', Readable.from('hello world'))
    const item = await folderStorage.retrieve('from-string')

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('hello world')
  })

  it('should still refuse a source emitting neither buffers nor strings', async () => {
    await expect(
      memoryStorage.storeStream('objects', Readable.from([{ not: 'bytes' }], { objectMode: true }))
    ).rejects.toThrow('Stream did not emit Uint8Array')
  })
})

describe('when streamToBuffer is given a size cap', () => {
  let source: Readable

  beforeEach(() => {
    source = Readable.from([Buffer.alloc(10), Buffer.alloc(10), Buffer.alloc(10)])
  })

  it('should reject once more than the cap has arrived', async () => {
    await expect(streamToBuffer(source, 15)).rejects.toThrow('Stream exceeded the maximum allowed size of 15 bytes')
  })

  it('should resolve when the content fits', async () => {
    expect((await streamToBuffer(source, 30)).length).toBe(30)
  })
})

describe('when the reserved directory holds an intent journal whose id is not a string', () => {
  // The guard was `!id`, which passes any truthy non-string, and the very next check hashes it:
  // `createHash('sha256').update(12345)` threw a TypeError that reconcile() turned into a permanent
  // refusal to start, over a body the malformed branch right below existed to discard.
  let root: string
  let intentPath: string

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'intent-type-'))
    // Boot once so the reserved directory is created and claimed legitimately.
    const first = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await first.storeStream('an-id', bufferToStream(Buffer.from('content')))
    await first.stop?.()

    intentPath = path.join(root, '.tmp-writes', `${createHash('sha256').update('12345').digest('hex')}.intent`)
    await nodeFsPromises.writeFile(
      intentPath,
      JSON.stringify({ op: 'raw', id: 12345, staged: `deadbeefdeadbeef-${'0'.repeat(32)}` })
    )
  })

  afterEach(() => {
    rmSync(root, { recursive: true, force: true })
  })

  it('should construct rather than refusing to start', async () => {
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await storage.stop?.()

    expect(await nodeFsPromises.readdir(path.join(root, '.tmp-writes'))).not.toContain(path.basename(intentPath))
  })

  it('should leave the previously stored content readable', async () => {
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    const item = await storage.retrieve('an-id')
    await storage.stop?.()

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('content')
  })
})

describe('when many concurrent cold range reads inflate gzip-only content', () => {
  // Admission was checked only AFTER an inflated file was committed and the eviction it triggers cannot be
  // awaited, so the overshoot scaled with the CALLER's concurrency: 50 concurrent reads measured 36x over
  // budget. Separately, LRU protected only the single most-recent entry, so a reader's file could be
  // unlinked before it opened it — a spurious ENOENT on content that was never missing.
  const CONCURRENT_READS = 20
  let root: string
  let storage: IContentStorageComponent
  let ids: string[]
  let outcomes: PromiseSettledResult<number>[]

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'cache-bound-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      {
        disablePrefixHash: true,
        // One inflated file (30_000 bytes of highly compressible content) does not fit the budget, so this
        // is also the case where the previously permanent stall latch applied.
        decompressCacheMaxSize: 40_000,
        decompressCacheEvictionInterval: 60_000
      }
    )
    ids = Array.from({ length: CONCURRENT_READS }, (_, index) => `id-${index}`)
    for (const id of ids) {
      await storage.storeStreamAndCompress(id, bufferToStream(Buffer.alloc(30_000, 0x41)))
    }

    outcomes = await Promise.allSettled(
      ids.map(async (id) => {
        const item = await storage.retrieve(id, { start: 0, end: 9 })
        if (!item) throw new Error(`retrieve returned undefined for ${id}`)
        return (await streamToBuffer(await item.asStream())).length
      })
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should serve every read rather than losing one to its own eviction pass', () => {
    expect(outcomes.filter((outcome) => outcome.status === 'rejected')).toEqual([])
  })

  it('should serve the requested range for every read', () => {
    expect(outcomes.map((outcome) => (outcome as PromiseFulfilledResult<number>).value)).toEqual(
      Array.from({ length: CONCURRENT_READS }, () => 10)
    )
  })

  it('should keep the derived files on disk close to the budget rather than one per read', async () => {
    // Without the admission bound this settled at CONCURRENT_READS x 30_000; the pins are released as each
    // consumer opens its stream, so eviction can then reclaim down towards the budget.
    const shard = await nodeFsPromises.readdir(root, { withFileTypes: true })
    const derived = shard.filter((entry) => entry.isFile() && !entry.name.endsWith('.gzip'))

    expect(derived.length).toBeLessThan(CONCURRENT_READS)
  })
})

describe('when a previous run left decompressed cache files untracked', () => {
  // The tracker is in memory only. A clean stop() evicts what it knows about, but an unclean exit left one
  // decompressed copy per range-read id that the next boot knew nothing about: invisible to eviction and to
  // evictAll(), and no longer counted against the budget.
  let root: string
  let orphanPath: string

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'cache-adopt-'))
    const first = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await first.storeStreamAndCompress('compressible', bufferToStream(Buffer.alloc(20_000, 0x41)))
    // A ranged read materializes the decompressed copy beside its gzip.
    await first.retrieve('compressible', { start: 0, end: 9 })
    orphanPath = path.join(root, 'compressible')
    // No stop(): this models the unclean exit that leaves the copy behind untracked.
  })

  afterEach(() => {
    rmSync(root, { recursive: true, force: true })
  })

  it('should have left the derived copy on disk to begin with', async () => {
    await expect(nodeFsPromises.stat(orphanPath)).resolves.toMatchObject({ size: 20_000 })
  })

  it('should re-adopt it so a clean shutdown reclaims it', async () => {
    const second = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await second.start?.({} as any)
    await second.stop?.()

    await expect(nodeFsPromises.stat(orphanPath)).rejects.toMatchObject({ code: 'ENOENT' })
  })

  it('should leave the compressed primary intact', async () => {
    const second = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await second.start?.({} as any)
    await second.stop?.()

    await expect(nodeFsPromises.stat(`${orphanPath}.gzip`)).resolves.toBeDefined()
  })
})

describe('when the filesystem adapter is a class instance whose methods need their receiver', () => {
  // `const { rename } = components.fs` dropped the receiver at all three commit sites, so such an adapter
  // passed construction and every read and then failed with a TypeError at the commit of EVERY write.
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'class-adapter-'))
    const base = createFsComponent()

    class ClassBasedFs {
      private readonly delegate = base
      createReadStream: IFileSystemComponent['createReadStream'] = (...args: any[]) =>
        (this.delegate.createReadStream as any)(...args)
      createWriteStream: IFileSystemComponent['createWriteStream'] = (...args: any[]) =>
        (this.delegate.createWriteStream as any)(...args)
      opendir: IFileSystemComponent['opendir'] = (...args: any[]) => (this.delegate.opendir as any)(...args)
      stat: IFileSystemComponent['stat'] = (...args: any[]) => (this.delegate.stat as any)(...args)
      unlink: IFileSystemComponent['unlink'] = (...args: any[]) => (this.delegate.unlink as any)(...args)
      rename: IFileSystemComponent['rename'] = (...args: any[]) => (this.delegate.rename as any)(...args)
      mkdir: IFileSystemComponent['mkdir'] = (...args: any[]) => (this.delegate.mkdir as any)(...args)
      readdir: IFileSystemComponent['readdir'] = (...args: any[]) => (this.delegate.readdir as any)(...args)
      readFile: IFileSystemComponent['readFile'] = (...args: any[]) => (this.delegate.readFile as any)(...args)
      existPath: IFileSystemComponent['existPath'] = (target: string) => this.delegate.existPath(target)
    }

    storage = await createFolderBasedFileSystemContentStorage(
      { fs: new ClassBasedFs() as unknown as IFileSystemComponent, logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should commit a store rather than failing at the rename', async () => {
    await expect(storage.storeStream('an-id', bufferToStream(Buffer.from('content')))).resolves.toBeUndefined()
  })

  it('should read the stored content back', async () => {
    await storage.storeStream('an-id', bufferToStream(Buffer.from('content')))
    const item = await storage.retrieve('an-id')

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('content')
  })
})

describe('when a range is retrieved and the caller then mutates the range object it passed', () => {
  // `size` and the clamped end were computed eagerly while the stream creator read `range.start` LAZILY, so
  // a mutation in between decided which bytes were served under an already-advertised length.
  let root: string
  let storage: IContentStorageComponent
  let range: { start: number; end: number }

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'range-mutation-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await storage.storeStream('an-id', bufferToStream(Buffer.from('0123456789')))
    range = { start: 0, end: 4 }
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should serve the bytes the item was built for', async () => {
    const item = await storage.retrieve('an-id', range)
    range.start = 2

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('01234')
  })

  it('should serve exactly as many bytes as it advertised', async () => {
    const item = await storage.retrieve('an-id', range)
    range.start = 5

    expect((await streamToBuffer(await item!.asStream())).length).toBe(item!.size)
  })
})

describe('when an S3 endpoint returns a continuation token it has already issued', () => {
  // The stop condition was relaxed to `IsTruncated !== false` to support gateways that omit the flag — but
  // that trades a guaranteed stop for one that depends on a fresh token each page, and a gateway echoing
  // the request's token back made enumeration re-yield the same page forever.
  let storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>
  let requests: number

  beforeEach(async () => {
    s3 = createFakeS3Client()
    requests = 0
    s3.on('ListObjectsV2Command', () => {
      requests++
      return { Contents: [{ Key: 'a-key' }], IsTruncated: true, NextContinuationToken: 'always-the-same' }
    })
    storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
    // Construction issues its own ListObjectsV2 reachability probe; zeroed here so the count below
    // measures the enumeration alone.
    requests = 0
  })

  it('should stop instead of enumerating forever', async () => {
    const ids: string[] = []
    for await (const id of storage.allFileIds()) ids.push(id)

    expect(ids).toEqual(['a-key', 'a-key'])
  })

  it('should stop after re-requesting the repeated token only once', async () => {
    for await (const _id of storage.allFileIds()) {
      // drained for its side effect on the request count
    }

    expect(requests).toBe(2)
  })
})

describe('when an S3 listing contains an entry with no key', () => {
  let storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>

  beforeEach(async () => {
    s3 = createFakeS3Client()
    s3.on('ListObjectsV2Command', () => ({
      Contents: [{ Key: 'first' }, { Size: 3 }, { Key: 'second' }],
      IsTruncated: false
    }))
    storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
  })

  it('should skip it rather than yielding undefined into a caller sweep', async () => {
    const ids: string[] = []
    for await (const id of storage.allFileIds()) ids.push(id)

    expect(ids).toEqual(['first', 'second'])
  })
})

describe('when an S3 object is replaced between the metadata read and the stream being opened', () => {
  // Metadata comes from HeadObject while the bytes come from a GetObject issued when the consumer opens the
  // stream, so a re-store in between served one version's bytes under another version's advertised length.
  let storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>

  beforeEach(async () => {
    s3 = createFakeS3Client()
    storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
    await storage.storeStream('an-id', bufferToStream(Buffer.alloc(100, 0x41)))
  })

  it('should fail the read loudly rather than serving mismatched bytes and length', async () => {
    const item = await storage.retrieve('an-id', { start: 90, end: 99 })
    // The replacement changes the object's ETag, which the GetObject precondition is pinned to.
    s3.objects.set('an-id', { body: Buffer.alloc(95, 0x42) })
    s3.on('GetObjectCommand', ({ Key, IfMatch }) => {
      const found = s3.objects.get(Key)!
      if (IfMatch !== undefined && IfMatch !== `"${Key}-v2"`) {
        throw Object.assign(new Error('PreconditionFailed'), {
          name: 'PreconditionFailed',
          $metadata: { httpStatusCode: 412 }
        })
      }
      return { Body: Readable.from([found.body]), ContentLength: found.body.length }
    })

    await expect(item!.asStream()).rejects.toMatchObject({ name: 'PreconditionFailed' })
  })

  it('should pass the precondition through when the object has not changed', async () => {
    const item = await storage.retrieve('an-id', { start: 90, end: 99 })

    expect((await streamToBuffer(await item!.asStream())).length).toBe(10)
  })
})

describe('when a gzip placed under a shard has more than one member', () => {
  // `asStream()` decodes every member because zlib does, while the trailer read only ever sees the LAST
  // member's ISIZE — so the advertised logical size can be a small fraction of what the item yields.
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'multi-member-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await storage.storeStream('seed', bufferToStream(Buffer.from('seed')))
    await nodeFsPromises.writeFile(
      path.join(root, 'multi.gzip'),
      Buffer.concat([gzipSync(Buffer.alloc(50_000, 0x41)), gzipSync(Buffer.from('TAIL'))])
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should still decode every member, which is the documented asStream contract', async () => {
    const item = await storage.retrieve('multi')

    expect((await streamToBuffer(await item!.asStream())).length).toBe(50_004)
  })

  it('should report the trailer size, which is documented as a display-only hint', async () => {
    // Pinned deliberately: detecting the discrepancy costs an O(n) inflate on a metadata call, so the
    // caveat is documented on `readGzipOriginalSize` and `FileInfo.contentSize` instead of paid for here.
    expect((await storage.fileInfo('multi'))!.contentSize).toBe(4)
  })
})
