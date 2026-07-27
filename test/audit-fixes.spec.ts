import { createHash } from 'crypto'
import { mkdtempSync, rmSync, createReadStream, promises as nodeFsPromises } from 'fs'
import os from 'os'
import path from 'path'
import { Readable, Writable } from 'stream'
import { brotliCompressSync, gzipSync } from 'zlib'
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
  PathNotContainedError,
  RangeNotSupportedError
} from '../src'
import { createFakeS3Client } from './fake-s3-client'
import { createDecompressCache } from '../src/folder-based/decompress-cache'

/** The real detector is ESM-only and reached through an import Jest's registry does not own. */
const undetectingLoader: FileTypeLoader = async () => ({ fileTypeFromBuffer: async () => undefined })

/**
 * Polls `predicate` until it holds, returning whether it ever did.
 *
 * For asserting on eviction, which is deliberately fire-and-forget — `record` starts a pass it cannot await,
 * because the pass needs the path lock the committing read still holds. A snapshot taken the instant the
 * reads finish is therefore a race, and a fixed sleep is the same race with a different constant: this waits
 * for the state to arrive and gives up loudly if it never does.
 */
async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 5_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return true
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  return predicate()
}

/** Drains `allFileIds` into an array, so a rejection mid-enumeration surfaces as a rejected promise. */
async function collectIds(storage: IContentStorageComponent, prefix?: string): Promise<string[]> {
  const ids: string[] = []
  for await (const id of storage.allFileIds(prefix)) ids.push(id)
  return ids
}

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

  it('should not raise an unhandled error event for a consumer that pipes without an error handler', async () => {
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
      const sink = new Writable({ write: (_chunk, _encoding, callback) => callback() })
      stream.pipe(sink)
      // Awaits the stream's own terminal event instead of a fixed delay. A `setTimeout` here made an
      // assertion of ABSENCE pass vacuously whenever the ENOENT landed after the window, and would also
      // capture any unrelated async error raised in it.
      await new Promise((resolve) => stream.once('close', resolve))
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
      expect(await collectIds(storage)).toEqual(['a/b'])
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
    await s3Storage.stop?.()
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

  it('should still accept it on the S3 backend, whose keys are documented as opaque', async () => {
    // Deliberately NOT enforced for S3: keys are opaque to S3, a bucket may already hold keys these rules
    // reject, and refusing them would make that content unwritable while reads still serve it. README,
    // "Id validation". An earlier revision of this change enforced it here for cross-backend parity, which
    // is the wrong trade.
    await expect(s3Storage.storeStream(overLongId, bufferToStream(Buffer.from('x')))).resolves.toBeUndefined()
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

  it.each(['foo.gzip', 'foo.GZIP', 'foo.gzip ', 'foo.gzip.', 'foo.gzip  ..'])(
    'should reject a store of %j',
    async (id: string) => {
      await expect(storage.storeStream(id, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
        PathNotContainedError
      )
    }
  )

  it('should still accept an id that merely contains the suffix without ending in it', async () => {
    await expect(storage.storeStream('foo.gzip.txt', bufferToStream(Buffer.from('x')))).resolves.toBeUndefined()
  })

  it.each(['foo.gzip\t', 'foo.gzip\n', 'foo.gzip '])(
    'should accept %j, because no filesystem folds that character away',
    async (id: string) => {
      await expect(storage.storeStream(id, bufferToStream(Buffer.from('x')))).resolves.toBeUndefined()
    }
  )
})

describe('when a stored id would only be rejected by the store-side rules', () => {
  // The store-side rules must NOT reach the read path. Content a previous version legitimately stored
  // under a name the folded reserved-suffix rule now refuses to CREATE has to stay readable, deletable and
  // consistent with enumeration — otherwise a GC sweep enumerates the id and then fails its own delete
  // batch on it, forever.
  let root: string
  let storage: IContentStorageComponent
  let legacyPath: string

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'legacy-name-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    // Written directly: this models a file a previous version of the library accepted.
    legacyPath = path.join(root, 'legacy.gzip.')
    await nodeFsPromises.writeFile(legacyPath, 'LEGACY CONTENT')
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should still report it as present', async () => {
    expect(await storage.exist('legacy.gzip.')).toBe(true)
  })

  it('should still serve its content', async () => {
    const item = await storage.retrieve('legacy.gzip.')

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('LEGACY CONTENT')
  })

  it('should still enumerate it', async () => {
    expect(await collectIds(storage)).toEqual(['legacy.gzip.'])
  })

  it('should still delete it rather than stranding it on disk', async () => {
    await storage.delete(['legacy.gzip.'])

    await expect(nodeFsPromises.stat(legacyPath)).rejects.toMatchObject({ code: 'ENOENT' })
  })

  it('should not fail a batch that includes it', async () => {
    await expect(storage.existMultiple(['legacy.gzip.', 'other'])).resolves.toEqual(
      new Map([
        ['legacy.gzip.', true],
        ['other', false]
      ])
    )
  })
})

describe('when an id contains a backslash on a platform where that is an ordinary filename character', () => {
  // Splitting the id on backslash under-counted segment lengths on POSIX: two 200-byte halves looked
  // storable while the real filename was 401 bytes, so the store failed the commit rename with a bare
  // ENAMETOOLONG — verbatim the outcome the segment rule exists to remove.
  let storage: IContentStorageComponent
  let backslashId: string

  beforeEach(() => {
    storage = createInMemoryStorage()
    backslashId = `${'x'.repeat(200)}\\${'y'.repeat(200)}`
  })

  it('should reject it as one over-long segment on POSIX', async () => {
    if (path.sep === '\\') return

    await expect(storage.storeStream(backslashId, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })

  it('should still accept a short id containing a backslash', async () => {
    await expect(storage.storeStream('a\\b', bufferToStream(Buffer.from('x')))).resolves.toBeUndefined()
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

describe('when a source is read in an encoding mode that does not round-trip as utf8', () => {
  // Every backend turns string chunks back into bytes as utf8, so a latin1/hex/base64 source stored bytes
  // that were not the bytes read — silent corruption under an id that is then never re-fetched.
  let root: string
  let payload: string
  let folderStorage: IContentStorageComponent
  let memoryStorage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'encoding-mode-'))
    payload = path.join(root, 'payload.bin')
    // Bytes that are not valid utf8, so the round trip is provably lossy.
    await nodeFsPromises.writeFile(payload, Buffer.from([0x41, 0xe9, 0xff, 0x42]))
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

  it('should refuse it on the folder-based backend rather than storing re-encoded bytes', async () => {
    await expect(folderStorage.storeStream('latin1', createReadStream(payload, 'latin1'))).rejects.toThrow(
      'encoding mode'
    )
  })

  it('should refuse it on the in-memory backend too', async () => {
    await expect(memoryStorage.storeStream('latin1', createReadStream(payload, 'latin1'))).rejects.toThrow(
      'encoding mode'
    )
  })

  it('should still accept a utf8 source, which round-trips exactly', async () => {
    await folderStorage.storeStream('utf8', Readable.from('héllo', { encoding: 'utf8' }))
    const item = await folderStorage.retrieve('utf8')

    expect((await streamToBuffer(await item!.asStream())).toString('utf8')).toBe('héllo')
  })

  it('should still store the same bytes when read in binary mode', async () => {
    await folderStorage.storeStream('binary', createReadStream(payload))
    const item = await folderStorage.retrieve('binary')

    expect([...(await streamToBuffer(await item!.asStream()))]).toEqual([0x41, 0xe9, 0xff, 0x42])
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

  it('should resolve for content that fits', async () => {
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
  const BUDGET = 40_000
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
        decompressCacheMaxSize: BUDGET,
        // Long enough that nothing here can be attributed to the periodic tick: every eviction this
        // exercises is admission- or pin-release-driven.
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

  it('should converge on the budget rather than retaining one derived file per read', async () => {
    // Asserted against the BUDGET, not merely "fewer than one per read": at 30_000 bytes a file and a 40_000
    // byte budget, `< CONCURRENT_READS` still passed at 19 files — 570 KB against 40 KB, i.e. the very
    // overshoot this describe exists to pin. The bound that matters is `maxSize` plus the inflations that can
    // be in flight, so allow a generous multiple of it and no more. Awaited rather than sampled, because
    // eviction is fire-and-forget; before the fix this never converged, so the wait expires and fails.
    const withinBudget = async (): Promise<boolean> => {
      const shard = await nodeFsPromises.readdir(root, { withFileTypes: true })
      const derived = shard.filter((entry) => entry.isFile() && !entry.name.endsWith('.gzip'))
      return derived.length * 30_000 <= BUDGET * 4
    }

    expect(await waitFor(withinBudget)).toBe(true)
  })
})

describe('when a previous run left decompressed cache files untracked', () => {
  // CHARACTERIZATION of a documented limitation, not of a fix. A startup sweep that adopted "any raw file
  // with a .gzip sibling" was written and then removed: that inference cannot be acted on destructively,
  // because the same shape is produced by a quarantined mixed state (raw = new primary), by a store
  // committing during the walk, and by foreign files under the root — and adopting any of those made
  // eviction delete live content. See the note above `allFileIdsRec`. What is pinned here is that a
  // subsequent boot leaves both files ALONE, which is the safe behaviour; the cost is disk, not
  // correctness.
  let root: string
  let orphanPath: string
  let second: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'cache-orphan-'))
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
    second = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    await second.start?.({} as any)
    await second.stop?.()
  })

  afterEach(() => {
    rmSync(root, { recursive: true, force: true })
  })

  it('should leave the untracked derived copy on disk rather than guessing it is reclaimable', async () => {
    await expect(nodeFsPromises.stat(orphanPath)).resolves.toMatchObject({ size: 20_000 })
  })

  it('should leave the compressed primary intact', async () => {
    await expect(nodeFsPromises.stat(`${orphanPath}.gzip`)).resolves.toBeDefined()
  })

  it('should still serve the id correctly across the restart', async () => {
    const third = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    const item = await third.retrieve('compressible')
    const bytes = (await streamToBuffer(await item!.asStream())).length
    await third.stop?.()

    expect(bytes).toBe(20_000)
  })
})

describe('when a store is rejected because its source is unusable', () => {
  // Validating the id before the source put the side-effecting `getFilePath` (which mkdir -p's) ahead of
  // the source check, so every store rejected for a bad source permanently created its directory tree —
  // an unbounded empty-tree leak per rejected upload for caller-supplied nested ids.
  let root: string
  let storage: IContentStorageComponent
  let source: Readable

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'no-dirs-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    source = Readable.from([Buffer.from('AAAA'), Buffer.from('BBBB')])
    // Partially consumed, so the source check rejects it.
    await source[Symbol.asyncIterator]().next()
    await storage.storeStream('attacker/controlled/deep/nested/id', source).catch(() => undefined)
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should not have created the id directory tree', async () => {
    const entries = await nodeFsPromises.readdir(root)

    expect(entries.filter((entry) => entry !== '.tmp-writes')).toEqual([])
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

    // PROTOTYPE methods, not arrow-function class properties. Arrow properties are bound to the instance at
    // construction, so `const { rename } = components.fs` never loses its receiver and the test passes even
    // against the unfixed code — it proved nothing. Prototype methods are the shape that actually breaks:
    // detached, `this` is undefined and reading `this.delegate` throws.
    class ClassBasedFs {
      private readonly delegate = base
      createReadStream(...args: any[]): any {
        return (this.delegate.createReadStream as any)(...args)
      }
      createWriteStream(...args: any[]): any {
        return (this.delegate.createWriteStream as any)(...args)
      }
      opendir(...args: any[]): any {
        return (this.delegate.opendir as any)(...args)
      }
      stat(...args: any[]): any {
        return (this.delegate.stat as any)(...args)
      }
      unlink(...args: any[]): any {
        return (this.delegate.unlink as any)(...args)
      }
      rename(...args: any[]): any {
        return (this.delegate.rename as any)(...args)
      }
      mkdir(...args: any[]): any {
        return (this.delegate.mkdir as any)(...args)
      }
      readdir(...args: any[]): any {
        return (this.delegate.readdir as any)(...args)
      }
      readFile(...args: any[]): any {
        return (this.delegate.readFile as any)(...args)
      }
      existPath(target: string): Promise<boolean> {
        return this.delegate.existPath(target)
      }
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

  afterEach(() => {
    s3.destroy()
  })

  it('should reject rather than enumerating forever', async () => {
    // Rejecting, not ending early: a silently short listing is the failure the relaxed stop condition
    // exists to avoid, and a GC sweep acting on a partial view could delete content it never saw.
    await expect(collectIds(storage)).rejects.toThrow('the endpoint returned continuation token')
  })

  it('should stop after re-requesting the repeated token only once', async () => {
    await collectIds(storage).catch(() => undefined)

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

  afterEach(() => {
    s3.destroy()
  })

  it('should skip it rather than yielding undefined into a caller sweep', async () => {
    expect(await collectIds(storage)).toEqual(['first', 'second'])
  })
})

describe('when an S3 object is replaced between the metadata read and the stream being opened', () => {
  // CHARACTERIZATION of a documented window, not of a fix. `size`/`encoding`/`contentSize` come from the
  // HeadObject `retrieve()` issues; the bytes come from a GetObject issued when the consumer opens the lazy
  // stream. An `IfMatch` precondition closed that window and was removed: it fires on any ETag CHANGE rather
  // than any content change, so on a bucket where the ETag is not a digest of the body (SSE-KMS, SSE-C), or
  // when two writers pick different multipart part boundaries, re-storing IDENTICAL bytes made in-flight
  // reads fail. Content is addressed by its own hash, so an id is not overwritten with different content.
  //
  // What is pinned here is that a read is NOT rejected for a version change, and that the unchanged case —
  // the only one this storage's model produces — is served exactly.
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

  afterEach(() => {
    s3.destroy()
  })

  it('should serve the range exactly when the object has not changed', async () => {
    const item = await storage.retrieve('an-id', { start: 90, end: 99 })

    expect((await streamToBuffer(await item!.asStream())).length).toBe(10)
  })

  it('should not reject a read because the ETag rotated under a byte-identical re-store', async () => {
    // The SSE-KMS / differing-part-boundary case: same bytes, new ETag. Sending a precondition made this a
    // 412 for content that never differed, which is why it was removed.
    const item = await storage.retrieve('an-id')
    s3.objects.set('an-id', { body: Buffer.alloc(100, 0x41), etag: '"an-id-rotated"' })

    expect((await streamToBuffer(await item!.asStream())).length).toBe(100)
  })

  it('should still serve a whole-object read after an ordinary re-store', async () => {
    const item = await storage.retrieve('an-id')
    await storage.storeStream('an-id', bufferToStream(Buffer.alloc(100, 0x41)))

    expect((await streamToBuffer(await item!.asStream())).length).toBe(100)
  })

  it('should serve the new bytes under the previous advertised length when content does change', async () => {
    // The documented window, asserted so it is a decision on record rather than a surprise: a caller that
    // both overwrites an id with different content AND forwards `size` as a Content-Length must re-check
    // after streaming. Closing this needs a precondition whose cost falls on correct usage instead.
    const item = await storage.retrieve('an-id', { start: 90, end: 99 })
    s3.objects.set('an-id', { body: Buffer.alloc(95, 0x42), etag: '"an-id-v2"' })
    const served = await streamToBuffer(await item!.asStream())

    expect({ advertised: item!.size, served: served.length }).toEqual({ advertised: 10, served: 5 })
  })
})

describe('when the S3 backend is configured with a partSize', () => {
  // Unvalidated, every way of getting this wrong failed late: below 5 MiB every store rejected with a bare
  // SDK `EntityTooSmall`, `0`/`NaN` were swallowed by lib-storage so the option silently did nothing, and a
  // non-integer passed lib-storage's own guard and then killed any multi-part upload after the bytes had
  // crossed the wire.
  let s3: ReturnType<typeof createFakeS3Client>
  let logs: Awaited<ReturnType<typeof createLogComponent>>

  beforeEach(async () => {
    s3 = createFakeS3Client()
    logs = await createLogComponent({})
  })

  afterEach(() => {
    s3.destroy()
  })

  it.each([
    ['below S3 minimum', 1024],
    ['zero', 0],
    ['negative', -1],
    ['non-integer', 5 * 1024 * 1024 + 0.5],
    ['NaN', Number.NaN],
    ['above S3 maximum', 6 * 1024 * 1024 * 1024]
  ])('should refuse to construct with a %s partSize', async (_label: string, partSize: number) => {
    await expect(
      createS3BasedFileSystemContentStorage({ logs }, s3, {
        Bucket: 'a-bucket',
        fileTypeLoader: undetectingLoader,
        partSize
      })
    ).rejects.toThrow('partSize')
  })

  it('should construct with a valid partSize', async () => {
    await expect(
      createS3BasedFileSystemContentStorage({ logs }, s3, {
        Bucket: 'a-bucket',
        fileTypeLoader: undetectingLoader,
        partSize: 64 * 1024 * 1024
      })
    ).resolves.toBeDefined()
  })

  it('should still store and read content back with a valid partSize', async () => {
    const storage = await createS3BasedFileSystemContentStorage({ logs }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader,
      partSize: 8 * 1024 * 1024
    })
    await storage.storeStream('an-id', bufferToStream(Buffer.from('content')))
    const item = await storage.retrieve('an-id')

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('content')
  })
})

// NOT covered here on purpose: the `instanceof AbortMultipartUploadCommand` exemption and the
// `isCreateMultipartUpload` input-shape fallback. Both exist for a build whose bundler has MANGLED class
// names, and lib-storage constructs those commands internally, so reproducing that would mean mutating a
// shared SDK class's `name` from a test — which leaks into every other suite in the process. The existing
// multipart cancellation coverage (`s3-based-storage-component.spec.ts`, "When the abort lands
// mid-multipart" and "S3 Storage multipart cleanup") proves the path with unmangled names, and both
// additions are strictly additive: they can only make the match succeed where it previously failed.

describe('when an S3 object carries a transfer coding in its Content-Encoding', () => {
  // `aws-chunked` describes the TRANSFER of the bytes, not the content, and is already undone by the time a
  // body reaches us — which is why `contentCodingOf` ignores it when deciding whether `asStream()` decodes,
  // and why such an object is rangeable. But the exposed metadata kept reporting it, so a caller received a
  // `Content-Encoding` to forward for bytes that are not encoded at all. Both surfaces now run the raw header
  // through the same predicate that decides decoding.
  let storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>

  beforeEach(async () => {
    s3 = createFakeS3Client()
    storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
  })

  afterEach(() => {
    s3.destroy()
  })

  describe('and the coding is bare aws-chunked, so the bytes are plain', () => {
    beforeEach(() => {
      s3.objects.set('chunked', { body: Buffer.from('the real content'), contentEncoding: 'aws-chunked' })
    })

    it('should report a null encoding from fileInfo', async () => {
      expect((await storage.fileInfo('chunked'))!.encoding).toBeNull()
    })

    it('should report a known contentSize from fileInfo', async () => {
      expect((await storage.fileInfo('chunked'))!.contentSize).toBe(16)
    })

    it('should report a null encoding from retrieve', async () => {
      expect((await storage.retrieve('chunked'))!.encoding).toBeNull()
    })

    it('should report a known contentSize from retrieve', async () => {
      expect((await storage.retrieve('chunked'))!.contentSize).toBe(16)
    })

    it('should serve the plain bytes unchanged', async () => {
      const item = await storage.retrieve('chunked')

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('the real content')
    })

    it('should still be rangeable, since nothing is actually encoded', async () => {
      const item = await storage.retrieve('chunked', { start: 0, end: 3 })

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('the ')
    })
  })

  describe('and the coding is gzip alongside aws-chunked', () => {
    beforeEach(() => {
      s3.objects.set('both', { body: gzipSync(Buffer.from('the real content')), contentEncoding: 'gzip, aws-chunked' })
    })

    it('should report only the coding still applied, so a forwarded header is correct', async () => {
      expect((await storage.fileInfo('both'))!.encoding).toBe('gzip')
    })

    it('should agree between fileInfo and retrieve', async () => {
      expect((await storage.retrieve('both'))!.encoding).toBe((await storage.fileInfo('both'))!.encoding)
    })

    it('should report an unknown contentSize, because S3 keeps no uncompressed size', async () => {
      expect((await storage.fileInfo('both'))!.contentSize).toBeNull()
    })

    it('should still decode through asStream', async () => {
      const item = await storage.retrieve('both')

      expect((await streamToBuffer(await item!.asStream())).toString()).toBe('the real content')
    })

    it('should refuse a range, because the range would address the compressed bytes', async () => {
      await expect(storage.retrieve('both', { start: 0, end: 3 })).rejects.toBeInstanceOf(RangeNotSupportedError)
    })
  })

  describe('and the coding is identity', () => {
    beforeEach(() => {
      s3.objects.set('plain', { body: Buffer.from('the real content'), contentEncoding: 'identity' })
    })

    it('should report a null encoding from fileInfo', async () => {
      expect((await storage.fileInfo('plain'))!.encoding).toBeNull()
    })

    it('should report a null encoding from retrieve', async () => {
      expect((await storage.retrieve('plain'))!.encoding).toBeNull()
    })
  })
})

describe('when an S3 object carries more than one real content coding', () => {
  // `asStream()` applies at most one decoder, so `gzip, br` had Brotli undone and was handed back STILL
  // GZIPPED — under a contract that says the stream yields decompressed content, with nothing to say
  // otherwise. Refusing is the same answer already given for a coding this storage cannot decode at all.
  let storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>
  let doublyEncoded: Buffer

  beforeEach(async () => {
    s3 = createFakeS3Client()
    storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
    // Genuinely gzip-then-brotli encoded, so a partially applied decode would be observable.
    doublyEncoded = brotliCompressSync(gzipSync(Buffer.from('the real content')))
    s3.objects.set('doubly', { body: doublyEncoded, contentEncoding: 'gzip, br' })
  })

  afterEach(() => {
    s3.destroy()
  })

  it('should report both codings, since both are still applied', async () => {
    expect((await storage.fileInfo('doubly'))!.encoding).toBe('gzip, br')
  })

  it('should report an unknown contentSize', async () => {
    expect((await storage.fileInfo('doubly'))!.contentSize).toBeNull()
  })

  it('should refuse to decode rather than returning half-decoded bytes', async () => {
    const item = await storage.retrieve('doubly')

    await expect(item!.asStream()).rejects.toThrow('multiple content codings')
  })

  it('should name asRawStream in the refusal, so the bytes stay reachable', async () => {
    const item = await storage.retrieve('doubly')

    await expect(item!.asStream()).rejects.toThrow('asRawStream()')
  })

  it('should still hand over the stored bytes through asRawStream', async () => {
    const item = await storage.retrieve('doubly')

    expect([...(await streamToBuffer(await item!.asRawStream()))]).toEqual([...doublyEncoded])
  })

  it('should refuse a range, because something is still applied to the stored bytes', async () => {
    await expect(storage.retrieve('doubly', { start: 0, end: 3 })).rejects.toBeInstanceOf(RangeNotSupportedError)
  })

  it('should still decode a single coding alongside a transfer coding', async () => {
    s3.objects.set('single', { body: gzipSync(Buffer.from('the real content')), contentEncoding: 'gzip, aws-chunked' })
    const item = await storage.retrieve('single')

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('the real content')
  })
})

describe('when an S3 object declares chunked as its content encoding', () => {
  // `chunked` is a Transfer-Encoding value, not a content coding, so this metadata is simply wrong. It used
  // to be folded away as "nothing applied", which served the bytes as though the header agreed; it is now
  // left unrecognised so the refusal names `asRawStream()`.
  let storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>

  beforeEach(async () => {
    s3 = createFakeS3Client()
    storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
    s3.objects.set('chunky', { body: Buffer.from('the real content'), contentEncoding: 'chunked' })
  })

  afterEach(() => {
    s3.destroy()
  })

  it('should report it rather than folding it away', async () => {
    expect((await storage.fileInfo('chunky'))!.encoding).toBe('chunked')
  })

  it('should refuse to decode it', async () => {
    const item = await storage.retrieve('chunky')

    await expect(item!.asStream()).rejects.toThrow('unsupported encoding')
  })

  it('should still hand over the stored bytes through asRawStream', async () => {
    const item = await storage.retrieve('chunky')

    expect((await streamToBuffer(await item!.asRawStream())).toString()).toBe('the real content')
  })
})

describe('when concurrent range reads hit an already-materialized cache file', () => {
  // Only the call that INFLATED a cache file wrapped its item in the pin, so a cache HIT — the common case
  // once a range has been served for an id — handed back a lazy stream with no protection. A burst of such
  // reads touches several entries and then lets an eviction pass unlink one whose reader has not opened it
  // yet, failing a read for content that was present.
  const CONCURRENT_READS = 20
  let root: string
  let storage: IContentStorageComponent
  let ids: string[]
  let outcomes: PromiseSettledResult<number>[]

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'cache-hit-pin-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      {
        disablePrefixHash: true,
        // Smaller than one inflated file, so every admission is over budget and eviction is always hunting.
        decompressCacheMaxSize: 20_000,
        decompressCacheEvictionInterval: 60_000
      }
    )
    ids = Array.from({ length: CONCURRENT_READS }, (_, index) => `id-${index}`)
    for (const id of ids) {
      await storage.storeStreamAndCompress(id, bufferToStream(Buffer.alloc(30_000, 0x41)))
    }
    // MATERIALIZE first, sequentially, so the burst below is entirely cache hits rather than inflations.
    for (const id of ids) {
      const warm = await storage.retrieve(id, { start: 0, end: 9 })
      await streamToBuffer(await warm!.asStream())
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

  it('should serve every read rather than losing one to a concurrent eviction', () => {
    expect(outcomes.filter((outcome) => outcome.status === 'rejected')).toEqual([])
  })

  it('should serve the requested range for every read that hit the cache', () => {
    expect(outcomes.map((outcome) => (outcome as PromiseFulfilledResult<number>).value)).toEqual(
      Array.from({ length: CONCURRENT_READS }, () => 10)
    )
  })
})

describe('when a caller mutates the range object before awaiting retrieve', () => {
  // The snapshot used to be taken after the metadata read, so a caller that did not await immediately could
  // change the bounds while that request was in flight — between `validateRange` accepting them and the
  // clamp using them.
  let root: string
  let folderStorage: IContentStorageComponent
  let s3Storage: IContentStorageComponent
  let s3: ReturnType<typeof createFakeS3Client>
  let range: { start: number; end: number }

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'range-presnapshot-'))
    folderStorage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    s3 = createFakeS3Client()
    s3Storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, s3, {
      Bucket: 'a-bucket',
      fileTypeLoader: undetectingLoader
    })
    await folderStorage.storeStream('an-id', bufferToStream(Buffer.from('0123456789')))
    await s3Storage.storeStream('an-id', bufferToStream(Buffer.from('0123456789')))
    range = { start: 0, end: 4 }
  })

  afterEach(async () => {
    await folderStorage.stop?.()
    await s3Storage.stop?.()
    s3.destroy()
    rmSync(root, { recursive: true, force: true })
  })

  it('should serve the bounds it was called with on the folder-based backend', async () => {
    const pending = folderStorage.retrieve('an-id', range)
    range.start = 9
    const item = await pending

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('01234')
  })

  it('should serve the bounds it was called with on the S3 backend', async () => {
    const pending = s3Storage.retrieve('an-id', range)
    range.start = 9
    const item = await pending

    expect((await streamToBuffer(await item!.asStream())).toString()).toBe('01234')
  })

  it('should advertise a size matching what it serves on the S3 backend', async () => {
    const pending = s3Storage.retrieve('an-id', range)
    range.end = 0
    const item = await pending

    expect((await streamToBuffer(await item!.asStream())).length).toBe(item!.size)
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

describe('when a pin is taken and the cache file at that path is then replaced', () => {
  // Pins used to be keyed by PATH alone, so one taken for a file that `forget()`/`remove()` later dropped
  // went on protecting whatever a subsequent inflation recorded at the same path — silently exempting an
  // entry nobody was reading from the size budget for the rest of the grace window. Binding a pin to the
  // tracked entry's generation makes the successor a different thing, which it is.
  let unlinked: string[]
  let cache: ReturnType<typeof createDecompressCache>

  beforeEach(() => {
    unlinked = []
    cache = createDecompressCache(
      {
        logger: { log() {}, info() {}, debug() {}, warn() {}, error() {} } as any,
        fsInvariants: {
          existsForInvariant: async () => false,
          noFailUnlink: async (target: string) => {
            unlinked.push(target)
            return true
          }
        }
      },
      { ttl: 3_600_000, maxSize: 10 }
    )
  })

  it('should not let a stale pin protect the next generation at that path', async () => {
    cache.record('/a', 100)
    cache.pin('/a', 60_000)
    // A store landing at this path promotes it to primary content and drops the entry.
    cache.forget('/a')
    // A later inflation records a NEW cache file at the same path, which the old pin must not cover.
    cache.record('/a', 100)
    cache.record('/b', 100)

    // Awaited rather than sampled: `record` starts a pass it cannot await, so an explicit `evict()` may
    // return the one already in flight rather than a fresh walk.
    expect(await waitFor(async () => unlinked.includes('/a'), 2_000)).toBe(true)
  })

  it('should still protect the generation the pin was actually taken for', async () => {
    // THREE entries, because the LRU walk always leaves the most recent one alone: with two, the only
    // candidate is the pinned entry and nothing is evicted either way, so the assertion would hold vacuously.
    cache.record('/a', 100)
    cache.pin('/a', 60_000)
    cache.record('/b', 100)
    cache.record('/c', 100)

    // '/b' being reclaimed proves a pass ran and was productive, so '/a' surviving is a real skip.
    expect(await waitFor(async () => unlinked.includes('/b'), 2_000)).toBe(true)
    expect(unlinked).not.toContain('/a')
  })

  it('should be a no-op for a path with no tracked entry', async () => {
    const release = cache.pin('/never-recorded', 60_000)
    cache.record('/never-recorded', 100)
    cache.record('/b', 100)
    release()

    expect(await waitFor(async () => unlinked.includes('/never-recorded'), 2_000)).toBe(true)
  })
})

describe('when a range read observes a cache file another read materialized between its pin and its stat', () => {
  // The pin binds to the entry tracked at the moment it is taken, and it is taken BEFORE the raw-path stat.
  // So a path that is untracked at pin time gets a no-op pin — and a concurrent range read can inflate and
  // record the file in the gap, leaving this call serving a now-tracked cache file with no real protection.
  // A later admission-triggered eviction then unlinks it before the consumer opens the lazy stream.
  let root: string
  let storage: IContentStorageComponent
  let rawPathOf: (id: string) => string
  let statsEntered: number
  let releaseGate: () => void
  let firstStatEntered: Promise<void>
  let gatedId: string
  // The gate is ARMED only once setup is done: the stores below stat the same raw path themselves (a gzip
  // commit checks its raw counterpart), and gating those deadlocks the fixture.
  let armed: boolean

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'pin-after-produce-'))
    gatedId = 'gated'
    rawPathOf = (id: string) => path.join(root, id)
    statsEntered = 0
    armed = false
    let signalEntered: () => void = () => undefined
    firstStatEntered = new Promise<void>((resolve) => (signalEntered = resolve))
    const gate = new Promise<void>((resolve) => (releaseGate = resolve))

    const realFs = createFsComponent()
    const gatedFs: IFileSystemComponent = {
      ...realFs,
      stat: (async (target: any, options?: any) => {
        // Only the FIRST stat of the gated id's raw path waits: that is the probe belonging to the read whose
        // pin was a no-op. Every later stat — including the other read's, which must be free to inflate and
        // record — proceeds normally.
        if (armed && String(target) === rawPathOf(gatedId)) {
          statsEntered++
          if (statsEntered === 1) {
            signalEntered()
            await gate
          }
        }
        return realFs.stat(target, options)
      }) as IFileSystemComponent['stat']
    }

    storage = await createFolderBasedFileSystemContentStorage(
      { fs: gatedFs, logs: await createLogComponent({}) },
      root,
      {
        disablePrefixHash: true,
        // Exactly one inflated file fits, so the second admission below is what triggers eviction.
        decompressCacheMaxSize: 30_000,
        decompressCacheEvictionInterval: 60_000
      }
    )
    await storage.storeStreamAndCompress(gatedId, bufferToStream(Buffer.alloc(30_000, 0x41)))
    await storage.storeStreamAndCompress('other', bufferToStream(Buffer.alloc(30_000, 0x42)))
    armed = true
  })

  afterEach(async () => {
    releaseGate()
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should still serve the read whose pin was taken before the entry existed', async () => {
    // The gated read starts first: it pins (a no-op, nothing is tracked) and then blocks in its stat.
    const gatedRead = storage.retrieve(gatedId, { start: 0, end: 9 })
    await firstStatEntered

    // A second read inflates and records the same path, then finishes so its own pin is released.
    const warming = await storage.retrieve(gatedId, { start: 0, end: 9 })
    await streamToBuffer(await warming!.asStream())

    // Let the gated read's stat through: it now observes a TRACKED cache file it never pinned.
    releaseGate()
    const item = await gatedRead
    expect(item).toBeDefined()

    // Push the cache over budget so eviction runs while the gated read's stream is still unopened. Its
    // candidate is the gated id's entry, which is the oldest.
    const pressure = await storage.retrieve('other', { start: 0, end: 9 })
    await streamToBuffer(await pressure!.asStream())

    expect((await streamToBuffer(await item!.asStream())).length).toBe(10)
  })
})
