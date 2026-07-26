import { createHash } from 'crypto'
import { mkdtempSync, rmSync, promises as nodeFsPromises } from 'fs'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import { createLogComponent } from '@well-known-components/logger'
import {
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
  RangeNotSupportedError,
  UncommittedIntentSurvivedError
} from '../src'
import { createFakeS3Client } from './fake-s3-client'
import { MAX_BUFFERED_DIRECTORY_ENTRIES } from '../src/folder-based-storage-component'

/** The real detector is ESM-only and reached through an import Jest's registry does not own. */
const undetectingLoader: FileTypeLoader = async () => ({ fileTypeFromBuffer: async () => undefined })

describe('when an error reaches a caller through a public method', () => {
  // An error a public method can actually throw at a caller is contract, and a caller can only act on
  // it if the class is importable as a RUNTIME value — `instanceof` against a type-only import is not a
  // thing. The README asks callers to treat `PathNotContainedError` as a bad request and
  // `RangeNotSupportedError` as a 416, so both have to be reachable from the package root; withdrawing
  // them while keeping that documented would have left the advice unfollowable.
  //
  // The converse is pinned too: `DecompressionLimitExceededError` is caught by `retrieve()` and
  // reported as `undefined`, so it is deliberately NOT exported and this suite asserts the behaviour
  // callers actually observe instead.
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'public-errors-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root,
      // Small enough that a ranged read of the compressible id below trips the decompression cap.
      { decompressMaxFileSize: 50 }
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should export the escaping error classes as runtime values from the package root', () => {
    for (const errorClass of [PathNotContainedError, RangeNotSupportedError, UncommittedIntentSurvivedError]) {
      expect(typeof errorClass).toBe('function')
      expect(Object.getPrototypeOf(errorClass.prototype)).toBe(Error.prototype)
    }
  })

  it.each(['exist', 'fileInfo'] as const)(
    'should let an unaddressable id be recognised by instanceof from %s',
    async (surface) => {
      await expect(storage[surface]('../evil')).rejects.toBeInstanceOf(PathNotContainedError)
    }
  )

  it('should let an unaddressable id be recognised by instanceof from delete', async () => {
    await expect(storage.delete(['../evil'])).rejects.toBeInstanceOf(PathNotContainedError)
  })

  it('should let an unaddressable id be recognised by instanceof from storeStream', async () => {
    await expect(storage.storeStream('../evil', bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })

  it('should report an unaddressable id as nothing to serve from retrieve, not as a throw', async () => {
    await expect(storage.retrieve('../evil')).resolves.toBeUndefined()
  })

  it('should report a decompression-cap breach as nothing to serve rather than a typed error', async () => {
    await storage.storeStreamAndCompress('over-cap', bufferToStream(Buffer.from('a'.repeat(20000))))

    // Gzip-primary, so serving a range has to inflate — which trips the 50-byte cap above.
    expect((await storage.fileInfo('over-cap'))?.encoding).toBe('gzip')
    await expect(storage.retrieve('over-cap', { start: 0, end: 10 })).resolves.toBeUndefined()
  })
})

describe('when a store is handed a source that has already been consumed', () => {
  // Piping a consumed stream writes ZERO bytes and RESOLVES, so this committed an empty object under
  // the id and reported success. In a content-addressed store that is permanent: `exist(id)` answers
  // true, so nothing ever re-fetches and the real content never lands. Reachable from any caller that
  // hashes or measures a body before storing it, and from a retry that reuses its source. The
  // in-memory backend already rejected — the three had to agree.
  const payload = Buffer.from('the content that never reached the storage')

  const consumedSource = async (): Promise<Readable> => {
    const source = bufferToStream(payload)
    await streamToBuffer(source)
    return source
  }

  describe('and the backend is folder-based', () => {
    let storage: IContentStorageComponent
    let root: string

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'consumed-folder-'))
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        root
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should reject a plain store rather than commit empty content', async () => {
      await expect(storage.storeStream('an-id', await consumedSource())).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })

    it('should leave the id absent after the rejected plain store', async () => {
      await storage.storeStream('an-id', await consumedSource()).catch(() => undefined)

      expect(await storage.exist('an-id')).toBe(false)
    })

    it('should reject a compressing store rather than commit empty content', async () => {
      await expect(storage.storeStreamAndCompress('another-id', await consumedSource())).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })
  })

  describe('and the backend is in-memory', () => {
    it('should reject rather than commit empty content', async () => {
      const storage = createInMemoryStorage()

      await expect(storage.storeStream('an-id', await consumedSource())).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })
  })

  describe('and the backend is S3', () => {
    let storage: IContentStorageComponent
    let fake: ReturnType<typeof createFakeS3Client>

    beforeEach(async () => {
      fake = createFakeS3Client()
      storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, fake, {
        Bucket: 'test',
        fileTypeLoader: undetectingLoader
      })
    })

    it('should reject rather than upload an empty object', async () => {
      await expect(storage.storeStream('an-id', await consumedSource())).rejects.toMatchObject({
        code: 'ERR_STREAM_PREMATURE_CLOSE'
      })
    })

    it('should put nothing in the bucket', async () => {
      await storage.storeStream('an-id', await consumedSource()).catch(() => undefined)

      expect([...fake.objects.keys()]).toEqual([])
    })
  })
})

describe('when an id ends in the reserved compressed suffix in a different case', () => {
  // On a case-folding filesystem (APFS, NTFS, an SMB/CIFS mount) `<id>.GZIP` IS `<id>.gzip`, so storing
  // it overwrote the compressed representation of ANOTHER id: that id's reads then failed to inflate,
  // its `contentSize` came out of the wrong file's last four bytes, and `allFileIds()` reported it while
  // never listing the id that clobbered it. The exact-case check was the whole gap.
  let storage: IContentStorageComponent
  let root: string

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'case-gzip-'))
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

  it.each(['victim.gzip', 'victim.GZIP', 'victim.GzIp', 'victim.gZIP'])('should reject the id %s', async (id) => {
    await expect(storage.exist(id)).rejects.toBeInstanceOf(PathNotContainedError)
  })

  it('should refuse the store that would clobber another id', async () => {
    await storage.storeStreamAndCompress('victim', bufferToStream(Buffer.from('V'.repeat(5000))))

    await expect(storage.storeStream('victim.GZIP', bufferToStream(Buffer.from('ATTACKER')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })

  it('should leave the clobbered id intact and readable', async () => {
    const original = Buffer.from('V'.repeat(5000))
    await storage.storeStreamAndCompress('victim', bufferToStream(original))
    await storage.storeStream('victim.GZIP', bufferToStream(Buffer.from('ATTACKER'))).catch(() => undefined)

    const item = await storage.retrieve('victim')

    expect(await streamToBuffer(await item!.asStream())).toEqual(original)
  })
})

describe('when an id resolves into the reserved staging directory in a different case', () => {
  // Flat mode makes the root itself the content namespace, so the reserved name is reachable as an id.
  // A case variant slipped past the exact-case check, landed a file inside the staging directory where
  // `allFileIds()` cannot see it, and made the NEXT construction refuse to start over a file this
  // storage "did not create" — a permanent startup failure needing manual cleanup.
  let storage: IContentStorageComponent
  let root: string

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'case-temp-'))
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

  it.each(['.tmp-writes/x', '.TMP-WRITES/x', '.Tmp-Writes/x'])('should reject a store of %s', async (id) => {
    await expect(storage.storeStream(id, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
      PathNotContainedError
    )
  })
})

describe('when an id changes representation while allFileIds is enumerating', () => {
  // The gzip/raw dedup is answered from a set built in a first `opendir` pass and applied in a second,
  // so it was STALE with respect to the pass that used it: a transition landing in the gap made pass 2
  // skip a raw whose gzip no longer existed, and an id holding a complete representation for the whole
  // enumeration was yielded by NEITHER pass. `exist()` answered true for it the entire time.
  const compressible = Buffer.from('a'.repeat(5000))
  // Random-looking but deterministic, so gzip cannot beat the 1.1 rule and the store commits raw.
  const incompressible = createHash('sha512').update('seed').digest()

  /**
   * Enumerates a flat root while a representation transition lands mid-walk.
   *
   * `padding` pushes the directory past MAX_BUFFERED_DIRECTORY_ENTRIES so the enumeration takes the
   * two-read fallback instead of the one-read path — the two decide the raw/gzip question differently,
   * so BOTH have to survive this. The padded names are announced by the listing without existing on
   * disk, so assertions filter to the id under test.
   *
   * The transition fires from inside the read that matters: at the start of the second `opendir` for
   * the fallback (the gap the omission bug lived in), and part-way through the single read for the
   * buffered path (its only window).
   */
  const enumerateWhile = async (
    prime: (storage: IContentStorageComponent) => Promise<void>,
    transition: (storage: IContentStorageComponent) => Promise<void>,
    padding = 0
  ): Promise<string[]> => {
    const root = mkdtempSync(path.join(os.tmpdir(), 'enum-transition-'))
    const base = createFsComponent()
    // The gated `opendir` has to reach the storage that owns it, so the reference is held in a box
    // rather than a variable the closure would capture before it is assigned.
    const built: { storage?: IContentStorageComponent } = {}
    const padNames = Array.from({ length: padding }, (_, index) => `pad-${String(index).padStart(6, '0')}`)
    let opendirCalls = 0
    let fired = false
    const fireOnce = async (): Promise<void> => {
      if (fired) return
      fired = true
      await transition(built.storage!)
    }

    const gated: IFileSystemComponent = {
      ...base,
      opendir: (async (dir: any, opts: any) => {
        if (dir !== root) return base.opendir(dir, opts)
        opendirCalls++
        const call = opendirCalls
        const real = await base.opendir(dir, opts)
        return {
          async *[Symbol.asyncIterator]() {
            // The fallback path reads twice; the window is the gap before the second read.
            if (padding > 0 && call === 2) await fireOnce()
            let index = 0
            for await (const entry of real) {
              // The buffered path reads once, so its only window is inside that read.
              if (padding === 0 && call === 1 && index === 1) await fireOnce()
              index++
              yield entry
            }
            for (const name of padNames) {
              yield { name, isDirectory: () => false } as any
            }
          }
        }
      }) as IFileSystemComponent['opendir']
    }

    const storage = await createFolderBasedFileSystemContentStorage(
      { fs: gated, logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    built.storage = storage
    await prime(storage)

    const listed: string[] = []
    for await (const each of storage.allFileIds()) listed.push(each)
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
    if (!fired) throw new Error('the transition never fired, so this test proved nothing')
    return listed.filter((id) => id === 'the-id')
  }

  describe('and the directory is decided from a single read', () => {
    describe('and the id transitions from compressed to raw', () => {
      let listed: string[]

      beforeEach(async () => {
        listed = await enumerateWhile(
          (storage) => storage.storeStreamAndCompress('the-id', bufferToStream(compressible)),
          (storage) => storage.storeStream('the-id', bufferToStream(incompressible))
        )
      })

      it('should still yield the id exactly once', () => {
        expect(listed).toEqual(['the-id'])
      })
    })

    describe('and the id transitions from raw to compressed', () => {
      let listed: string[]

      beforeEach(async () => {
        listed = await enumerateWhile(
          (storage) => storage.storeStream('the-id', bufferToStream(incompressible)),
          (storage) => storage.storeStreamAndCompress('the-id', bufferToStream(compressible))
        )
      })

      it('should still yield the id exactly once', () => {
        expect(listed).toEqual(['the-id'])
      })
    })
  })

  describe('and the directory is too large to buffer, so it is read twice', () => {
    const padding = MAX_BUFFERED_DIRECTORY_ENTRIES + 1

    describe('and the id transitions from compressed to raw', () => {
      let listed: string[]

      beforeEach(async () => {
        listed = await enumerateWhile(
          (storage) => storage.storeStreamAndCompress('the-id', bufferToStream(compressible)),
          (storage) => storage.storeStream('the-id', bufferToStream(incompressible)),
          padding
        )
      })

      it('should still yield the id exactly once', () => {
        expect(listed).toEqual(['the-id'])
      })
    })

    describe('and the id transitions from raw to compressed', () => {
      let listed: string[]

      beforeEach(async () => {
        listed = await enumerateWhile(
          (storage) => storage.storeStream('the-id', bufferToStream(incompressible)),
          (storage) => storage.storeStreamAndCompress('the-id', bufferToStream(compressible)),
          padding
        )
      })

      it('should still yield the id exactly once', () => {
        expect(listed).toEqual(['the-id'])
      })
    })
  })
})

describe('when allFileIds walks a directory', () => {
  // A directory small enough to hold in memory is decided from ONE read, which halves the `getdents`
  // traffic of a full walk — with hash prefixes a shard holds total/65,536 entries, so this is every
  // directory of a sharded root. A directory too large to hold falls back to reading it twice, which is
  // what keeps a flat-mode root of hundreds of thousands of ids streaming instead of retaining ~300
  // bytes per entry before the first id comes out.
  /** Enumerates a listing of `entryNames`, reporting how many times the directory was opened. */
  const walk = async (entryNames: string[]): Promise<{ listed: string[]; reads: number; statted: string[] }> => {
    const root = mkdtempSync(path.join(os.tmpdir(), 'enum-reads-'))
    const base = createFsComponent()
    let reads = 0
    const statted: string[] = []
    const observed: IFileSystemComponent = {
      ...base,
      stat: (async (target: any, ...rest: any[]) => {
        statted.push(String(target))
        return (base.stat as any)(target, ...rest)
      }) as IFileSystemComponent['stat'],
      opendir: (async (dir: any, opts: any) => {
        if (dir !== root) return base.opendir(dir, opts)
        reads++
        return {
          async *[Symbol.asyncIterator]() {
            for (const name of entryNames) yield { name, isDirectory: () => false } as any
          }
        }
      }) as IFileSystemComponent['opendir']
    }

    const storage = await createFolderBasedFileSystemContentStorage(
      { fs: observed, logs: await createLogComponent({}) },
      root,
      { disablePrefixHash: true }
    )
    // The named entries are written for real, so the listing DESCRIBES the directory. The two-read path
    // confirms a raw/gzip skip against the filesystem, and a listing announcing a `.gzip` that was never
    // created is not the state these tests mean to pin — it is the id-hiding bug that confirmation
    // exists to catch. The `pad-` filler is left synthetic: it is bulk, and no skip is decided from it.
    for (const name of entryNames.filter((each) => !each.startsWith('pad-'))) {
      await nodeFsPromises.writeFile(path.join(root, name), 'x')
    }
    reads = 0
    statted.length = 0
    const listed: string[] = []
    for await (const each of storage.allFileIds()) listed.push(each)
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
    return { listed, reads, statted }
  }

  const padTo = (count: number, ...names: string[]): string[] => [
    ...names,
    ...Array.from({ length: Math.max(0, count - names.length) }, (_, i) => `pad-${String(i).padStart(6, '0')}`)
  ]

  describe('and it is small enough to hold in memory', () => {
    it('should read it only once', async () => {
      expect((await walk(['a', 'b', 'c'])).reads).toBe(1)
    })

    it('should hide a raw that has a compressed sibling', async () => {
      expect((await walk(['thing', 'thing.gzip', 'other'])).listed.sort()).toEqual(['other', 'thing'])
    })

    it('should hide it regardless of the order the two are listed in', async () => {
      expect((await walk(['thing.gzip', 'thing'])).listed).toEqual(['thing'])
    })

    it('should not stat the compressed sibling to confirm the skip', async () => {
      // The skip is justified by an entry from the SAME read, which this loop provably also visits, so
      // there is nothing to confirm — that syscall is only owed by the two-read path.
      const { statted } = await walk(['thing', 'thing.gzip'])

      expect(statted.filter((target) => target.endsWith('thing.gzip'))).toEqual([])
    })
  })

  describe('and it holds more entries than can be buffered', () => {
    it('should fall back to reading it twice', async () => {
      expect((await walk(padTo(MAX_BUFFERED_DIRECTORY_ENTRIES + 1))).reads).toBe(2)
    })

    it('should still hide a raw that has a compressed sibling', async () => {
      const { listed } = await walk(padTo(MAX_BUFFERED_DIRECTORY_ENTRIES + 1, 'thing', 'thing.gzip'))

      expect(listed.filter((id) => id === 'thing')).toEqual(['thing'])
    })

    it('should yield every entry exactly once', async () => {
      const { listed } = await walk(padTo(MAX_BUFFERED_DIRECTORY_ENTRIES + 1))

      expect(new Set(listed).size).toBe(listed.length)
    })
  })

  describe('and it holds exactly as many entries as can be buffered', () => {
    it('should still decide it from a single read', async () => {
      expect((await walk(padTo(MAX_BUFFERED_DIRECTORY_ENTRIES))).reads).toBe(1)
    })

    it('should yield all of them', async () => {
      expect((await walk(padTo(MAX_BUFFERED_DIRECTORY_ENTRIES))).listed).toHaveLength(MAX_BUFFERED_DIRECTORY_ENTRIES)
    })
  })

  describe('and the same tree is walked either side of the buffering threshold', () => {
    it('should yield the same ids', async () => {
      const names = ['alpha', 'beta', 'beta.gzip', 'gamma.gzip', 'delta']

      const small = await walk(names)
      const large = await walk(padTo(MAX_BUFFERED_DIRECTORY_ENTRIES + 1, ...names))

      expect(small.reads).toBe(1)
      expect(large.reads).toBe(2)
      expect(small.listed.sort()).toEqual(large.listed.filter((id) => !id.startsWith('pad-')).sort())
    })
  })
})

describe('when a delete batch contains an id too long for the filesystem', () => {
  // ENAMETOOLONG means no file of that name CAN exist, so it is provably absent. The read path already
  // classified it that way while the delete invariant did not, and the disagreement was observable: the
  // whole batch rejected with a bare errno and failed identically on every retry.
  let storage: IContentStorageComponent
  let root: string
  let outcome: unknown

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'long-id-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root
    )
    await storage.storeStream('first', bufferToStream(Buffer.from('one')))
    await storage.storeStream('second', bufferToStream(Buffer.from('two')))
    outcome = await storage.delete(['x'.repeat(300), 'first', 'second']).then(
      () => 'resolved',
      (error: unknown) => error
    )
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should resolve rather than reject with the raw errno', () => {
    expect(outcome).toBe('resolved')
  })

  it('should have deleted the ids that do exist', async () => {
    expect(await storage.exist('first')).toBe(false)
    expect(await storage.exist('second')).toBe(false)
  })

  it('should report the over-long id as absent from the read path too', async () => {
    expect(await storage.exist('x'.repeat(300))).toBe(false)
  })
})

describe('when the in-memory backend hands content to a consumer', () => {
  // `fromBuffer` streams the buffer it is given as-is, so handing over the STORED one let a consumer
  // that writes into a chunk it received rewrite stored content in place. Every other backend reads
  // from a disk or a socket and cannot be aliased.
  let storage: IContentStorageComponent

  beforeEach(async () => {
    storage = createInMemoryStorage()
    await storage.storeStream('an-id', bufferToStream(Buffer.from('ORIGINAL-CONTENT')))
  })

  it('should not let a mutation of the delivered chunk change the stored content', async () => {
    const item = await storage.retrieve('an-id')
    for await (const chunk of await item!.asStream()) (chunk as Buffer).write('CLOBBER!')

    const reread = await storage.retrieve('an-id')

    expect((await streamToBuffer(await reread!.asStream())).toString()).toBe('ORIGINAL-CONTENT')
  })

  it('should not let a mutation of a ranged chunk change the stored content', async () => {
    const item = await storage.retrieve('an-id', { start: 0, end: 7 })
    for await (const chunk of await item!.asStream()) (chunk as Buffer).write('XXXXXXXX')

    const reread = await storage.retrieve('an-id')

    expect((await streamToBuffer(await reread!.asStream())).toString()).toBe('ORIGINAL-CONTENT')
  })
})

describe('when the in-memory backend deletes a list containing an unaddressable id', () => {
  // The folder-based backend resolves and removes id by id, so it deletes what it can and then rejects.
  // Validating the whole list up front made the same call leave every id in place, so a service
  // exercised against this backend in tests saw the opposite outcome in production.
  let storage: IContentStorageComponent

  beforeEach(async () => {
    storage = createInMemoryStorage()
    await storage.storeStream('victim', bufferToStream(Buffer.from('v')))
  })

  it('should reject', async () => {
    await expect(storage.delete(['victim', '../evil'])).rejects.toBeInstanceOf(PathNotContainedError)
  })

  it('should have deleted the ids preceding the invalid one', async () => {
    await storage.delete(['victim', '../evil']).catch(() => undefined)

    expect(await storage.exist('victim')).toBe(false)
  })
})

describe('when a bucket page carries a continuation token but no IsTruncated flag', () => {
  // AWS always sets `IsTruncated`, but an S3-compatible endpoint (MinIO, Ceph, a gateway) may omit it.
  // Requiring it to be true ended the enumeration after one page, and a GC or sync sweep concluded the
  // bucket held only its first keys.
  let listed: string[]

  beforeEach(async () => {
    const fake = createFakeS3Client()
    const pages: Record<string, { Contents: { Key: string }[]; NextContinuationToken?: string }> = {
      start: { Contents: [{ Key: 'a' }, { Key: 'b' }], NextContinuationToken: 'second' },
      second: { Contents: [{ Key: 'c' }], NextContinuationToken: undefined }
    }
    fake.on('ListObjectsV2Command', ({ ContinuationToken }: { ContinuationToken?: string }) => {
      // IsTruncated deliberately absent from every page.
      return pages[ContinuationToken ?? 'start']
    })
    const storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, fake, {
      Bucket: 'test',
      fileTypeLoader: undetectingLoader
    })

    listed = []
    for await (const each of storage.allFileIds()) listed.push(each)
  })

  it('should keep listing until the token is exhausted', () => {
    expect(listed).toEqual(['a', 'b', 'c'])
  })
})

describe('when incompressible content is stored through the compressing store', () => {
  // The 1.1 rule used to be applied only AFTER compressing the whole file, so 8MB of media (a PNG, a
  // JPEG, a GLB) cost a measured 128ms of CPU plus an 8MB staged write and an 8MB read back to produce
  // a file that was then deleted. The compression now stops as soon as its output has passed the point
  // where the rule is already guaranteed to reject it — which must not change WHICH files end up
  // compressed.
  let storage: IContentStorageComponent
  let root: string
  // Deterministic and incompressible: gzip cannot beat the 10% threshold on hash output.
  const incompressible = Buffer.concat(
    Array.from({ length: 400 }, (_, index) => createHash('sha512').update(`block-${index}`).digest())
  )

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'bail-out-'))
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      root
    )
    await storage.storeStreamAndCompress('media', bufferToStream(incompressible))
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  it('should store it uncompressed', async () => {
    expect((await storage.fileInfo('media'))?.encoding).toBeNull()
  })

  it('should serve back exactly what was stored', async () => {
    const item = await storage.retrieve('media')

    expect(await streamToBuffer(await item!.asStream())).toEqual(incompressible)
  })

  it('should leave no compressed output behind', async () => {
    const shard = createHash('sha1').update('media').digest('hex').slice(0, 4)

    await expect(nodeFsPromises.stat(path.join(root, shard, 'media.gzip'))).rejects.toMatchObject({ code: 'ENOENT' })
  })
})

describe('when report403AsAbsent is enabled', () => {
  // The lenient mode is the caller's explicit choice now, so both of its startup outcomes are worth a
  // distinct, actionable log line: one says the flag is doing something dangerous, the other says it is
  // pointless and should be removed.
  const forbidden = () =>
    Object.assign(new Error('Access Denied'), { name: 'AccessDenied', $metadata: { httpStatusCode: 403 } })

  const build = async (probeFails: boolean) => {
    const logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    const fake = createFakeS3Client()
    if (probeFails) {
      fake.on('ListObjectsV2Command', () => {
        throw forbidden()
      })
    }
    const storage = await createS3BasedFileSystemContentStorage({ logs: { getLogger: () => logger } } as any, fake, {
      Bucket: 'test',
      fileTypeLoader: undetectingLoader,
      report403AsAbsent: true
    })
    return { storage, logger, fake }
  }

  describe('and the startup probe is denied', () => {
    it('should warn that a misspelled bucket would now report everything as absent', async () => {
      const { logger } = await build(true)

      const warnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('report403AsAbsent is enabled'))
      expect(warnings).toHaveLength(1)
    })

    it('should report a 403 read as absent', async () => {
      const { storage, fake } = await build(true)
      fake.on('HeadObjectCommand', () => {
        throw forbidden()
      })

      expect(await storage.exist('an-id')).toBe(false)
    })
  })

  describe('and the principal can in fact list the bucket', () => {
    it('should warn that the option is pointless and should be removed', async () => {
      const { logger } = await build(false)

      const warnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('Remove it'))
      expect(warnings).toHaveLength(1)
    })
  })
})

describe('when S3 answers a read with a body that is not a stream', () => {
  // `Body` is optional in the v3 types and its runtime shape is platform-dependent, so a misconfigured
  // client must fail with something that names the problem rather than handing a non-stream to consumers.
  let failure: any

  beforeEach(async () => {
    const fake = createFakeS3Client()
    const storage = await createS3BasedFileSystemContentStorage({ logs: await createLogComponent({}) }, fake, {
      Bucket: 'test',
      fileTypeLoader: undetectingLoader
    })
    await storage.storeStream('an-id', bufferToStream(Buffer.from('content')))
    fake.on('GetObjectCommand', () => ({ Body: { notAStream: true }, ContentLength: 7 }))

    const item = await storage.retrieve('an-id')
    failure = await item!.asStream().then(
      () => undefined,
      (error: unknown) => error
    )
  })

  it('should reject naming the key and what arrived instead', () => {
    expect(failure?.message).toContain('an-id')
    expect(failure?.message).toContain('Object')
  })
})

describe('when a failed multipart upload was already aborted by the SDK', () => {
  // lib-storage issues its own AbortMultipartUpload on every path that runs BEFORE
  // CompleteMultipartUpload, so the call-site cleanup then gets `NoSuchUpload` — and reported it as
  // "its parts may persist in the bucket", telling operators to hunt a leak that does not exist on the
  // most common failure of all.
  let logger: { warn: jest.Mock; debug: jest.Mock; error: jest.Mock; info: jest.Mock; log: jest.Mock }
  let failure: unknown
  const completeFailure = Object.assign(new Error('we encountered an internal error'), {
    name: 'InternalError',
    $metadata: { httpStatusCode: 500 }
  })

  beforeEach(async () => {
    logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    const fake = createFakeS3Client()
    fake.on('CompleteMultipartUploadCommand', () => {
      throw completeFailure
    })
    fake.on('AbortMultipartUploadCommand', () => {
      throw Object.assign(new Error('The specified upload does not exist'), {
        name: 'NoSuchUpload',
        $metadata: { httpStatusCode: 404 }
      })
    })
    const storage = await createS3BasedFileSystemContentStorage({ logs: { getLogger: () => logger } } as any, fake, {
      Bucket: 'test',
      fileTypeLoader: undetectingLoader
    })

    // Larger than lib-storage's 5MB default part size, so the upload really is multipart.
    failure = await storage.storeStream('mp', bufferToStream(Buffer.alloc(6 * 1024 * 1024, 1))).then(
      () => undefined,
      (error: unknown) => error
    )
  })

  it('should surface the upload failure rather than the cleanup failure', () => {
    expect(failure).toBe(completeFailure)
  })

  it('should not warn that parts may persist', () => {
    const partWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('parts may persist'))

    expect(partWarnings).toEqual([])
  })
})
