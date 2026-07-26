import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { createHash } from 'crypto'
import { gzipSync } from 'zlib'
import { createFolderBasedFileSystemContentStorage, createFsComponent, IContentStorageComponent } from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import { createLogComponent } from '@well-known-components/logger'
import { PathNotContainedError } from '../src/folder-based/errors'

const fs = createFsComponent()

const shardOf = (id: string): string => createHash('sha1').update(id).digest('hex').substring(0, 4)

describe('folder-based storage read contract', () => {
  let root: string
  let storage: IContentStorageComponent

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'read-contract-'))
    storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root)
  })

  afterEach(async () => {
    await storage.stop?.()
    rmSync(root, { recursive: true, force: true })
  })

  describe('when a gzip-backed id is retrieved without a range', () => {
    let original: Buffer
    let item: NonNullable<Awaited<ReturnType<IContentStorageComponent['retrieve']>>>

    beforeEach(async () => {
      // Highly compressible, so the stored size and the logical size differ by orders of magnitude
      // and a confusion between them is unmistakable.
      original = Buffer.alloc(100_000, 0x61)
      await storage.storeStreamAndCompress('gzip-id', bufferToStream(original))
      item = (await storage.retrieve('gzip-id'))!
    })

    it('should report the stored gzip size as size', async () => {
      expect(item.size).toBe((await storage.fileInfo('gzip-id'))!.size)
    })

    it('should report the uncompressed size as contentSize rather than the compressed one', () => {
      // `contentSize` is documented as the logical size and at least one consumer bounds range
      // requests with `contentSize ?? size`; defaulting it to `size` handed them the compressed count.
      expect(item.contentSize).toBe(original.length)
    })

    it('should agree with fileInfo on the content size', async () => {
      expect(item.contentSize).toBe((await storage.fileInfo('gzip-id'))!.contentSize)
    })
  })

  describe('when an uncompressed id is retrieved', () => {
    let item: NonNullable<Awaited<ReturnType<IContentStorageComponent['retrieve']>>>

    beforeEach(async () => {
      await storage.storeStream('raw-id', bufferToStream(Buffer.from('hello')))
      item = (await storage.retrieve('raw-id'))!
    })

    it('should report the content size as equal to the stored size', () => {
      expect(item.contentSize).toBe(item.size)
    })
  })

  describe('when the gzip vanishes between its stat and the trailer read', () => {
    let item: Awaited<ReturnType<IContentStorageComponent['retrieve']>>

    beforeEach(async () => {
      // A store transitioning gzip -> raw lands exactly here: the trailer read must fall through to
      // the raw representation instead of reporting the id absent.
      const gzipPath = path.join(root, shardOf('racing-id'), 'racing-id.gzip')
      await nodeFs.mkdir(path.dirname(gzipPath), { recursive: true })
      await nodeFs.writeFile(gzipPath, gzipSync(Buffer.from('compressed')))
      await nodeFs.writeFile(path.join(root, shardOf('racing-id'), 'racing-id'), 'raw fallback')

      const realCreateReadStream = fs.createReadStream
      jest.spyOn(fs, 'createReadStream').mockImplementationOnce(((target: string, options: any) => {
        // The trailer read is the first ranged read; delete the gzip underneath it.
        rmSync(gzipPath, { force: true })
        return realCreateReadStream(target, options)
      }) as typeof fs.createReadStream)

      item = await storage.retrieve('racing-id')
    })

    afterEach(() => {
      jest.restoreAllMocks()
    })

    it('should fall through to the raw representation instead of reporting the id absent', () => {
      expect(item!.encoding).toBeNull()
    })
  })

  describe('when a stored id becomes unreadable', () => {
    let shardPath: string
    // `chmod 000` denies nothing to root, which is the default user in most CI containers, so the
    // premise of these two cases simply does not hold there.
    const canDenyAccess = typeof process.getuid === 'function' && process.getuid() !== 0
    const itUnlessRoot = canDenyAccess ? it : it.skip

    beforeEach(async () => {
      await storage.storeStream('locked-id', bufferToStream(Buffer.from('secret')))
      shardPath = path.join(root, shardOf('locked-id'))
      await nodeFs.chmod(shardPath, 0o000)
    })

    afterEach(async () => {
      await nodeFs.chmod(shardPath, 0o755)
    })

    itUnlessRoot('should reject exist rather than reporting the present-but-unreadable id as absent', async () => {
      // `existPath` tests F_OK|R_OK, so this used to answer `false` — the "a broken store looks like
      // an empty one" answer the read contract exists to remove, and one `fileInfo` already refuses.
      await expect(storage.exist('locked-id')).rejects.toMatchObject({ code: 'EACCES' })
    })

    itUnlessRoot('should reject fileInfo for the same id, as it already did', async () => {
      await expect(storage.fileInfo('locked-id')).rejects.toMatchObject({ code: 'EACCES' })
    })
  })

  describe('when an id that was never stored is checked', () => {
    it('should report it as absent rather than rejecting', async () => {
      await expect(storage.exist('never-stored-id')).resolves.toBe(false)
    })
  })
})

describe('folder-based storage ids that name a directory rather than a file', () => {
  // `''` and `'.'` are the only ids that resolve to the containment directory itself. Allowing them
  // meant `exist()` stat'd that directory and answered `true`, and `retrieve()` handed back a
  // ContentItem whose stream fails with EISDIR — in flat mode the directory is the storage root, so
  // both ids aliased straight onto it.
  describe.each([
    ['with hash prefixes', false],
    ['in flat mode', true]
  ])('%s', (_label, disablePrefixHash) => {
    let root: string
    let storage: IContentStorageComponent

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'degenerate-id-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
        disablePrefixHash
      })
      await storage.storeStream('real-id', bufferToStream(Buffer.from('content')))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    describe.each([
      ['empty', ''],
      ['dot', '.']
    ])('and the id is %s', (_name, degenerate) => {
      it('should reject exist instead of reporting the directory as present content', async () => {
        await expect(storage.exist(degenerate)).rejects.toBeInstanceOf(PathNotContainedError)
      })

      it('should report retrieve as nothing to serve, like any other non-containable id', async () => {
        await expect(storage.retrieve(degenerate)).resolves.toBeUndefined()
      })

      it('should reject a store instead of writing over the directory', async () => {
        await expect(storage.storeStream(degenerate, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })
    })

    it('should still resolve a normal id', async () => {
      await expect(storage.exist('real-id')).resolves.toBe(true)
    })
  })
})

describe('folder-based storage ids that normalize onto another id', () => {
  // `path.join` normalizes what it builds, so an id like `a/../victim` lands on the file of a
  // DIFFERENT logical id. Containment does not catch it — the result is still inside the root, it is
  // just somebody else's content — so a caller accepting untrusted ids could overwrite, read or
  // delete another id. With hash prefixes it needs a shard collision, which is only ~2^16 work.
  describe.each([
    ['with hash prefixes', false],
    ['in flat mode', true]
  ])('%s', (_label, disablePrefixHash) => {
    let root: string
    let storage: IContentStorageComponent
    let victimBytes: Buffer

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'aliasing-id-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
        disablePrefixHash
      })
      victimBytes = Buffer.from('VICTIM ORIGINAL')
      await storage.storeStream('victim', bufferToStream(victimBytes))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    describe.each([
      ['a parent segment', 'a/../victim'],
      ['a current-directory segment', './victim'],
      ['an empty segment', 'a//../victim'],
      ['a trailing separator', 'victim/'],
      ['an absolute path', '/victim'],
      ['a current-directory segment mid-path', 'a/./victim']
    ])('and the id uses %s', (_name, aliasing) => {
      it('should reject a store rather than overwrite the id it normalizes onto', async () => {
        await expect(storage.storeStream(aliasing, bufferToStream(Buffer.from('ATTACKER')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })

      it('should leave the other id untouched', async () => {
        await storage.storeStream(aliasing, bufferToStream(Buffer.from('ATTACKER'))).catch(() => undefined)
        const item = await storage.retrieve('victim')

        expect(await streamToBuffer(await item!.asStream())).toEqual(victimBytes)
      })

      it('should reject a delete rather than remove the id it normalizes onto', async () => {
        await expect(storage.delete([aliasing])).rejects.toBeInstanceOf(PathNotContainedError)
        expect(await storage.exist('victim')).toBe(true)
      })
    })

    describe.each([
      ['ends in .gzip', 'victim.gzip'],
      ['contains a NUL byte', 'vic\0tim']
    ])('and the id %s', (_name, unaddressable) => {
      it('should reject a store', async () => {
        await expect(storage.storeStream(unaddressable, bufferToStream(Buffer.from('x')))).rejects.toBeInstanceOf(
          PathNotContainedError
        )
      })

      it('should reject exist', async () => {
        await expect(storage.exist(unaddressable)).rejects.toBeInstanceOf(PathNotContainedError)
      })
    })

    it('should report an over-long name as absent rather than as a storage fault', async () => {
      // No file of that name can exist, so it is a miss. Throwing failed whole existMultiple batches.
      await expect(storage.exist('x'.repeat(300))).resolves.toBe(false)
    })

    it('should still accept a legitimately nested id', async () => {
      await storage.storeStream('nested/legit/id', bufferToStream(Buffer.from('fine')))

      expect(await storage.exist('nested/legit/id')).toBe(true)
    })

    describe('and the id contains a backslash', () => {
      // The check is platform-aware because `path.join`/`path.relative` are: on POSIX a backslash is
      // an ordinary filename character, so `a\..\victim` names its own file and aliases nothing, and
      // rejecting it would refuse a valid id. On Windows the very same equality rejects it, because
      // there it IS a separator and the id normalizes away.
      beforeEach(async () => {
        await storage.storeStream('a\\..\\victim', bufferToStream(Buffer.from('OWN FILE')))
      })

      it('should store it under its own literal name', async () => {
        const item = await storage.retrieve('a\\..\\victim')

        expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('OWN FILE'))
      })

      it('should leave the id it would alias on a separator platform untouched', async () => {
        const item = await storage.retrieve('victim')

        expect(await streamToBuffer(await item!.asStream())).toEqual(victimBytes)
      })
    })
  })
})

describe('folder-based storage enumeration', () => {
  describe('when ids containing path separators are stored with hash prefixes', () => {
    let root: string
    let storage: IContentStorageComponent
    let listed: string[]

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'enumeration-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root)
      await storage.storeStream('aa/bb/nested-id', bufferToStream(Buffer.from('nested')))
      await storage.storeStream('flat-id', bufferToStream(Buffer.from('flat')))
      listed = []
      for await (const each of storage.allFileIds()) listed.push(each)
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should yield the full nested id rather than only its last segment', () => {
      // Yielding the basename produced an id that does not resolve back to the file it came from.
      expect(listed.sort()).toEqual(['aa/bb/nested-id', 'flat-id'])
    })

    it('should yield ids that round-trip through retrieve', async () => {
      await expect(storage.retrieve('aa/bb/nested-id')).resolves.toBeDefined()
    })
  })

  describe('when ids containing path separators are stored in flat mode', () => {
    let root: string
    let storage: IContentStorageComponent
    let listed: string[]

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'enumeration-flat-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
        disablePrefixHash: true
      })
      await storage.storeStream('deep/nested/id', bufferToStream(Buffer.from('nested')))
      listed = []
      for await (const each of storage.allFileIds()) listed.push(each)
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should yield the id relative to the storage root', () => {
      expect(listed).toEqual(['deep/nested/id'])
    })
  })

  describe('when a prefix is given', () => {
    let root: string
    let storage: IContentStorageComponent
    let listed: string[]

    beforeEach(async () => {
      root = mkdtempSync(path.join(os.tmpdir(), 'enumeration-prefix-'))
      storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root)
      await storage.storeStreamAndCompress('wanted-id', bufferToStream(Buffer.alloc(10_000, 0x61)))
      await storage.storeStream('other-id', bufferToStream(Buffer.from('other')))
      listed = []
      for await (const each of storage.allFileIds('wanted')) listed.push(each)
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should match the prefix against the id, including for a compressed representation', () => {
      expect(listed).toEqual(['wanted-id'])
    })
  })
})
