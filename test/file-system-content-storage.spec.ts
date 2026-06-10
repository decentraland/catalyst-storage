import { createHash } from 'crypto'
import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  IContentStorageComponent,
  IFileSystemComponent
} from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import { createLogComponent } from '@well-known-components/logger'

describe('fileSystemContentStorage', () => {
  const fs = createFsComponent()
  let tmpRootDir: string
  let fileSystemContentStorage: IContentStorageComponent

  // sha1('some-id') = 9584b661c135a43f2fbbe43cc5104f7bd693d048
  const id: string = 'some-id'
  const content = Buffer.from('123')
  let filePath: string

  // sha1('another-id') = ea6cf57af4e7e1a5041298624af4bff04d245e71
  const id2: string = 'another-id'
  const content2 = Buffer.from('456')
  let filePath2: string

  beforeEach(async () => {
    tmpRootDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-'))
    fileSystemContentStorage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpRootDir
    )
    filePath = path.join(tmpRootDir, '9584', id)
    filePath2 = path.join(tmpRootDir, 'ea6c', id2)
  })

  afterEach(async () => {
    await fileSystemContentStorage.stop?.()
    rmSync(tmpRootDir, { recursive: true, force: false })
  })

  it(`When content is stored, then the correct file structure is created`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    expect(await fs.existPath(filePath)).toBeTruthy()
  })

  it(`When content is deleted, then the backing file is also deleted`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    expect(await fs.existPath(filePath)).toBeTruthy()
    await fileSystemContentStorage.delete([id])
    expect(await fs.existPath(filePath)).toBeFalsy()
  })

  it(`When multiple content is stored, then the correct file structure is created`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))
    expect(await fs.existPath(filePath)).toBeTruthy()
    expect(await fs.existPath(filePath2)).toBeTruthy()
  })

  it(`When multiple content is stored and one is deleted, then the correct file is deleted`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))
    await fileSystemContentStorage.delete([id2])
    expect(await fs.existPath(filePath)).toBeTruthy()
    expect(await fs.existPath(filePath2)).toBeFalsy()
  })

  it(`When a content with bad compression ratio is stored and compressed, then it is not stored as .gzip`, async () => {
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(content))
    expect(await fs.existPath(filePath)).toBeTruthy()
    expect(await fs.existPath(filePath + '.gzip')).toBeFalsy()
  })

  it(`When a content with good compression ratio is stored and compressed, then it is stored as .gzip and non-compressed file is deleted`, async () => {
    const goodCompresstionRatioContent = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(goodCompresstionRatioContent))
    const compressedFile = await fileSystemContentStorage.retrieve(id)
    expect(compressedFile).toBeDefined()
    expect(compressedFile?.encoding).toBe('gzip')
    expect(await fs.existPath(filePath)).toBeFalsy()
    expect(await fs.existPath(filePath + '.gzip')).toBeTruthy()
  })

  it(`When content is stored, then all the ids are retrieved`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))
    const fileIds = fileSystemContentStorage.allFileIds()
    const seenIds: string[] = []
    for await (const fileId of fileIds) seenIds.push(fileId)
    expect(seenIds).toEqual(expect.arrayContaining([id, id2]))
  })

  it(`When content is stored compressed, then all the ids are retrieved without the compress extension`, async () => {
    const goodCompresstionRatioContent = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(goodCompresstionRatioContent))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))
    const fileIds = fileSystemContentStorage.allFileIds()
    const seenIds: string[] = []
    for await (const fileId of fileIds) seenIds.push(fileId)
    expect(seenIds).toEqual(expect.arrayContaining([id, id2]))
  })

  it(`When content is stored compressed, then the raw content stream has the uncompressed data`, async () => {
    const itemSize = 100
    const goodCompresstionRatioContent = Buffer.from(new Uint8Array(itemSize).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(goodCompresstionRatioContent))
    const compressedItem = await fileSystemContentStorage.retrieve(id)
    const compressedItemSize = compressedItem?.size
    expect(compressedItemSize).toBeDefined()
    if (compressedItemSize) {
      expect(compressedItemSize < 100).toBeTruthy()
      const buffer = await streamToBuffer(await compressedItem?.asRawStream())
      expect(buffer.length).toBe(compressedItemSize)
    }
  })

  it(`When an id is outside of the root folder it should return undefined even if present`, async () => {
    expect(await fileSystemContentStorage.retrieve(`../${id}`)).toBeUndefined()
  })

  it(`When content exists, then it is possible to iterate over all keys in storage`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))

    async function check(prefix: string, expected: string[]) {
      const filtered = []
      for await (const key of await fileSystemContentStorage.allFileIds(prefix)) {
        filtered.push(key)
      }
      expect(filtered.length).toBe(expected.length)
      for (const filteredKey of expected) {
        expect(filtered).toContain(filteredKey)
      }
      return filtered
    }

    await check('an', ['another-id'])
    await check('so', ['some-id'])
    await check(undefined as any, ['another-id', 'some-id'])
  })

  it(`When content is stored, then a range can be retrieved`, async () => {
    const data = Buffer.from('Hello, World!')
    await fileSystemContentStorage.storeStream(id, bufferToStream(data))

    const item = await fileSystemContentStorage.retrieve(id, { start: 0, end: 4 })
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('Hello'))
    expect(item!.size).toBe(5)
  })

  it(`When content is stored, then a range in the middle can be retrieved`, async () => {
    const data = Buffer.from('Hello, World!')
    await fileSystemContentStorage.storeStream(id, bufferToStream(data))

    const item = await fileSystemContentStorage.retrieve(id, { start: 7, end: 11 })
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('World'))
    expect(item!.size).toBe(5)
  })

  it(`When a range with end beyond file size is requested, then it clamps to file size`, async () => {
    const data = Buffer.from('Hello, World!')
    await fileSystemContentStorage.storeStream(id, bufferToStream(data))

    const item = await fileSystemContentStorage.retrieve(id, { start: 7, end: 999 })
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('World!'))
    expect(item!.size).toBe(6)
  })

  it(`When a range with start > end is requested, then it throws a RangeError`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await expect(fileSystemContentStorage.retrieve(id, { start: 5, end: 2 })).rejects.toThrow(RangeError)
  })

  it(`When a range with negative start is requested, then it throws a RangeError`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await expect(fileSystemContentStorage.retrieve(id, { start: -1, end: 2 })).rejects.toThrow(RangeError)
  })

  it(`When a range with start past end of file is requested, then it throws a RangeError`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await expect(fileSystemContentStorage.retrieve(id, { start: 10, end: 20 })).rejects.toThrow(RangeError)
  })

  it(`When a range is requested on a non-existent file, then it returns undefined`, async () => {
    const item = await fileSystemContentStorage.retrieve('non-existent-id', { start: 0, end: 4 })
    expect(item).toBeUndefined()
  })

  it(`When a single-byte range is requested, then it returns that byte`, async () => {
    const data = Buffer.from('Hello, World!')
    await fileSystemContentStorage.storeStream(id, bufferToStream(data))

    const item = await fileSystemContentStorage.retrieve(id, { start: 4, end: 4 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(1)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('o'))
  })

  it(`When content is stored with bad compression ratio, then a range can be retrieved from the uncompressed file`, async () => {
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from('Hello, World!')))

    const item = await fileSystemContentStorage.retrieve(id, { start: 0, end: 4 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(5)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('Hello'))
  })

  it(`When content is stored compressed (gzip only), then a range retrieve decompresses and serves the range`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    const item = await fileSystemContentStorage.retrieve(id, { start: 0, end: 9 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(10)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))
  })

  it(`When a gzip-only file is range-requested, then the uncompressed file is cached to disk`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    expect(await fs.existPath(filePath)).toBeFalsy()
    await fileSystemContentStorage.retrieve(id, { start: 0, end: 9 })
    expect(await fs.existPath(filePath)).toBeTruthy()
  })

  it(`When a gzip-only file is range-requested twice, then the second request reads from the cached file`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    const item1 = await fileSystemContentStorage.retrieve(id, { start: 0, end: 9 })
    expect(await streamToBuffer(await item1!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))

    const item2 = await fileSystemContentStorage.retrieve(id, { start: 50, end: 59 })
    expect(item2).toBeDefined()
    expect(item2!.size).toBe(10)
    expect(await streamToBuffer(await item2!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))
  })

  it(`When a cached file is deleted via storage.delete(), then it is removed from the cache`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    await fileSystemContentStorage.retrieve(id, { start: 0, end: 9 })
    expect(await fs.existPath(filePath)).toBeTruthy()

    await fileSystemContentStorage.delete([id])
    expect(await fs.existPath(filePath)).toBeFalsy()
    expect(await fs.existPath(filePath + '.gzip')).toBeFalsy()
  })

  it(`When concurrent range requests hit the same gzip-only file, then only one decompression occurs`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    const [item1, item2] = await Promise.all([
      fileSystemContentStorage.retrieve(id, { start: 0, end: 9 }),
      fileSystemContentStorage.retrieve(id, { start: 50, end: 59 })
    ])

    expect(item1).toBeDefined()
    expect(item2).toBeDefined()
    expect(await streamToBuffer(await item1!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))
    expect(await streamToBuffer(await item2!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))
    expect(await fs.existPath(filePath)).toBeTruthy()
  })

  it(`When a gzip-only file is cached, then allFileIds does not yield duplicates`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))

    // Trigger cache — both file and file.gzip now exist for id
    await fileSystemContentStorage.retrieve(id, { start: 0, end: 9 })
    expect(await fs.existPath(filePath)).toBeTruthy()
    expect(await fs.existPath(filePath + '.gzip')).toBeTruthy()

    const seenIds: string[] = []
    for await (const fileId of fileSystemContentStorage.allFileIds()) seenIds.push(fileId)
    const idOccurrences = seenIds.filter((x) => x === id)
    expect(idOccurrences.length).toBe(1)
  })

  it(`When storeStreamAndCompress is called after a cached decompression, then the cache entry is cleared`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    // Trigger cache
    await fileSystemContentStorage.retrieve(id, { start: 0, end: 9 })
    expect(await fs.existPath(filePath)).toBeTruthy()

    // Re-store and compress — should clear the cache entry
    const newData = Buffer.from(new Uint8Array(200).fill(1))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(newData))

    // The cached uncompressed file should be gone (deleted by storeStreamAndCompress)
    const compressedFile = await fileSystemContentStorage.retrieve(id)
    expect(compressedFile).toBeDefined()
  })

  describe('decompression cache eviction', () => {
    beforeEach(() => {
      jest.useFakeTimers()
    })

    afterEach(() => {
      jest.useRealTimers()
    })

    it(`When the cache TTL expires, then the cached uncompressed file is cleaned up`, async () => {
      const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-cache-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        tmpDir,
        { decompressCacheTTL: 60000, decompressCacheEvictionInterval: 30000 }
      )
      await storage.start?.({} as any)
      const cachedFilePath = path.join(tmpDir, '9584', id)

      try {
        const data = Buffer.from(new Uint8Array(100).fill(0))
        await storage.storeStreamAndCompress(id, bufferToStream(data))
        await storage.retrieve(id, { start: 0, end: 9 })
        expect(await fs.existPath(cachedFilePath)).toBeTruthy()

        // Advance past TTL + eviction interval and flush async work
        await jest.advanceTimersByTimeAsync(60000 + 30000)

        expect(await fs.existPath(cachedFilePath)).toBeFalsy()
        expect(await fs.existPath(cachedFilePath + '.gzip')).toBeTruthy()
      } finally {
        await storage.stop?.()
        rmSync(tmpDir, { recursive: true, force: true })
      }
    })

    it(`When the cache exceeds max size, then LRU files are evicted`, async () => {
      const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-cache-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        tmpDir,
        { decompressCacheMaxSize: 150, decompressCacheEvictionInterval: 30000 }
      )
      await storage.start?.({} as any)
      const cachedFilePath1 = path.join(tmpDir, '9584', id)
      const cachedFilePath2 = path.join(tmpDir, 'ea6c', id2)

      try {
        // Store two 100-byte files as gzip-only
        const data = Buffer.from(new Uint8Array(100).fill(0))
        await storage.storeStreamAndCompress(id, bufferToStream(data))
        await storage.storeStreamAndCompress(id2, bufferToStream(Buffer.from(new Uint8Array(100).fill(1))))

        // Trigger cache for first file
        await storage.retrieve(id, { start: 0, end: 9 })
        expect(await fs.existPath(cachedFilePath1)).toBeTruthy()

        // Advance time so id2 has a newer lastAccess
        jest.advanceTimersByTime(1000)

        // Trigger cache for second file — total cache now exceeds 150 bytes
        await storage.retrieve(id2, { start: 0, end: 9 })
        expect(await fs.existPath(cachedFilePath2)).toBeTruthy()

        // Advance past eviction interval and flush async work
        await jest.advanceTimersByTimeAsync(30000)

        // LRU file (id, accessed first) should be evicted, id2 should remain
        expect(await fs.existPath(cachedFilePath1)).toBeFalsy()
        expect(await fs.existPath(cachedFilePath2)).toBeTruthy()
      } finally {
        await storage.stop?.()
        rmSync(tmpDir, { recursive: true, force: true })
      }
    })

    it(`When the cache is evicted by TTL, then a subsequent range request re-decompresses successfully`, async () => {
      const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-cache-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        tmpDir,
        { decompressCacheTTL: 60000, decompressCacheEvictionInterval: 30000 }
      )
      await storage.start?.({} as any)
      const cachedFilePath = path.join(tmpDir, '9584', id)

      try {
        const data = Buffer.from(new Uint8Array(100).fill(0))
        await storage.storeStreamAndCompress(id, bufferToStream(data))

        // First range request — triggers decompression and cache
        const item1 = await storage.retrieve(id, { start: 0, end: 9 })
        expect(item1).toBeDefined()
        expect(await fs.existPath(cachedFilePath)).toBeTruthy()

        // Advance past TTL + eviction interval to evict
        await jest.advanceTimersByTimeAsync(60000 + 30000)
        expect(await fs.existPath(cachedFilePath)).toBeFalsy()

        // Second range request — should re-decompress and serve correctly
        const item2 = await storage.retrieve(id, { start: 50, end: 59 })
        expect(item2).toBeDefined()
        expect(item2!.size).toBe(10)
        expect(await streamToBuffer(await item2!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))
        expect(await fs.existPath(cachedFilePath)).toBeTruthy()
      } finally {
        await storage.stop?.()
        rmSync(tmpDir, { recursive: true, force: true })
      }
    })

    it(`When stop() is called, then all cached files are evicted regardless of TTL`, async () => {
      const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-cache-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        tmpDir,
        { decompressCacheTTL: 999999, decompressCacheEvictionInterval: 999999 }
      )
      const cachedFilePath = path.join(tmpDir, '9584', id)

      try {
        const data = Buffer.from(new Uint8Array(100).fill(0))
        await storage.storeStreamAndCompress(id, bufferToStream(data))
        await storage.retrieve(id, { start: 0, end: 9 })
        expect(await fs.existPath(cachedFilePath)).toBeTruthy()

        // stop() should evict all cached files even though TTL hasn't expired
        await storage.stop?.()
        expect(await fs.existPath(cachedFilePath)).toBeFalsy()
        expect(await fs.existPath(cachedFilePath + '.gzip')).toBeTruthy()
      } finally {
        rmSync(tmpDir, { recursive: true, force: true })
      }
    })
  })

  it(`When decompression fails due to a corrupt gzip file, then the partial file is cleaned up and retrieve returns undefined`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-corrupt-'))
    const storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, tmpDir)
    const corruptId = 'corrupt-file'
    // sha1('corrupt-file') = first 4 chars
    const hash = createHash('sha1').update(corruptId).digest('hex').substring(0, 4)
    const gzipPath = path.join(tmpDir, hash, corruptId + '.gzip')
    const uncompressedPath = path.join(tmpDir, hash, corruptId)

    try {
      // Write garbage data as a .gzip file to simulate corruption
      await fs.mkdir(path.join(tmpDir, hash), { recursive: true })
      await nodeFs.writeFile(gzipPath, Buffer.from('this is not valid gzip data'))

      // Range request should trigger decompression which fails
      const item = await storage.retrieve(corruptId, { start: 0, end: 4 })
      expect(item).toBeUndefined()

      // The partial uncompressed file should have been cleaned up
      expect(await fs.existPath(uncompressedPath)).toBeFalsy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When many range requests race for the same cold gzip, then it is decompressed only once and all return correct data`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-race-'))
    const cachedFilePath = path.join(tmpDir, '9584', id)

    // Wrap the fs component to count how many times the uncompressed cache file is written.
    // Without deduplication, concurrent cold-cache range requests each decompress it, writing
    // the file once per request (and double-counting its size against the cache budget).
    const realFs = createFsComponent()
    let decompressionWrites = 0
    const spyFs: IFileSystemComponent = {
      ...realFs,
      createWriteStream: ((target: any, options?: any) => {
        if (target === cachedFilePath) decompressionWrites++
        return realFs.createWriteStream(target, options)
      }) as typeof realFs.createWriteStream
    }

    const storage = await createFolderBasedFileSystemContentStorage(
      { fs: spyFs, logs: await createLogComponent({}) },
      tmpDir
    )

    try {
      const data = Buffer.from('ABCDEFGH'.repeat(200)) // 1600 bytes, compressible
      await storage.storeStreamAndCompress(id, bufferToStream(data))
      expect(await realFs.existPath(cachedFilePath)).toBeFalsy() // cold uncompressed cache
      decompressionWrites = 0 // count only the decompression phase

      const range = { start: 100, end: 199 }
      const expected = data.subarray(100, 200)

      const results = await Promise.all(
        Array.from({ length: 16 }, async () => {
          const item = await storage.retrieve(id, range)
          return streamToBuffer(await item!.asStream())
        })
      )

      for (const buffer of results) {
        expect(buffer).toEqual(expected)
      }
      // Deduplicated: the gzip is decompressed to the cache file exactly once.
      expect(decompressionWrites).toBe(1)
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When a gzip item inflates beyond the max decompressed size, then the range request is refused and no oversized file is written`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-bomb-'))
    // Cap decompression at 50 bytes; the payload below inflates to 1000.
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressMaxFileSize: 50 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
      // 1000 zero bytes compress to a tiny gzip but inflate well past the 50-byte cap.
      const data = Buffer.from(new Uint8Array(1000).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))
      expect(await fs.existPath(cachedFilePath + '.gzip')).toBeTruthy()

      // The range request triggers decompression, which is aborted at the cap.
      const item = await storage.retrieve(id, { start: 0, end: 9 })
      expect(item).toBeUndefined()

      // No oversized uncompressed cache file is left behind; the gzip is untouched.
      expect(await fs.existPath(cachedFilePath)).toBeFalsy()
      expect(await fs.existPath(cachedFilePath + '.gzip')).toBeTruthy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When a gzip inflates to exactly the max decompressed size, then the range request succeeds`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-boundary-'))
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressMaxFileSize: 1000 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
      // Inflates to exactly 1000 bytes — at (not over) the cap, so it must be allowed.
      const data = Buffer.from(new Uint8Array(1000).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))

      const item = await storage.retrieve(id, { start: 0, end: 9 })
      expect(item).toBeDefined()
      expect(item!.size).toBe(10)
      expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.alloc(10, 0))
      // Decompression succeeded and was cached.
      expect(await fs.existPath(cachedFilePath)).toBeTruthy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When two range requests race for a gzip that exceeds the cap, then both are refused and nothing is left behind`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-bomb-race-'))
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressMaxFileSize: 50 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
      const data = Buffer.from(new Uint8Array(1000).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))

      // Two simultaneous range requests exercise the inflight-decompression guard on the error path.
      const [a, b] = await Promise.all([
        storage.retrieve(id, { start: 0, end: 9 }),
        storage.retrieve(id, { start: 0, end: 9 })
      ])
      expect(a).toBeUndefined()
      expect(b).toBeUndefined()
      expect(await fs.existPath(cachedFilePath)).toBeFalsy()

      // The guard is not left stuck: a subsequent request is still cleanly refused.
      expect(await storage.retrieve(id, { start: 0, end: 9 })).toBeUndefined()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When decompressMaxFileSize is unset, then it inherits decompressCacheMaxSize and allows files within it`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-default-ok-'))
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressCacheMaxSize: 2000 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
      // 1000 bytes is within the inherited 2000-byte limit, so it decompresses normally.
      const data = Buffer.from(new Uint8Array(1000).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))

      const item = await storage.retrieve(id, { start: 0, end: 9 })
      expect(item).toBeDefined()
      expect(item!.size).toBe(10)
      expect(await fs.existPath(cachedFilePath)).toBeTruthy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When decompressMaxFileSize is unset, then a file larger than decompressCacheMaxSize is refused`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-default-cap-'))
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressCacheMaxSize: 500 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
      // 1000 bytes exceeds the inherited 500-byte limit (proving the cap came from
      // decompressCacheMaxSize, not the multi-GB fallback), so it is refused.
      const data = Buffer.from(new Uint8Array(1000).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))

      expect(await storage.retrieve(id, { start: 0, end: 9 })).toBeUndefined()
      expect(await fs.existPath(cachedFilePath)).toBeFalsy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When content is stored compressed (gzip only), then exist returns true`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))
    // Verify only .gzip exists on disk
    expect(await fs.existPath(filePath)).toBeFalsy()
    expect(await fs.existPath(filePath + '.gzip')).toBeTruthy()

    expect(await fileSystemContentStorage.exist(id)).toBe(true)
  })

  it(`When content does not exist, then exist returns false`, async () => {
    expect(await fileSystemContentStorage.exist('non-existent-id')).toBe(false)
  })

  it(`When multiple content is stored, then existMultiple returns correct results`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id2, bufferToStream(data))

    const result = await fileSystemContentStorage.existMultiple([id, id2, 'non-existent'])
    expect(result.get(id)).toBe(true)
    expect(result.get(id2)).toBe(true)
    expect(result.get('non-existent')).toBe(false)
  })

  it(`When content is stored compressed (gzip only), then fileInfo returns compressed encoding and size`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    const info = await fileSystemContentStorage.fileInfo(id)
    expect(info).toBeDefined()
    expect(info!.encoding).toBe('gzip')
    expect(info!.size).toBeDefined()
    expect(info!.size).toBeGreaterThan(0)
    expect(info!.size).toBeLessThan(100)
  })

  it(`When content is stored compressed (gzip only), then fileInfo returns the correct contentSize from the gzip trailer`, async () => {
    const data = Buffer.from(new Uint8Array(100).fill(0))
    await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(data))

    const info = await fileSystemContentStorage.fileInfo(id)
    expect(info).toBeDefined()
    expect(info!.encoding).toBe('gzip')
    expect(info!.contentSize).toBe(100)
  })

  it(`When content is stored uncompressed, then fileInfo returns contentSize equal to size`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))

    const info = await fileSystemContentStorage.fileInfo(id)
    expect(info).toBeDefined()
    expect(info!.contentSize).toBe(info!.size)
    expect(info!.contentSize).toBe(3)
  })

  it(`When a cached file is accessed via range, then its lastAccess is updated and it survives LRU eviction`, async () => {
    jest.useFakeTimers()
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-touch-'))
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressCacheMaxSize: 150, decompressCacheEvictionInterval: 30000 }
    )
    await storage.start?.({} as any)
    const cachedFilePath1 = path.join(tmpDir, '9584', id)
    const cachedFilePath2 = path.join(tmpDir, 'ea6c', id2)

    try {
      const data = Buffer.from(new Uint8Array(100).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))
      await storage.storeStreamAndCompress(id2, bufferToStream(Buffer.from(new Uint8Array(100).fill(1))))

      // Trigger cache for both files
      await storage.retrieve(id, { start: 0, end: 9 })
      jest.advanceTimersByTime(1000)
      await storage.retrieve(id2, { start: 0, end: 9 })

      // Now touch id (the older one) so it becomes most-recently-accessed
      jest.advanceTimersByTime(1000)
      await storage.retrieve(id, { start: 0, end: 9 })

      // Advance past eviction interval
      await jest.advanceTimersByTimeAsync(30000)

      // id2 (least recently accessed) should be evicted, id should remain
      expect(await fs.existPath(cachedFilePath1)).toBeTruthy()
      expect(await fs.existPath(cachedFilePath2)).toBeFalsy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
      jest.useRealTimers()
    }
  })

  it(`When start() is not called, then range requests and caching still work`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-nostart-'))
    const storage = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, tmpDir)
    // Intentionally do NOT call storage.start()
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
      const data = Buffer.from(new Uint8Array(100).fill(0))
      await storage.storeStreamAndCompress(id, bufferToStream(data))

      const item = await storage.retrieve(id, { start: 0, end: 9 })
      expect(item).toBeDefined()
      expect(item!.size).toBe(10)
      expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from(new Uint8Array(10).fill(0)))
      expect(await fs.existPath(cachedFilePath)).toBeTruthy()
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When content is stored, then we can check file info`, async function () {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))

    const exists = await fileSystemContentStorage.fileInfoMultiple([id, id2])

    expect(exists.get(id)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(exists.get(id2)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(await fileSystemContentStorage.fileInfo(id)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(await fileSystemContentStorage.fileInfo(id2)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(await fileSystemContentStorage.fileInfo('non-existent-id')).toBeUndefined()
  })

  describe('path containment', () => {
    it(`When an id escapes the root via a sibling-prefix path, then it is rejected and nothing is written outside the root`, async () => {
      // disablePrefixHash makes the root itself the containment dir, which is where the
      // sibling-prefix bypass would escape (e.g. "/data/contents" vs "/data/contents-evil").
      const root = mkdtempSync(path.join(os.tmpdir(), 'cs-traversal-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        root,
        { disablePrefixHash: true }
      )
      // A sibling directory sharing the root's name prefix: ".../<rootBasename>X".
      const siblingDir = path.join(path.dirname(root), path.basename(root) + 'X')
      const escapingId = path.join('..', path.basename(root) + 'X', 'escaped')

      try {
        await expect(storage.storeStream(escapingId, bufferToStream(Buffer.from('x')))).rejects.toThrow(
          /outside of the root/
        )
        await expect(storage.exist(escapingId)).rejects.toThrow(/outside of the root/)
        expect(await fs.existPath(siblingDir)).toBeFalsy()
      } finally {
        await storage.stop?.()
        rmSync(root, { recursive: true, force: true })
        rmSync(siblingDir, { recursive: true, force: true })
      }
    })

    it(`When an id traverses above the root, then it is rejected`, async () => {
      const root = mkdtempSync(path.join(os.tmpdir(), 'cs-traversal-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        root,
        { disablePrefixHash: true }
      )

      try {
        await expect(storage.storeStream('../../../tmp/escaped', bufferToStream(Buffer.from('x')))).rejects.toThrow(
          /outside of the root/
        )
      } finally {
        await storage.stop?.()
        rmSync(root, { recursive: true, force: true })
      }
    })

    it(`When a normal id is used, then it is stored and retrieved within the root`, async () => {
      const root = mkdtempSync(path.join(os.tmpdir(), 'cs-traversal-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        root,
        { disablePrefixHash: true }
      )

      try {
        await storage.storeStream('normal-id', bufferToStream(Buffer.from('hello')))
        const item = await storage.retrieve('normal-id')
        expect(item).toBeDefined()
        expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('hello'))
        expect(await fs.existPath(path.join(root, 'normal-id'))).toBeTruthy()
      } finally {
        await storage.stop?.()
        rmSync(root, { recursive: true, force: true })
      }
    })
  })
})
