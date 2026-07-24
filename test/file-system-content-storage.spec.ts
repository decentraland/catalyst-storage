import { createHash } from 'crypto'
import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { PassThrough, Readable } from 'stream'
import { gzipSync } from 'zlib'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  IContentStorageComponent,
  IFileSystemComponent
} from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import * as compressionModule from '../src/extras/compression'
import { createLogComponent } from '@well-known-components/logger'

describe('fileSystemContentStorage', () => {
  const fs = createFsComponent()

  // The eviction runs real (threadpool) unlinks that jest's fake timers do not await, so asserting
  // existPath immediately after advanceTimersByTimeAsync races the I/O and flakes under load. Each
  // awaited existPath forces a real event-loop turn (letting pending unlinks complete), and each
  // iteration also advances fake time by another eviction interval so a tick that was missed under
  // load is re-fired rather than waited on forever. Bounded: if the file is never removed, the
  // caller's assertion still fails.
  async function waitUntilRemoved(filePath: string, attempts = 100): Promise<void> {
    for (let i = 0; i < attempts; i++) {
      if (!(await fs.existPath(filePath))) return
      await jest.advanceTimersByTimeAsync(1000)
    }
  }
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

    it(`When start is called more than once, then it does not schedule a second eviction timer`, async () => {
      const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-start-'))
      const storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        tmpDir,
        { decompressCacheEvictionInterval: 30000 }
      )

      try {
        await storage.start?.({} as any)
        const timersAfterFirstStart = jest.getTimerCount()

        await storage.start?.({} as any)

        // The repeated start replaces the timer rather than leaking an extra one.
        expect(jest.getTimerCount()).toBe(timersAfterFirstStart)
      } finally {
        await storage.stop?.()
        rmSync(tmpDir, { recursive: true, force: true })
      }
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

        // Advance past TTL + eviction interval, then wait out the eviction's real unlink I/O
        await jest.advanceTimersByTimeAsync(60000 + 30000)
        await waitUntilRemoved(cachedFilePath)

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

        // Advance past eviction interval, then wait out the eviction's real unlink I/O
        await jest.advanceTimersByTimeAsync(30000)
        await waitUntilRemoved(cachedFilePath1)

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

        // Advance past TTL + eviction interval to evict, then wait out the real unlink I/O
        await jest.advanceTimersByTimeAsync(60000 + 30000)
        await waitUntilRemoved(cachedFilePath)
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

    describe('when a cached path is promoted to primary content before the eviction fires', () => {
      let promotedStorage: IContentStorageComponent
      let promotedRoot: string
      let cachedFilePath: string

      beforeEach(async () => {
        promotedRoot = mkdtempSync(path.join(os.tmpdir(), 'content-storage-promoted-'))
        promotedStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          promotedRoot,
          { decompressCacheTTL: 60000, decompressCacheEvictionInterval: 30000 }
        )
        await promotedStorage.start?.({} as any)
        cachedFilePath = path.join(promotedRoot, '9584', id)
        // Cache the decompressed file, then overwrite the id with plain (incompressible) content:
        // the canonical path now holds primary content, not a re-derivable cache.
        await promotedStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
        await promotedStorage.retrieve(id, { start: 0, end: 9 })
        await promotedStorage.storeStream(id, bufferToStream(content))
        // Fire the eviction well past the TTL — it must not unlink the promoted file.
        await jest.advanceTimersByTimeAsync(60000 + 30000)
        await jest.advanceTimersByTimeAsync(30000)
      })

      afterEach(async () => {
        await promotedStorage.stop?.()
        rmSync(promotedRoot, { recursive: true, force: true })
      })

      it('should not delete the promoted primary file', async () => {
        expect(await fs.existPath(cachedFilePath)).toBe(true)
      })

      it('should keep serving the promoted bytes', async () => {
        const item = await promotedStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })
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
      // The staged temp file used by the decompression should also be gone
      expect(await nodeFs.readdir(path.join(tmpDir, '.tmp-writes'))).toEqual([])
    } finally {
      await storage.stop?.()
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it(`When many range requests race for the same cold gzip, then it is decompressed only once and all return correct data`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-race-'))
    // Derive the shard the same way the storage does, rather than hardcoding the hash prefix.
    const shard = createHash('sha1').update(id).digest('hex').substring(0, 4)
    const cachedFilePath = path.join(tmpDir, shard, id)

    // Wrap the fs component to count how many times the uncompressed cache file lands at its
    // canonical path (decompression stages in the temp dir and renames into place). Without
    // deduplication, concurrent cold-cache range requests each decompress it, renaming the file
    // once per request (and double-counting its size against the cache budget).
    const realFs = createFsComponent()
    let decompressionWrites = 0
    const spyFs: IFileSystemComponent = {
      ...realFs,
      rename: (async (from: any, to: any) => {
        if (to === cachedFilePath) decompressionWrites++
        return realFs.rename!(from, to)
      }) as typeof realFs.rename
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

  it(`When decompressMaxFileSize is unset, then the per-file cap is its own default, independent of decompressCacheMaxSize`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-default-cap-'))
    // Cache budget far below the per-file default (256MB). A 1000-byte file is larger than the
    // cache budget but well under the per-file cap, so it must still decompress — proving the
    // per-file cap is no longer inherited from decompressCacheMaxSize.
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressCacheMaxSize: 500 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
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

  it(`When decompressMaxFileSize is explicitly set, then it overrides the default and caps decompression`, async () => {
    const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-explicit-cap-'))
    // A small explicit per-file cap (50 bytes) still applies even with a large cache budget,
    // confirming the override path is unaffected by the new default.
    const storage = await createFolderBasedFileSystemContentStorage(
      { fs, logs: await createLogComponent({}) },
      tmpDir,
      { decompressCacheMaxSize: 5 * 1024 * 1024 * 1024, decompressMaxFileSize: 50 }
    )
    const cachedFilePath = path.join(tmpDir, '9584', id)

    try {
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

      // Advance past eviction interval, then wait out the eviction's real unlink I/O
      await jest.advanceTimersByTimeAsync(30000)
      await waitUntilRemoved(cachedFilePath2)

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

  describe('atomic storeStream', () => {
    describe('when a store completes successfully', () => {
      beforeEach(async () => {
        await fileSystemContentStorage.storeStream(id, bufferToStream(content))
      })

      it('should leave only the content file in the shard directory, with no temp file', async () => {
        expect(await nodeFs.readdir(path.dirname(filePath))).toEqual([id])
      })
    })

    describe('when the source stream errors mid-write', () => {
      let thrownError: Error | undefined

      beforeEach(async () => {
        thrownError = undefined
        const failingStream = new Readable({
          read() {
            this.destroy(new Error('stream boom'))
          }
        })
        try {
          await fileSystemContentStorage.storeStream(id, failingStream)
        } catch (error: any) {
          thrownError = error
        }
      })

      it('should reject the store', () => {
        expect(thrownError).toBeDefined()
      })

      it('should not create a file at the canonical content path', async () => {
        expect(await fs.existPath(filePath)).toBe(false)
      })

      it('should not leave a temp file behind in the reserved temp directory', async () => {
        const tempDir = path.join(tmpRootDir, '.tmp-writes')
        const entries = (await fs.existPath(tempDir)) ? await nodeFs.readdir(tempDir) : []
        expect(entries).toEqual([])
      })
    })

    describe('when content is stored with compression', () => {
      beforeEach(async () => {
        await fileSystemContentStorage.storeStreamAndCompress(
          id,
          bufferToStream(Buffer.from(new Uint8Array(100).fill(0)))
        )
      })

      it('should place the gzip at its canonical path', async () => {
        expect(await fs.existPath(filePath + '.gzip')).toBe(true)
      })

      it('should leave no staging residue in the reserved temp directory', async () => {
        expect(await nodeFs.readdir(path.join(tmpRootDir, '.tmp-writes'))).toEqual([])
      })
    })

    describe('when a gzip-backed id is overwritten with incompressible content', () => {
      beforeEach(async () => {
        await fileSystemContentStorage.storeStreamAndCompress(
          id,
          bufferToStream(Buffer.from(new Uint8Array(100).fill(0)))
        )
        await fileSystemContentStorage.storeStreamAndCompress(id, bufferToStream(content))
      })

      it('should retrieve the newly stored bytes, not the stale gzip', async () => {
        const item = await fileSystemContentStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should remove the stale canonical gzip', async () => {
        expect(await fs.existPath(filePath + '.gzip')).toBe(false)
      })
    })

    describe('when a gzip-backed id is overwritten with a plain storeStream', () => {
      beforeEach(async () => {
        await fileSystemContentStorage.storeStreamAndCompress(
          id,
          bufferToStream(Buffer.from(new Uint8Array(100).fill(0)))
        )
        await fileSystemContentStorage.storeStream(id, bufferToStream(content))
      })

      it('should serve the newly stored bytes, not the stale gzip', async () => {
        const item = await fileSystemContentStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should remove the stale gzip of the previous version', async () => {
        expect(await fs.existPath(filePath + '.gzip')).toBe(false)
      })
    })

    describe('when a custom tempDirectoryName is configured in flat mode', () => {
      let customStorage: IContentStorageComponent
      let customRoot: string

      beforeEach(async () => {
        customRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-custom-temp-'))
        customStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          customRoot,
          { disablePrefixHash: true, tempDirectoryName: '.staging' }
        )
      })

      afterEach(async () => {
        await customStorage.stop?.()
        rmSync(customRoot, { recursive: true, force: true })
      })

      it('should stage into the custom directory and store content normally', async () => {
        await customStorage.storeStream(id, bufferToStream(content))
        const item = await customStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should reject ids under the custom reserved name', async () => {
        await expect(customStorage.storeStream('.staging/foo', bufferToStream(content))).rejects.toThrow(
          /reserved temp-write/
        )
      })

      it('should leave the default reserved name addressable as a content id', async () => {
        await customStorage.storeStream('.tmp-writes', bufferToStream(content))
        const item = await customStorage.retrieve('.tmp-writes')
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })
    })

    describe('when the compression of a staged store fails', () => {
      let compressSpy: jest.SpyInstance
      let storeOutcome: 'resolved' | Error

      beforeEach(async () => {
        // The compression reads the operation-owned STAGED raw, so a failure means nothing was
        // committed: the store rejects and the previous canonical version stays fully intact.
        await fileSystemContentStorage.storeStream(id, bufferToStream(content))
        compressSpy = jest.spyOn(compressionModule, 'compressContentFile').mockImplementationOnce(async () => {
          throw Object.assign(new Error('EIO: gzip write failed'), { code: 'EIO' })
        })
        storeOutcome = await fileSystemContentStorage
          .storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
          .then(
            () => 'resolved' as const,
            (error: Error) => error
          )
      })

      afterEach(() => {
        compressSpy.mockRestore()
      })

      it('should reject the failed store', () => {
        expect((storeOutcome as Error).message).toContain('EIO')
      })

      it('should keep the previous version intact', async () => {
        const item = await fileSystemContentStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should leave no staging residue', async () => {
        expect(await nodeFs.readdir(path.join(tmpRootDir, '.tmp-writes'))).toEqual([])
      })
    })

    describe('when a flat-mode deployment holds legacy content under the reserved directory', () => {
      let flatRoot: string
      let legacyStagedShapedPath: string
      let constructionError: Error | undefined

      beforeEach(async () => {
        // The directory pre-exists with content BEFORE the storage ever runs, so ownership cannot
        // be proven — even a filename matching the staged shape may be a legacy content id. The
        // factory must refuse to start rather than silently hide (or delete) that content.
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-flat-legacy-'))
        await nodeFs.mkdir(path.join(flatRoot, '.tmp-writes'), { recursive: true })
        legacyStagedShapedPath = path.join(flatRoot, '.tmp-writes', 'deadbeefdeadbeef-0123456789abcdef0123456789abcdef')
        await nodeFs.writeFile(legacyStagedShapedPath, Buffer.from('legacy'))
        constructionError = undefined
        try {
          await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, flatRoot, {
            disablePrefixHash: true
          })
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should refuse to start with an actionable message', () => {
        expect(constructionError?.message).toContain('Refusing to start')
      })

      it('should point the operator at the migration options', () => {
        expect(constructionError?.message).toContain('tempDirectoryName')
      })

      it('should leave the legacy file untouched', async () => {
        expect(await fs.existPath(legacyStagedShapedPath)).toBe(true)
      })
    })

    describe('when construction finds an interrupted raw commit (new raw plus stale gzip)', () => {
      let mixedRoot: string
      let rawPath: string
      let gzipPath: string
      let mixedStorage: IContentStorageComponent

      beforeEach(async () => {
        // A crash between storeStream's rename and its gzip cleanup leaves the new raw next to the
        // previous version's gzip — which non-range reads would prefer. The surviving intent lets
        // construction reconcile in favor of the committed raw.
        mixedRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-mixed-raw-'))
        const shardDir = path.join(mixedRoot, '9584')
        await nodeFs.mkdir(shardDir, { recursive: true })
        rawPath = path.join(shardDir, id)
        gzipPath = rawPath + '.gzip'
        await nodeFs.writeFile(rawPath, content)
        await nodeFs.writeFile(gzipPath, Buffer.from('stale gzip of the previous version'))
        await nodeFs.mkdir(path.join(mixedRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(
          path.join(mixedRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
          Buffer.from(JSON.stringify({ op: 'raw', id, staged: 'deadbeefdeadbeef-00000000000000000000000000000000' }))
        )
        mixedStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          mixedRoot
        )
      })

      afterEach(async () => {
        await mixedStorage.stop?.()
        rmSync(mixedRoot, { recursive: true, force: true })
      })

      it('should remove the stale gzip so reads cannot prefer it', async () => {
        expect(await fs.existPath(gzipPath)).toBe(false)
      })

      it('should serve the committed raw bytes', async () => {
        const item = await mixedStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should discharge the intent', async () => {
        const entries = await nodeFs.readdir(path.join(mixedRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })
    })

    describe('when construction finds an interrupted gzip commit (new gzip plus stale raw)', () => {
      let mixedRoot: string
      let rawPath: string
      let newBytes: Buffer
      let mixedStorage: IContentStorageComponent

      beforeEach(async () => {
        // A crash between storeStreamAndCompress's gzip rename and its raw cleanup leaves the new
        // gzip next to the previous version's raw — which range reads would serve. The surviving
        // intent lets construction reconcile in favor of the committed gzip.
        mixedRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-mixed-gzip-'))
        const shardDir = path.join(mixedRoot, '9584')
        await nodeFs.mkdir(shardDir, { recursive: true })
        rawPath = path.join(shardDir, id)
        newBytes = Buffer.from(new Uint8Array(100).fill(9))
        await nodeFs.writeFile(rawPath, Buffer.from('stale raw of the previous version'))
        await nodeFs.writeFile(rawPath + '.gzip', gzipSync(newBytes))
        await nodeFs.mkdir(path.join(mixedRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(
          path.join(mixedRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
          Buffer.from(JSON.stringify({ op: 'gzip', id, staged: 'deadbeefdeadbeef-00000000000000000000000000000000' }))
        )
        mixedStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          mixedRoot
        )
      })

      afterEach(async () => {
        await mixedStorage.stop?.()
        rmSync(mixedRoot, { recursive: true, force: true })
      })

      it('should remove the stale raw so range reads cannot serve it', async () => {
        expect(await fs.existPath(rawPath)).toBe(false)
      })

      it('should serve the committed gzip bytes', async () => {
        const item = await mixedStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(newBytes)
      })
    })

    describe('when the counterpart cleanup fails after a committed rename', () => {
      let failRoot: string
      let gzipPath: string
      let intentPath: string
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // The rename lands but the stale gzip cannot be unlinked: the store must fail loudly (this
        // process would keep serving the stale gzip) and the intent must survive so the next
        // construction repairs the mixed state.
        failRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-unlink-fail-'))
        const shardDir = path.join(failRoot, '9584')
        gzipPath = path.join(shardDir, id) + '.gzip'
        intentPath = path.join(failRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let failuresLeft = 1
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (String(target) === gzipPath && failuresLeft-- > 0) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          failRoot
        )
        await failingStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(failRoot, { recursive: true, force: true })
      })

      it('should fail the store instead of resolving while the stale gzip is still preferred', () => {
        expect((storeOutcome as Error).message).toContain('failed to remove its previous gzip representation')
      })

      it('should keep the intent as the recovery signal', async () => {
        expect(await fs.existPath(intentPath)).toBe(true)
      })

      it('should repair the mixed state at the next construction', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          failRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(false)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content)
        } finally {
          await repaired.stop?.()
        }
      })

      it('should repair on a retried store in the same process before committing', async () => {
        // The retry finds the pending intent, applies the repair first (the injected failure is
        // exhausted), and only then commits — the recovery signal is never overwritten unapplied.
        await failingStorage.storeStream(id, bufferToStream(content))
        expect(await fs.existPath(gzipPath)).toBe(false)
        expect(await fs.existPath(intentPath)).toBe(false)
        const item = await failingStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })
    })

    describe('when the commit rename fails after the intent is written', () => {
      let renameFailRoot: string
      let gzipPath: string
      let originalBytes: Buffer
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // The intent is journaled, then the rename itself fails: the commit never happened, so the
        // intent must not survive — a later repair would otherwise treat the failed commit as
        // successful and delete the valid gzip primary in favor of its own decompressed raw cache.
        renameFailRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-rename-fail-'))
        const rawPath = path.join(renameFailRoot, '9584', id)
        gzipPath = rawPath + '.gzip'
        const realFs = createFsComponent()
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          rename: (async (from: any, to: any) => {
            if (armed && String(to) === rawPath) {
              armed = false
              throw Object.assign(new Error('EIO: rename failed'), { code: 'EIO' })
            }
            return realFs.rename!(from, to)
          }) as typeof realFs.rename
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          renameFailRoot
        )
        originalBytes = Buffer.from(new Uint8Array(100).fill(5))
        await failingStorage.storeStreamAndCompress(id, bufferToStream(originalBytes))
        // Materialize the decompressed raw cache so BOTH representations legitimately exist.
        await failingStorage.retrieve(id, { start: 0, end: 9 })
        armed = true
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(renameFailRoot, { recursive: true, force: true })
      })

      it('should reject the failed store', () => {
        expect((storeOutcome as Error).message).toContain('EIO')
      })

      it('should clear the intent so the failed commit can never be applied', async () => {
        const entries = await nodeFs.readdir(path.join(renameFailRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })

      it('should keep serving the original version after a restart', async () => {
        const restarted = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          renameFailRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(true)
          const item = await restarted.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        } finally {
          await restarted.stop?.()
        }
      })
    })

    describe('when the commit rename fails and the intent cannot be cleared', () => {
      let doubleFailRoot: string
      let gzipPath: string
      let intentPath: string
      let originalBytes: Buffer
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // Double failure: the rename fails AND the pre-rename intent cannot be removed. The staged
        // file is then the only proof the commit never landed — the store's cleanup must preserve
        // it, or the next reconciliation would apply the failed commit and delete the valid gzip.
        doubleFailRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-double-fail-'))
        const rawPath = path.join(doubleFailRoot, '9584', id)
        gzipPath = rawPath + '.gzip'
        intentPath = path.join(doubleFailRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          rename: (async (from: any, to: any) => {
            if (armed && String(to) === rawPath) {
              throw Object.assign(new Error('EIO: rename failed'), { code: 'EIO' })
            }
            return realFs.rename!(from, to)
          }) as typeof realFs.rename,
          unlink: (async (target: any) => {
            if (armed && String(target) === intentPath) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          doubleFailRoot
        )
        originalBytes = Buffer.from(new Uint8Array(100).fill(5))
        await failingStorage.storeStreamAndCompress(id, bufferToStream(originalBytes))
        // Materialize the decompressed raw cache so BOTH representations legitimately exist.
        await failingStorage.retrieve(id, { start: 0, end: 9 })
        armed = true
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
        armed = false
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(doubleFailRoot, { recursive: true, force: true })
      })

      it('should fail the store explaining the proof is preserved', () => {
        expect((storeOutcome as Error).message).toContain('preserved as proof')
      })

      it('should keep the intent journal', async () => {
        expect(await fs.existPath(intentPath)).toBe(true)
      })

      it('should preserve the staged-file proof', async () => {
        const entries = await nodeFs.readdir(path.join(doubleFailRoot, '.tmp-writes'))
        expect(entries.filter((entry) => /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry))).toHaveLength(1)
      })

      it('should keep the valid gzip primary', async () => {
        expect(await fs.existPath(gzipPath)).toBe(true)
      })

      it('should discard the failed commit at the next construction and keep the previous version', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          doubleFailRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(true)
          expect(await fs.existPath(intentPath)).toBe(false)
          const staged = (await nodeFs.readdir(path.join(doubleFailRoot, '.tmp-writes'))).filter((entry) =>
            /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry)
          )
          expect(staged).toEqual([])
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when the intent removal cannot even be verified after a failed rename', () => {
      let unverifiableRoot: string
      let gzipPath: string
      let intentPath: string
      let originalBytes: Buffer
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // rename fails, the intent unlink fails, AND the verification stat throws EACCES: not being
        // able to PROVE the journal is gone must preserve the staged proof exactly like a proven
        // survivor — an untyped EACCES escaping would let the caller cleanup destroy the proof.
        unverifiableRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-unverifiable-'))
        const rawPath = path.join(unverifiableRoot, '9584', id)
        gzipPath = rawPath + '.gzip'
        intentPath = path.join(unverifiableRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          rename: (async (from: any, to: any) => {
            if (armed && String(to) === rawPath) {
              throw Object.assign(new Error('EIO: rename failed'), { code: 'EIO' })
            }
            return realFs.rename!(from, to)
          }) as typeof realFs.rename,
          unlink: (async (target: any) => {
            if (armed && String(target) === intentPath) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink,
          stat: (async (target: any) => {
            if (armed && String(target) === intentPath) {
              // Present but unstat-able: a missing file still reports ENOENT normally.
              await realFs.stat(target)
              throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' })
            }
            return realFs.stat(target)
          }) as typeof realFs.stat
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          unverifiableRoot
        )
        originalBytes = Buffer.from(new Uint8Array(100).fill(5))
        await failingStorage.storeStreamAndCompress(id, bufferToStream(originalBytes))
        await failingStorage.retrieve(id, { start: 0, end: 9 })
        armed = true
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
        armed = false
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(unverifiableRoot, { recursive: true, force: true })
      })

      it('should fail the store explaining the journal removal could not be proven', () => {
        expect((storeOutcome as Error).message).toContain('failed to prove its intent journal was removed')
      })

      it('should preserve the staged-file proof', async () => {
        const entries = await nodeFs.readdir(path.join(unverifiableRoot, '.tmp-writes'))
        expect(entries.filter((entry) => /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry))).toHaveLength(1)
      })

      it('should not apply the failed commit at the next construction', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          unverifiableRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(true)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when a compressed store hits the double failure on its gzip commit', () => {
      let compressedFailRoot: string
      let rawPath: string
      let intentPath: string
      let originalBytes: Buffer
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // Same double failure through storeStreamAndCompress's gzip commit: its own staging cleanup
        // must preserve the staged gzip named by the typed error while removing the raw residue.
        compressedFailRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-compress-double-'))
        rawPath = path.join(compressedFailRoot, '9584', id)
        const gzipPath = rawPath + '.gzip'
        intentPath = path.join(compressedFailRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          rename: (async (from: any, to: any) => {
            if (armed && String(to) === gzipPath) {
              throw Object.assign(new Error('EIO: rename failed'), { code: 'EIO' })
            }
            return realFs.rename!(from, to)
          }) as typeof realFs.rename,
          unlink: (async (target: any) => {
            if (armed && String(target) === intentPath) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          compressedFailRoot
        )
        originalBytes = Buffer.from('raw primary content')
        await failingStorage.storeStream(id, bufferToStream(originalBytes))
        armed = true
        storeOutcome = await failingStorage
          .storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(9))))
          .then(
            () => 'resolved' as const,
            (error: Error) => error
          )
        armed = false
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(compressedFailRoot, { recursive: true, force: true })
      })

      it('should fail the store explaining the proof is preserved', () => {
        expect((storeOutcome as Error).message).toContain('preserved')
      })

      it('should preserve exactly the staged gzip proof', async () => {
        const entries = await nodeFs.readdir(path.join(compressedFailRoot, '.tmp-writes'))
        expect(entries.filter((entry) => /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry))).toHaveLength(1)
      })

      it('should keep the raw primary and heal at the next construction', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          compressedFailRoot
        )
        try {
          expect(await fs.existPath(intentPath)).toBe(false)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when an incompressible store hits the double failure on its raw commit', () => {
      let incompressibleFailRoot: string
      let gzipPath: string
      let intentPath: string
      let originalBytes: Buffer
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // Same double failure through storeStreamAndCompress's raw (not-beneficial) commit.
        incompressibleFailRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-incompress-double-'))
        const rawPath = path.join(incompressibleFailRoot, '9584', id)
        gzipPath = rawPath + '.gzip'
        intentPath = path.join(incompressibleFailRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          rename: (async (from: any, to: any) => {
            if (armed && String(to) === rawPath) {
              throw Object.assign(new Error('EIO: rename failed'), { code: 'EIO' })
            }
            return realFs.rename!(from, to)
          }) as typeof realFs.rename,
          unlink: (async (target: any) => {
            if (armed && String(target) === intentPath) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          incompressibleFailRoot
        )
        originalBytes = Buffer.from(new Uint8Array(100).fill(5))
        await failingStorage.storeStreamAndCompress(id, bufferToStream(originalBytes))
        armed = true
        storeOutcome = await failingStorage.storeStreamAndCompress(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
        armed = false
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(incompressibleFailRoot, { recursive: true, force: true })
      })

      it('should fail the store explaining the proof is preserved', () => {
        expect((storeOutcome as Error).message).toContain('preserved')
      })

      it('should preserve exactly the staged raw proof', async () => {
        const entries = await nodeFs.readdir(path.join(incompressibleFailRoot, '.tmp-writes'))
        expect(entries.filter((entry) => /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry))).toHaveLength(1)
      })

      it('should keep the gzip primary and heal at the next construction', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          incompressibleFailRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(true)
          expect(await fs.existPath(intentPath)).toBe(false)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when an invalidated decompression fails to read the replaced gzip', () => {
      let overwriteStorage: IContentStorageComponent
      let newContent: Buffer
      let rangeResult: Buffer | undefined

      beforeEach(async () => {
        // The overwrite invalidates the in-flight decompression AND its source stream then errors:
        // the failure belongs to the replaced gzip, so the range request must retry against the new
        // representation instead of surfacing a spurious undefined.
        const realFs = createFsComponent()
        const gzipPath = filePath + '.gzip'
        let gatedStream: PassThrough | undefined
        let gzipReadStarted: () => void = () => undefined
        const gzipReadStartedPromise = new Promise<void>((res) => (gzipReadStarted = res))
        let holdNextGzipRead = true
        const gatedFs: IFileSystemComponent = {
          ...realFs,
          createReadStream: ((target: any, opts?: any) => {
            if (String(target) !== gzipPath || !holdNextGzipRead) return realFs.createReadStream(target, opts)
            holdNextGzipRead = false
            gatedStream = new PassThrough()
            gzipReadStarted()
            return gatedStream
          }) as typeof realFs.createReadStream
        }
        overwriteStorage = await createFolderBasedFileSystemContentStorage(
          { fs: gatedFs, logs: await createLogComponent({}) },
          tmpRootDir
        )
        newContent = Buffer.from(new Uint8Array(100).fill(9))
        await overwriteStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(7))))
        const rangePromise = overwriteStorage.retrieve(id, { start: 0, end: 2 })
        await gzipReadStartedPromise
        await overwriteStorage.storeStreamAndCompress(id, bufferToStream(newContent))
        // End the gated stream truncated: the gunzip inside the decompression pipeline fails with a
        // genuine read error for the replaced gzip.
        gatedStream!.end()
        const item = await rangePromise
        rangeResult = item ? await streamToBuffer(await item.asStream()) : undefined
      })

      afterEach(async () => {
        await overwriteStorage.stop?.()
      })

      it('should serve the range from the new content via the retry', () => {
        expect(rangeResult).toEqual(newContent.subarray(0, 3))
      })
    })

    describe('when discarding an uncommitted intent fails', () => {
      let crashRoot: string
      let gzipPath: string
      let stagedPath: string
      let intentPath: string
      let originalBytes: Buffer
      let constructionError: Error | undefined

      beforeEach(async () => {
        // Pre-rename state (gzip primary + raw cache + staged + intent) where the intent journal
        // cannot be removed: construction must refuse WITHOUT destroying the staged-file proof —
        // deleting the proof first would let the next construction reinterpret the surviving
        // pre-rename intent as a committed transition and delete the valid gzip.
        crashRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-discard-fail-'))
        originalBytes = Buffer.from(new Uint8Array(100).fill(5))
        const preparer = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          crashRoot
        )
        await preparer.storeStreamAndCompress(id, bufferToStream(originalBytes))
        await preparer.retrieve(id, { start: 0, end: 9 })
        await preparer.stop?.()
        gzipPath = path.join(crashRoot, '9584', id) + '.gzip'
        const stagedName = `deadbeefdeadbeef-${'a'.repeat(32)}`
        stagedPath = path.join(crashRoot, '.tmp-writes', stagedName)
        intentPath = path.join(crashRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        await nodeFs.writeFile(stagedPath, Buffer.from('never committed'))
        await nodeFs.writeFile(intentPath, Buffer.from(JSON.stringify({ op: 'raw', id, staged: stagedName })))
        const realFs = createFsComponent()
        let failuresLeft = 1
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (String(target) === intentPath && failuresLeft-- > 0) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        constructionError = undefined
        try {
          await createFolderBasedFileSystemContentStorage(
            { fs: failingFs, logs: await createLogComponent({}) },
            crashRoot
          )
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        rmSync(crashRoot, { recursive: true, force: true })
      })

      it('should refuse to construct', () => {
        expect(constructionError?.message).toContain('Cannot discard the uncommitted')
      })

      it('should preserve the staged-file proof', async () => {
        expect(await fs.existPath(stagedPath)).toBe(true)
      })

      it('should keep the valid gzip primary', async () => {
        expect(await fs.existPath(gzipPath)).toBe(true)
      })

      it('should discard both artifacts and keep the previous version at the following construction', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          crashRoot
        )
        try {
          expect(await fs.existPath(stagedPath)).toBe(false)
          expect(await fs.existPath(intentPath)).toBe(false)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when a failed cleanup leaves the counterpart unreadable', () => {
      let unreadableRoot: string
      let gzipPath: string
      let intentPath: string
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // The unlink fails AND leaves the stale gzip unreadable (mode/ACL damage): an access-based
        // existence check would read it as absent and falsely consider the cleanup complete,
        // discharging the journal — the mixed state would resurface later with no repair signal.
        // The invariant check must treat only ENOENT/ENOTDIR as absent and fail loudly here.
        unreadableRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-unreadable-'))
        gzipPath = path.join(unreadableRoot, '9584', id) + '.gzip'
        intentPath = path.join(unreadableRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let gzipUnreadable = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (String(target) === gzipPath && !gzipUnreadable) {
              gzipUnreadable = true
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink,
          stat: (async (target: any) => {
            if (String(target) === gzipPath && gzipUnreadable) {
              throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' })
            }
            return realFs.stat(target)
          }) as typeof realFs.stat
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          unreadableRoot
        )
        await failingStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(unreadableRoot, { recursive: true, force: true })
      })

      it('should fail the store instead of falsely considering the cleanup complete', () => {
        expect((storeOutcome as Error).message).toContain('EACCES')
      })

      it('should keep the intent as the repair signal', async () => {
        expect(await fs.existPath(intentPath)).toBe(true)
      })

      it('should repair once the counterpart is readable again', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          unreadableRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(false)
          expect(await fs.existPath(intentPath)).toBe(false)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when an id with a pending intent is deleted', () => {
      let deleteRoot: string
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // A committed transition whose counterpart cleanup failed leaves a pending intent; the
        // caller then deletes the id on purpose. The delete must repair and discharge the journal —
        // an orphaned intent whose id has neither a staged file nor any representation would refuse
        // the next construction.
        deleteRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-delete-intent-'))
        const gzipPath = path.join(deleteRoot, '9584', id) + '.gzip'
        const realFs = createFsComponent()
        let failuresLeft = 1
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (String(target) === gzipPath && failuresLeft-- > 0) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          deleteRoot
        )
        await failingStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
        await failingStorage.delete([id])
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(deleteRoot, { recursive: true, force: true })
      })

      it('should have failed the store that left the pending intent', () => {
        expect((storeOutcome as Error).message).toContain('failed to remove its previous gzip representation')
      })

      it('should delete the id despite the pending intent', async () => {
        expect(await failingStorage.exist(id)).toBe(false)
      })

      it('should leave no intent journal behind', async () => {
        const entries = await nodeFs.readdir(path.join(deleteRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })

      it('should construct cleanly afterwards', async () => {
        const restarted = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          deleteRoot
        )
        try {
          expect(await restarted.exist(id)).toBe(false)
        } finally {
          await restarted.stop?.()
        }
      })
    })

    describe('when reconciliation cannot remove the stale counterpart', () => {
      let mixedRoot: string
      let gzipPath: string
      let intentPath: string
      let firstStorage: IContentStorageComponent | undefined
      let constructionError: Error | undefined

      beforeEach(async () => {
        // The reconciliation's own cleanup fails transiently: the intent — the only recovery
        // signal — must be kept so the next construction can retry, instead of being discharged
        // over a still-mixed state.
        mixedRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-reconcile-fail-'))
        const shardDir = path.join(mixedRoot, '9584')
        await nodeFs.mkdir(shardDir, { recursive: true })
        const rawPath = path.join(shardDir, id)
        gzipPath = rawPath + '.gzip'
        await nodeFs.writeFile(rawPath, content)
        await nodeFs.writeFile(gzipPath, Buffer.from('stale gzip of the previous version'))
        await nodeFs.mkdir(path.join(mixedRoot, '.tmp-writes'), { recursive: true })
        intentPath = path.join(mixedRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        await nodeFs.writeFile(
          intentPath,
          Buffer.from(JSON.stringify({ op: 'raw', id, staged: 'deadbeefdeadbeef-00000000000000000000000000000000' }))
        )
        const realFs = createFsComponent()
        let failuresLeft = 1
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (String(target) === gzipPath && failuresLeft-- > 0) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        constructionError = undefined
        try {
          firstStorage = await createFolderBasedFileSystemContentStorage(
            { fs: failingFs, logs: await createLogComponent({}) },
            mixedRoot
          )
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        await firstStorage?.stop?.()
        rmSync(mixedRoot, { recursive: true, force: true })
      })

      it('should refuse to construct over the unreconcilable mixed state', () => {
        expect(constructionError?.message).toContain('Refusing to start')
      })

      it('should keep the intent when its cleanup fails', async () => {
        expect(await fs.existPath(intentPath)).toBe(true)
      })

      it('should repair the mixed state at the following construction', async () => {
        const repaired = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          mixedRoot
        )
        try {
          expect(await fs.existPath(gzipPath)).toBe(false)
          expect(await fs.existPath(intentPath)).toBe(false)
          const item = await repaired.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content)
        } finally {
          await repaired.stop?.()
        }
      })
    })

    describe('when a no-rename flat-mode adapter uses the reserved namespace as content', () => {
      let legacyRoot: string
      let legacyStorage: IContentStorageComponent

      beforeEach(async () => {
        // Without rename there is no staging, no sweep and no reconciliation, so the reserved
        // namespace is neither created nor enforced: a legacy no-rename deployment that stored ids
        // under the default reserved name keeps working unchanged.
        legacyRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-norename-flat-'))
        await nodeFs.mkdir(path.join(legacyRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(path.join(legacyRoot, '.tmp-writes', 'foo'), Buffer.from('legacy bytes'))
        const fsWithoutRename: IFileSystemComponent = { ...createFsComponent(), rename: undefined }
        legacyStorage = await createFolderBasedFileSystemContentStorage(
          { fs: fsWithoutRename, logs: await createLogComponent({}) },
          legacyRoot,
          { disablePrefixHash: true }
        )
      })

      afterEach(async () => {
        await legacyStorage.stop?.()
        rmSync(legacyRoot, { recursive: true, force: true })
      })

      it('should construct despite the pre-existing content under the reserved name', async () => {
        expect(await legacyStorage.exist('.tmp-writes/foo')).toBe(true)
      })

      it('should retrieve the pre-existing id under the reserved name', async () => {
        const item = await legacyStorage.retrieve('.tmp-writes/foo')
        expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('legacy bytes'))
      })

      it('should store new ids under the reserved name', async () => {
        await legacyStorage.storeStream('.tmp-writes/bar', bufferToStream(content))
        const item = await legacyStorage.retrieve('.tmp-writes/bar')
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })
    })

    describe('when construction finds an intent whose commit and cleanup both completed', () => {
      let consistentRoot: string
      let newBytes: Buffer
      let consistentStorage: IContentStorageComponent

      beforeEach(async () => {
        // A crash after the rename AND the counterpart cleanup, but before the intent discharge:
        // the state is already consistent (raw only) and must be left exactly as it is.
        consistentRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-consistent-'))
        const shardDir = path.join(consistentRoot, '9584')
        await nodeFs.mkdir(shardDir, { recursive: true })
        newBytes = Buffer.from(new Uint8Array(100).fill(3))
        await nodeFs.writeFile(path.join(shardDir, id), newBytes)
        await nodeFs.mkdir(path.join(consistentRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(
          path.join(consistentRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
          Buffer.from(JSON.stringify({ op: 'raw', id, staged: 'deadbeefdeadbeef-00000000000000000000000000000000' }))
        )
        consistentStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          consistentRoot
        )
      })

      afterEach(async () => {
        await consistentStorage.stop?.()
        rmSync(consistentRoot, { recursive: true, force: true })
      })

      it('should keep the committed version untouched', async () => {
        const item = await consistentStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(newBytes)
      })

      it('should discharge the intent', async () => {
        const entries = await nodeFs.readdir(path.join(consistentRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })
    })

    describe('when a commit crashed after writing the intent but before the rename', () => {
      let crashRoot: string
      let gzipPath: string
      let stagedPath: string
      let originalBytes: Buffer
      let restartedStorage: IContentStorageComponent

      beforeEach(async () => {
        // The exact hazard: a gzip primary alongside its own decompressed raw cache (both exist,
        // legitimately), plus a pre-rename intent and its staged file. The intent must NOT be
        // applied as a completed commit — the staged file proves the rename never happened, so the
        // valid gzip primary stays and only the staged file and intent are discarded.
        crashRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-crash-pre-rename-'))
        originalBytes = Buffer.from(new Uint8Array(100).fill(5))
        const preparer = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          crashRoot
        )
        await preparer.storeStreamAndCompress(id, bufferToStream(originalBytes))
        // Materialize the decompressed raw cache so BOTH representations legitimately exist.
        await preparer.retrieve(id, { start: 0, end: 9 })
        await preparer.stop?.()
        gzipPath = path.join(crashRoot, '9584', id) + '.gzip'
        const stagedName = `deadbeefdeadbeef-${'a'.repeat(32)}`
        stagedPath = path.join(crashRoot, '.tmp-writes', stagedName)
        await nodeFs.writeFile(stagedPath, Buffer.from('the new raw bytes that never committed'))
        await nodeFs.writeFile(
          path.join(crashRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
          Buffer.from(JSON.stringify({ op: 'raw', id, staged: stagedName }))
        )
        restartedStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          crashRoot
        )
      })

      afterEach(async () => {
        await restartedStorage.stop?.()
        rmSync(crashRoot, { recursive: true, force: true })
      })

      it('should keep the valid gzip primary', async () => {
        expect(await fs.existPath(gzipPath)).toBe(true)
      })

      it('should keep serving the previous version', async () => {
        const item = await restartedStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
      })

      it('should discard the staged file of the uncommitted transition', async () => {
        expect(await fs.existPath(stagedPath)).toBe(false)
      })

      it('should discharge the intent', async () => {
        const entries = await nodeFs.readdir(path.join(crashRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })
    })

    describe('when a flat-mode deployment holds a legacy content file exactly at the reserved path', () => {
      let flatRoot: string
      let constructionError: Error | undefined

      beforeEach(async () => {
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-reserved-file-'))
        await nodeFs.writeFile(path.join(flatRoot, '.tmp-writes'), Buffer.from('legacy content id'))
        constructionError = undefined
        try {
          await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, flatRoot, {
            disablePrefixHash: true
          })
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should refuse to start with the migration guidance instead of a low-level mkdir error', () => {
        expect(constructionError?.message).toContain('exists as a file')
      })

      it('should point the operator at the tempDirectoryName option', () => {
        expect(constructionError?.message).toContain('tempDirectoryName')
      })

      it('should leave the legacy file untouched', async () => {
        expect(await nodeFs.readFile(path.join(flatRoot, '.tmp-writes'), 'utf8')).toBe('legacy content id')
      })
    })

    describe('when a flat-mode deployment holds a legacy content id at the marker path', () => {
      let flatRoot: string
      let markerSpoofPath: string
      let constructionError: Error | undefined

      beforeEach(async () => {
        // Before the reservation, '.tmp-writes/.owned-by-catalyst-storage' was itself a valid
        // flat-mode content id. Its existence alone must not be taken as proof of ownership.
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-marker-spoof-'))
        await nodeFs.mkdir(path.join(flatRoot, '.tmp-writes'), { recursive: true })
        markerSpoofPath = path.join(flatRoot, '.tmp-writes', '.owned-by-catalyst-storage')
        await nodeFs.writeFile(markerSpoofPath, Buffer.from('legacy content stored under the marker path'))
        constructionError = undefined
        try {
          await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, flatRoot, {
            disablePrefixHash: true
          })
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should refuse to start because the marker content is not the one this storage writes', () => {
        expect(constructionError?.message).toContain('may be a pre-existing content id')
      })

      it('should leave the legacy file untouched', async () => {
        expect(await nodeFs.readFile(markerSpoofPath, 'utf8')).toBe('legacy content stored under the marker path')
      })
    })

    describe('when a claimed flat-mode temp directory contains a file this storage did not create', () => {
      let flatRoot: string
      let foreignPath: string
      let constructionError: Error | undefined

      beforeEach(async () => {
        // A valid marker alongside an unrecognized sibling is not proof of ownership either: the
        // sibling may be a legacy content id that predates the (possibly spoofed) marker.
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-foreign-sibling-'))
        await nodeFs.mkdir(path.join(flatRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(
          path.join(flatRoot, '.tmp-writes', '.owned-by-catalyst-storage'),
          Buffer.from('reserved by catalyst-storage for atomic write staging\n')
        )
        foreignPath = path.join(flatRoot, '.tmp-writes', 'legacy-content-id')
        await nodeFs.writeFile(foreignPath, Buffer.from('precious'))
        constructionError = undefined
        try {
          await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, flatRoot, {
            disablePrefixHash: true
          })
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should refuse to start because of the unrecognized sibling', () => {
        expect(constructionError?.message).toContain('did not create')
      })

      it('should leave the unrecognized file untouched', async () => {
        expect(await nodeFs.readFile(foreignPath, 'utf8')).toBe('precious')
      })
    })

    describe('when a flat-mode root has a previously claimed temp directory', () => {
      let flatRoot: string
      let orphanPath: string
      let flatStorage: IContentStorageComponent

      beforeEach(async () => {
        // The ownership marker from an earlier run proves the directory is ours, so construction
        // succeeds and the sweep may remove foreign staged-shape leftovers.
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-flat-owned-'))
        await nodeFs.mkdir(path.join(flatRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(
          path.join(flatRoot, '.tmp-writes', '.owned-by-catalyst-storage'),
          Buffer.from('reserved by catalyst-storage for atomic write staging\n')
        )
        orphanPath = path.join(flatRoot, '.tmp-writes', 'deadbeefdeadbeef-0123456789abcdef0123456789abcdef')
        await nodeFs.writeFile(orphanPath, Buffer.from(''))
        flatStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          flatRoot,
          { disablePrefixHash: true }
        )
        await flatStorage.start?.({} as any)
        await flatStorage.stop?.()
      })

      afterEach(async () => {
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should sweep the foreign staged-shape leftover', async () => {
        expect(await fs.existPath(orphanPath)).toBe(false)
      })
    })

    describe('when the fs component lacks rename at construction', () => {
      let warnings: string[]

      beforeEach(async () => {
        warnings = []
        const recordingLogs = {
          getLogger: () => ({
            log: () => undefined,
            debug: () => undefined,
            info: () => undefined,
            warn: (message: string) => {
              warnings.push(message)
            },
            error: () => undefined
          })
        }
        const fsWithoutRename: IFileSystemComponent = { ...createFsComponent(), rename: undefined }
        const storage = await createFolderBasedFileSystemContentStorage(
          { fs: fsWithoutRename, logs: recordingLogs as any },
          tmpRootDir
        )
        await storage.stop?.()
      })

      it('should warn that writes are not crash-atomic', () => {
        expect(warnings.some((message) => message.includes('NOT be crash-atomic'))).toBe(true)
      })
    })

    describe('when a flat-mode temp directory is claimed while empty', () => {
      let flatRoot: string
      let orphanPath: string
      let flatStorage: IContentStorageComponent

      beforeEach(async () => {
        // The factory finds (creates) an empty reserved directory, claims it with the ownership
        // marker, and from then on the sweep may remove foreign staged-shape leftovers.
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-flat-claimed-'))
        flatStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          flatRoot,
          { disablePrefixHash: true }
        )
        orphanPath = path.join(flatRoot, '.tmp-writes', 'deadbeefdeadbeef-0123456789abcdef0123456789abcdef')
        await nodeFs.writeFile(orphanPath, Buffer.from(''))
        await flatStorage.start?.({} as any)
        await flatStorage.stop?.()
      })

      afterEach(async () => {
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should write the ownership marker', async () => {
        expect(await fs.existPath(path.join(flatRoot, '.tmp-writes', '.owned-by-catalyst-storage'))).toBe(true)
      })

      it('should sweep the foreign staged-shape leftover', async () => {
        expect(await fs.existPath(orphanPath)).toBe(false)
      })
    })

    describe('when the tempDirectoryName looks like a shard directory in hash-prefix mode', () => {
      it('should reject creating the storage', async () => {
        const badRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-shard-temp-'))
        try {
          await expect(
            createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, badRoot, {
              disablePrefixHash: false,
              tempDirectoryName: 'abcd'
            })
          ).rejects.toThrow(/shard directory/)
        } finally {
          rmSync(badRoot, { recursive: true, force: true })
        }
      })
    })

    describe('when the reserved temp path is a symbolic link', () => {
      let symlinkRoot: string
      let outsideDir: string
      let constructionError: Error | undefined

      beforeEach(async () => {
        // stat() follows symlinks, so without an lstat check a symlinked reserved path would pass
        // the directory check and route staged writes and the sweep outside the storage root.
        symlinkRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-symlink-root-'))
        outsideDir = mkdtempSync(path.join(os.tmpdir(), 'cs-symlink-target-'))
        await nodeFs.symlink(outsideDir, path.join(symlinkRoot, '.tmp-writes'))
        constructionError = undefined
        try {
          await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, symlinkRoot)
        } catch (error: any) {
          constructionError = error
        }
      })

      afterEach(async () => {
        rmSync(symlinkRoot, { recursive: true, force: true })
        rmSync(outsideDir, { recursive: true, force: true })
      })

      it('should refuse to start', () => {
        expect(constructionError?.message).toContain('is a symbolic link')
      })

      it('should not write anything through the symlink', async () => {
        expect(await nodeFs.readdir(outsideDir)).toEqual([])
      })
    })

    describe('when an invalid configuration is rejected', () => {
      it('should fail before any filesystem mutation', async () => {
        const parent = mkdtempSync(path.join(os.tmpdir(), 'cs-no-side-effects-'))
        const root = path.join(parent, 'never-created-root')
        try {
          await expect(
            createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
              disablePrefixHash: false,
              decompressCacheTTL: 0
            })
          ).rejects.toThrow(/positive safe integer/)
          expect(await fs.existPath(root)).toBe(false)
        } finally {
          rmSync(parent, { recursive: true, force: true })
        }
      })
    })

    describe('when a numeric cache option is not a positive safe integer', () => {
      it('should reject a NaN decompressMaxFileSize', async () => {
        const badRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-bad-cap-'))
        try {
          await expect(
            createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, badRoot, {
              disablePrefixHash: false,
              decompressMaxFileSize: Number.NaN
            })
          ).rejects.toThrow(/decompressMaxFileSize must be a positive safe integer/)
        } finally {
          rmSync(badRoot, { recursive: true, force: true })
        }
      })

      it('should reject a non-positive decompressCacheTTL', async () => {
        const badRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-bad-ttl-'))
        try {
          await expect(
            createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, badRoot, {
              disablePrefixHash: false,
              decompressCacheTTL: 0
            })
          ).rejects.toThrow(/decompressCacheTTL must be a positive safe integer/)
        } finally {
          rmSync(badRoot, { recursive: true, force: true })
        }
      })

      it('should reject an Infinity decompressCacheEvictionInterval', async () => {
        const badRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-bad-interval-'))
        try {
          await expect(
            createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, badRoot, {
              disablePrefixHash: false,
              decompressCacheEvictionInterval: Number.POSITIVE_INFINITY
            })
          ).rejects.toThrow(/decompressCacheEvictionInterval must be a positive safe integer/)
        } finally {
          rmSync(badRoot, { recursive: true, force: true })
        }
      })
    })

    describe('when the tempDirectoryName is not a single path segment', () => {
      it('should reject creating the storage', async () => {
        const badRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-bad-temp-'))
        try {
          await expect(
            createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, badRoot, {
              disablePrefixHash: false,
              tempDirectoryName: 'a/b'
            })
          ).rejects.toThrow(/single path segment/)
        } finally {
          rmSync(badRoot, { recursive: true, force: true })
        }
      })
    })

    describe('when the startup sweep runs with a file staged by this boot present', () => {
      let ownPrefixedPath: string
      let foreignPath: string
      let sweptStorage: IContentStorageComponent

      beforeEach(async () => {
        const realFs = createFsComponent()
        const tempDirPath = path.join(tmpRootDir, '.tmp-writes')
        const stagedNames: string[] = []
        const spyFs: IFileSystemComponent = {
          ...realFs,
          createWriteStream: ((target: any, options?: any) => {
            if (path.dirname(String(target)) === tempDirPath) stagedNames.push(path.basename(String(target)))
            return realFs.createWriteStream(target, options)
          }) as typeof realFs.createWriteStream
        }
        sweptStorage = await createFolderBasedFileSystemContentStorage(
          { fs: spyFs, logs: await createLogComponent({}) },
          tmpRootDir
        )
        // Learn this boot's staging prefix from a real store, then plant one file carrying it (as
        // if a write were in flight) and one leftover from a previous run.
        await sweptStorage.storeStream(id, bufferToStream(content))
        const bootPrefix = stagedNames[0].split('-')[0]
        // Both carry the staged-name shape, so only the boot prefix decides their fate.
        ownPrefixedPath = path.join(tempDirPath, `${bootPrefix}-${'a'.repeat(32)}`)
        foreignPath = path.join(tempDirPath, `deadbeefdeadbeef-${'b'.repeat(32)}`)
        await nodeFs.writeFile(ownPrefixedPath, Buffer.from(''))
        await nodeFs.writeFile(foreignPath, Buffer.from(''))
        await sweptStorage.start?.({} as any)
        // stop() awaits the background sweep, so it has completed by the time we assert.
        await sweptStorage.stop?.()
      })

      it('should keep the file staged by this boot', async () => {
        expect(await fs.existPath(ownPrefixedPath)).toBe(true)
      })

      it('should remove the leftover from a previous run', async () => {
        expect(await fs.existPath(foreignPath)).toBe(false)
      })
    })

    describe('when a file that does not match the staged-name shape sits in the reserved directory', () => {
      let legacyFilePath: string

      beforeEach(async () => {
        // In flat (disablePrefixHash) mode a deployment that predates the reservation may hold
        // legitimate content under `.tmp-writes/` — the sweep must never delete unrecognized files.
        legacyFilePath = path.join(tmpRootDir, '.tmp-writes', 'legacy-content-file')
        await nodeFs.writeFile(legacyFilePath, Buffer.from('precious'))
        await fileSystemContentStorage.start?.({} as any)
        await fileSystemContentStorage.stop?.()
      })

      it('should leave the unrecognized file untouched', async () => {
        expect(await fs.existPath(legacyFilePath)).toBe(true)
      })
    })

    describe('when content is overwritten while a range decompression is in flight', () => {
      let overwriteStorage: IContentStorageComponent
      let gzipBackedPath: string
      let rangeResult: Buffer | undefined

      beforeEach(async () => {
        const realFs = createFsComponent()
        gzipBackedPath = filePath
        const gzipPath = filePath + '.gzip'
        // Gate the FIRST read of the canonical gzip so the decompression stays in flight while the
        // id is overwritten underneath it.
        let releaseGzipRead: () => void = () => undefined
        const gzipReadGate = new Promise<void>((res) => (releaseGzipRead = res))
        let gzipReadStarted: () => void = () => undefined
        const gzipReadStartedPromise = new Promise<void>((res) => (gzipReadStarted = res))
        let holdNextGzipRead = true
        const gatedFs: IFileSystemComponent = {
          ...realFs,
          createReadStream: ((target: any, opts?: any) => {
            const real = realFs.createReadStream(target, opts)
            if (String(target) !== gzipPath || !holdNextGzipRead) return real
            holdNextGzipRead = false
            const gated = new PassThrough()
            gzipReadStarted()
            void gzipReadGate.then(() => real.pipe(gated))
            return gated
          }) as typeof realFs.createReadStream
        }
        overwriteStorage = await createFolderBasedFileSystemContentStorage(
          { fs: gatedFs, logs: await createLogComponent({}) },
          tmpRootDir
        )
        // Compressible content: stored gzip-only, so a range request must decompress.
        await overwriteStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(7))))
        // Start the range request; its decompression blocks on the gated gzip read.
        const rangePromise = overwriteStorage.retrieve(id, { start: 0, end: 2 })
        await gzipReadStartedPromise
        // Overwrite the id with incompressible content while the old gzip is still inflating.
        await overwriteStorage.storeStreamAndCompress(id, bufferToStream(content))
        releaseGzipRead()
        const item = await rangePromise
        rangeResult = item ? await streamToBuffer(await item.asStream()) : undefined
      })

      afterEach(async () => {
        await overwriteStorage.stop?.()
      })

      it('should keep the new content at the canonical path instead of the stale inflated bytes', async () => {
        expect(await nodeFs.readFile(gzipBackedPath)).toEqual(content)
      })

      it('should serve the whole new content on a later retrieve', async () => {
        const item = await overwriteStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should serve the range from the new content', () => {
        expect(rangeResult).toEqual(content.subarray(0, 3))
      })
    })

    describe('when content is overwritten with compressible bytes while a range decompression is in flight', () => {
      let overwriteStorage: IContentStorageComponent
      let newContent: Buffer
      let rangeResult: Buffer | undefined

      beforeEach(async () => {
        const realFs = createFsComponent()
        const gzipPath = filePath + '.gzip'
        // Gate the FIRST read of the canonical gzip so the decompression stays in flight while the
        // id is overwritten underneath it; the retry's read of the NEW gzip passes through.
        let releaseGzipRead: () => void = () => undefined
        const gzipReadGate = new Promise<void>((res) => (releaseGzipRead = res))
        let gzipReadStarted: () => void = () => undefined
        const gzipReadStartedPromise = new Promise<void>((res) => (gzipReadStarted = res))
        let holdNextGzipRead = true
        const gatedFs: IFileSystemComponent = {
          ...realFs,
          createReadStream: ((target: any, opts?: any) => {
            const real = realFs.createReadStream(target, opts)
            if (String(target) !== gzipPath || !holdNextGzipRead) return real
            holdNextGzipRead = false
            const gated = new PassThrough()
            gzipReadStarted()
            void gzipReadGate.then(() => real.pipe(gated))
            return gated
          }) as typeof realFs.createReadStream
        }
        overwriteStorage = await createFolderBasedFileSystemContentStorage(
          { fs: gatedFs, logs: await createLogComponent({}) },
          tmpRootDir
        )
        newContent = Buffer.from(new Uint8Array(100).fill(9))
        await overwriteStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(7))))
        const rangePromise = overwriteStorage.retrieve(id, { start: 0, end: 2 })
        await gzipReadStartedPromise
        // The overwrite is also compressible, so the new version ends up gzip-only: the discarded
        // old decompression leaves no uncompressed file and the range must retry against the new gzip.
        await overwriteStorage.storeStreamAndCompress(id, bufferToStream(newContent))
        releaseGzipRead()
        const item = await rangePromise
        rangeResult = item ? await streamToBuffer(await item.asStream()) : undefined
      })

      afterEach(async () => {
        await overwriteStorage.stop?.()
      })

      it('should serve the range from the new content via the retry instead of returning undefined', () => {
        expect(rangeResult).toEqual(newContent.subarray(0, 3))
      })

      it('should serve the whole new content on a later retrieve', async () => {
        const item = await overwriteStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(newContent)
      })
    })

    describe('when an id resolves inside the reserved temp-write namespace', () => {
      let flatStorage: IContentStorageComponent
      let flatRoot: string

      beforeEach(async () => {
        // disablePrefixHash makes the root itself the containment dir, which is the only mode where
        // an id can reach the reserved folder.
        flatRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-reserved-'))
        flatStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          flatRoot,
          { disablePrefixHash: true }
        )
      })

      afterEach(async () => {
        await flatStorage.stop?.()
        rmSync(flatRoot, { recursive: true, force: true })
      })

      it('should reject storing an id under the reserved folder', async () => {
        await expect(flatStorage.storeStream('.tmp-writes/foo', bufferToStream(content))).rejects.toThrow(
          /reserved temp-write/
        )
      })

      it('should reject the reserved folder name itself as an id', async () => {
        await expect(flatStorage.exist('.tmp-writes')).rejects.toThrow(/reserved temp-write/)
      })

      it('should still allow the reserved name as an id when hash prefixes are enabled', async () => {
        // With the default sha1 shard prefix the id resolves under a shard, outside the reserved folder.
        await fileSystemContentStorage.storeStream('.tmp-writes', bufferToStream(content))
        const item = await fileSystemContentStorage.retrieve('.tmp-writes')
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })
    })

    describe('when start is called repeatedly while a sweep may still be running', () => {
      let orphanPath: string

      beforeEach(async () => {
        // Repeated start() chains onto the previous sweep instead of replacing its promise, so no
        // sweep can dangle past stop() and the orphan is still removed.
        await fileSystemContentStorage.storeStream(id, bufferToStream(content))
        orphanPath = path.join(tmpRootDir, '.tmp-writes', 'deadbeefdeadbeef-0123456789abcdef0123456789abcdef')
        await nodeFs.writeFile(orphanPath, Buffer.from(''))
        await fileSystemContentStorage.start?.({} as any)
        await fileSystemContentStorage.start?.({} as any)
        await fileSystemContentStorage.stop?.()
      })

      it('should still remove the orphaned temp file', async () => {
        expect(await fs.existPath(orphanPath)).toBe(false)
      })

      it('should keep the real content file', async () => {
        expect(await fs.existPath(path.join(tmpRootDir, '9584', id))).toBe(true)
      })
    })

    describe('when an orphaned temp file exists in the reserved temp directory', () => {
      let seenIds: string[]

      beforeEach(async () => {
        await fileSystemContentStorage.storeStream(id, bufferToStream(content))
        await nodeFs.writeFile(
          path.join(tmpRootDir, '.tmp-writes', 'deadbeefdeadbeefdeadbeefdeadbeef'),
          Buffer.from('')
        )
        seenIds = []
        for await (const fileId of fileSystemContentStorage.allFileIds()) {
          seenIds.push(fileId)
        }
      })

      it('should still yield the stored content id', () => {
        expect(seenIds).toContain(id)
      })

      it('should not yield the orphaned temp file', () => {
        expect(seenIds).toContain(id)
        expect(seenIds).toHaveLength(1)
      })
    })

    describe('when start runs with an orphaned temp file present', () => {
      let orphanPath: string

      beforeEach(async () => {
        await fileSystemContentStorage.storeStream(id, bufferToStream(content))
        // Exactly the staged-name shape (<16-hex>-<32-hex>) with a foreign boot prefix.
        orphanPath = path.join(tmpRootDir, '.tmp-writes', 'deadbeefdeadbeef-0123456789abcdef0123456789abcdef')
        await nodeFs.writeFile(orphanPath, Buffer.from(''))
        await fileSystemContentStorage.start?.({} as any)
        // stop() awaits the background sweep, so it has completed by the time we assert.
        await fileSystemContentStorage.stop?.()
      })

      it('should remove the orphaned temp file', async () => {
        expect(await fs.existPath(orphanPath)).toBe(false)
      })

      it('should keep the real content file', async () => {
        expect(await fs.existPath(filePath)).toBe(true)
      })
    })

    describe('when an id collides with the old temp-file pattern', () => {
      // The exact shape the previous suffix-based filter would have hidden/deleted: `<...>.<32hex>.tmp`.
      const collidingId = 'asset.deadbeefdeadbeefdeadbeefdeadbeef.tmp'
      let collidingFilePath: string

      beforeEach(async () => {
        collidingFilePath = path.join(
          tmpRootDir,
          createHash('sha1').update(collidingId).digest('hex').substring(0, 4),
          collidingId
        )
        await fileSystemContentStorage.storeStream(collidingId, bufferToStream(content))
        await fileSystemContentStorage.start?.({} as any)
        await fileSystemContentStorage.stop?.()
      })

      it('should keep the content file after the startup sweep', async () => {
        expect(await fs.existPath(collidingFilePath)).toBe(true)
      })

      it('should still enumerate it in allFileIds', async () => {
        const seenIds: string[] = []
        for await (const fileId of fileSystemContentStorage.allFileIds()) seenIds.push(fileId)
        expect(seenIds).toContain(collidingId)
      })
    })

    describe('when the fs component does not provide rename', () => {
      let storageWithoutRename: IContentStorageComponent

      beforeEach(async () => {
        const fsWithoutRename: IFileSystemComponent = { ...createFsComponent(), rename: undefined }
        storageWithoutRename = await createFolderBasedFileSystemContentStorage(
          { fs: fsWithoutRename, logs: await createLogComponent({}) },
          tmpRootDir
        )
        await storageWithoutRename.storeStream(id, bufferToStream(content))
      })

      afterEach(async () => {
        await storageWithoutRename.stop?.()
      })

      it('should still store the content via the direct-write fallback', async () => {
        expect(await fs.existPath(filePath)).toBe(true)
      })

      it('should retrieve the stored content', async () => {
        const item = await storageWithoutRename.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
      })

      it('should compress in place and serve the gzip via the fallback', async () => {
        const compressible = Buffer.from(new Uint8Array(100).fill(0))
        await storageWithoutRename.storeStreamAndCompress(id2, bufferToStream(compressible))
        const item = await storageWithoutRename.retrieve(id2)
        expect(item?.encoding).toBe('gzip')
      })

      it('should serve a range through the in-place decompression fallback', async () => {
        const compressible = Buffer.from(new Uint8Array(100).fill(0))
        await storageWithoutRename.storeStreamAndCompress(id2, bufferToStream(compressible))
        const item = await storageWithoutRename.retrieve(id2, { start: 0, end: 9 })
        expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.alloc(10, 0))
      })

      describe('and a failing store is followed by a queued store for the same id', () => {
        let failedError: Error | undefined

        beforeEach(async () => {
          // A source that stays open until destroyed, so the first store holds the path lock while
          // the second store queues behind it.
          const failingStream = new Readable({ read() {} })
          const failedStore = storageWithoutRename.storeStream(id2, failingStream)
          // Wait until the failing store has opened the destination file — proof it holds the lock.
          for (let i = 0; i < 1000 && !(await fs.existPath(filePath2)); i++) {
            // each awaited existPath yields an event-loop turn
          }
          const queuedStore = storageWithoutRename.storeStream(id2, bufferToStream(content2))
          failingStream.destroy(new Error('boom'))
          failedError = await failedStore.then(
            () => undefined,
            (error: Error) => error
          )
          await queuedStore
        })

        it('should reject the failing store', () => {
          expect(failedError?.message).toContain('boom')
        })

        it('should keep the queued store content despite the failed-store cleanup', async () => {
          const item = await storageWithoutRename.retrieve(id2)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content2)
        })
      })

      describe('and a compressed store is followed by a queued store for the same id', () => {
        beforeEach(async () => {
          // The whole no-rename compressed store (raw write, compression, raw cleanup) runs under
          // the path lock, so the later store is strictly ordered after it and its cleanup can
          // never delete the newer raw.
          const gatedStream = new Readable({ read() {} })
          const compressedStore = storageWithoutRename.storeStreamAndCompress(id2, gatedStream)
          // Wait until the compressed store has opened the destination file — proof it holds the lock.
          for (let i = 0; i < 1000 && !(await fs.existPath(filePath2)); i++) {
            // each awaited existPath yields an event-loop turn
          }
          const queuedStore = storageWithoutRename.storeStream(id2, bufferToStream(content2))
          gatedStream.push(Buffer.from(new Uint8Array(100).fill(0)))
          gatedStream.push(null)
          await compressedStore
          await queuedStore
        })

        it('should keep the later store content as the final version', async () => {
          const item = await storageWithoutRename.retrieve(id2)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content2)
        })

        it('should not leave the compressed store gzip shadowing the later raw', async () => {
          expect(await fs.existPath(filePath2 + '.gzip')).toBe(false)
        })
      })

      describe('and the gzip cleanup of an in-place store fails', () => {
        let legacyRoot: string
        let originalBytes: Buffer
        let storeOutcome: 'resolved' | Error
        let legacyStorage: IContentStorageComponent

        beforeEach(async () => {
          // The in-place store must never resolve while the preferred gzip counterpart survives.
          // With no journal in this mode, the failed store rolls back and the previous version
          // stays cleanly intact.
          legacyRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-legacy-gzip-fail-'))
          const gzipPath = path.join(legacyRoot, '9584', id) + '.gzip'
          const realFs = createFsComponent()
          let armed = false
          const failingFs: IFileSystemComponent = {
            ...realFs,
            rename: undefined,
            unlink: (async (target: any) => {
              if (armed && String(target) === gzipPath) {
                armed = false
                throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
              }
              return realFs.unlink(target)
            }) as typeof realFs.unlink
          }
          legacyStorage = await createFolderBasedFileSystemContentStorage(
            { fs: failingFs, logs: await createLogComponent({}) },
            legacyRoot
          )
          originalBytes = Buffer.from(new Uint8Array(100).fill(0))
          await legacyStorage.storeStreamAndCompress(id, bufferToStream(originalBytes))
          armed = true
          storeOutcome = await legacyStorage.storeStream(id, bufferToStream(content)).then(
            () => 'resolved' as const,
            (error: Error) => error
          )
        })

        afterEach(async () => {
          await legacyStorage.stop?.()
          rmSync(legacyRoot, { recursive: true, force: true })
        })

        it('should reject instead of resolving while the old gzip is preferred', () => {
          expect((storeOutcome as Error).message).toContain('rolled back')
        })

        it('should keep serving the previous version', async () => {
          const item = await legacyStorage.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(originalBytes)
        })

        it('should succeed on a retry once the cleanup can complete', async () => {
          await legacyStorage.storeStream(id, bufferToStream(content))
          const item = await legacyStorage.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content)
        })
      })

      describe('and an in-place range decompression is followed by a queued store for the same id', () => {
        let gatedStorage: IContentStorageComponent

        beforeEach(async () => {
          // The in-place decompression runs entirely under the path lock, so the later store is
          // strictly ordered after it: the stale decompression can neither keep writing over the
          // newer raw nor leave its cache registration shadowing it.
          const realFs = createFsComponent()
          const gzipPath = filePath2 + '.gzip'
          let releaseGzipRead: () => void = () => undefined
          const gzipReadGate = new Promise<void>((res) => (releaseGzipRead = res))
          let gzipReadStarted: () => void = () => undefined
          const gzipReadStartedPromise = new Promise<void>((res) => (gzipReadStarted = res))
          let holdNextGzipRead = true
          const gatedFs: IFileSystemComponent = {
            ...realFs,
            rename: undefined,
            createReadStream: ((target: any, opts?: any) => {
              const real = realFs.createReadStream(target, opts)
              if (String(target) !== gzipPath || !holdNextGzipRead) return real
              holdNextGzipRead = false
              const gated = new PassThrough()
              gzipReadStarted()
              void gzipReadGate.then(() => real.pipe(gated))
              return gated
            }) as typeof realFs.createReadStream
          }
          gatedStorage = await createFolderBasedFileSystemContentStorage(
            { fs: gatedFs, logs: await createLogComponent({}) },
            tmpRootDir
          )
          // Compressible content stored via the in-place fallback ends up gzip-only.
          await gatedStorage.storeStreamAndCompress(id2, bufferToStream(Buffer.from(new Uint8Array(100).fill(7))))
          // The range request takes the path lock and blocks on the gated gzip read.
          const rangePromise = gatedStorage.retrieve(id2, { start: 0, end: 9 })
          await gzipReadStartedPromise
          const storePromise = gatedStorage.storeStream(id2, bufferToStream(content2))
          releaseGzipRead()
          // In no-rename mode reads are not guaranteed a complete version while an in-place write
          // is in flight (documented degradation), so the racy range outcome is not asserted —
          // only the final state below matters.
          await rangePromise.catch(() => undefined)
          await storePromise
        })

        afterEach(async () => {
          await gatedStorage.stop?.()
        })

        it('should keep the later store content as the final version', async () => {
          const item = await gatedStorage.retrieve(id2)
          expect(await streamToBuffer(await item!.asStream())).toEqual(content2)
        })
      })
    })
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
