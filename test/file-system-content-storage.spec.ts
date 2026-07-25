import { createHash, randomBytes } from 'crypto'
import { mkdtempSync, promises as nodeFs, rmSync, writeFileSync } from 'fs'
import os from 'os'
import path from 'path'
import { PassThrough, Readable, Writable } from 'stream'
import { gzipSync } from 'zlib'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  FileInfo,
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

  it(`When a range with a NaN bound is requested, then it throws a RangeError`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await expect(fileSystemContentStorage.retrieve(id, { start: Number.NaN, end: 2 })).rejects.toThrow(RangeError)
  })

  it(`When a range with a non-integer bound is requested, then it throws a RangeError`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await expect(fileSystemContentStorage.retrieve(id, { start: 0, end: 1.5 })).rejects.toThrow(RangeError)
  })

  it(`When a range with an Infinity bound is requested, then it throws a RangeError`, async () => {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await expect(fileSystemContentStorage.retrieve(id, { start: 0, end: Number.POSITIVE_INFINITY })).rejects.toThrow(
      RangeError
    )
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

    describe('when stop is called while an eviction tick is still running', () => {
      let gatedRoot: string
      let cachedFilePath: string
      let stopSettledBeforeRelease: boolean
      let gatedStorage: IContentStorageComponent

      beforeEach(async () => {
        // A second interval tick fires while the first eviction is blocked mid-unlink: evictCache()
        // must hand the in-flight promise to the tracked tick, so stop() awaits the REAL eviction
        // instead of the second tick's no-op.
        gatedRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-eviction-gate-'))
        cachedFilePath = path.join(gatedRoot, '9584', id)
        const realFs = createFsComponent()
        let releaseUnlink: () => void = () => undefined
        const unlinkGate = new Promise<void>((res) => (releaseUnlink = res))
        let gateArmed = false
        const gatedFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (gateArmed && String(target) === cachedFilePath) {
              gateArmed = false
              await unlinkGate
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        gatedStorage = await createFolderBasedFileSystemContentStorage(
          { fs: gatedFs, logs: await createLogComponent({}) },
          gatedRoot,
          { decompressCacheTTL: 10000, decompressCacheEvictionInterval: 30000 }
        )
        await gatedStorage.start?.({} as any)
        await gatedStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
        await gatedStorage.retrieve(id, { start: 0, end: 9 })
        gateArmed = true
        // Tick 1: the entry is past its TTL, the eviction starts and blocks on the gated unlink.
        await jest.advanceTimersByTimeAsync(30000)
        // Tick 2 fires while the first eviction is still in flight.
        await jest.advanceTimersByTimeAsync(30000)
        let stopSettled = false
        const stopPromise = gatedStorage.stop!().then(() => {
          stopSettled = true
        })
        // Flush microtasks: a stop() wrongly awaiting the second tick's no-op would settle here.
        await jest.advanceTimersByTimeAsync(0)
        stopSettledBeforeRelease = stopSettled
        releaseUnlink()
        await stopPromise
      })

      afterEach(async () => {
        rmSync(gatedRoot, { recursive: true, force: true })
      })

      it('should not settle stop before the in-flight eviction completes', () => {
        expect(stopSettledBeforeRelease).toBe(false)
      })

      it('should have completed the eviction by the time stop resolves', async () => {
        expect(await fs.existPath(cachedFilePath)).toBe(false)
      })
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

  it(`When decompression fails due to a corrupt gzip file, then the partial file is cleaned up and retrieve rejects`, async () => {
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

      // Range request should trigger decompression which fails. Corrupt STORED content is a local
      // integrity failure, not a miss: `exist`/`fileInfo` both report the id present, so answering
      // "not found" would hide unreadable content behind a 404 — the exact shape of failure that
      // makes a poison-pill file invisible. It rejects, and the operator sees the zlib error.
      await expect(storage.retrieve(corruptId, { start: 0, end: 4 })).rejects.toThrow()

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

  describe.each([
    {
      what: 'a compressed store',
      store: (storage: IContentStorageComponent, target: string, bytes: Buffer) =>
        storage.storeStreamAndCompress(target, bufferToStream(bytes))
    },
    {
      what: 'a plain store',
      store: (storage: IContentStorageComponent, target: string, bytes: Buffer) =>
        storage.storeStream(target, bufferToStream(bytes))
    }
  ])('when a cached shard directory is removed before $what', ({ store }) => {
    let healRoot: string
    let healStorage: IContentStorageComponent
    let firstOutcome: unknown
    let secondOutcome: unknown
    // Compressible, so the compressed variant genuinely commits a gzip.
    const bytes = Buffer.from(new Uint8Array(4096).fill(8))

    beforeEach(async () => {
      // Every write path resolves its target through the directory cache, so each one has to drop a
      // stale entry when the directory turns out to be gone — otherwise the retry keeps skipping the
      // mkdir and that shard fails permanently.
      healRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-heal-'))
      healStorage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        healRoot
      )
      await healStorage.storeStream(id, bufferToStream(content))
      rmSync(path.join(healRoot, '9584'), { recursive: true, force: true })
      firstOutcome = await store(healStorage, id, bytes).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
      secondOutcome = await store(healStorage, id, bytes).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await healStorage.stop?.()
      rmSync(healRoot, { recursive: true, force: true })
    })

    it('should fail the write that hit the missing directory', () => {
      expect((firstOutcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should recreate the directory on the retry', () => {
      expect(secondOutcome).toEqual('resolved')
    })

    it('should serve the retried content', async () => {
      const item = await healStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(bytes)
    })
  })

  describe('when a cached shard directory is removed before a no-rename adapter writes', () => {
    let legacyRoot: string
    let legacyStorage: IContentStorageComponent
    let firstOutcome: unknown
    let secondOutcome: unknown

    beforeEach(async () => {
      // The legacy direct-write path returns before the atomic path's error handling, so it needs the
      // same healing: its pipe writes straight to the canonical path under the cached directory.
      legacyRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-heal-legacy-'))
      const fsWithoutRename: IFileSystemComponent = { ...createFsComponent(), rename: undefined }
      legacyStorage = await createFolderBasedFileSystemContentStorage(
        { fs: fsWithoutRename, logs: await createLogComponent({}) },
        legacyRoot
      )
      await legacyStorage.storeStream(id, bufferToStream(content))
      rmSync(path.join(legacyRoot, '9584'), { recursive: true, force: true })
      firstOutcome = await legacyStorage.storeStream(id, bufferToStream(content2)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
      secondOutcome = await legacyStorage.storeStream(id, bufferToStream(content2)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await legacyStorage.stop?.()
      rmSync(legacyRoot, { recursive: true, force: true })
    })

    it('should fail the write that hit the missing directory', () => {
      expect((firstOutcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should recreate the directory on the retry', () => {
      expect(secondOutcome).toEqual('resolved')
    })

    it('should serve the retried content', async () => {
      const item = await legacyStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(content2)
    })
  })

  describe('when a cached shard directory is replaced by a file', () => {
    let obstructedRoot: string
    let obstructedStorage: IContentStorageComponent
    let obstructedShard: string
    let whileObstructed: unknown
    let afterRepair: unknown

    beforeEach(async () => {
      // A file at the shard path is not a directory writes can land in, so the cached entry is just as
      // stale as a removed one. This storage will not clear the obstruction — deleting something it
      // cannot prove it owns is what the reserved-namespace checks refuse to do — but once an operator
      // does, the next write must be able to recreate the tree instead of failing on the stale entry.
      obstructedRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-obstructed-'))
      obstructedShard = path.join(obstructedRoot, '9584')
      obstructedStorage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        obstructedRoot
      )
      await obstructedStorage.storeStream(id, bufferToStream(content))
      rmSync(obstructedShard, { recursive: true, force: true })
      await nodeFs.writeFile(obstructedShard, 'not a directory')
      whileObstructed = await obstructedStorage.storeStream(id, bufferToStream(content)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
      rmSync(obstructedShard, { force: true })
      afterRepair = await obstructedStorage.storeStream(id, bufferToStream(content)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await obstructedStorage.stop?.()
      rmSync(obstructedRoot, { recursive: true, force: true })
    })

    it('should fail the write while the path is obstructed', () => {
      expect(whileObstructed).not.toEqual('resolved')
    })

    it('should recreate the directory on the first write after the obstruction is cleared', () => {
      expect(afterRepair).toEqual('resolved')
    })

    it('should serve the content stored after the repair', async () => {
      const item = await obstructedStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(content)
    })
  })

  describe('when a cached shard directory is removed underneath the storage', () => {
    let firstOutcome: unknown
    let secondOutcome: unknown

    beforeEach(async () => {
      // The directory cache assumes the documented exclusive root ownership. If a directory
      // disappears anyway, the operation that needed it must fail loudly and drop the stale entry,
      // so a retry recreates the tree instead of failing forever.
      await fileSystemContentStorage.storeStream(id, bufferToStream(content))
      rmSync(path.dirname(filePath), { recursive: true, force: true })
      const attempt = () =>
        fileSystemContentStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: unknown) => error
        )
      firstOutcome = await attempt()
      secondOutcome = await attempt()
    })

    it('should fail the store that hit the missing directory', () => {
      expect((firstOutcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should recreate the directory on the next store', () => {
      expect(secondOutcome).toEqual('resolved')
    })

    it('should serve the content stored by the retry', async () => {
      const item = await fileSystemContentStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(content)
    })
  })

  describe('when the filesystem component is a custom adapter', () => {
    let adapterRoot: string
    let adapterStorage: IContentStorageComponent
    let streamedThroughAdapter: string[]
    const compressible = Buffer.from(new Uint8Array(4096).fill(3))

    beforeEach(async () => {
      // Compression reads and writes through the INJECTED component, so an adapter that instruments
      // or virtualizes paths gets compressed stores too — not just atomic raw writes. Native `fs`
      // would happily produce the same on-disk result here, so the assertions below check that the
      // bytes travelled through the adapter, which is the part that a custom adapter depends on.
      adapterRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-adapter-'))
      streamedThroughAdapter = []
      const realFs = createFsComponent()
      const recordingFs: IFileSystemComponent = {
        ...realFs,
        createReadStream(target: any, options?: any) {
          streamedThroughAdapter.push(`read:${target}`)
          return realFs.createReadStream(target, options)
        },
        createWriteStream(target: any, options?: any) {
          streamedThroughAdapter.push(`write:${target}`)
          return realFs.createWriteStream(target, options)
        }
      }
      adapterStorage = await createFolderBasedFileSystemContentStorage(
        { fs: recordingFs, logs: await createLogComponent({}) },
        adapterRoot
      )
      await adapterStorage.storeStreamAndCompress(id, bufferToStream(compressible))
    })

    afterEach(async () => {
      await adapterStorage.stop?.()
      rmSync(adapterRoot, { recursive: true, force: true })
    })

    it('should compress the store', async () => {
      expect((await adapterStorage.fileInfo(id))!.encoding).toEqual('gzip')
    })

    it('should read the staged raw back through the adapter to compress it', () => {
      // The first write of a compressed store is the staged raw; the compression is the only thing
      // that reads it back, so its presence here proves the compression used this component.
      const stagedRaw = streamedThroughAdapter[0].replace(/^write:/, '')
      expect(streamedThroughAdapter).toContain(`read:${stagedRaw}`)
    })

    it('should write the gzip output through the adapter', () => {
      const writes = streamedThroughAdapter.filter((entry) => entry.startsWith('write:'))
      expect(writes).toHaveLength(2)
    })

    it('should serve the stored content', async () => {
      const item = await adapterStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(compressible)
    })
  })

  describe('when a custom adapter provides no lstat', () => {
    let noLstatRoot: string
    let noLstatStorage: IContentStorageComponent
    const compressible = Buffer.from(new Uint8Array(4096).fill(4))

    beforeEach(async () => {
      // `lstat` is optional on IFileSystemComponent, so the compression's size comparison has to fall
      // back to `stat` — an adapter without it must still get compressed stores.
      noLstatRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-no-lstat-'))
      const noLstatFs: IFileSystemComponent = { ...createFsComponent(), lstat: undefined }
      noLstatStorage = await createFolderBasedFileSystemContentStorage(
        { fs: noLstatFs, logs: await createLogComponent({}) },
        noLstatRoot
      )
      await noLstatStorage.storeStreamAndCompress(id, bufferToStream(compressible))
    })

    afterEach(async () => {
      await noLstatStorage.stop?.()
      rmSync(noLstatRoot, { recursive: true, force: true })
    })

    it('should still compress the store', async () => {
      expect((await noLstatStorage.fileInfo(id))!.encoding).toEqual('gzip')
    })

    it('should serve the stored content', async () => {
      const item = await noLstatStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(compressible)
    })
  })

  describe('when the storage itself cannot read a present id', () => {
    let faultyRoot: string
    let faultyStorage: IContentStorageComponent
    let outcome: unknown

    beforeEach(async () => {
      // A fault in the storage's own tree is not a miss. Answering "not found" would make an
      // unreadable disk look like an empty node — the caller stops retrying and starts serving 404s
      // for content it has.
      faultyRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-faulty-'))
      const realFs = createFsComponent()
      const faultyFs: IFileSystemComponent = {
        ...realFs,
        mkdir: (async (target: any, options?: any) => {
          // Only the shard directory fails, so construction (root + reserved dir) still succeeds.
          if (String(target).endsWith('9584')) {
            throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' })
          }
          return realFs.mkdir(target, options)
        }) as IFileSystemComponent['mkdir']
      }
      faultyStorage = await createFolderBasedFileSystemContentStorage(
        { fs: faultyFs, logs: await createLogComponent({}) },
        faultyRoot
      )
      outcome = await faultyStorage.retrieve(id).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await faultyStorage.stop?.()
      rmSync(faultyRoot, { recursive: true, force: true })
    })

    it('should reject with the underlying filesystem error', () => {
      expect((outcome as { code?: string }).code).toEqual('EACCES')
    })
  })

  describe('when the shard directory of a stored id is replaced by a file', () => {
    let brokenRoot: string
    let brokenStorage: IContentStorageComponent
    let retrieveOutcome: unknown
    let fileInfoOutcome: unknown
    let storeWhileObstructed: unknown
    let storeAfterRepair: unknown

    beforeEach(async () => {
      // An access check passes for a regular file sitting at the shard path, while every stat beneath
      // it fails with ENOTDIR — so "the parent is present" is not enough to call this id absent. Two
      // ids in different shards, because surfacing the fault also drops the cache entry.
      brokenRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-shard-file-'))
      brokenStorage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        brokenRoot
      )
      await brokenStorage.storeStream(id, bufferToStream(content))
      await brokenStorage.storeStream(id2, bufferToStream(content2))
      const obstruct = async (shard: string): Promise<void> => {
        rmSync(path.join(brokenRoot, shard), { recursive: true, force: true })
        await nodeFs.writeFile(path.join(brokenRoot, shard), 'not a directory')
      }
      await obstruct('9584')
      await obstruct('ea6c')
      retrieveOutcome = await brokenStorage.retrieve(id).then(
        (value) => value,
        (error: unknown) => error
      )
      fileInfoOutcome = await brokenStorage.fileInfo(id2).then(
        (value) => value,
        (error: unknown) => error
      )
      // The obstruction is NOT removed on our behalf: a write keeps failing while it is there.
      storeWhileObstructed = await brokenStorage.storeStream(id, bufferToStream(content)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
      // Once an operator clears it, the dropped cache entry lets the tree be recreated.
      rmSync(path.join(brokenRoot, '9584'), { force: true })
      storeAfterRepair = await brokenStorage.storeStream(id, bufferToStream(content)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await brokenStorage.stop?.()
      rmSync(brokenRoot, { recursive: true, force: true })
    })

    it('should reject the read rather than report the id absent', () => {
      expect((retrieveOutcome as { code?: string }).code).toEqual('ENOTDIR')
    })

    it('should reject fileInfo as well', () => {
      expect((fileInfoOutcome as { code?: string }).code).toEqual('ENOTDIR')
    })

    it('should keep failing writes while the path is obstructed', () => {
      expect(storeWhileObstructed).not.toEqual('resolved')
    })

    it('should recreate the directory once the obstruction is cleared', () => {
      expect(storeAfterRepair).toEqual('resolved')
    })

    it('should serve the content stored after the repair', async () => {
      const item = await brokenStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(content)
    })
  })

  describe('when the shard directory of a stored id is removed', () => {
    let goneRoot: string
    let goneStorage: IContentStorageComponent
    let retrieveOutcome: unknown
    let fileInfoOutcome: unknown
    let laterStore: unknown

    beforeEach(async () => {
      // A stat ENOENT means "this id is absent" only while the shard directory is still there. If the
      // directory this storage owns has been removed, every id inside it is gone: that is a storage
      // fault, and answering "absent" would present a broken store as an empty one.
      goneRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-shard-gone-'))
      goneStorage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        goneRoot
      )
      // Two ids in different shards, because surfacing the fault also heals it: the failed read drops
      // the stale cache entry, so the next call through that shard recreates the directory and then
      // legitimately reports a miss. Each method therefore gets its own shard to fault on.
      await goneStorage.storeStream(id, bufferToStream(content))
      await goneStorage.storeStream(id2, bufferToStream(content2))
      rmSync(path.join(goneRoot, '9584'), { recursive: true, force: true })
      rmSync(path.join(goneRoot, 'ea6c'), { recursive: true, force: true })
      retrieveOutcome = await goneStorage.retrieve(id).then(
        (value) => value,
        (error: unknown) => error
      )
      fileInfoOutcome = await goneStorage.fileInfo(id2).then(
        (value) => value,
        (error: unknown) => error
      )
      // The failed read also invalidated the cached directory, so a write heals the tree.
      laterStore = await goneStorage.storeStream(id, bufferToStream(content)).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await goneStorage.stop?.()
      rmSync(goneRoot, { recursive: true, force: true })
    })

    it('should reject the read rather than report the id absent', () => {
      expect((retrieveOutcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should reject fileInfo as well', () => {
      expect((fileInfoOutcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should let a later write recreate the directory', () => {
      expect(laterStore).toEqual('resolved')
    })

    it('should report an id whose shard directory is intact as simply absent', async () => {
      expect(await goneStorage.retrieve('never-stored-id')).toBeUndefined()
      expect(await goneStorage.fileInfo('never-stored-id')).toBeUndefined()
    })
  })

  describe('when the gzip trailer read fails with a storage error', () => {
    let trailerRoot: string
    let trailerStorage: IContentStorageComponent
    let outcome: unknown
    const compressible = Buffer.from(new Uint8Array(4096).fill(2))

    beforeEach(async () => {
      // `contentSize: null` legitimately means "the format cannot express this size", so it must not
      // also mean "the read failed" — callers cannot tell those apart, and one of them bounds range
      // requests with `contentSize ?? size`, which would silently substitute the compressed size.
      jest.useRealTimers()
      trailerRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-trailer-'))
      const realFs = createFsComponent()
      let failTrailer = false
      const trailerFailingFs: IFileSystemComponent = {
        ...realFs,
        createReadStream(target: any, options?: any) {
          // The trailer read is the only ranged read of a .gzip path.
          if (failTrailer && String(target).endsWith('.gzip') && options?.start !== undefined) {
            const stream = new Readable({ read() {} })
            process.nextTick(() => stream.destroy(Object.assign(new Error('EIO: i/o error, read'), { code: 'EIO' })))
            return stream as any
          }
          return realFs.createReadStream(target, options)
        }
      }
      trailerStorage = await createFolderBasedFileSystemContentStorage(
        { fs: trailerFailingFs, logs: await createLogComponent({}) },
        trailerRoot
      )
      await trailerStorage.storeStreamAndCompress(id, bufferToStream(compressible))
      failTrailer = true
      outcome = await trailerStorage.fileInfo(id).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await trailerStorage.stop?.()
      rmSync(trailerRoot, { recursive: true, force: true })
    })

    it('should reject rather than report an unknown content size', () => {
      expect((outcome as { code?: string }).code).toEqual('EIO')
    })
  })

  describe('when the gzip vanishes between its stat and the trailer read', () => {
    let raceRoot: string
    let raceStorage: IContentStorageComponent
    let outcome: FileInfo | undefined
    const compressible = Buffer.from(new Uint8Array(4096).fill(1))

    beforeEach(async () => {
      // A store transitioning gzip -> raw lands exactly here, so the id is not absent: the raw
      // representation must be reported instead of a file that is no longer there.
      jest.useRealTimers()
      raceRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-trailer-race-'))
      const realFs = createFsComponent()
      let armed = false
      const racingFs: IFileSystemComponent = {
        ...realFs,
        createReadStream(target: any, options?: any) {
          if (armed && String(target).endsWith('.gzip') && options?.start !== undefined) {
            armed = false
            // The gzip goes away and the raw takes over, as a gzip -> raw transition would leave it.
            rmSync(String(target), { force: true })
            writeFileSync(String(target).replace(/\.gzip$/, ''), content)
          }
          return realFs.createReadStream(target, options)
        }
      }
      raceStorage = await createFolderBasedFileSystemContentStorage(
        { fs: racingFs, logs: await createLogComponent({}) },
        raceRoot
      )
      await raceStorage.storeStreamAndCompress(id, bufferToStream(compressible))
      armed = true
      outcome = await raceStorage.fileInfo(id)
    })

    afterEach(async () => {
      await raceStorage.stop?.()
      rmSync(raceRoot, { recursive: true, force: true })
    })

    it('should report the raw representation instead', () => {
      expect(outcome).toEqual({ encoding: null, size: content.length, contentSize: content.length })
    })
  })

  describe('when stat fails with a non-miss error on a present file', () => {
    let statFailRoot: string
    let statFailStorage: IContentStorageComponent
    let retrieveOutcome: unknown
    let fileInfoOutcome: unknown

    beforeEach(async () => {
      // An unreadable file is not an absent one. Reporting it as missing is the "broken storage looks
      // like a 404" behaviour this contract removes, so only ENOENT/ENOTDIR may answer "absent".
      statFailRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-stat-fail-'))
      const realFs = createFsComponent()
      let failStats = false
      const statFailingFs: IFileSystemComponent = {
        ...realFs,
        stat: (async (target: any, options?: any) => {
          if (failStats && String(target).includes('9584')) {
            throw Object.assign(new Error('EIO: i/o error, stat'), { code: 'EIO' })
          }
          return realFs.stat(target, options)
        }) as IFileSystemComponent['stat']
      }
      statFailStorage = await createFolderBasedFileSystemContentStorage(
        { fs: statFailingFs, logs: await createLogComponent({}) },
        statFailRoot
      )
      await statFailStorage.storeStream(id, bufferToStream(content))
      failStats = true
      retrieveOutcome = await statFailStorage.retrieve(id).then(
        (value) => value,
        (error: unknown) => error
      )
      fileInfoOutcome = await statFailStorage.fileInfo(id).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await statFailStorage.stop?.()
      rmSync(statFailRoot, { recursive: true, force: true })
    })

    it('should reject the read instead of reporting the id absent', () => {
      expect((retrieveOutcome as { code?: string }).code).toEqual('EIO')
    })

    it('should reject fileInfo as well', () => {
      expect((fileInfoOutcome as { code?: string }).code).toEqual('EIO')
    })
  })

  describe.each([
    {
      what: 'removed',
      code: 'ENOENT',
      damage: (shard: string) => rmSync(shard, { recursive: true, force: true })
    },
    {
      what: 'replaced by a file',
      code: 'ENOTDIR',
      damage: (shard: string) => {
        rmSync(shard, { recursive: true, force: true })
        writeFileSync(shard, 'not a directory')
      }
    }
  ])('when the shard directory is $what between the gzip stat and its open', ({ code, damage }) => {
    let damagedRoot: string
    let damagedStorage: IContentStorageComponent
    let outcome: unknown
    const compressible = Buffer.from(new Uint8Array(4096).fill(9))

    beforeEach(async () => {
      // The inflation stats the gzip, then opens it. Damage landing in that window makes the open
      // fail exactly like a concurrently deleted source — but the shard being gone means every id in
      // it is unreadable, which is a storage fault and must not be reported as this id being absent.
      jest.useRealTimers()
      damagedRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-shard-race-'))
      const realFs = createFsComponent()
      let armed = false
      const damagingFs: IFileSystemComponent = {
        ...realFs,
        createReadStream(target: any, options?: any) {
          if (armed && String(target).endsWith('.gzip')) {
            armed = false
            damage(path.join(damagedRoot, '9584'))
          }
          return realFs.createReadStream(target, options)
        }
      }
      damagedStorage = await createFolderBasedFileSystemContentStorage(
        { fs: damagingFs, logs: await createLogComponent({}) },
        damagedRoot
      )
      // Gzip-only, so serving a range must inflate it.
      await damagedStorage.storeStreamAndCompress(id, bufferToStream(compressible))
      armed = true
      outcome = await damagedStorage.retrieve(id, { start: 0, end: 3 }).then(
        (value) => (value === undefined ? 'reported-absent' : 'served'),
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await damagedStorage.stop?.()
      rmSync(damagedRoot, { recursive: true, force: true })
    })

    it('should reject rather than report the id absent', () => {
      expect((outcome as { code?: string }).code).toEqual(code)
    })
  })

  describe('when the decompression staging write fails asynchronously', () => {
    let asyncFailRoot: string
    let asyncFailStorage: IContentStorageComponent
    let outcome: unknown
    const compressible = Buffer.from(new Uint8Array(4096).fill(7))

    beforeEach(async () => {
      // The native shape of a vanished staging directory: `createWriteStream` returns a stream that
      // then emits ENOENT. `pipeline` destroys the upstream gzip stream WITH that error, so the
      // failure arrives on the source stream as the very same object a deleted source would produce —
      // it can only be told apart by checking whether the source is actually gone.
      jest.useRealTimers()
      asyncFailRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-async-staging-'))
      const realFs = createFsComponent()
      let failStaging = false
      const asyncFailingFs: IFileSystemComponent = {
        ...realFs,
        createWriteStream(target: any, options?: any) {
          if (failStaging && String(target).includes('.tmp-writes')) {
            // A real write stream over a directory that does not exist: emits ENOENT on open.
            return realFs.createWriteStream(path.join(asyncFailRoot, 'no-such-dir', 'staged'), options)
          }
          return realFs.createWriteStream(target, options)
        }
      }
      asyncFailStorage = await createFolderBasedFileSystemContentStorage(
        { fs: asyncFailingFs, logs: await createLogComponent({}) },
        asyncFailRoot
      )
      // Gzip-only, so serving a range has to inflate through the staging area.
      await asyncFailStorage.storeStreamAndCompress(id, bufferToStream(compressible))
      failStaging = true
      outcome = await asyncFailStorage.retrieve(id, { start: 0, end: 3 }).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await asyncFailStorage.stop?.()
      rmSync(asyncFailRoot, { recursive: true, force: true })
    })

    it('should reject rather than report the id absent', () => {
      expect((outcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should keep the gzip content intact', async () => {
      const item = await asyncFailStorage.retrieve(id)
      expect(await streamToBuffer(await item!.asStream())).toEqual(compressible)
    })
  })

  describe('when the decompression staging raises ENOENT', () => {
    let stagingFailRoot: string
    let stagingFailStorage: IContentStorageComponent
    let outcome: unknown
    let gzipReads: Readable[]
    // Incompressible and large, so the source is provably still mid-read when the destination fails —
    // small content finishes and closes itself, which would hide a leaked descriptor.
    const incompressible = randomBytes(8 << 20)

    beforeEach(async () => {
      // An ENOENT from the storage's OWN machinery (here the staging directory) is a fault, not a
      // miss: the content is present and readable, only the cache write failed. Classifying every
      // ENOENT as absence would hide exactly that.
      //
      // The destination is also constructed AFTER the source, so a synchronous failure here is the
      // case where nothing has taken ownership of the source yet and it must still be torn down.
      jest.useRealTimers()
      stagingFailRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-staging-fail-'))
      const realFs = createFsComponent()
      let failStaging = false
      gzipReads = []
      const stagingFailingFs: IFileSystemComponent = {
        ...realFs,
        createReadStream(target: any, options?: any) {
          const stream = realFs.createReadStream(target, options)
          // Whole-file reads of the gzip only: the trailer probe is a ranged read.
          if (String(target).endsWith('.gzip') && options?.start === undefined) {
            gzipReads.push(stream as Readable)
          }
          return stream
        },
        createWriteStream(target: any, options?: any) {
          if (failStaging && String(target).includes('.tmp-writes')) {
            throw Object.assign(new Error(`ENOENT: no such file or directory, open '${target}'`), {
              code: 'ENOENT'
            })
          }
          return realFs.createWriteStream(target, options)
        }
      }
      stagingFailStorage = await createFolderBasedFileSystemContentStorage(
        { fs: stagingFailingFs, logs: await createLogComponent({}) },
        stagingFailRoot
      )
      // Written directly rather than through storeStreamAndCompress: the state needed here is
      // gzip-only content whose gzip is LARGE, and a store would have discarded a gzip this
      // incompressible in favour of the raw, leaving nothing to inflate.
      await nodeFs.mkdir(path.join(stagingFailRoot, '9584'), { recursive: true })
      await nodeFs.writeFile(path.join(stagingFailRoot, '9584', `${id}.gzip`), gzipSync(incompressible))
      failStaging = true
      outcome = await stagingFailStorage.retrieve(id, { start: 0, end: 3 }).then(
        (value) => value,
        (error: unknown) => error
      )
      // Teardown is asynchronous; give the destroy a turn to land.
      await new Promise((resolve) => setImmediate(resolve))
    })

    afterEach(async () => {
      await stagingFailStorage.stop?.()
      rmSync(stagingFailRoot, { recursive: true, force: true })
    })

    it('should reject rather than report the id absent', () => {
      expect((outcome as { code?: string }).code).toEqual('ENOENT')
    })

    it('should have opened the gzip source before the destination failed', () => {
      expect(gzipReads.length).toBeGreaterThan(0)
    })

    it('should destroy the source rather than leak its descriptor', () => {
      expect(gzipReads.map((stream) => stream.destroyed)).not.toContain(false)
    })
  })

  describe('when a file vanishes between being observed and being read', () => {
    let vanishRoot: string
    let vanishStorage: IContentStorageComponent
    let result: unknown
    const vanishing = Buffer.from(new Uint8Array(4096).fill(5))

    beforeEach(async () => {
      // A delete landing mid-read is an expected race, not a fault: the id is genuinely gone, so the
      // read reports it absent rather than rejecting. The delete is performed at the exact moment the
      // inflation opens the gzip, so the ENOENT is a real one carrying the real path — racing an
      // actual concurrent unlink is not deterministic, since POSIX keeps an already-open file
      // readable.
      // Earlier tests in this file install fake timers; the inflate pipeline's teardown needs a real
      // setImmediate to settle.
      jest.useRealTimers()
      vanishRoot = mkdtempSync(path.join(os.tmpdir(), 'fs-vanish-'))
      const realFs = createFsComponent()
      let armed = false
      const racingFs: IFileSystemComponent = {
        ...realFs,
        createReadStream(target: any, options?: any) {
          if (armed && String(target).endsWith('.gzip')) {
            armed = false
            // The delete lands here: between the existence check that admitted this read and the
            // open that is about to fail.
            rmSync(target, { force: true })
          }
          return realFs.createReadStream(target, options)
        }
      }
      vanishStorage = await createFolderBasedFileSystemContentStorage(
        { fs: racingFs, logs: await createLogComponent({}) },
        vanishRoot
      )
      // Gzip-only, so serving a range has to inflate it — the read that observes the vanishing file.
      await vanishStorage.storeStreamAndCompress(id, bufferToStream(vanishing))
      armed = true
      result = await vanishStorage.retrieve(id, { start: 0, end: 3 }).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    afterEach(async () => {
      await vanishStorage.stop?.()
      rmSync(vanishRoot, { recursive: true, force: true })
    })

    it('should report the id as absent', () => {
      expect(result).toBeUndefined()
    })
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

    describe('when the staged write fails for a real reason while the caller aborts', () => {
      let enospcRoot: string
      let diskFullError: Error
      let storeOutcome: 'resolved' | unknown
      let enospcStorage: IContentStorageComponent

      beforeEach(async () => {
        // The staged write fails with ENOSPC while the caller happens to abort: the real storage
        // error is not teardown-caused and must surface as itself, not as the cancellation reason.
        enospcRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-enospc-race-'))
        diskFullError = Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' })
        const controller = new AbortController()
        const realFs = createFsComponent()
        let armed = true
        const failingFs: IFileSystemComponent = {
          ...realFs,
          createWriteStream: ((target: any, opts?: any) => {
            if (armed && /[0-9a-f]{16}-[0-9a-f]{32}$/.test(String(target))) {
              armed = false
              return new Writable({
                write(_chunk, _encoding, callback) {
                  controller.abort(new Error('cancelled while the disk was filling up'))
                  callback(diskFullError)
                }
              }) as any
            }
            return realFs.createWriteStream(target, opts)
          }) as typeof realFs.createWriteStream
        }
        enospcStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          enospcRoot
        )
        storeOutcome = await enospcStorage.storeStream(id, bufferToStream(content), controller.signal).then(
          () => 'resolved' as const,
          (error: unknown) => error
        )
      })

      afterEach(async () => {
        await enospcStorage.stop?.()
        rmSync(enospcRoot, { recursive: true, force: true })
      })

      it('should surface the real storage error instead of the cancellation reason', () => {
        expect(storeOutcome).toBe(diskFullError)
      })

      it('should not commit anything', async () => {
        expect(await enospcStorage.exist(id)).toBe(false)
      })
    })

    describe('when the signal aborts during the counterpart check of a fresh id', () => {
      let freshAbortRoot: string
      let reason: Error
      let storeOutcome: 'resolved' | unknown
      let freshAbortStorage: IContentStorageComponent

      beforeEach(async () => {
        // A fresh id has no counterpart: no intent is journaled, so the intent-cleanup checkpoint
        // is skipped — only the unconditional pre-rename checkpoint can stop the commit when the
        // abort lands during the awaited counterpart existence check.
        freshAbortRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-fresh-abort-'))
        const counterpartPath = path.join(freshAbortRoot, '9584', id) + '.gzip'
        const realFs = createFsComponent()
        const controller = new AbortController()
        reason = new Error('cancelled during the counterpart check')
        const abortingFs: IFileSystemComponent = {
          ...realFs,
          stat: (async (target: any) => {
            if (String(target) === counterpartPath) {
              // The abort lands inside this await; the check itself reports "absent" as reality would.
              controller.abort(reason)
            }
            return realFs.stat(target)
          }) as typeof realFs.stat
        }
        freshAbortStorage = await createFolderBasedFileSystemContentStorage(
          { fs: abortingFs, logs: await createLogComponent({}) },
          freshAbortRoot
        )
        storeOutcome = await freshAbortStorage.storeStream(id, bufferToStream(content), controller.signal).then(
          () => 'resolved' as const,
          (error: unknown) => error
        )
      })

      afterEach(async () => {
        await freshAbortStorage.stop?.()
        rmSync(freshAbortRoot, { recursive: true, force: true })
      })

      it('should reject with the abort reason instead of committing the fresh object', () => {
        expect(storeOutcome).toBe(reason)
      })

      it('should not commit anything', async () => {
        expect(await freshAbortStorage.exist(id)).toBe(false)
      })

      it('should leave no staged residue', async () => {
        const staged = (await nodeFs.readdir(path.join(freshAbortRoot, '.tmp-writes'))).filter((entry) =>
          /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry)
        )
        expect(staged).toEqual([])
      })
    })

    describe('when the signal aborts during the pre-rename work inside the commit', () => {
      let preRenameRoot: string
      let previousBytes: Buffer
      let reason: Error
      let storeOutcome: 'resolved' | unknown
      let preRenameStorage: IContentStorageComponent

      beforeEach(async () => {
        // The abort lands while the commit is journaling its intent — after every earlier
        // checkpoint, with the source long consumed. The commit must still cancel before the
        // irreversible rename, discarding the just-written journal so no repair can ever apply it.
        preRenameRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-pre-rename-abort-'))
        const realFs = createFsComponent()
        const controller = new AbortController()
        reason = new Error('cancelled while the intent was being journaled')
        const abortingFs: IFileSystemComponent = {
          ...realFs,
          createWriteStream: ((target: any, opts?: any) => {
            if (String(target).endsWith('.intent')) {
              controller.abort(reason)
            }
            return realFs.createWriteStream(target, opts)
          }) as typeof realFs.createWriteStream
        }
        preRenameStorage = await createFolderBasedFileSystemContentStorage(
          { fs: abortingFs, logs: await createLogComponent({}) },
          preRenameRoot
        )
        // A gzip primary, so the raw commit below has a counterpart and journals an intent.
        previousBytes = Buffer.from(new Uint8Array(100).fill(5))
        await preRenameStorage.storeStreamAndCompress(id, bufferToStream(previousBytes))
        storeOutcome = await preRenameStorage.storeStream(id, bufferToStream(content), controller.signal).then(
          () => 'resolved' as const,
          (error: unknown) => error
        )
      })

      afterEach(async () => {
        await preRenameStorage.stop?.()
        rmSync(preRenameRoot, { recursive: true, force: true })
      })

      it('should reject with the abort reason instead of committing', () => {
        expect(storeOutcome).toBe(reason)
      })

      it('should keep serving the previous version', async () => {
        const item = await preRenameStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(previousBytes)
      })

      it('should not tag the caller-owned abort reason with internal markers', () => {
        // The commit phase's checkpoints throw the caller's reason; the commit wrapper must not
        // brand that caller-owned object with this module's internal non-cancellation symbol.
        expect(Object.getOwnPropertySymbols(reason)).toEqual([])
      })

      it('should discard the just-written intent so no repair can apply it', async () => {
        const entries = await nodeFs.readdir(path.join(preRenameRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })

      it('should leave no staged residue', async () => {
        const staged = (await nodeFs.readdir(path.join(preRenameRoot, '.tmp-writes'))).filter((entry) =>
          /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry)
        )
        expect(staged).toEqual([])
      })
    })

    describe('when the signal aborts while the commit itself fails', () => {
      let maskingRoot: string
      let reason: Error
      let storeOutcome: 'resolved' | unknown
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // The abort lands DURING an irreversible commit whose counterpart cleanup fails: the
        // resulting committed-but-unreconciled error (quarantine) is a real storage failure that
        // operators need to see — the cancellation translation must not mask it behind the abort
        // reason.
        maskingRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-abort-masking-'))
        const gzipPath = path.join(maskingRoot, '9584', id) + '.gzip'
        const realFs = createFsComponent()
        const controller = new AbortController()
        reason = new Error('cancelled during the commit')
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (armed && String(target) === gzipPath) {
              armed = false
              // The abort arrives exactly while the commit's counterpart cleanup is failing.
              controller.abort(reason)
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          maskingRoot
        )
        await failingStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(5))))
        armed = true
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content), controller.signal).then(
          () => 'resolved' as const,
          (error: unknown) => error
        )
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(maskingRoot, { recursive: true, force: true })
      })

      it('should surface the quarantine error instead of the abort reason', () => {
        expect((storeOutcome as Error).message).toContain('quarantined')
      })

      it('should not reject with the cancellation reason', () => {
        expect(storeOutcome).not.toBe(reason)
      })
    })

    describe('when the signal aborts while the store is queued on the path lock', () => {
      let lockRoot: string
      let firstWriterBytes: Buffer
      let reason: Error
      let queuedOutcome: 'resolved' | unknown
      let lockedStorage: IContentStorageComponent

      beforeEach(async () => {
        // Writer A holds the path lock (its commit rename is gated); writer B consumes its source,
        // passes the pre-lock checkpoint and queues on the lock. The abort lands while B is queued
        // — with the source already consumed, only the inside-lock checkpoint can stop the commit.
        lockRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-lock-abort-'))
        const rawPath = path.join(lockRoot, '9584', id)
        const realFs = createFsComponent()
        let releaseRename: () => void = () => undefined
        const renameGate = new Promise<void>((res) => (releaseRename = res))
        let renameStarted: () => void = () => undefined
        const renameStartedPromise = new Promise<void>((res) => (renameStarted = res))
        let holdNextRename = true
        const gatedFs: IFileSystemComponent = {
          ...realFs,
          rename: (async (from: any, to: any) => {
            if (String(to) === rawPath && holdNextRename) {
              holdNextRename = false
              renameStarted()
              await renameGate
            }
            return realFs.rename!(from, to)
          }) as typeof realFs.rename
        }
        lockedStorage = await createFolderBasedFileSystemContentStorage(
          { fs: gatedFs, logs: await createLogComponent({}) },
          lockRoot
        )
        firstWriterBytes = Buffer.from('first writer bytes')
        const firstStore = lockedStorage.storeStream(id, bufferToStream(firstWriterBytes))
        await renameStartedPromise
        reason = new Error('cancelled while queued on the lock')
        const controller = new AbortController()
        const queuedStore = lockedStorage.storeStream(id, bufferToStream(content), controller.signal)
        // Capture the outcome NOW rather than after `await firstStore`. The queued store can reject
        // while that await is still pending, and with no handler attached yet that surfaces as an
        // unhandled rejection instead of the value this test wants to inspect.
        const queuedSettled = queuedStore.then(
          () => 'resolved' as const,
          (error: unknown) => error
        )
        // Let the queued store consume its source and reach the lock queue.
        const tempDirPath = path.join(lockRoot, '.tmp-writes')
        for (let i = 0; i < 1000; i++) {
          const staged = (await nodeFs.readdir(tempDirPath)).filter((entry) =>
            /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry)
          )
          if (staged.length >= 2) break
        }
        await new Promise<void>((resolve) => setImmediate(resolve))
        controller.abort(reason)
        releaseRename()
        await firstStore
        queuedOutcome = await queuedSettled
      })

      afterEach(async () => {
        await lockedStorage.stop?.()
        rmSync(lockRoot, { recursive: true, force: true })
      })

      it('should reject the queued store with the abort reason', () => {
        expect(queuedOutcome).toBe(reason)
      })

      it('should keep the first writer content as the committed version', async () => {
        const item = await lockedStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(firstWriterBytes)
      })

      it('should leave no staging residue', async () => {
        const staged = (await nodeFs.readdir(path.join(lockRoot, '.tmp-writes'))).filter((entry) =>
          /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry)
        )
        expect(staged).toEqual([])
      })
    })

    describe('when the signal aborts after the source was fully consumed', () => {
      let compressSpy: jest.SpyInstance
      let reason: Error
      let storeOutcome: 'resolved' | unknown

      beforeEach(async () => {
        // The source has been consumed, so destroying it does nothing — the abort is observed
        // during the compression phase. The store must stop at the next checkpoint and reject with
        // the caller's reason instead of continuing the expensive work and committing the object.
        reason = new Error('cancelled after the source ended')
        const controller = new AbortController()
        compressSpy = jest.spyOn(compressionModule, 'compressContentFile').mockImplementationOnce(async () => {
          controller.abort(reason)
          return true
        })
        storeOutcome = await fileSystemContentStorage
          .storeStreamAndCompress(
            'signal-consumed',
            bufferToStream(Buffer.from(new Uint8Array(100).fill(0))),
            controller.signal
          )
          .then(
            () => 'resolved' as const,
            (error: unknown) => error
          )
      })

      afterEach(() => {
        compressSpy.mockRestore()
      })

      it('should reject with the abort reason', () => {
        expect(storeOutcome).toBe(reason)
      })

      it('should not commit the object', async () => {
        expect(await fileSystemContentStorage.exist('signal-consumed')).toBe(false)
      })

      it('should leave no staging residue', async () => {
        expect(await nodeFs.readdir(path.join(tmpRootDir, '.tmp-writes'))).toEqual([])
      })
    })

    describe('when the signalled compression pipeline is torn down by the abort', () => {
      let compressSpy: jest.SpyInstance
      let reason: Error
      let storeOutcome: 'resolved' | unknown

      beforeEach(async () => {
        // The staged path hands the signal to the compression pipeline, so an abort-shaped rejection
        // from it is provably this cancellation's own teardown: the caller must observe THEIR reason,
        // not the pipeline's AbortError — this is the attribution the generic translation no longer
        // makes on shape alone.
        reason = new Error('cancelled mid-compression pipeline')
        const controller = new AbortController()
        compressSpy = jest.spyOn(compressionModule, 'compressContentFile').mockImplementationOnce(async () => {
          controller.abort(reason)
          throw Object.assign(new Error('The operation was aborted'), { name: 'AbortError', code: 'ABORT_ERR' })
        })
        storeOutcome = await fileSystemContentStorage
          .storeStreamAndCompress(
            'signalled-compression',
            bufferToStream(Buffer.from(new Uint8Array(100).fill(0))),
            controller.signal
          )
          .then(
            () => 'resolved' as const,
            (error: unknown) => error
          )
      })

      afterEach(() => {
        compressSpy.mockRestore()
      })

      it('should reject with the caller reason rather than the pipeline abort error', () => {
        expect(storeOutcome).toBe(reason)
      })

      it('should not commit the object', async () => {
        expect(await fileSystemContentStorage.exist('signalled-compression')).toBe(false)
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

    describe('when a failed post-rename cleanup quarantines the id', () => {
      let quarantineRoot: string
      let gzipPath: string
      let intentPath: string
      let storeOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent
      let disarm: () => void

      beforeEach(async () => {
        // A raw commit lands but the stale gzip cannot be removed: without a guard, full reads
        // would keep serving the OLD gzip while range reads see the NEW raw — two versions of one
        // id from the same process. The id must be quarantined: reads repair-or-refuse, never
        // exposing the mixed state.
        quarantineRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-quarantine-'))
        gzipPath = path.join(quarantineRoot, '9584', id) + '.gzip'
        intentPath = path.join(quarantineRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
        const realFs = createFsComponent()
        let armed = true
        disarm = () => {
          armed = false
        }
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (armed && String(target) === gzipPath) {
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          quarantineRoot
        )
        await failingStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(5))))
        storeOutcome = await failingStorage.storeStream(id, bufferToStream(content)).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(quarantineRoot, { recursive: true, force: true })
      })

      it('should fail the store and announce the quarantine', () => {
        expect((storeOutcome as Error).message).toContain('quarantined')
      })

      // The id is PRESENT — both representations are on disk and `allFileIds` still enumerates it —
      // so an unrepairable mixed state is a "cannot be read", not a "not here". Reporting absence
      // handed back a 404 for content sitting on the disk, and contradicted the store that had
      // already failed announcing the quarantine.
      it('should reject range reads rather than exposing the new bytes while quarantined', async () => {
        await expect(failingStorage.retrieve(id, { start: 0, end: 2 })).rejects.toThrow(/mixed state/)
      })

      it('should reject full reads rather than exposing the old version while quarantined', async () => {
        await expect(failingStorage.retrieve(id)).rejects.toThrow(/mixed state/)
      })

      it('should reject exist rather than reporting the present id as absent while quarantined', async () => {
        await expect(failingStorage.exist(id)).rejects.toThrow(/mixed state/)
      })

      it('should reject fileInfo while quarantined', async () => {
        await expect(failingStorage.fileInfo(id)).rejects.toThrow(/mixed state/)
      })

      it('should repair through a read once the cleanup can complete', async () => {
        disarm()
        const item = await failingStorage.retrieve(id)
        expect(await streamToBuffer(await item!.asStream())).toEqual(content)
        expect(await fs.existPath(gzipPath)).toBe(false)
        expect(await fs.existPath(intentPath)).toBe(false)
        expect(await failingStorage.exist(id)).toBe(true)
      })
    })

    describe('when an intent body names a different id than its filename', () => {
      let mismatchRoot: string
      let victimGzipPath: string
      let mismatchStorage: IContentStorageComponent

      beforeEach(async () => {
        // Valid-JSON corruption or an operator mistake: the intent file for one id names another.
        // Applying it would reconcile the WRONG id (deleting the victim's valid gzip); it must be
        // treated as malformed and discarded.
        mismatchRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-mismatch-'))
        const victimShard = path.join(mismatchRoot, 'ea6c')
        await nodeFs.mkdir(victimShard, { recursive: true })
        victimGzipPath = path.join(victimShard, id2) + '.gzip'
        await nodeFs.writeFile(path.join(victimShard, id2), Buffer.from('victim raw'))
        await nodeFs.writeFile(victimGzipPath, Buffer.from('victim gzip'))
        await nodeFs.mkdir(path.join(mismatchRoot, '.tmp-writes'), { recursive: true })
        await nodeFs.writeFile(
          path.join(mismatchRoot, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
          Buffer.from(
            JSON.stringify({ op: 'raw', id: id2, staged: 'deadbeefdeadbeef-00000000000000000000000000000000' })
          )
        )
        mismatchStorage = await createFolderBasedFileSystemContentStorage(
          { fs, logs: await createLogComponent({}) },
          mismatchRoot
        )
      })

      afterEach(async () => {
        await mismatchStorage.stop?.()
        rmSync(mismatchRoot, { recursive: true, force: true })
      })

      it('should discard the mismatched intent instead of applying it', async () => {
        const entries = await nodeFs.readdir(path.join(mismatchRoot, '.tmp-writes'))
        expect(entries.filter((entry) => entry.endsWith('.intent'))).toEqual([])
      })

      it('should leave the named id untouched', async () => {
        expect(await fs.existPath(victimGzipPath)).toBe(true)
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

    describe('when deleting an id whose cached raw cannot be removed', () => {
      let cacheDeleteRoot: string
      let gzipPath: string
      let deleteOutcome: 'resolved' | Error
      let failingStorage: IContentStorageComponent

      beforeEach(async () => {
        // The cached decompressed raw survives its unlink during delete(): the delete must reject
        // BEFORE removing the gzip — resolving would leave the untracked cache file readable as
        // primary content after a "successful" delete.
        cacheDeleteRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-cache-delete-'))
        const cachedFilePath = path.join(cacheDeleteRoot, '9584', id)
        gzipPath = cachedFilePath + '.gzip'
        const realFs = createFsComponent()
        let armed = false
        const failingFs: IFileSystemComponent = {
          ...realFs,
          unlink: (async (target: any) => {
            if (armed && String(target) === cachedFilePath) {
              armed = false
              throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
            }
            return realFs.unlink(target)
          }) as typeof realFs.unlink
        }
        failingStorage = await createFolderBasedFileSystemContentStorage(
          { fs: failingFs, logs: await createLogComponent({}) },
          cacheDeleteRoot
        )
        await failingStorage.storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(100).fill(0))))
        // Materialize the decompressed raw cache alongside the gzip.
        await failingStorage.retrieve(id, { start: 0, end: 9 })
        armed = true
        deleteOutcome = await failingStorage.delete([id]).then(
          () => 'resolved' as const,
          (error: Error) => error
        )
      })

      afterEach(async () => {
        await failingStorage.stop?.()
        rmSync(cacheDeleteRoot, { recursive: true, force: true })
      })

      it('should reject the delete instead of resurrecting the cache as primary', () => {
        expect((deleteOutcome as Error).message).toContain('cached decompressed content')
      })

      it('should keep the gzip representation untouched', async () => {
        expect(await fs.existPath(gzipPath)).toBe(true)
      })

      it('should delete fully on a retry once the cleanup can complete', async () => {
        await failingStorage.delete([id])
        expect(await failingStorage.exist(id)).toBe(false)
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

      describe('and the signal aborts after the in-place write has replaced the previous raw', () => {
        let inPlaceRoot: string
        let newBytes: Buffer
        let storeOutcome: 'resolved' | unknown
        let inPlaceStorage: IContentStorageComponent

        beforeEach(async () => {
          // The in-place pipe has already overwritten the previous raw when the abort is observed:
          // "rolling back" would unlink the only committed object (the previous version is
          // unrecoverable without rename support), so the store must be treated as COMPLETED — the
          // regression was cancellation deleting an existing raw-backed id entirely.
          inPlaceRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-inplace-abort-'))
          const gzipPath = path.join(inPlaceRoot, '9584', id) + '.gzip'
          const realFs = createFsComponent()
          const controller = new AbortController()
          let armed = false
          const abortingFs: IFileSystemComponent = {
            ...realFs,
            rename: undefined,
            unlink: (async (target: any) => {
              // The gzip cleanup runs right after the pipe: abort exactly there (post-write).
              if (armed && String(target) === gzipPath) {
                armed = false
                controller.abort(new Error('cancelled after the in-place write'))
              }
              return realFs.unlink(target)
            }) as typeof realFs.unlink
          }
          inPlaceStorage = await createFolderBasedFileSystemContentStorage(
            { fs: abortingFs, logs: await createLogComponent({}) },
            inPlaceRoot
          )
          await inPlaceStorage.storeStream(id, bufferToStream(Buffer.from('previous raw version')))
          newBytes = Buffer.from('new raw version')
          armed = true
          storeOutcome = await inPlaceStorage.storeStream(id, bufferToStream(newBytes), controller.signal).then(
            () => 'resolved' as const,
            (error: unknown) => error
          )
        })

        afterEach(async () => {
          await inPlaceStorage.stop?.()
          rmSync(inPlaceRoot, { recursive: true, force: true })
        })

        it('should treat the store as completed', () => {
          expect(storeOutcome).toBe('resolved')
        })

        it('should keep the id readable with the new content instead of deleting it', async () => {
          const item = await inPlaceStorage.retrieve(id)
          expect(await streamToBuffer(await item!.asStream())).toEqual(newBytes)
        })
      })

      describe('and the signal aborts while an in-place store is queued on the path lock', () => {
        let previousBytes: Buffer
        let reason: Error
        let queuedOutcome: 'resolved' | unknown

        beforeEach(async () => {
          // Cancellation in no-rename mode is honored BEFORE the destructive write begins: writer A
          // holds the lock for its whole in-place write (gated source), writer B queues, the abort
          // lands, and B must reject at the pre-write checkpoint with A's version intact.
          previousBytes = Buffer.from(new Uint8Array(64).fill(3))
          const gatedSource = new Readable({ read() {} })
          const firstStore = storageWithoutRename.storeStream(id2, gatedSource)
          // Wait until A has opened the destination — proof it holds the lock mid-write.
          for (let i = 0; i < 1000 && !(await fs.existPath(filePath2)); i++) {
            // each awaited existPath yields an event-loop turn
          }
          reason = new Error('cancelled while queued behind an in-place write')
          const controller = new AbortController()
          const queuedStore = storageWithoutRename.storeStream(id2, bufferToStream(content2), controller.signal)
          controller.abort(reason)
          gatedSource.push(previousBytes)
          gatedSource.push(null)
          await firstStore
          queuedOutcome = await queuedStore.then(
            () => 'resolved' as const,
            (error: unknown) => error
          )
        })

        it('should reject the queued store with the abort reason', () => {
          expect(queuedOutcome).toBe(reason)
        })

        it('should keep the previous version intact', async () => {
          const item = await storageWithoutRename.retrieve(id2)
          expect(await streamToBuffer(await item!.asStream())).toEqual(previousBytes)
        })
      })

      describe('and the signal tears the compression down after the in-place raw commit', () => {
        let rawBytes: Buffer
        let compressSpy: jest.SpyInstance
        let storeOutcome: 'resolved' | unknown

        beforeEach(async () => {
          // The abort tears the compression pipeline down mid-flight — but the in-place raw commit
          // already completed, so the store must resolve with the raw as primary instead of
          // rejecting an already-completed store.
          rawBytes = Buffer.from(new Uint8Array(64).fill(9))
          const controller = new AbortController()
          compressSpy = jest.spyOn(compressionModule, 'compressContentFile').mockImplementationOnce(async () => {
            controller.abort(new Error('cancelled mid-compression'))
            throw Object.assign(new Error('The operation was aborted'), { name: 'AbortError' })
          })
          storeOutcome = await storageWithoutRename
            .storeStreamAndCompress(id2, bufferToStream(rawBytes), controller.signal)
            .then(
              () => 'resolved' as const,
              (error: unknown) => error
            )
        })

        afterEach(() => {
          compressSpy.mockRestore()
        })

        it('should treat the store as completed', () => {
          expect(storeOutcome).toBe('resolved')
        })

        it('should keep the raw as the primary representation', async () => {
          const item = await storageWithoutRename.retrieve(id2)
          expect(await streamToBuffer(await item!.asStream())).toEqual(rawBytes)
        })
      })

      describe('and a real compression failure races a caller abort', () => {
        let rawBytes: Buffer
        let diskFullError: Error
        let compressSpy: jest.SpyInstance
        let storeOutcome: 'resolved' | unknown

        beforeEach(async () => {
          // The compression fails for a real reason (ENOSPC) while the caller happens to abort:
          // suppressing it would misreport a failed compressed store as success, and leaving it
          // unmarked would hide it behind the cancellation reason. It must surface as-is.
          rawBytes = Buffer.from(new Uint8Array(64).fill(7))
          diskFullError = Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' })
          const controller = new AbortController()
          compressSpy = jest.spyOn(compressionModule, 'compressContentFile').mockImplementationOnce(async () => {
            controller.abort(new Error('cancelled while compression was failing'))
            throw diskFullError
          })
          storeOutcome = await storageWithoutRename
            .storeStreamAndCompress(id2, bufferToStream(rawBytes), controller.signal)
            .then(
              () => 'resolved' as const,
              (error: unknown) => error
            )
        })

        afterEach(() => {
          compressSpy.mockRestore()
        })

        it('should reject with the real storage error, not resolve and not the abort reason', () => {
          expect(storeOutcome).toBe(diskFullError)
        })

        it('should keep the committed raw readable', async () => {
          const item = await storageWithoutRename.retrieve(id2)
          expect(await streamToBuffer(await item!.asStream())).toEqual(rawBytes)
        })
      })

      describe('and an aborted compression leaves a partial gzip that cannot be removed', () => {
        let partialRoot: string
        let gzipPath: string
        let compressSpy: jest.SpyInstance
        let storeOutcome: 'resolved' | unknown
        let partialStorage: IContentStorageComponent

        beforeEach(async () => {
          // The teardown is abort-caused, but the partial canonical gzip survives its cleanup:
          // resolving would let reads prefer the corrupt gzip over the committed raw, so the store
          // must fail loudly with the invariant error — visible even past the abort translation.
          partialRoot = mkdtempSync(path.join(os.tmpdir(), 'cs-partial-gzip-'))
          gzipPath = path.join(partialRoot, '9584', id) + '.gzip'
          const realFs = createFsComponent()
          let unlinkFails = false
          const failingFs: IFileSystemComponent = {
            ...realFs,
            rename: undefined,
            unlink: (async (target: any) => {
              if (unlinkFails && String(target) === gzipPath) {
                throw Object.assign(new Error('EPERM: operation not permitted'), { code: 'EPERM' })
              }
              return realFs.unlink(target)
            }) as typeof realFs.unlink
          }
          partialStorage = await createFolderBasedFileSystemContentStorage(
            { fs: failingFs, logs: await createLogComponent({}) },
            partialRoot
          )
          const controller = new AbortController()
          compressSpy = jest.spyOn(compressionModule, 'compressContentFile').mockImplementationOnce(async () => {
            // Simulate a torn-down pipeline whose partial-output cleanup failed: the partial
            // canonical gzip is left behind and stays unremovable for the store's verification.
            await nodeFs.writeFile(gzipPath, Buffer.from('partial gzip bytes'))
            unlinkFails = true
            controller.abort(new Error('cancelled mid-compression'))
            throw Object.assign(new Error('The operation was aborted'), { name: 'AbortError' })
          })
          storeOutcome = await partialStorage
            .storeStreamAndCompress(id, bufferToStream(Buffer.from(new Uint8Array(64).fill(1))), controller.signal)
            .then(
              () => 'resolved' as const,
              (error: unknown) => error
            )
        })

        afterEach(async () => {
          compressSpy.mockRestore()
          await partialStorage.stop?.()
          rmSync(partialRoot, { recursive: true, force: true })
        })

        it('should reject with the invariant error instead of resolving over a corrupt gzip', () => {
          expect((storeOutcome as Error).message).toContain('could not be removed')
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
