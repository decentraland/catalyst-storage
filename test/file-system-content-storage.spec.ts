import { mkdtempSync, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { createFolderBasedFileSystemContentStorage, createFsComponent, IContentStorageComponent } from '../src'
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

  afterEach(() => {
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

  it(`When content is stored, then we can check file info`, async function () {
    await fileSystemContentStorage.storeStream(id, bufferToStream(content))
    await fileSystemContentStorage.storeStream(id2, bufferToStream(content2))

    const exists = await fileSystemContentStorage.fileInfoMultiple([id, id2])

    expect(exists.get(id)).toEqual({ encoding: null, size: 3 })
    expect(exists.get(id2)).toEqual({ encoding: null, size: 3 })
    expect(await fileSystemContentStorage.fileInfo(id)).toEqual({ encoding: null, size: 3 })
    expect(await fileSystemContentStorage.fileInfo(id2)).toEqual({ encoding: null, size: 3 })
    expect(await fileSystemContentStorage.fileInfo('non-existent-id')).toBeUndefined()
  })
})
