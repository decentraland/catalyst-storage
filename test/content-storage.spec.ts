import path from 'path'
import { createFolderBasedFileSystemContentStorage, createFsComponent, IContentStorageComponent } from '../src'
import { bufferToStream, streamToBuffer } from '../src/content-item'
import { FileSystemUtils as fsu } from './file-system-utils'
import { createLogComponent } from '@well-known-components/logger'

describe('ContentStorageWithStreams', () => {
  let storage: IContentStorageComponent
  let id: string
  let content: Buffer
  let id2: string
  let content2: Buffer

  it('starts the env', async () => {
    const root = fsu.createTempDirectory()
    const contentFolder = path.join(root, 'contents')
    storage = await createFolderBasedFileSystemContentStorage(
      { fs: createFsComponent(), logs: await createLogComponent({}) },
      contentFolder
    )

    id = 'some-id'
    content = Buffer.from('123')
    id2 = 'another-id'
    content2 = Buffer.from('456')
  })

  it(`When content is stored, then it can be retrieved`, async () => {
    await storage.storeStream(id, bufferToStream(content))

    await retrieveAndExpectStoredContentToBe(id, content)
  })

  it(`When content is stored, then we can check if it exists`, async function () {
    await storage.storeStream(id, bufferToStream(content))

    const exists = await storage.existMultiple([id])

    expect(exists.get(id)).toEqual(true)
    expect(await storage.exist(id)).toBe(true)
  })

  it(`When content is stored on already existing id, then it overwrites the previous content`, async function () {
    const newContent = Buffer.from('456')

    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id, bufferToStream(newContent))

    await retrieveAndExpectStoredContentToBe(id, newContent)
  })

  it(`When content is deleted, then it is no longer available`, async function () {
    await storage.storeStream(id, bufferToStream(content))

    let exists = await storage.existMultiple([id])
    expect(exists.get(id)).toBe(true)
    expect(await storage.exist(id)).toBe(true)

    await storage.delete([id])

    exists = await storage.existMultiple([id])
    expect(await storage.exist(id)).toBe(false)
    expect(exists.get(id)).toBe(false)
  })

  it(`When multiple content is stored, then multiple content exist`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id2, bufferToStream(content2))
    expect((await storage.existMultiple([id, id2, 'notStored'])).entries()).toEqual(
      new Map([
        [id, true],
        [id2, true],
        ['notStored', false]
      ]).entries()
    )
  })

  it(`When multiple content is stored, then multiple content is correct`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id2, bufferToStream(content2))

    await retrieveAndExpectStoredContentToBe(id, content)
    await retrieveAndExpectStoredContentToBe(id2, content2)
  })

  it(`When content is deleted, then the correct contented is deleted`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id2, bufferToStream(content2))
    await storage.delete([id2])
    expect(await storage.exist(id)).toBeTruthy()
    expect(await storage.exist(id2)).toBeFalsy()
    await retrieveAndExpectStoredContentToBe(id, content)
  })

  it(`When a content with bad compression ratio is stored and compressed, then it is not stored compressed`, async () => {
    await storage.storeStreamAndCompress(id, bufferToStream(content))
    const retrievedContent = await storage.retrieve(id)
    expect(retrievedContent?.encoding).toBeNull()
    expect(await streamToBuffer(await retrievedContent!.asStream())).toEqual(content)
  })

  it(`When a content with good compression ratio is stored and compressed, then it is stored compressed`, async () => {
    const goodCompresstionRatioContent = Buffer.from(new Uint8Array(100).fill(0))
    await storage.storeStreamAndCompress(id, bufferToStream(goodCompresstionRatioContent))
    const retrievedContent = await storage.retrieve(id)
    expect(retrievedContent?.encoding).toBe('gzip')
    expect(await streamToBuffer(await retrievedContent!.asStream())).toEqual(goodCompresstionRatioContent)
  })

  async function retrieveAndExpectStoredContentToBe(idToRetrieve: string, expectedContent: Buffer) {
    const retrievedContent = await storage.retrieve(idToRetrieve)
    expect(await streamToBuffer(await retrievedContent!.asStream())).toEqual(expectedContent)
  }
})
