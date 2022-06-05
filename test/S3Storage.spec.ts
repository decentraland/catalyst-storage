import { createReadStream, readFileSync } from "fs"
import path from "path"
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  createS3BasedFileSystemContentStorage,
  FolderBasedContentStorage,
  IContentStorageComponent,
} from "../src"
import { bufferToStream, streamToBuffer } from "../src/content-item"
import { FileSystemUtils as fsu } from "./FileSystemUtils"
var AWSMock = require("mock-aws-s3")

describe("S3 Storage", () => {
  let storage: IContentStorageComponent
  let id: string
  let content: Buffer
  let id2: string
  let content2: Buffer

  it("starts the env", async () => {
    const root = fsu.createTempDirectory()
    const contentFolder = path.join(root, "buckets")
    AWSMock.config.basePath = contentFolder // Can configure a basePath for your local buckets
    var s3 = AWSMock.S3({
      params: { Bucket: "example" },
    })
    storage = await createS3BasedFileSystemContentStorage({}, s3, { Bucket: "example" })

    id = "some-id"
    content = Buffer.from("123")
    id2 = "another-id"
    content2 = Buffer.from("456")
  })

  describe("Buffer utils", () => {
    it("unit test small", async () => {
      const b = Buffer.from("123")
      const s = bufferToStream(b)
      expect(await streamToBuffer(s)).toEqual(b)
    })
    it("unit test small uses buffer", async () => {
      const b = Buffer.from("123")
      for await (const chunk of bufferToStream(b)) {
        expect(Buffer.isBuffer(chunk)).toBe(true)
      }
    })
    it("streamToBuffer package.json", async () => {
      const stream = createReadStream(__filename)
      const raw = readFileSync(__filename)
      expect(await streamToBuffer(stream)).toEqual(raw)
    })
    it("streamToBuffer package.json uses buffer", async () => {
      for await (const chunk of createReadStream(__filename)) {
        expect(Buffer.isBuffer(chunk)).toBe(true)
      }
    })
    it("unit test big", async () => {
      const b = Buffer.from(new Uint8Array(1000000).fill(0))
      const s = bufferToStream(b)
      expect(await streamToBuffer(s)).toEqual(b)
    })
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
    const newContent = Buffer.from("456")

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
    expect(Array.from((await storage.existMultiple([id, id2, "notStored"])).entries())).toEqual([
      [id, true],
      [id2, true],
      ["notStored", false],
    ])
  })

  it(`When multiple content is stored, then multiple content is correct`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id2, bufferToStream(content2))

    await retrieveAndExpectStoredContentToBe(id, content)
    await retrieveAndExpectStoredContentToBe(id2, content2)
  })

  it(`When a content with bad compression ratio is stored and compressed, then it is not stored compressed`, async () => {
    await storage.storeStreamAndCompress(id, bufferToStream(content))
    const retrievedContent = await storage.retrieve(id)
    expect(retrievedContent?.encoding).toBeNull()
    expect(await streamToBuffer(await retrievedContent!.asStream())).toEqual(content)
  })

  async function retrieveAndExpectStoredContentToBe(idToRetrieve: string, expectedContent: Buffer) {
    const retrievedContent = await storage.retrieve(idToRetrieve)
    expect(await streamToBuffer(await retrievedContent!.asStream())).toEqual(expectedContent)
  }
})
