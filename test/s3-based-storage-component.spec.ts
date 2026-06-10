import path from 'path'
import {
  createAwsS3BasedFileSystemContentStorage,
  createS3BasedFileSystemContentStorage,
  IContentStorageComponent
} from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import { FileSystemUtils as fsu } from './file-system-utils'
import AWSMock from 'mock-aws-s3'
import { Readable } from 'stream'
import { once } from 'events'
import { createLogComponent } from '@well-known-components/logger'
import { createConfigComponent } from '@well-known-components/env-config-provider'

describe('S3 Storage using ', () => {
  it('creates storage with right config', async () => {
    await expect(
      createAwsS3BasedFileSystemContentStorage(
        {
          config: createConfigComponent({ AWS_REGION: 'eu-west-1' }),
          logs: await createLogComponent({})
        },
        'some-bucket'
      )
    ).resolves.toBeDefined()
  })
})

describe('S3 Storage', () => {
  let storage: IContentStorageComponent
  let id: string
  let content: Buffer
  let id2: string
  let content2: Buffer

  beforeEach(async () => {
    const root = fsu.createTempDirectory()
    AWSMock.config.basePath = path.join(root, 'buckets') // Can configure a basePath for your local buckets
    const s3 = new AWSMock.S3({
      params: { Bucket: 'example' }
    })
    const logs = await createLogComponent({})
    storage = await createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: 'example' })

    id = 'some-id'
    content = Buffer.from('123')
    id2 = 'another-id'
    content2 = Buffer.from('456')
  })

  it(`When content is stored, then it can be retrieved`, async () => {
    await storage.storeStream(id, bufferToStream(content))

    await retrieveAndExpectStoredContentToBe(id, content)
  })

  it(`When a large multi-chunk stream is stored, then the full content is preserved across the MIME-detection boundary`, async () => {
    // Larger than the 4100-byte MIME-detection head, delivered in 1KB chunks so the head spans
    // several chunks. Exercises the peek-and-restream path that avoids buffering the whole file.
    const largeContent = Buffer.alloc(10000, 7)
    const chunks: Buffer[] = []
    for (let offset = 0; offset < largeContent.length; offset += 1000) {
      chunks.push(largeContent.subarray(offset, offset + 1000))
    }

    await storage.storeStream('large-id', Readable.from(chunks))

    await retrieveAndExpectStoredContentToBe('large-id', largeContent)
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
    expect(Array.from((await storage.existMultiple([id, id2, 'notStored'])).entries())).toEqual([
      [id, true],
      [id2, true],
      ['notStored', false]
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

  it(`When attempting to retrieve content by nonexistent key, then it is returns undefined`, async () => {
    await storage.storeStreamAndCompress(id, bufferToStream(content))
    const retrievedContent = await storage.retrieve('saraza')
    expect(retrievedContent?.encoding).toBeUndefined()
  })

  // Note: mock-aws-s3 does not support the Range parameter, so stream content
  // assertions are skipped. These tests verify our size calculation and validation logic.

  it(`When a range is requested on a non-existent key, then it returns undefined`, async () => {
    const item = await storage.retrieve('non-existent', { start: 0, end: 4 })
    expect(item).toBeUndefined()
  })

  it(`When a single-byte range is requested, then it returns correct size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 4, end: 4 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(1)
  })

  it(`When content is stored, then a range retrieve returns correct size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 0, end: 4 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(5)
  })

  it(`When content is stored, then a range in the middle returns correct size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 7, end: 11 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(5)
  })

  it(`When a range with end beyond file size is requested, then it clamps to file size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 7, end: 999 })
    expect(item).toBeDefined()
    expect(item!.size).toBe(6)
  })

  it(`When a range with start > end is requested, then it throws a RangeError`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await expect(storage.retrieve(id, { start: 5, end: 2 })).rejects.toThrow(RangeError)
  })

  it(`When a range with negative start is requested, then it throws a RangeError`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await expect(storage.retrieve(id, { start: -1, end: 2 })).rejects.toThrow(RangeError)
  })

  it(`When a range with start past end of file is requested, then it throws a RangeError`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await expect(storage.retrieve(id, { start: 10, end: 20 })).rejects.toThrow(RangeError)
  })

  async function retrieveAndExpectStoredContentToBe(idToRetrieve: string, expectedContent: Buffer) {
    const retrievedContent = await storage.retrieve(idToRetrieve)
    expect(await streamToBuffer(await retrievedContent!.asStream())).toEqual(expectedContent)
  }

  it(`When content exists, then it is possible to iterate over all keys in storage`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id2, bufferToStream(content2))

    async function check(prefix: string, expected: string[]) {
      const filtered = []
      for await (const key of await storage.allFileIds(prefix)) {
        filtered.push(key)
      }
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
    await storage.storeStream(id, bufferToStream(content))

    const exists = await storage.fileInfoMultiple([id])

    expect(exists.get(id)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(await storage.fileInfo(id)).toEqual({ encoding: null, size: 3, contentSize: 3 })

    expect(await storage.fileInfo('non-existent-id')).toBeUndefined()
  })

  it(`When multiple files exist, then fileInfoMultiple returns correct results for existing and non-existing keys`, async () => {
    await storage.storeStream(id, bufferToStream(content))
    await storage.storeStream(id2, bufferToStream(content2))

    const result = await storage.fileInfoMultiple([id, id2, 'non-existent'])
    expect(result.get(id)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(result.get(id2)).toEqual({ encoding: null, size: 3, contentSize: 3 })
    expect(result.get('non-existent')).toBeUndefined()
  })
})

describe('S3 Storage MIME type detection', () => {
  let storage: IContentStorageComponent
  let uploadSpy: jest.SpyInstance

  // Each payload is padded well beyond the detection window so the test proves the type is
  // detected from the head alone, without buffering the whole file.
  const padding = Buffer.alloc(8192, 0)
  const png = Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, 0x49, 0x48, 0x44, 0x52]),
    padding
  ])
  const jpeg = Buffer.concat([Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0x10, 0x4a, 0x46, 0x49, 0x46, 0]), padding])
  const glb = Buffer.concat([Buffer.from('glTF'), Buffer.from([2, 0, 0, 0, 0x10, 0, 0, 0]), padding])
  const gltfJson = Buffer.from(JSON.stringify({ asset: { version: '2.0' } }))

  beforeEach(async () => {
    const root = fsu.createTempDirectory()
    AWSMock.config.basePath = path.join(root, 'buckets')
    const s3 = new AWSMock.S3({ params: { Bucket: 'example' } })
    uploadSpy = jest.spyOn(s3, 'upload')
    const logs = await createLogComponent({})
    storage = await createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: 'example' })
  })

  const uploadedContentType = (): string => uploadSpy.mock.calls[0][0].ContentType

  it(`When a PNG larger than the detection window is stored, then it is uploaded as image/png`, async () => {
    await storage.storeStream('png-id', bufferToStream(png))

    expect(uploadedContentType()).toBe('image/png')
  })

  it(`When a JPEG larger than the detection window is stored, then it is uploaded as image/jpeg`, async () => {
    await storage.storeStream('jpeg-id', bufferToStream(jpeg))

    expect(uploadedContentType()).toBe('image/jpeg')
  })

  it(`When a binary glTF (GLB) larger than the detection window is stored, then it is uploaded as model/gltf-binary`, async () => {
    await storage.storeStream('glb-id', bufferToStream(glb))

    expect(uploadedContentType()).toBe('model/gltf-binary')
  })

  it(`When a text-based glTF (JSON) is stored, then it falls back to application/octet-stream`, async () => {
    await storage.storeStream('gltf-id', bufferToStream(gltfJson))

    expect(uploadedContentType()).toBe('application/octet-stream')
  })
})

describe('S3 Storage edge cases', () => {
  it(`When a file has ContentLength 0, then fileInfo returns size 0 instead of null`, async () => {
    const headObjectResponse = { ETag: '"abc"', ContentLength: 0, ContentEncoding: undefined }
    const mockS3 = {
      headObject: jest.fn().mockReturnValue({ promise: () => Promise.resolve(headObjectResponse) }),
      upload: jest.fn().mockReturnValue({ promise: () => Promise.resolve() }),
      getObject: jest.fn().mockReturnValue({ createReadStream: () => bufferToStream(Buffer.alloc(0)) }),
      deleteObjects: jest.fn().mockReturnValue({ promise: () => Promise.resolve() }),
      listObjectsV2: jest.fn().mockReturnValue({ promise: () => Promise.resolve({ Contents: [], IsTruncated: false }) })
    }
    const logs = await createLogComponent({})
    const storage = await createS3BasedFileSystemContentStorage({ logs }, mockS3 as any, { Bucket: 'test' })

    const info = await storage.fileInfo('empty-file')
    expect(info).toEqual({ encoding: null, size: 0, contentSize: 0 })
  })

  it(`When headObject returns no ContentLength, then a range retrieve returns null size`, async () => {
    const headObjectResponse = { ETag: '"abc"', ContentEncoding: undefined }
    const mockS3 = {
      headObject: jest.fn().mockReturnValue({ promise: () => Promise.resolve(headObjectResponse) }),
      upload: jest.fn().mockReturnValue({ promise: () => Promise.resolve() }),
      getObject: jest.fn().mockReturnValue({ createReadStream: () => bufferToStream(Buffer.from('Hello')) }),
      deleteObjects: jest.fn().mockReturnValue({ promise: () => Promise.resolve() }),
      listObjectsV2: jest.fn().mockReturnValue({ promise: () => Promise.resolve({ Contents: [], IsTruncated: false }) })
    }
    const logs = await createLogComponent({})
    const storage = await createS3BasedFileSystemContentStorage({ logs }, mockS3 as any, { Bucket: 'test' })

    const item = await storage.retrieve('some-file', { start: 0, end: 4 })
    expect(item).toBeDefined()
    expect(item!.size).toBeNull()
  })

  it(`When the upload fails, then the source stream is released`, async () => {
    const mockS3 = {
      headObject: jest.fn().mockReturnValue({ promise: () => Promise.resolve({}) }),
      upload: jest.fn().mockReturnValue({ promise: () => Promise.reject(new Error('upload failed')) }),
      getObject: jest.fn(),
      deleteObjects: jest.fn().mockReturnValue({ promise: () => Promise.resolve() }),
      listObjectsV2: jest.fn().mockReturnValue({ promise: () => Promise.resolve({ Contents: [], IsTruncated: false }) })
    }
    const logs = await createLogComponent({})
    const storage = await createS3BasedFileSystemContentStorage({ logs }, mockS3 as any, { Bucket: 'test' })

    // Two chunks so the body still has unread data after the head is peeked.
    const source = Readable.from([Buffer.alloc(5000, 1), Buffer.alloc(5000, 1)])
    const closed = once(source, 'close')

    await expect(storage.storeStream('fail-id', source)).rejects.toThrow('upload failed')
    await closed

    expect(source.destroyed).toBe(true)
  })
})

describe('S3 Storage retrieve error logging', () => {
  function createSpyLogs() {
    const logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    return { logs: { getLogger: () => logger }, logger }
  }

  async function retrieveWithHeadError(headError: any) {
    const { logs, logger } = createSpyLogs()
    const mockS3 = {
      headObject: jest.fn().mockReturnValue({ promise: () => Promise.reject(headError) }),
      getObject: jest.fn(),
      upload: jest.fn(),
      deleteObjects: jest.fn(),
      listObjectsV2: jest.fn()
    }
    const storage = await createS3BasedFileSystemContentStorage({ logs } as any, mockS3 as any, { Bucket: 'test' })
    const result = await storage.retrieve('some-key')
    return { result, logger }
  }

  it(`When headObject returns 403 Forbidden, then it warns with the key and does not error`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error(), { code: 'Forbidden', statusCode: 403 })
    )

    expect(result).toBeUndefined()
    expect(logger.error).not.toHaveBeenCalled()
    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(logger.warn.mock.calls[0][1]).toMatchObject({ key: 'some-key', statusCode: 403 })
  })

  it(`When headObject returns NotFound, then it logs nothing and returns undefined`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error(), { code: 'NotFound', statusCode: 404 })
    )

    expect(result).toBeUndefined()
    expect(logger.warn).not.toHaveBeenCalled()
    expect(logger.error).not.toHaveBeenCalled()
  })

  it(`When headObject fails with a non-403 error, then it logs an error with the key`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error('boom'), { code: 'InternalError', statusCode: 500 })
    )

    expect(result).toBeUndefined()
    expect(logger.warn).not.toHaveBeenCalled()
    expect(logger.error).toHaveBeenCalledTimes(1)
    expect(logger.error.mock.calls[0][1]).toMatchObject({ key: 'some-key', code: 'InternalError' })
  })
})
