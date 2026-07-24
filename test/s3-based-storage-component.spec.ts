import {
  createAwsS3BasedFileSystemContentStorage,
  createS3BasedFileSystemContentStorage,
  IContentStorageComponent
} from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import { createFakeS3Client, FakeS3Client } from './fake-s3-client'
import { Readable } from 'stream'
import { Upload } from '@aws-sdk/lib-storage'
import { PutObjectCommand } from '@aws-sdk/client-s3'
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
    const logs = await createLogComponent({})
    storage = await createS3BasedFileSystemContentStorage({ logs }, createFakeS3Client(), { Bucket: 'example' })

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

  // The in-memory double honors Range, so these assert the served BYTES as well as the computed
  // size — content assertions the previous v2 emulator could not support.

  it(`When a range is requested on a non-existent key, then it returns undefined`, async () => {
    const item = await storage.retrieve('non-existent', { start: 0, end: 4 })
    expect(item).toBeUndefined()
  })

  it(`When a single-byte range is requested, then it returns correct size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 4, end: 4 })
    expect(item!.size).toBe(1)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('o'))
  })

  it(`When content is stored, then a range retrieve returns correct size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 0, end: 4 })
    expect(item!.size).toBe(5)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('Hello'))
  })

  it(`When content is stored, then a range in the middle returns correct size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 7, end: 11 })
    expect(item!.size).toBe(5)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('World'))
  })

  it(`When a range with end beyond file size is requested, then it clamps to file size`, async () => {
    const data = Buffer.from('Hello, World!')
    await storage.storeStream(id, bufferToStream(data))

    const item = await storage.retrieve(id, { start: 7, end: 999 })
    expect(item!.size).toBe(6)
    expect(await streamToBuffer(await item!.asStream())).toEqual(Buffer.from('World!'))
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
  let fakeS3: FakeS3Client

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
    fakeS3 = createFakeS3Client()
    const logs = await createLogComponent({})
    storage = await createS3BasedFileSystemContentStorage({ logs }, fakeS3, { Bucket: 'example' })
  })

  const uploadedContentType = (key: string): string => fakeS3.objects.get(key)!.contentType!

  it(`When a PNG larger than the detection window is stored, then it is uploaded as image/png`, async () => {
    await storage.storeStream('png-id', bufferToStream(png))

    expect(uploadedContentType('png-id')).toBe('image/png')
  })

  it(`When a JPEG larger than the detection window is stored, then it is uploaded as image/jpeg`, async () => {
    await storage.storeStream('jpeg-id', bufferToStream(jpeg))

    expect(uploadedContentType('jpeg-id')).toBe('image/jpeg')
  })

  it(`When a binary glTF (GLB) larger than the detection window is stored, then it is uploaded as model/gltf-binary`, async () => {
    await storage.storeStream('glb-id', bufferToStream(glb))

    expect(uploadedContentType('glb-id')).toBe('model/gltf-binary')
  })

  it(`When a text-based glTF (JSON) is stored, then it falls back to application/octet-stream`, async () => {
    await storage.storeStream('gltf-id', bufferToStream(gltfJson))

    expect(uploadedContentType('gltf-id')).toBe('application/octet-stream')
  })
})

describe('S3 Storage edge cases', () => {
  it(`When a file has ContentLength 0, then fileInfo returns size 0 instead of null`, async () => {
    const fake = createFakeS3Client()
    fake.on('HeadObjectCommand', () => ({ ETag: '"abc"', ContentLength: 0, ContentEncoding: undefined }))
    const logs = await createLogComponent({})
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'test' })

    const info = await storage.fileInfo('empty-file')
    expect(info).toEqual({ encoding: null, size: 0, contentSize: 0 })
  })

  it(`When headObject returns no ContentLength, then a range retrieve returns null size`, async () => {
    const fake = createFakeS3Client()
    fake.on('HeadObjectCommand', () => ({ ETag: '"abc"', ContentEncoding: undefined }))
    fake.on('GetObjectCommand', () => ({ Body: bufferToStream(Buffer.from('Hello')) }))
    const logs = await createLogComponent({})
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'test' })

    const item = await storage.retrieve('some-file', { start: 0, end: 4 })
    expect(item).toBeDefined()
    expect(item!.size).toBeNull()
  })

  it(`When the upload fails, then the source stream is released`, async () => {
    const fake = createFakeS3Client()
    const uploadFailure = () => {
      throw new Error('upload failed')
    }
    // lib-storage may take either the single-part or multipart route depending on buffering.
    fake.on('PutObjectCommand', uploadFailure)
    fake.on('CreateMultipartUploadCommand', uploadFailure)
    const logs = await createLogComponent({})
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'test' })

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
    const fake = createFakeS3Client()
    fake.on('HeadObjectCommand', () => {
      throw headError
    })
    const storage = await createS3BasedFileSystemContentStorage({ logs } as any, fake, { Bucket: 'test' })
    const result = await storage.retrieve('some-key')
    return { result, logger }
  }

  it(`When headObject returns 403 Forbidden, then it warns with the key and does not error`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error(), { name: 'Forbidden', $metadata: { httpStatusCode: 403 } })
    )

    expect(result).toBeUndefined()
    expect(logger.error).not.toHaveBeenCalled()
    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(logger.warn.mock.calls[0][1]).toMatchObject({ key: 'some-key', statusCode: 403 })
  })

  it(`When headObject returns NotFound, then it logs nothing and returns undefined`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error(), { name: 'NotFound', $metadata: { httpStatusCode: 404 } })
    )

    expect(result).toBeUndefined()
    expect(logger.warn).not.toHaveBeenCalled()
    expect(logger.error).not.toHaveBeenCalled()
  })

  it(`When headObject fails with a non-403 error, then it logs an error with the key`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error('boom'), { name: 'InternalError', $metadata: { httpStatusCode: 500 } })
    )

    expect(result).toBeUndefined()
    expect(logger.warn).not.toHaveBeenCalled()
    expect(logger.error).toHaveBeenCalledTimes(1)
    expect(logger.error.mock.calls[0][1]).toMatchObject({ key: 'some-key', code: 'InternalError' })
  })
})

describe('S3 Storage response bodies', () => {
  it(`When GetObject returns no body, then reading the item fails with an actionable error`, async () => {
    // `Body` is optional in the v3 types, so a misconfigured client (or a non-Node runtime shape)
    // could hand back something that is not a stream. Casting it would push the failure out to a
    // consumer; this keeps it at the boundary with a message naming what arrived.
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    await fake.send(new PutObjectCommand({ Bucket: 'example', Key: 'bodiless-id', Body: Buffer.from('x') }))
    fake.on('GetObjectCommand', () => ({ ContentLength: 1 }))
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })

    const item = await storage.retrieve('bodiless-id')

    await expect(item!.asStream()).rejects.toThrow(/no readable body for bodiless-id; received undefined/)
  })

  it(`When GetObject returns a non-stream body, then the error names what arrived`, async () => {
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    await fake.send(new PutObjectCommand({ Bucket: 'example', Key: 'blob-id', Body: Buffer.from('x') }))
    fake.on('GetObjectCommand', () => ({ Body: new Date(), ContentLength: 1 }))
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })

    const item = await storage.retrieve('blob-id')

    await expect(item!.asStream()).rejects.toThrow(/received a Date/)
  })
})

describe('S3 Storage upload cancellation', () => {
  it(`When the signal aborts while the upload is in flight, then it is torn down and the call rejects with the reason`, async () => {
    // The fake honors abortSignal like the real client, so this drives lib-storage's genuine
    // Upload.abort() path: the store must reject with the caller's reason and commit nothing.
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    const inFlight = fake.hang('PutObjectCommand')
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
    const controller = new AbortController()
    const reason = new Error('deployment deadline exceeded')

    // Larger than the MIME-detection window, so the store is genuinely mid-upload when aborted.
    const pending = storage.storeStream('wedged-id', bufferToStream(Buffer.alloc(5000, 1)), controller.signal)
    await inFlight.started
    controller.abort(reason)

    await expect(pending).rejects.toBe(reason)
    expect(fake.objects.has('wedged-id')).toBe(false)
    // Asserting absence before the request settles would pass even if the abort had only stopped
    // lib-storage's own loop: release the transport and let it run to where a surviving request
    // would have written the object.
    inFlight.release()
    await new Promise((resolve) => setImmediate(resolve))
    expect(fake.objects.has('wedged-id')).toBe(false)
    inFlight.release()
  })

  it(`When a real failure races an abort we caused, then lib-storage's abort race still reports the reason`, async () => {
    // Documents an SDK-level limitation, not our attribution: `Upload.done()` races the upload
    // against an abort watcher that rejects with lib-storage's own AbortError the instant abort()
    // fires, so a genuine failure arriving in that window is discarded ABOVE this layer and cannot
    // be recovered here. Our attribution then correctly reports the caller's reason.
    const logs = await createLogComponent({})
    const accessDenied = Object.assign(new Error('AccessDenied'), { name: 'AccessDenied' })
    const fake = createFakeS3Client()
    let started: () => void = () => undefined
    const uploadStarted = new Promise<void>((resolve) => (started = resolve))
    fake.on('PutObjectCommand', async () => {
      started()
      await new Promise<void>((resolve) => setTimeout(resolve, 10))
      throw accessDenied
    })
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
    const controller = new AbortController()

    const reason = new Error('cancelled while the upload was failing')
    const pending = storage.storeStream('racing-fault-id', bufferToStream(Buffer.alloc(5000, 1)), controller.signal)
    await uploadStarted
    controller.abort(reason)

    await expect(pending).rejects.toBe(reason)
    expect(fake.objects.has('racing-fault-id')).toBe(false)
  })

  it(`When the abort lands during the head read, then no upload command is ever sent`, async () => {
    // The checkpoint before the upload is created must stop the store outright.
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    const sent: string[] = []
    for (const command of ['PutObjectCommand', 'CreateMultipartUploadCommand']) {
      fake.on(command, () => {
        sent.push(command)
        return { ETag: '"x"', UploadId: 'u' }
      })
    }
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
    const controller = new AbortController()
    const reason = new Error('cancelled during the head read')
    const source = bufferToStream(Buffer.alloc(5000, 1))
    // Aborts in the same event as the head read completing, before any upload exists.
    source.on('end', () => controller.abort(reason))

    await expect(storage.storeStream('head-abort-id', source, controller.signal)).rejects.toBe(reason)
    expect(sent).toEqual([])
  })

  it(`When the abort lands mid-PutObject, then the request is torn down and the key never appears`, async () => {
    // lib-storage issues `client.send(new PutObjectCommand(...))` with NO request options, so its
    // own abort() only wins the race inside done() while that HTTP request keeps going — it could
    // still commit the object after this store had already rejected as cancelled. This asserts the
    // request itself is cancelled: the held transport is released afterwards and given time to run,
    // and the commit handler must never be reached.
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    let commitsAttempted = 0
    const inFlight = fake.hang('PutObjectCommand')
    fake.on('PutObjectCommand', (input: any) => {
      commitsAttempted++
      fake.objects.set(input.Key, { body: Buffer.alloc(0) })
      return { ETag: '"x"' }
    })
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
    const controller = new AbortController()
    const reason = new Error('cancelled mid-put')

    const pending = storage.storeStream('mid-put-id', bufferToStream(Buffer.alloc(5000, 1)), controller.signal)
    await inFlight.started
    controller.abort(reason)

    await expect(pending).rejects.toBe(reason)
    // Release the held request and drain: a request that was not really aborted would reach its
    // handler here and write the key.
    inFlight.release()
    await new Promise((resolve) => setImmediate(resolve))
    await new Promise((resolve) => setImmediate(resolve))

    expect(commitsAttempted).toEqual(0)
    expect(fake.objects.has('mid-put-id')).toBe(false)
  })

  it(`When the abort lands mid-multipart, then no part commits and the multipart upload is still cleaned up`, async () => {
    // The same hole exists on the multipart path (UploadPart and CompleteMultipartUpload are sent
    // without request options), with one extra requirement: the AbortMultipartUpload that removes
    // the uploaded parts must NOT be cancelled by the very signal that triggered it, or the parts
    // accumulate and are billed until a lifecycle rule reaps them.
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    let completes = 0
    let cleanups = 0
    fake.on('CompleteMultipartUploadCommand', () => {
      completes++
      return { ETag: '"x"' }
    })
    fake.on('AbortMultipartUploadCommand', () => {
      cleanups++
      return {}
    })
    const inFlight = fake.hang('UploadPartCommand')
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
    const controller = new AbortController()
    const reason = new Error('cancelled mid-multipart')

    // Larger than lib-storage's 5MB minimum part size, so this is a genuine multipart upload.
    const pending = storage.storeStream('mid-part-id', bufferToStream(Buffer.alloc(12 << 20, 1)), controller.signal)
    await inFlight.started
    controller.abort(reason)

    await expect(pending).rejects.toBe(reason)
    inFlight.release()
    await new Promise((resolve) => setImmediate(resolve))
    await new Promise((resolve) => setImmediate(resolve))

    expect(completes).toEqual(0)
    expect(fake.objects.has('mid-part-id')).toBe(false)
    expect(cleanups).toBeGreaterThan(0)
  })

  it(`When Upload.abort() throws, then the store is still cancelled and nothing is committed`, async () => {
    // Teardown is best-effort: a throwing abort() must not escape the signal's event dispatch. It
    // must also not leave the upload running — the transport is torn down through the abort
    // controller this storage owns, which does not depend on Upload.abort() working at all.
    const logs = await createLogComponent({})
    const abortSpy = jest.spyOn(Upload.prototype, 'abort').mockImplementation(() => {
      throw new Error('abort exploded')
    })
    try {
      const fake = createFakeS3Client()
      const inFlight = fake.hang('PutObjectCommand')
      const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
      const controller = new AbortController()
      const reason = new Error('cancelled with a broken abort')

      const pending = storage.storeStream('broken-abort-id', bufferToStream(Buffer.alloc(5000, 1)), controller.signal)
      await inFlight.started
      controller.abort(reason)
      // Released so a request that survived the abort would run to completion and commit.
      inFlight.release()

      await expect(pending).rejects.toBe(reason)
      expect(abortSpy).toHaveBeenCalled()
      expect(fake.objects.has('broken-abort-id')).toBe(false)
    } finally {
      abortSpy.mockRestore()
    }
  })

  it(`When the upload fails and the source destroy throws, then the upload error is preserved`, async () => {
    const logs = await createLogComponent({})
    const uploadFault = Object.assign(new Error('AccessDenied'), { name: 'AccessDenied' })
    let uploadRejected = false
    const fake = createFakeS3Client()
    fake.on('PutObjectCommand', () => {
      uploadRejected = true
      throw uploadFault
    })
    const storage = await createS3BasedFileSystemContentStorage({ logs }, fake, { Bucket: 'example' })
    const source = bufferToStream(Buffer.from('some content'))
    // Throw only for the cleanup destroy after the upload failed: an unconditional override would
    // also break the async-iterator teardown the head read performs before the upload exists.
    const realDestroy = source.destroy.bind(source)
    source.destroy = ((...args: unknown[]) => {
      if (uploadRejected) {
        throw new Error('destroy exploded')
      }
      return (realDestroy as (...a: unknown[]) => Readable)(...args)
    }) as typeof source.destroy

    await expect(storage.storeStream('destroy-throws-id', source)).rejects.toBe(uploadFault)
  })
})
