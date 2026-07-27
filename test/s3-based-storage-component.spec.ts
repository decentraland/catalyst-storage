import {
  createAwsS3BasedFileSystemContentStorage,
  createS3BasedFileSystemContentStorage,
  FileTypeLoader,
  IContentStorageComponent
} from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import { createFakeS3Client, FakeS3Client } from './fake-s3-client'
import { Readable } from 'stream'
import { Upload } from '@aws-sdk/lib-storage'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { createLogComponent } from '@well-known-components/logger'
import { createConfigComponent } from '@well-known-components/env-config-provider'

/**
 * The real detector is ESM-only and reached through a dynamic import that Jest's module registry
 * does not own, so once ANY test file's environment is torn down it fails for every file after it —
 * intermittently failing this suite's MIME assertions and the run's exit code. Every construction
 * here therefore injects a detector.
 *
 * What that gives up is `file-type`'s own identification, which is not this repository's to verify.
 * What it keeps is the integration this component owns: that `peekHead` hands the detector the first
 * bytes of the content even when the body is far larger than the detection window, and that the type
 * it returns is what gets stored.
 */
const undetectingLoader: FileTypeLoader = async () => ({ fileTypeFromBuffer: async () => undefined })

const createStorage = (
  components: Parameters<typeof createS3BasedFileSystemContentStorage>[0],
  client: Parameters<typeof createS3BasedFileSystemContentStorage>[1],
  options: Parameters<typeof createS3BasedFileSystemContentStorage>[2]
) => createS3BasedFileSystemContentStorage(components, client, { fileTypeLoader: undetectingLoader, ...options })

const createAwsStorage = (
  components: Parameters<typeof createAwsS3BasedFileSystemContentStorage>[0],
  bucket: string,
  options?: Parameters<typeof createAwsS3BasedFileSystemContentStorage>[2]
) => createAwsS3BasedFileSystemContentStorage(components, bucket, { fileTypeLoader: undetectingLoader, ...options })

/**
 * Identifies the three formats this suite stores, from the head it is handed. Stands in for
 * `file-type` so the assertions stay about THIS component's contribution: that the detector receives
 * the leading bytes of content far larger than the detection window, and that what it returns is what
 * ends up on the object.
 */
const magicByteLoader: FileTypeLoader = async () => ({
  fileTypeFromBuffer: async (buffer: Uint8Array) => {
    const head = Buffer.from(buffer)
    if (head.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) {
      return { mime: 'image/png' }
    }
    if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff) return { mime: 'image/jpeg' }
    if (head.subarray(0, 4).toString('utf8') === 'glTF') return { mime: 'model/gltf-binary' }
    return undefined
  }
})

describe('when the AWS factory builds its own client', () => {
  let send: jest.SpyInstance

  beforeEach(() => {
    // Stubbed at the prototype, because this factory constructs the client internally and there is
    // nothing to inject. Unstubbed, construction issued the startup ListBucket probe against the real
    // AWS endpoint.
    send = jest.spyOn(S3Client.prototype, 'send').mockResolvedValue({ Contents: [], IsTruncated: false } as never)
    jest.spyOn(S3Client.prototype, 'destroy').mockImplementation(() => undefined)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  it('should construct a usable storage from the region config alone', async () => {
    const storage = await createAwsStorage(
      { config: createConfigComponent({ AWS_REGION: 'eu-west-1' }), logs: await createLogComponent({}) },
      'some-bucket'
    )

    expect(typeof storage.storeStream).toBe('function')
  })

  it('should verify bucket access before returning', async () => {
    await createAwsStorage(
      { config: createConfigComponent({ AWS_REGION: 'eu-west-1' }), logs: await createLogComponent({}) },
      'some-bucket'
    )

    expect(send.mock.calls.map(([command]) => command.constructor.name)).toContain('ListObjectsV2Command')
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
    storage = await createStorage({ logs }, createFakeS3Client(), { Bucket: 'example' })

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

  // Each payload is padded well past the 4100-byte detection window, so passing proves the type was
  // identified from the HEAD alone while the body streamed through.
  const padding = Buffer.alloc(8192, 0)
  const png = Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, 0x49, 0x48, 0x44, 0x52]),
    padding
  ])
  const jpeg = Buffer.concat([Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0x10, 0x4a, 0x46, 0x49, 0x46, 0]), padding])
  const glb = Buffer.concat([Buffer.from('glTF'), Buffer.from([2, 0, 0, 0, 0x10, 0, 0, 0]), padding])

  beforeEach(async () => {
    fakeS3 = createFakeS3Client()
    storage = await createStorage({ logs: await createLogComponent({}) }, fakeS3, {
      Bucket: 'example',
      fileTypeLoader: magicByteLoader
    })
  })

  describe.each([
    ['a PNG', 'png-id', png, 'image/png'],
    ['a JPEG', 'jpeg-id', jpeg, 'image/jpeg'],
    ['a binary glTF', 'glb-id', glb, 'model/gltf-binary']
  ])('when %s larger than the detection window is stored', (_name, id, payload, expected) => {
    beforeEach(async () => {
      await storage.storeStream(id, bufferToStream(payload))
    })

    it(`should upload it as ${expected}`, () => {
      expect(fakeS3.objects.get(id)!.contentType).toBe(expected)
    })

    it('should still upload every byte of the payload', () => {
      expect(fakeS3.objects.get(id)!.body.equals(payload)).toBe(true)
    })
  })

  describe('when the content has no signature the detector recognizes', () => {
    beforeEach(async () => {
      await storage.storeStream('gltf-id', bufferToStream(Buffer.from(JSON.stringify({ asset: { version: '2.0' } }))))
    })

    it('should fall back to application/octet-stream', () => {
      expect(fakeS3.objects.get('gltf-id')!.contentType).toBe('application/octet-stream')
    })
  })
})

describe('S3 Storage enumeration', () => {
  describe('when a page reports itself truncated but carries no continuation token', () => {
    let storage: IContentStorageComponent
    let requests: number

    beforeEach(async () => {
      requests = 0
      const fake = createFakeS3Client()
      fake.on('ListObjectsV2Command', () => {
        requests++
        return { Contents: [{ Key: 'only-key' }], IsTruncated: true, NextContinuationToken: undefined }
      })
      storage = await createStorage({ logs: await createLogComponent({}) }, fake, {
        Bucket: 'example'
      })
      // Construction probes s3:ListBucket once to decide whether a 403 can mean "absent"; this
      // assertion is about what ENUMERATION issues, so it counts from here.
      requests = 0
    })

    it('should reject rather than completing a partial listing', async () => {
      const listed: string[] = []
      await expect(async () => {
        for await (const each of storage.allFileIds()) listed.push(each)
      }).rejects.toThrow('truncated listing without a continuation token')
      expect(listed).toEqual(['only-key'])
    })

    it('should stop after the page it could not continue from', async () => {
      await expect(async () => {
        for await (const _each of storage.allFileIds()) {
          // drain the iterator so the pagination failure surfaces
        }
      }).rejects.toThrow('This enumeration is incomplete')

      expect(requests).toBe(1)
    })
  })
})

describe('S3 Storage client lifecycle', () => {
  describe('when the factory constructed the client itself', () => {
    let storage: IContentStorageComponent
    let destroy: jest.SpyInstance

    beforeEach(async () => {
      // This factory builds its OWN `S3Client`, so there is nothing to inject: without stubbing the
      // prototype, constructing it reached the real `s3.eu-west-1.amazonaws.com` for the startup
      // ListBucket probe. That passed only because no credentials resolve in CI — on a machine that
      // has them, `some-bucket` answers NoSuchBucket and construction (correctly) refuses to start,
      // failing this test for a reason that has nothing to do with what it checks.
      jest.spyOn(S3Client.prototype, 'send').mockResolvedValue({ Contents: [], IsTruncated: false } as never)
      destroy = jest.spyOn(S3Client.prototype, 'destroy').mockImplementation(() => undefined)
      storage = await createAwsStorage(
        {
          config: createConfigComponent({ AWS_REGION: 'eu-west-1' }),
          logs: await createLogComponent({})
        },
        'some-bucket'
      )
    })

    afterEach(() => {
      jest.restoreAllMocks()
    })

    it('should destroy the client it owns when stopped', async () => {
      // The SDK's guidance is to destroy a client explicitly in Node, or its sockets stay open long
      // after the last request. Asserting that `stop()` RESOLVES proved nothing — it returns
      // `Promise<void>`, so the assertion held with the `destroy()` call deleted.
      await storage.stop?.()

      expect(destroy).toHaveBeenCalledTimes(1)
    })
  })

  describe('when the client is injected by the caller', () => {
    let storage: IContentStorageComponent

    beforeEach(async () => {
      storage = await createStorage({ logs: await createLogComponent({}) }, createFakeS3Client(), {
        Bucket: 'example'
      })
    })

    it('should not take ownership of a client it did not create', () => {
      expect(storage.stop).toBeUndefined()
    })
  })
})

describe('S3 Storage delete', () => {
  let fakeS3: FakeS3Client
  let storage: IContentStorageComponent

  beforeEach(async () => {
    fakeS3 = createFakeS3Client()
    const logs = await createLogComponent({})
    storage = await createStorage({ logs }, fakeS3, { Bucket: 'example' })
  })

  describe('when more ids than one DeleteObjects request accepts are deleted', () => {
    let ids: string[]

    beforeEach(async () => {
      // S3 caps the request at 1000 keys and the SDK does not split the list, so the whole delete
      // used to be rejected as MalformedXML.
      ids = Array.from({ length: 2500 }, (_, index) => `id-${index}`)
      for (const id of ids) await storage.storeStream(id, bufferToStream(Buffer.from(id)))
      await storage.delete(ids)
    })

    it('should remove every object across the chunked requests', () => {
      expect(fakeS3.objects.size).toBe(0)
    })
  })

  describe('when no ids are given', () => {
    it('should resolve without issuing a request S3 would reject', async () => {
      await expect(storage.delete([])).resolves.toBeUndefined()
    })
  })

  describe('when S3 reports per-key failures inside a successful response', () => {
    beforeEach(async () => {
      await storage.storeStream('kept-id', bufferToStream(Buffer.from('x')))
      // DeleteObjects answers 200 with the failures listed in `Errors`, so a resolved send is not a
      // completed delete: ignoring it reported success while the object was still readable.
      fakeS3.on('DeleteObjectsCommand', () => ({
        Deleted: [],
        Errors: [{ Key: 'kept-id', Code: 'AccessDenied' }]
      }))
    })

    it('should reject naming the key that survived', async () => {
      await expect(storage.delete(['kept-id'])).rejects.toThrow(/kept-id \(AccessDenied\)/)
    })
  })
})

describe('S3 Storage content size', () => {
  describe('when the stored object is content-encoded', () => {
    let item: NonNullable<Awaited<ReturnType<IContentStorageComponent['retrieve']>>>

    beforeEach(async () => {
      const fake = createFakeS3Client()
      const logs = await createLogComponent({})
      // `size` is the COMPRESSED length for an encoded object, so defaulting contentSize to it hands
      // callers doing `contentSize ?? size` the wrong number under a field documented as the
      // uncompressed one. S3 stores no uncompressed size, so `null` ("unknown") is the honest answer.
      fake.on('HeadObjectCommand', () => ({ ETag: '"abc"', ContentLength: 133, ContentEncoding: 'gzip' }))
      const storage = await createStorage({ logs }, fake, { Bucket: 'test' })
      item = (await storage.retrieve('encoded-id'))!
    })

    it('should report the stored size', () => {
      expect(item.size).toBe(133)
    })

    it('should report the content size as unknown rather than as the compressed size', () => {
      expect(item.contentSize).toBeNull()
    })

    it('should agree with fileInfo, which reports the same unknown content size', async () => {
      const fake = createFakeS3Client()
      const logs = await createLogComponent({})
      fake.on('HeadObjectCommand', () => ({ ETag: '"abc"', ContentLength: 133, ContentEncoding: 'gzip' }))
      const storage = await createStorage({ logs }, fake, { Bucket: 'test' })

      expect((await storage.fileInfo('encoded-id'))!.contentSize).toBe(item.contentSize)
    })
  })
})

describe('S3 Storage retrieve error logging', () => {
  function createSpyLogs() {
    const logger = { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    return { logs: { getLogger: () => logger }, logger }
  }

  async function storageWithHeadError(headError: any, { canListBucket = true, report403AsAbsent = false } = {}) {
    const { logs, logger } = createSpyLogs()
    const fake = createFakeS3Client()
    const storage = await (async () => {
      // Denying the probe models a principal without s3:ListBucket, for which a missing key genuinely
      // answers 403. It is NO LONGER what decides the read: a 403 `AccessDenied` on the probe is also
      // what a policy scoped to a different (or misspelled) bucket returns, so the lenient mode is the
      // caller's explicit `report403AsAbsent` and the probe only reports what it saw.
      if (!canListBucket) {
        fake.on('ListObjectsV2Command', () => {
          throw Object.assign(new Error('Access Denied'), {
            name: 'AccessDenied',
            $metadata: { httpStatusCode: 403 }
          })
        })
      }
      const created = await createStorage({ logs } as any, fake, { Bucket: 'test', report403AsAbsent })
      fake.on('HeadObjectCommand', () => {
        throw headError
      })
      return created
    })()
    return { storage, logger }
  }

  async function retrieveWithHeadError(
    headError: any,
    options?: { canListBucket?: boolean; report403AsAbsent?: boolean }
  ) {
    const { storage, logger } = await storageWithHeadError(headError, options)
    const result = await storage.retrieve('some-key')
    return { result, logger }
  }

  const forbidden = () => Object.assign(new Error(), { name: 'Forbidden', $metadata: { httpStatusCode: 403 } })

  // Scoped to warnings ABOUT THE KEY: construction also warms the MIME detector, which warns on its
  // own when the ESM loader is unavailable (as it is under Jest). Counting every warning would
  // couple these assertions to an unrelated log line.
  const retrievalWarnings = (logger: { warn: jest.Mock }) =>
    logger.warn.mock.calls.filter((call) => (call[1] as { key?: string })?.key === 'some-key')

  describe('when headObject returns 403 Forbidden', () => {
    // Whether a 403 means "absent" depends on whether a MISSING key would have answered 404 for this
    // principal — which only the operator can state, because no response distinguishes it.
    describe('and the caller has declared that a 403 cannot be told from a missing key', () => {
      it('should report the content as absent and warn with the key', async () => {
        const { result, logger } = await retrieveWithHeadError(forbidden(), {
          canListBucket: false,
          report403AsAbsent: true
        })

        expect(result).toBeUndefined()
        expect(logger.error).not.toHaveBeenCalled()
        expect(retrievalWarnings(logger)).toHaveLength(1)
        expect(retrievalWarnings(logger)[0][1]).toMatchObject({ key: 'some-key', statusCode: 403 })
      })
    })

    describe('and the startup probe was denied but the caller has declared nothing', () => {
      it('should reject rather than report the content as absent', async () => {
        // A 403 `AccessDenied` on the probe is NOT evidence that this principal lacks s3:ListBucket on
        // an existing bucket: a policy scoped to `arn:aws:s3:::my-bucket` denies every OTHER bucket
        // the same way, including a MISSPELLED one, because the implicit deny is evaluated before S3
        // checks that the bucket exists — so the NoSuchBucket guard never fires. Inferring the lenient
        // mode from it turned a one-character typo into a node that answered "absent" for every id for
        // its whole lifetime while its writes rejected loudly.
        const headError = forbidden()
        const { storage } = await storageWithHeadError(headError, { canListBucket: false })

        await expect(storage.retrieve('some-key')).rejects.toBe(headError)
        await expect(storage.exist('some-key')).rejects.toBe(headError)
        await expect(storage.fileInfo('some-key')).rejects.toBe(headError)
      })
    })

    describe('and the startup probe failed with a credential or clock 403', () => {
      let storage: IContentStorageComponent
      let headError: Error

      beforeEach(async () => {
        // `RequestTimeTooSkewed`, `InvalidAccessKeyId` and `SignatureDoesNotMatch` are 403s that say
        // NOTHING about which permissions this principal holds. Crediting them would flip reads into
        // the lenient mode on a startup blip, and the node would then answer "absent" for every id
        // for its whole lifetime — the empty-node failure the probe exists to remove.
        const { logs } = createSpyLogs()
        const fake = createFakeS3Client()
        fake.on('ListObjectsV2Command', () => {
          throw Object.assign(new Error('The difference between the request time and the current time is too large'), {
            name: 'RequestTimeTooSkewed',
            $metadata: { httpStatusCode: 403 }
          })
        })
        storage = await createStorage({ logs } as any, fake, { Bucket: 'test' })
        headError = forbidden()
        fake.on('HeadObjectCommand', () => {
          throw headError
        })
      })

      it('should still reject a 403 read rather than report it as absence', async () => {
        await expect(storage.retrieve('some-key')).rejects.toBe(headError)
      })

      it('should construct rather than refuse over a fault unrelated to permissions', () => {
        expect(storage).toBeDefined()
      })
    })

    describe('and the principal can s3:ListBucket', () => {
      // A missing key answers 404 for this principal, so a 403 can only be a real authorization
      // failure — rotated credentials, clock skew, a revoked policy. Reporting those as absent made
      // the whole node claim it held nothing while its writes rejected loudly.
      it('should reject instead of reporting the content as absent', async () => {
        const headError = forbidden()
        const { storage } = await storageWithHeadError(headError)

        await expect(storage.retrieve('some-key')).rejects.toBe(headError)
      })

      it('should reject exist and fileInfo for the same reason', async () => {
        const headError = forbidden()
        const { storage } = await storageWithHeadError(headError)

        await expect(storage.exist('some-key')).rejects.toBe(headError)
        await expect(storage.fileInfo('some-key')).rejects.toBe(headError)
      })
    })
  })

  it(`When headObject returns NotFound, then it logs nothing about the key and returns undefined`, async () => {
    const { result, logger } = await retrieveWithHeadError(
      Object.assign(new Error(), { name: 'NotFound', $metadata: { httpStatusCode: 404 } })
    )

    expect(result).toBeUndefined()
    expect(retrievalWarnings(logger)).toHaveLength(0)
    expect(logger.error).not.toHaveBeenCalled()
  })

  describe('when headObject fails with something other than a not-found', () => {
    // Reporting a 500, a 503 SlowDown or an unreachable bucket as `undefined` told the caller the
    // content was permanently absent, so a broken node read as an empty one and stopped being
    // retried. The folder-based backend already refuses to do this.
    let storage: IContentStorageComponent
    let logger: ReturnType<typeof createSpyLogs>['logger']
    let headError: Error

    beforeEach(async () => {
      headError = Object.assign(new Error('boom'), { name: 'InternalError', $metadata: { httpStatusCode: 500 } })
      ;({ storage, logger } = await storageWithHeadError(headError))
    })

    it('should reject the retrieve instead of reporting the content as absent', async () => {
      await expect(storage.retrieve('some-key')).rejects.toBe(headError)
    })

    it('should reject exist instead of reporting the content as absent', async () => {
      await expect(storage.exist('some-key')).rejects.toBe(headError)
    })

    it('should reject fileInfo instead of reporting the content as absent', async () => {
      await expect(storage.fileInfo('some-key')).rejects.toBe(headError)
    })

    it('should log the failure with the key and the error code', async () => {
      await storage.retrieve('some-key').catch(() => undefined)

      expect(logger.error.mock.calls[0][1]).toMatchObject({ key: 'some-key', code: 'InternalError' })
    })
  })

  describe('when headObject reports the object as missing', () => {
    let storage: IContentStorageComponent

    beforeEach(async () => {
      ;({ storage } = await storageWithHeadError(
        Object.assign(new Error(), { name: 'NotFound', $metadata: { httpStatusCode: 404 } })
      ))
    })

    it('should report exist as false', async () => {
      await expect(storage.exist('some-key')).resolves.toBe(false)
    })

    it('should report fileInfo as undefined', async () => {
      await expect(storage.fileInfo('some-key')).resolves.toBeUndefined()
    })
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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })

    const item = await storage.retrieve('bodiless-id')

    await expect(item!.asStream()).rejects.toThrow(/no readable body for bodiless-id; received undefined/)
  })

  it(`When GetObject returns a non-stream body, then the error names what arrived`, async () => {
    const logs = await createLogComponent({})
    const fake = createFakeS3Client()
    await fake.send(new PutObjectCommand({ Bucket: 'example', Key: 'blob-id', Body: Buffer.from('x') }))
    fake.on('GetObjectCommand', () => ({ Body: new Date(), ContentLength: 1 }))
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })

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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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
      const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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

  it(`When Upload.abort() rejects asynchronously, then the rejection does not escape`, async () => {
    // `abort()` is declared async and runs fire-and-forget inside the signal's event dispatch, so a
    // rejected promise has nowhere to land: unabsorbed it becomes an unhandled rejection, which
    // terminates the process by default. The real implementation cannot reject (it only calls
    // AbortController.abort()), but a double or a future SDK version can.
    const logs = await createLogComponent({})
    const unhandled: unknown[] = []
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandledRejection)
    const abortSpy = jest.spyOn(Upload.prototype, 'abort').mockImplementation(async () => {
      throw new Error('abort rejected asynchronously')
    })
    try {
      const fake = createFakeS3Client()
      const inFlight = fake.hang('PutObjectCommand')
      const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
      const controller = new AbortController()
      const reason = new Error('cancelled with an async-rejecting abort')

      const pending = storage.storeStream('async-abort-id', bufferToStream(Buffer.alloc(5000, 1)), controller.signal)
      await inFlight.started
      controller.abort(reason)

      // The store still reports the caller's reason: teardown runs through the abort controller,
      // which does not depend on abort() resolving.
      await expect(pending).rejects.toBe(reason)
      inFlight.release()
      // Unhandled rejections surface after the microtask queue drains, so give them room to appear.
      await new Promise((resolve) => setImmediate(resolve))
      await new Promise((resolve) => setImmediate(resolve))

      expect(abortSpy).toHaveBeenCalled()
      expect(unhandled).toEqual([])
      expect(fake.objects.has('async-abort-id')).toBe(false)
    } finally {
      abortSpy.mockRestore()
      process.off('unhandledRejection', onUnhandledRejection)
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
    const storage = await createStorage({ logs }, fake, { Bucket: 'example' })
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

describe('S3 Storage review regressions', () => {
  describe('when the bucket does not exist', () => {
    let fake: FakeS3Client

    beforeEach(() => {
      // A missing or misnamed bucket answers HeadObject with a 404 byte-identical to a missing key,
      // so every id reported absent and nothing was logged — a silently empty node. It is a
      // deployment error, so it has to fail where an operator sees it.
      fake = createFakeS3Client()
      fake.on('ListObjectsV2Command', () => {
        throw Object.assign(new Error('The specified bucket does not exist'), {
          name: 'NoSuchBucket',
          $metadata: { httpStatusCode: 404 }
        })
      })
    })

    it('should refuse to construct rather than report every id as absent', async () => {
      await expect(createStorage({ logs: await createLogComponent({}) }, fake, { Bucket: 'absent' })).rejects.toThrow(
        /Refusing to start/
      )
    })
  })

  describe('when a range is requested on an object stored with a content encoding', () => {
    let storage: IContentStorageComponent

    beforeEach(async () => {
      // The Range header slices the STORED (compressed) bytes, so the item handed back a fragment of
      // a gzip stream that asStream() then failed to inflate — or, for a range starting at 0,
      // inflated into the whole object while advertising the requested length.
      const fake = createFakeS3Client()
      fake.on('HeadObjectCommand', () => ({ ContentLength: 40, ContentEncoding: 'gzip', ETag: '"e"' }))
      storage = await createStorage({ logs: await createLogComponent({}) }, fake, {
        Bucket: 'example'
      })
    })

    it('should reject with an actionable error rather than serve a compressed fragment', async () => {
      await expect(storage.retrieve('encoded-id', { start: 10, end: 20 })).rejects.toThrow(/Content-Encoding/)
    })

    it('should still serve the whole object', async () => {
      await expect(storage.retrieve('encoded-id')).resolves.toBeDefined()
    })
  })
})

describe('S3 Storage multipart cleanup', () => {
  let fake: FakeS3Client
  let storage: IContentStorageComponent
  let aborted: string[]

  beforeEach(async () => {
    // lib-storage only aborts the upload on paths that run BEFORE CompleteMultipartUpload, so a
    // complete request that fails leaves every uploaded part in the bucket, billed indefinitely.
    fake = createFakeS3Client()
    aborted = []
    storage = await createStorage({ logs: await createLogComponent({}) }, fake, {
      Bucket: 'example'
    })
    fake.on('CompleteMultipartUploadCommand', () => {
      throw Object.assign(new Error('InternalError'), { name: 'InternalError', $metadata: { httpStatusCode: 500 } })
    })
    fake.on('AbortMultipartUploadCommand', ({ UploadId }: any) => {
      aborted.push(UploadId)
      return {}
    })
  })

  it('should abort the upload when the complete request fails', async () => {
    await expect(storage.storeStream('big-id', bufferToStream(Buffer.alloc(6 * 1024 * 1024, 7)))).rejects.toBeDefined()

    expect(aborted).toEqual(['upload-big-id'])
  })
})

describe('S3 Storage identity content encoding', () => {
  let storage: IContentStorageComponent
  let size: number

  beforeEach(async () => {
    // `identity` is the RFC 9110 token for NOT encoded. Ranging and decoding already treated it that
    // way; the metadata surfaces did not, so an object an operator or migration tagged with it
    // reported its logical size as unknown while its bytes were plainly readable.
    size = 12
    const fake = createFakeS3Client()
    fake.on('HeadObjectCommand', () => ({ ContentLength: size, ContentEncoding: 'identity', ETag: '"e"' }))
    fake.on('GetObjectCommand', () => ({ Body: bufferToStream(Buffer.from('twelve bytes')), ContentLength: size }))
    storage = await createStorage({ logs: await createLogComponent({}) }, fake, {
      Bucket: 'example'
    })
  })

  it('should report fileInfo contentSize as the known size, not unknown', async () => {
    expect(await storage.fileInfo('id')).toEqual({ encoding: null, size, contentSize: size })
  })

  it('should have retrieve agree with fileInfo about the same id', async () => {
    const item = await storage.retrieve('id')

    expect({ encoding: item!.encoding, size: item!.size, contentSize: item!.contentSize }).toEqual(
      await storage.fileInfo('id')
    )
  })

  it('should serve a range of it', async () => {
    const item = await storage.retrieve('id', { start: 0, end: 3 })

    expect(item!.size).toBe(4)
  })
})

describe('S3 Storage injected content-type detector', () => {
  describe('and the loader resolves at construction', () => {
    let calls: number
    let storage: IContentStorageComponent

    beforeEach(async () => {
      // "Called once, during construction" has to hold for an INJECTED loader too. The bundled one
      // memoizes internally, so passing the loader itself to every store hid this: a custom loader
      // was re-invoked per store and could fail after construction had already succeeded.
      calls = 0
      storage = await createStorage({ logs: await createLogComponent({}) }, createFakeS3Client(), {
        Bucket: 'example',
        fileTypeLoader: async () => {
          calls++
          return { fileTypeFromBuffer: async () => ({ mime: 'image/png' }) }
        }
      })
    })

    it('should call it once for construction alone', () => {
      expect(calls).toBe(1)
    })

    it('should not call it again for any number of stores', async () => {
      await storage.storeStream('one', bufferToStream(Buffer.from('first')))
      await storage.storeStream('two', bufferToStream(Buffer.from('second')))

      expect(calls).toBe(1)
    })

    it('should use the resolved detector for those stores', async () => {
      await storage.storeStream('one', bufferToStream(Buffer.from('first')))

      expect(await storage.fileInfo('one')).toBeDefined()
    })
  })

  describe('and the loader rejects at construction', () => {
    let calls: number
    let storage: IContentStorageComponent
    let fake: FakeS3Client

    beforeEach(async () => {
      // A transient failure must not permanently downgrade every later store, so the loader is
      // retried — the same reason the bundled loader refuses to cache a rejection.
      calls = 0
      fake = createFakeS3Client()
      storage = await createStorage({ logs: await createLogComponent({}) }, fake, {
        Bucket: 'example',
        fileTypeLoader: async () => {
          calls++
          if (calls === 1) throw new Error('detector unavailable')
          return { fileTypeFromBuffer: async () => ({ mime: 'image/png' }) }
        }
      })
    })

    it('should construct rather than fail over an unavailable detector', () => {
      expect(storage).toBeDefined()
    })

    it('should retry the loader on a later store', async () => {
      await storage.storeStream('one', bufferToStream(Buffer.from('first')))

      expect(calls).toBe(2)
    })

    it('should recover the detected type once the loader works', async () => {
      await storage.storeStream('one', bufferToStream(Buffer.from('first')))

      expect(fake.objects.get('one')!.contentType).toBe('image/png')
    })
  })
})
