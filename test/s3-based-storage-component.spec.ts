import {
  createAwsS3BasedFileSystemContentStorage,
  createS3BasedFileSystemContentStorage,
  IContentStorageComponent
} from '../src'
import { AwsClientStub, mockClient } from 'aws-sdk-client-mock'
import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client
} from '@aws-sdk/client-s3'
import { Readable } from 'stream'
import { sdkStreamMixin } from '@aws-sdk/util-stream-node'
import { streamToBuffer } from '../src/content-item'
import { createLogComponent } from '@well-known-components/logger'
import { createConfigComponent } from '@well-known-components/env-config-provider'
import { beforeEach, describe, expect, it } from 'vitest'

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
  const id = 'some-id'
  const id2 = 'another-id'
  let s3: AwsClientStub<S3Client>

  beforeEach(async () => {
    s3 = mockClient(S3Client)
    storage = await createS3BasedFileSystemContentStorage(
      { logs: await createLogComponent({}) },
      s3 as unknown as S3Client,
      { Bucket: 'example' }
    )
  })

  it('exists works', async () => {
    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id }).resolves({})
    expect(await storage.exist(id)).toBe(false)

    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id }).resolves({ ETag: 'something' })
    expect(await storage.exist(id)).toBe(true)

    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id }).rejects({})
    expect(await storage.exist(id)).toBe(false)
  })

  it('existsMultiple works', async () => {
    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id }).resolves({})
    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id2 }).resolves({})
    expect(await storage.existMultiple([id, id2])).toEqual(
      new Map([
        [id, false],
        [id2, false]
      ])
    )

    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id }).resolves({})
    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id2 }).resolves({ ETag: 'something' })
    expect(await storage.existMultiple([id, id2])).toEqual(
      new Map([
        [id, false],
        [id2, true]
      ])
    )

    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id }).rejects({})
    s3.on(HeadObjectCommand, { Bucket: 'example', Key: id2 }).resolves({ ETag: 'something' })
    expect(await storage.existMultiple([id, id2])).toEqual(
      new Map([
        [id, false],
        [id2, true]
      ])
    )
  })

  it('storeStream works with big files (multipart upload)', async () => {
    const stream = new Readable()
    stream.push('x'.repeat(100 * 1024 * 1024)) // 100 MB)
    stream.push(null) // end of stream

    const sdkStream = sdkStreamMixin(stream)

    s3.on(CreateMultipartUploadCommand, { Bucket: 'example', Key: 'some-id' }).resolves({
      UploadId: '20'
    })
    s3.on(CompleteMultipartUploadCommand, {
      Bucket: 'example',
      Key: 'some-id',
      MultipartUpload: { Parts: [] },
      UploadId: '20'
    }).resolves({ ETag: '1' })

    await storage.storeStream(id, sdkStream)

    // expect(s3).toHaveReceivedCommand(CreateMultipartUploadCommand)
    // expect(s3).toHaveReceivedCommand(CompleteMultipartUploadCommand)
  })

  it.skip('storeStream works with small files (putobject)', async () => {
    /* This one is skipped for now as the mocking library is misbehaving and not handling
       correctly the mocking of the client. What is doing wrong is not to create a config object
       with a proper requestHandler, so the sdk hits a NPE and fails */
    const stream = new Readable()
    stream.push('x'.repeat(100 * 1024)) // 100 KB)
    stream.push(null) // end of stream

    const sdkStream = sdkStreamMixin(stream)

    s3.on(CreateMultipartUploadCommand, { Bucket: 'example', Key: 'some-id' }).resolves({
      UploadId: '20'
    })
    // s3.on(UploadPartCommand).resolves({ ETag: '1' })
    s3.on(CompleteMultipartUploadCommand, {
      Bucket: 'example',
      Key: 'some-id',
      MultipartUpload: { Parts: [] },
      UploadId: '20'
    }).resolves({ ETag: '1' })

    await storage.storeStream(id, sdkStream)

    // expect(s3).toHaveReceivedCommand(CreateMultipartUploadCommand)
    // expect(s3).toHaveReceivedCommand(UploadPartCommand)
    // expect(s3).toHaveReceivedCommand(CompleteMultipartUploadCommand)
  })

  it('retrieve works', async () => {
    const stream = new Readable()
    stream.push('hello world')
    stream.push(null) // end of stream

    const sdkStream = sdkStreamMixin(stream)

    s3.on(GetObjectCommand, { Bucket: 'example', Key: 'some-id' }).resolves({
      Body: sdkStream,
      ContentLength: sdkStream.readableLength
    })

    const retrieved = await storage.retrieve(id)

    expect(retrieved).toBeDefined()
    expect(retrieved?.size).toBe(11)
    const str = (await streamToBuffer(await retrieved!.asStream())).toString()
    expect(str).toBe('hello world')
    // expect(s3).toHaveReceivedCommand(GetObjectCommand)
  })

  it('retrieve works with missing key', async () => {
    const stream = new Readable()
    stream.push('hello world')
    stream.push(null) // end of stream

    const sdkStream = sdkStreamMixin(stream)

    s3.on(GetObjectCommand, { Bucket: 'example', Key: 'some-id' }).resolves({
      Body: sdkStream,
      ContentLength: sdkStream.readableLength
    })

    const retrieved = await storage.retrieve('other-id')

    expect(retrieved).toBeUndefined()
    // expect(s3).toHaveReceivedCommand(GetObjectCommand)
  })

  it('retrieve works when S3 network error', async () => {
    s3.on(GetObjectCommand, { Bucket: 'example', Key: id }).rejects('error')

    const retrieved = await storage.retrieve(id)

    expect(retrieved).toBeUndefined()
    // expect(s3).toHaveReceivedCommand(GetObjectCommand)
  })

  it('storeStreamAndCompress works with big files (multipart upload)', async () => {
    const stream = new Readable()
    stream.push('x'.repeat(100 * 1024 * 1024)) // 100 MB)
    stream.push(null) // end of stream

    const sdkStream = sdkStreamMixin(stream)

    s3.on(CreateMultipartUploadCommand, { Bucket: 'example', Key: 'some-id' }).resolves({
      UploadId: '20'
    })
    s3.on(CompleteMultipartUploadCommand, {
      Bucket: 'example',
      Key: 'some-id',
      MultipartUpload: { Parts: [] },
      UploadId: '20'
    }).resolves({ ETag: '1' })

    await storage.storeStreamAndCompress(id, sdkStream)

    // expect(s3).toHaveReceivedCommand(CreateMultipartUploadCommand)
    // expect(s3).toHaveReceivedCommand(CompleteMultipartUploadCommand)
  })

  it('delete works', async () => {
    s3.on(DeleteObjectsCommand, { Bucket: 'example', Delete: { Objects: [{ Key: id }, { Key: id2 }] } }).resolves({})

    await storage.delete([id, id2])

    // expect(s3).toHaveReceivedCommandWith(DeleteObjectsCommand, {
    //   Bucket: 'example',
    //   Delete: { Objects: [{ Key: id }, { Key: id2 }] }
    // })
  })

  it('allFileIds works', async () => {
    s3.on(ListObjectsV2Command, { Bucket: 'example', ContinuationToken: undefined }).resolvesOnce({
      Contents: [{ Key: 'id1' }, { Key: 'id2' }],
      NextContinuationToken: 'token',
      IsTruncated: true
    })
    s3.on(ListObjectsV2Command, { Bucket: 'example', ContinuationToken: 'token' }).resolvesOnce({
      Contents: [{ Key: 'id3' }, { Key: 'id4' }],
      IsTruncated: false
    })

    const allFileIds = await storage.allFileIds()
    const result = []
    for await (const fileId of allFileIds) {
      result.push(fileId)
    }
    expect(result).toEqual(['id1', 'id2', 'id3', 'id4'])
    // expect(s3).toHaveReceivedCommandTimes(ListObjectsV2Command, 2)
  })

  it('allFileIds works with prefix', async () => {
    s3.on(ListObjectsV2Command, { Bucket: 'example', ContinuationToken: undefined, Prefix: 'id' }).resolvesOnce({
      Contents: [{ Key: 'id1' }, { Key: 'id2' }],
      NextContinuationToken: 'token',
      IsTruncated: true
    })
    s3.on(ListObjectsV2Command, { Bucket: 'example', ContinuationToken: 'token', Prefix: 'id' }).resolvesOnce({
      Contents: [{ Key: 'id3' }],
      IsTruncated: false
    })

    const allFileIds = await storage.allFileIds('id')
    const result = []
    for await (const fileId of allFileIds) {
      result.push(fileId)
    }
    expect(result).toEqual(['id1', 'id2', 'id3'])
    // expect(s3).toHaveReceivedCommandTimes(ListObjectsV2Command, 2)
  })
})
