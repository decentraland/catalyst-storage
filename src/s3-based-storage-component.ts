import { S3 } from 'aws-sdk'
import { Readable } from 'stream'
import { AppComponents, ContentItem, IContentStorageComponent } from './types'
import { SimpleContentItem } from './content-item'
import { ListObjectsV2Output } from 'aws-sdk/clients/s3'

/**
 * @public
 */
export async function createAwsS3BasedFileSystemContentStorage(
  components: Pick<AppComponents, 'config' | 'logs'>,
  bucket: string
): Promise<IContentStorageComponent> {
  const { config, logs } = components

  const s3 = new S3({
    region: await config.requireString('AWS_REGION')
  })

  const getKey = (hash: string) => hash

  return createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: bucket, getKey })
}

/**
 * @public
 */
export async function createS3BasedFileSystemContentStorage(
  components: Pick<AppComponents, 'logs'>,
  s3: Pick<S3, 'headObject' | 'upload' | 'getObject' | 'deleteObjects' | 'listObjectsV2'>,
  options: { Bucket: string; getKey?: (hash: string) => string }
): Promise<IContentStorageComponent> {
  const logger = components.logs.getLogger('s3-based-content-storage')
  const getKey = options.getKey || ((hash: string) => hash)
  const Bucket = options.Bucket

  async function exist(id: string): Promise<boolean> {
    try {
      const obj = await s3.headObject({ Bucket, Key: getKey(id) }).promise()
      return !!obj.ETag
    } catch {
      return false
    }
  }

  async function storeStream(id: string, stream: Readable): Promise<void> {
    await s3
      .upload(
        {
          Bucket,
          Key: getKey(id),
          Body: stream
        },
        {
          // Forcing chunks of 5Mb to improve upload of large files
          partSize: 5 * 1024 * 1024
        }
      )
      .promise()
  }

  async function retrieve(id: string): Promise<ContentItem | undefined> {
    try {
      const obj = await s3.headObject({ Bucket, Key: getKey(id) }).promise()

      return new SimpleContentItem(
        async () => s3.getObject({ Bucket, Key: getKey(id) }).createReadStream(),
        obj.ContentLength || null,
        obj.ContentEncoding || null
      )
    } catch (error: any) {
      if (!(error.name === 'NoSuchKey')) {
        logger.error(error)
      }
    }
    return undefined
  }

  async function storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
    // In AWS S3 we don't compress
    await storeStream(id, stream)
  }

  async function deleteFn(ids: string[]): Promise<void> {
    await s3
      .deleteObjects({
        Bucket,
        Delete: {
          Objects: ids.map(($) => ({ Key: getKey($) }))
        }
      })
      .promise()
  }

  async function existMultiple(cids: string[]): Promise<Map<string, boolean>> {
    return new Map(await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)])))
  }

  async function* allFileIds(prefix?: string): AsyncIterable<string> {
    const params: S3.Types.ListObjectsV2Request = {
      Bucket,
      ContinuationToken: undefined
    }

    if (prefix) {
      params.Prefix = prefix
    }

    let output: ListObjectsV2Output
    do {
      output = await s3.listObjectsV2(params).promise()
      if (output.Contents) {
        for (const content of output.Contents) {
          yield content.Key!
        }
      }
      params.ContinuationToken = output.NextContinuationToken
    } while (output.IsTruncated)
  }

  return {
    exist,
    storeStream,
    retrieve,
    storeStreamAndCompress,
    delete: deleteFn,
    existMultiple,
    allFileIds
  }
}
