import { Upload } from '@aws-sdk/lib-storage'
import {
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  ListObjectsV2Request,
  S3Client
} from '@aws-sdk/client-s3'
import { Readable } from 'stream'
import { AppComponents, ContentItem, IContentStorageComponent } from './types'
import { SimpleContentItem } from './content-item'

/**
 * @public
 */
export async function createAwsS3BasedFileSystemContentStorage(
  components: Pick<AppComponents, 'fs' | 'config'>,
  bucket: string
): Promise<IContentStorageComponent> {
  const { config } = components

  const s3 = new S3Client({
    region: await config.requireString('AWS_REGION')
  })

  const getKey = (hash: string) => hash

  return createS3BasedFileSystemContentStorage({}, s3, { Bucket: bucket, getKey })
}

/**
 * @public
 */
export async function createS3BasedFileSystemContentStorage(
  components: Partial<AppComponents>,
  s3: S3Client,
  options: { Bucket: string; getKey?: (hash: string) => string }
): Promise<IContentStorageComponent> {
  const getKey = options.getKey || ((hash: string) => hash)
  const Bucket = options.Bucket

  async function exist(id: string): Promise<boolean> {
    try {
      const command = new HeadObjectCommand({ Bucket, Key: getKey(id) })
      const obj = await s3.send(command)
      return !!obj.ETag
    } catch {
      return false
    }
  }

  async function storeStream(id: string, stream: Readable): Promise<void> {
    await new Upload({
      client: s3,
      params: {
        Bucket,
        Key: getKey(id),
        Body: stream
      },

      // Forcing chunks of 5Mb to improve upload of large files
      partSize: 5 * 1024 * 1024
    }).done()
  }

  async function retrieve(id: string): Promise<ContentItem | undefined> {
    try {
      const command = new GetObjectCommand({ Bucket, Key: getKey(id) })
      const getObjectCommandOutput = await s3.send(command)

      // const getObjectCommandOutput = await s3.getObject({ Bucket, Key: getKey(id) })
      const body = getObjectCommandOutput?.Body
      if (!body) {
        return undefined
      }

      return new SimpleContentItem(
        () => Readable.fromWeb(body.transformToWebStream() as any) as any,
        getObjectCommandOutput.ContentLength || null,
        getObjectCommandOutput.ContentEncoding || null
      )
    } catch (error) {
      console.error(error)
    }
    return undefined
  }

  async function storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
    // In AWS S3 we don't compress
    await storeStream(id, stream)
  }

  async function deleteFn(ids: string[]): Promise<void> {
    const command = new DeleteObjectsCommand({
      Bucket,
      Delete: {
        Objects: ids.map(($) => ({ Key: getKey($) }))
      }
    })
    await s3.send(command)
  }

  async function existMultiple(cids: string[]): Promise<Map<string, boolean>> {
    const entries = await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]))
    return new Map(entries)
  }

  async function* allFileIds(prefix?: string): AsyncIterable<string> {
    const params: ListObjectsV2Request = {
      Bucket,
      ContinuationToken: undefined
    }

    if (prefix) {
      params.Prefix = prefix
    }

    let fetched: ListObjectsV2CommandOutput
    do {
      const command = new ListObjectsV2Command(params)
      fetched = await s3.send(command)
      if (fetched.Contents) {
        for (const content of fetched.Contents) {
          yield content.Key!
        }
      }
      params.ContinuationToken = fetched.NextContinuationToken
    } while (fetched.IsTruncated)
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
