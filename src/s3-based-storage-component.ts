import { Upload } from '@aws-sdk/lib-storage'
import { ListObjectsV2CommandOutput, ListObjectsV2Request, S3 } from '@aws-sdk/client-s3'
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

  const s3 = new S3({
    region: await config.requireString('AWS_REGION')
  })

  const getKey = (hash: string) => hash

  return createS3BasedFileSystemContentStorage({}, s3, { Bucket: bucket, getKey })
}

/**
 * @beta
 */
export async function createS3BasedFileSystemContentStorage(
  components: Partial<AppComponents>,
  s3: Pick<
    S3,
    'config' | 'headObject' | 'getObject' | 'deleteObjects' | 'listObjectsV2' | 'destroy' | 'middlewareStack' | 'send'
  >,
  options: { Bucket: string; getKey?: (hash: string) => string }
): Promise<IContentStorageComponent> {
  const getKey = options.getKey || ((hash: string) => hash)
  const Bucket = options.Bucket

  async function exist(id: string): Promise<boolean> {
    try {
      const obj = await s3.headObject({ Bucket, Key: getKey(id) })
      return !!obj.ETag
    } catch {
      return false
    }
  }

  return {
    exist,
    async storeStream(id: string, stream: Readable): Promise<void> {
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
    },
    async retrieve(id: string): Promise<ContentItem | undefined> {
      try {
        // const obj = await s3.headObject({ Bucket, Key: getKey(id) })
        //
        const getObjectCommandOutput = await s3.getObject({ Bucket, Key: getKey(id) })
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
    },
    async storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
      await new Upload({
        client: s3,

        params: {
          Bucket,
          Key: getKey(id),
          Body: stream
        }
      }).done()
    },
    async delete(ids: string[]): Promise<void> {
      await s3.deleteObjects({
        Bucket,
        Delete: { Objects: ids.map(($) => ({ Key: getKey($) })) }
      })
    },

    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      const entries = await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]))
      return new Map(entries)
    },

    allFileIds: async function* allFileIds(prefix?: string): AsyncIterable<string> {
      const params: ListObjectsV2Request = {
        Bucket,
        ContinuationToken: undefined
      }

      if (prefix) {
        params.Prefix = prefix
      }

      let fetched: ListObjectsV2CommandOutput
      do {
        fetched = await s3.listObjectsV2(params)
        if (fetched.Contents) {
          for (const content of fetched.Contents) {
            yield content.Key!
          }
        }
        params.ContinuationToken = fetched.NextContinuationToken
      } while (fetched.IsTruncated)
    }
  }
}
