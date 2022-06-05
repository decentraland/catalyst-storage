import { S3 } from "aws-sdk"
import { Readable } from "stream"
import { AppComponents } from "./types"
import { SimpleContentItem } from "./content-item"
import { ContentItem, IContentStorageComponent } from "./types"

/**
 * @public
 */
export async function createAwsS3BasedFileSystemContentStorage(
  components: Pick<AppComponents, "fs" | "config">,
  bucket: string
): Promise<IContentStorageComponent> {
  const { config } = components

  const s3 = new S3({
    region: await config.requireString("AWS_REGION"),
  })

  const getKey = (hash: string) => hash

  return createS3BasedFileSystemContentStorage({}, s3, { Bucket: bucket, getKey })
}

/**
 * @beta
 */
export async function createS3BasedFileSystemContentStorage(
  components: {},
  s3: Pick<S3, "headObject" | "putObject" | "getObject" | "deleteObjects">,
  options: { Bucket: string; getKey?: (hash: string) => string }
): Promise<IContentStorageComponent> {
  const getKey = options.getKey || ((hash: string) => hash)

  async function exist(id: string): Promise<boolean> {
    try {
      const obj = await s3.headObject({ ...extraArgs, Key: getKey(id) }).promise()
      return !!obj.LastModified
    } catch {
      return false
    }
  }

  const extraArgs: { Bucket: string } = {
    Bucket: options.Bucket,
  }

  return {
    exist,
    async storeStream(id: string, stream: Readable): Promise<void> {
      await s3
        .putObject({
          ...extraArgs,
          Key: getKey(id),
          Body: stream,
        })
        .promise()
    },
    async retrieve(id: string): Promise<ContentItem | undefined> {
      try {
        const obj = await s3.headObject({ ...extraArgs, Key: getKey(id) }).promise()

        return new SimpleContentItem(
          async () => s3.getObject({ ...extraArgs, Key: getKey(id) }).createReadStream(),
          obj.ContentLength || null,
          obj.ContentEncoding || null
        )
      } catch (error) {
        console.error(error)
      }
      return undefined
    },
    async storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
      await s3
        .putObject({
          ...extraArgs,
          Key: getKey(id),
          Body: stream,
        })
        .promise()
    },
    async delete(ids: string[]): Promise<void> {
      await s3
        .deleteObjects({
          ...extraArgs,
          Delete: { Objects: ids.map(($) => ({ Key: getKey($) })) },
        })
        .promise()
    },

    async existMultiple(cids: string[]): Promise<Map<string, boolean>> {
      const entries = await Promise.all(cids.map(async (cid): Promise<[string, boolean]> => [cid, await exist(cid)]))
      return new Map(entries)
    },
  }
}
