import { S3 } from 'aws-sdk'
import { Readable } from 'stream'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem } from './content-item'
import { ListObjectsV2Output } from 'aws-sdk/clients/s3'
// Workaround: TS "commonjs" transforms import() to require().
// This indirection preserves the native import() needed for ESM-only packages.
const _importDynamic = Function('modulePath', 'return import(modulePath)') as (modulePath: string) => Promise<any>

/**
 * Helper function to convert a buffer to a readable stream.
 */
function bufferToStream(buffer: Buffer): Readable {
  const stream = new Readable()
  stream.push(buffer)
  stream.push(null) // End of stream
  return stream
}

/**
 * Helper function to buffer a stream for MIME type detection and further usage.
 */
async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = []
  for await (const chunk of stream) {
    // Ensure chunk is converted to Buffer (handles Uint8Array, Buffer, etc.)
    const bufferChunk = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    chunks.push(bufferChunk)
  }
  return Buffer.concat(chunks)
}

/**
 * Detects the MIME type from a buffer.
 * Uses only the first bytes of the buffer for detection.
 * file-type v21 only needs the first ~4100 bytes to detect any file type.
 */
async function detectMimeTypeFromBuffer(buffer: Buffer | Uint8Array): Promise<string> {
  const maxBytesForDetection = 4100
  const bytesToUse = Math.min(maxBytesForDetection, buffer.length)
  const detectionBuffer = buffer.slice(0, bytesToUse)

  try {
    const { fileTypeFromBuffer } = await _importDynamic('file-type')
    const mime = await fileTypeFromBuffer(detectionBuffer)
    return mime?.mime || 'application/octet-stream'
  } catch (error: any) {
    return 'application/octet-stream'
  }
}

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
    // Buffer the entire stream to detect MIME type and upload
    const fullBuffer = await streamToBuffer(stream)

    // Detect MIME type from the buffer
    const mimeType = await detectMimeTypeFromBuffer(fullBuffer)

    // Upload to S3 using the buffered data
    await s3
      .upload({
        Bucket,
        Key: getKey(id),
        Body: bufferToStream(fullBuffer),
        ContentType: mimeType
      })
      .promise()
  }

  async function retrieve(id: string, range?: { start: number; end: number }): Promise<ContentItem | undefined> {
    if (range) validateRange(range)
    try {
      const obj = await s3.headObject({ Bucket, Key: getKey(id) }).promise()

      const size = obj.ContentLength ?? null
      const clampedEnd = range && size !== null ? clampRange(range, size) : undefined

      return new SimpleContentItem(
        async () =>
          s3
            .getObject({
              Bucket,
              Key: getKey(id),
              Range: range ? `bytes=${range.start}-${clampedEnd ?? range.end}` : undefined
            })
            .createReadStream(),
        range ? (clampedEnd !== undefined ? clampedEnd - range.start + 1 : null) : size,
        obj.ContentEncoding || null
      )
    } catch (error: any) {
      if (error instanceof RangeError) throw error
      if (error.code !== 'NotFound') {
        logger.error(error)
      }
    }
    return undefined
  }

  async function storeStreamAndCompress(id: string, stream: Readable): Promise<void> {
    // In AWS S3 we don't compress, we directly store the stream
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

  async function fileInfo(id: string): Promise<FileInfo | undefined> {
    try {
      const obj = await s3.headObject({ Bucket, Key: getKey(id) }).promise()
      const size = obj.ContentLength ?? null
      return {
        encoding: obj.ContentEncoding || null,
        size,
        contentSize: obj.ContentEncoding ? null : size
      }
    } catch {
      return undefined
    }
  }

  async function fileInfoMultiple(cids: string[]): Promise<Map<string, FileInfo | undefined>> {
    return new Map(
      await Promise.all(cids.map(async (cid): Promise<[string, FileInfo | undefined]> => [cid, await fileInfo(cid)]))
    )
  }

  return {
    exist,
    fileInfo,
    fileInfoMultiple,
    storeStream,
    retrieve,
    storeStreamAndCompress,
    delete: deleteFn,
    existMultiple,
    allFileIds
  }
}
