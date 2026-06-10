import { S3 } from 'aws-sdk'
import { Readable } from 'stream'
import { AppComponents, clampRange, ContentItem, FileInfo, IContentStorageComponent, validateRange } from './types'
import { SimpleContentItem } from './content-item'
import { ListObjectsV2Output } from 'aws-sdk/clients/s3'
// Workaround: TS "commonjs" transforms import() to require().
// This indirection preserves the native import() needed for ESM-only packages.
const _importDynamic = Function('modulePath', 'return import(modulePath)') as (modulePath: string) => Promise<any>

const MIME_DETECTION_BYTES = 4100

/**
 * Reads at least the first `byteCount` bytes from the stream for inspection (chunk-granular, so it
 * may read a bit more — up to a whole chunk past the target), then returns those bytes together
 * with a Readable that re-emits them followed by the remainder of the original stream. This lets us
 * detect the MIME type from the head while streaming the body straight to S3, so large files are
 * never buffered in memory in full.
 */
async function peekHead(stream: Readable, byteCount: number): Promise<{ head: Buffer; body: Readable }> {
  const iterator = stream[Symbol.asyncIterator]()
  const headChunks: Buffer[] = []
  let headLength = 0
  let finished = false

  while (headLength < byteCount) {
    const next = await iterator.next()
    if (next.done) {
      finished = true
      break
    }
    const chunk = Buffer.isBuffer(next.value) ? next.value : Buffer.from(next.value)
    headChunks.push(chunk)
    headLength += chunk.length
  }

  const head = Buffer.concat(headChunks)

  const body = Readable.from(
    (async function* () {
      try {
        yield head
        if (!finished) {
          let next = await iterator.next()
          while (!next.done) {
            yield Buffer.isBuffer(next.value) ? next.value : Buffer.from(next.value)
            next = await iterator.next()
          }
        }
      } finally {
        // Release the source stream whenever consumption stops — normal end, or early
        // termination such as the body being destroyed after a failed upload — so its
        // underlying resources (e.g. file descriptors) are not leaked.
        await iterator.return?.()
      }
    })()
  )

  return { head, body }
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
    // Inspect only the head for MIME detection, then stream the body straight to S3 so large
    // files are never buffered in memory. The AWS SDK's managed upload performs a multipart
    // upload, buffering only part-sized chunks rather than the whole file.
    const { head, body } = await peekHead(stream, MIME_DETECTION_BYTES)
    const mimeType = await detectMimeTypeFromBuffer(head)

    try {
      await s3
        .upload({
          Bucket,
          Key: getKey(id),
          Body: body,
          ContentType: mimeType
        })
        .promise()
    } catch (error) {
      // Release the source stream if the upload stopped consuming the body (e.g. it failed before
      // reading anything, so peekHead's generator never started and can't self-clean). Destroying
      // the source releases its underlying resources (e.g. file descriptors). No-op if already
      // ended/destroyed.
      stream.destroy()
      throw error
    }
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
      // A missing object returns NotFound (404) when the principal has s3:ListBucket; there is
      // nothing to serve, so fall through and return undefined.
      if (error.code !== 'NotFound') {
        const logContext = { key: getKey(id), code: error.code, statusCode: error.statusCode }
        if (error.statusCode === 403) {
          // S3 returns 403 (with an empty body, hence a null message on HEAD) instead of 404 for a
          // missing key when the principal lacks s3:ListBucket. It can also be a genuine access
          // denial. Surface it as an actionable warning rather than a bare, message-less error.
          logger.warn(
            'S3 returned 403 Forbidden retrieving content; returning not-found. If the object is simply missing, grant the principal s3:ListBucket so missing keys return 404; otherwise check the object/bucket permissions.',
            logContext
          )
        } else {
          logger.error(`Failed to retrieve content from S3: ${error.message || error.code || 'unknown error'}`, {
            ...logContext
          })
        }
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
