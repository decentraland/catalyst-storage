import { S3Client } from '@aws-sdk/client-s3'
import { Readable } from 'stream'

type CommandHandler = (input: any, options?: any) => Promise<any> | any

export type FakeS3Client = S3Client & {
  /**
   * The stored objects, keyed by S3 key — assertable from tests.
   *
   * `etag` is optional: when absent, one is derived from the key, which is what a plain re-store produces.
   * Set it explicitly to model an object whose ETag changed (a new version, or an SSE-KMS bucket where the
   * ETag is not a digest of the body), which is what `IfMatch` is checked against.
   */
  objects: Map<string, { body: Buffer; contentType?: string; etag?: string; contentEncoding?: string }>
  /** Overrides the handler for one command (by class name), e.g. to make HeadObject fail. */
  on: (commandName: string, handler: CommandHandler) => void
  /** Makes the given command hang until `release()` is called, to model an in-flight request. */
  hang: (commandName: string) => { release: () => void; started: Promise<void> }
}

function notFound(): Error {
  return Object.assign(new Error('NotFound'), { name: 'NotFound', $metadata: { httpStatusCode: 404 } })
}

function preconditionFailed(): Error {
  return Object.assign(new Error('At least one of the pre-conditions you specified did not hold'), {
    name: 'PreconditionFailed',
    $metadata: { httpStatusCode: 412 }
  })
}

function abortError(): Error {
  return Object.assign(new Error('Request aborted'), { name: 'AbortError' })
}

async function bodyToBuffer(body: unknown): Promise<Buffer> {
  if (body === undefined || body === null) return Buffer.alloc(0)
  if (Buffer.isBuffer(body)) return body
  if (body instanceof Uint8Array) return Buffer.from(body)
  if (typeof body === 'string') return Buffer.from(body)
  const chunks: Buffer[] = []
  for await (const chunk of body as Readable) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  return Buffer.concat(chunks)
}

/**
 * In-memory S3 test double: a REAL `S3Client` (so `@aws-sdk/lib-storage` can read the endpoint and
 * signing config it needs) with only `send` intercepted, backed by a Map. Covers the commands this
 * storage issues, including both upload shapes lib-storage can take — a single PutObject, or the
 * multipart trio.
 *
 * It deliberately honors `options.abortSignal` the way the real client does, so tests exercise the
 * genuine `Upload.abort()` path rather than a hand-rolled stand-in for it.
 */
export function createFakeS3Client(): FakeS3Client {
  const objects = new Map<string, { body: Buffer; contentType?: string; etag?: string; contentEncoding?: string }>()
  const etagOf = (key: string, stored: { etag?: string }): string => stored.etag ?? `"${key}"`
  const parts = new Map<string, Map<number, Buffer>>()
  const overrides = new Map<string, CommandHandler>()
  const hangs = new Map<string, { promise: Promise<void>; release: () => void; started: () => void }>()

  const defaults: Record<string, CommandHandler> = {
    HeadObjectCommand: ({ Key }) => {
      const found = objects.get(Key)
      if (!found) throw notFound()
      return {
        ETag: etagOf(Key, found),
        ContentLength: found.body.length,
        ContentType: found.contentType,
        // Set only when a test asked for it, so an ordinary object still has no Content-Encoding at all.
        ContentEncoding: found.contentEncoding
      }
    },
    GetObjectCommand: ({ Key, Range, IfMatch }) => {
      const found = objects.get(Key)
      if (!found) throw notFound()
      // ENFORCED, like the real service. Ignoring it made any test of the read's `IfMatch` pin vacuous: it
      // passed whether the header was sent, omitted, or sent with a wrong value.
      if (IfMatch !== undefined && IfMatch !== etagOf(Key, found)) throw preconditionFailed()
      let body = found.body
      if (Range) {
        const [, start, end] = /bytes=(\d+)-(\d+)?/.exec(Range) ?? []
        body = body.subarray(Number(start), end === undefined ? undefined : Number(end) + 1)
      }
      return { Body: Readable.from([body]), ContentLength: body.length }
    },
    PutObjectCommand: async ({ Key, Body, ContentType }) => {
      objects.set(Key, { body: await bodyToBuffer(Body), contentType: ContentType })
      return { ETag: `"${Key}"` }
    },
    CreateMultipartUploadCommand: ({ Key }) => {
      parts.set(Key, new Map())
      return { UploadId: `upload-${Key}` }
    },
    UploadPartCommand: async ({ Key, PartNumber, Body }) => {
      const forKey = parts.get(Key) ?? new Map<number, Buffer>()
      forKey.set(PartNumber, await bodyToBuffer(Body))
      parts.set(Key, forKey)
      return { ETag: `"part-${PartNumber}"` }
    },
    CompleteMultipartUploadCommand: ({ Key, ContentType }) => {
      const forKey = parts.get(Key) ?? new Map<number, Buffer>()
      const ordered = [...forKey.entries()].sort((a, b) => a[0] - b[0]).map(([, chunk]) => chunk)
      objects.set(Key, { body: Buffer.concat(ordered), contentType: ContentType })
      parts.delete(Key)
      return { ETag: `"${Key}"` }
    },
    AbortMultipartUploadCommand: ({ Key }) => {
      parts.delete(Key)
      return {}
    },
    DeleteObjectsCommand: ({ Delete }) => {
      // S3 rejects an empty list and anything past 1000 keys with MalformedXML, and the SDK does not
      // split the batch — modelled here so a caller that exceeds either limit fails in tests too.
      const requested = Delete?.Objects ?? []
      if (requested.length === 0 || requested.length > 1000) {
        throw Object.assign(new Error('The XML you provided was not well-formed'), {
          name: 'MalformedXML',
          $metadata: { httpStatusCode: 400 }
        })
      }
      for (const { Key } of requested) objects.delete(Key)
      return { Deleted: requested.map(({ Key }: { Key: string }) => ({ Key })) }
    },
    ListObjectsV2Command: ({ Prefix, ContinuationToken }) => {
      const keys = [...objects.keys()].filter((key) => !Prefix || key.startsWith(Prefix)).sort()
      const from = ContinuationToken ? Number(ContinuationToken) : 0
      const page = keys.slice(from, from + 1000)
      const nextIndex = from + page.length
      return {
        Contents: page.map((Key) => ({ Key })),
        IsTruncated: nextIndex < keys.length,
        NextContinuationToken: nextIndex < keys.length ? String(nextIndex) : undefined
      }
    }
  }

  const client = new S3Client({
    region: 'test-region',
    credentials: { accessKeyId: 'test-key', secretAccessKey: 'test-secret' }
  }) as FakeS3Client

  return Object.assign(client, {
    objects,
    on(commandName: string, handler: CommandHandler) {
      overrides.set(commandName, handler)
    },
    hang(commandName: string) {
      let release: () => void = () => undefined
      let started: () => void = () => undefined
      const startedPromise = new Promise<void>((resolve) => (started = resolve))
      hangs.set(commandName, {
        promise: new Promise<void>((resolve) => (release = resolve)),
        release,
        started
      })
      return { release: () => release(), started: startedPromise }
    },
    async send(command: any, options?: any) {
      const name = command.constructor.name
      // The real client rejects immediately when handed an already-aborted signal.
      if (options?.abortSignal?.aborted) throw abortError()

      const hanging = hangs.get(name)
      if (hanging) {
        hangs.delete(name)
        hanging.started()
        // Race the hold against the abort, exactly as a real in-flight request would.
        await new Promise<void>((resolve, reject) => {
          void hanging.promise.then(resolve)
          options?.abortSignal?.addEventListener?.('abort', () => reject(abortError()), { once: true })
        })
      }

      const handler = overrides.get(name) ?? defaults[name]
      if (!handler) throw new Error(`FakeS3Client received an unhandled command: ${name}`)
      return handler(command.input, options)
    }
  })
}
