import { pipeline, Readable, Transform } from 'stream'
import { createBrotliDecompress, createGunzip, createInflate } from 'zlib'
import { ContentItem } from './types'
import { destroyQuietly } from './stream-teardown'

/**
 * Content-coding tokens that mean "these bytes are not encoded" (RFC 9110 §8.4.1). Normalized to
 * `null` so callers never have to special-case them, and so `contentSize` defaults to `size` as it
 * does for any other unencoded content instead of reporting the logical size as unknown.
 */
const IDENTITY_ENCODINGS = new Set(['', 'identity'])

/**
 * The value a `ContentItem` reports as its `encoding`, given a raw `Content-Encoding`.
 *
 * Only the identity tokens collapse to `null`: they mean "not encoded", so reporting them verbatim
 * forces every caller to special-case a value that carries no information. Any other coding is passed
 * through unchanged, because a caller forwarding the header needs the original. Exported so backends
 * normalize identically and their `fileInfo`/`retrieve` surfaces cannot disagree about one id.
 *
 * @public
 */
export function normalizeContentEncoding(encoding: string | null | undefined): string | null {
  return typeof encoding === 'string' && IDENTITY_ENCODINGS.has(encoding.toLowerCase()) ? null : (encoding ?? null)
}

/**
 * Codings that describe the TRANSFER of the bytes rather than the content itself, and so are already
 * undone by the time a body reaches us. `aws-chunked` is written by S3 on flexible-checksum uploads,
 * frequently alongside a real coding (`gzip, aws-chunked`).
 */
const TRANSPARENT_CODINGS = new Set(['aws-chunked', 'chunked'])

/**
 * The content coding that still has to be undone, or `null` when the bytes are already plain.
 *
 * `Content-Encoding` is a comma-separated LIST applied in order, so the value is not a single token:
 * treating it as one made `gzip, aws-chunked` — the ordinary shape for an S3 object uploaded with a
 * checksum — unreadable through `asStream()`, along with a bare `aws-chunked` that needs no decoding
 * at all.
 */
export function contentCodingOf(encoding: string | null): string | null {
  if (encoding === null) return null
  const codings = encoding
    .split(',')
    .map((each) => each.trim().toLowerCase())
    .filter((each) => each.length > 0 && !TRANSPARENT_CODINGS.has(each) && !IDENTITY_ENCODINGS.has(each))
  // Only the outermost coding can be undone here; a genuine multi-coding body would need to be
  // unwrapped in reverse, which no backend in this library produces.
  return codings.length === 0 ? null : codings[codings.length - 1]
}

/**
 * Builds the decoder for a content coding. Only called once the coding is known to be non-identity.
 *
 * An UNRECOGNIZED coding throws rather than passing the encoded bytes through: `asStream()` is
 * documented as yielding decompressed content, so returning still-compressed bytes under that
 * contract hands the caller unreadable data with nothing to indicate it. Backends read this value
 * from stored metadata (S3's `ContentEncoding` is arbitrary object metadata), so it is not
 * constrained to the codings this library writes.
 */
function createDecoderFor(encoding: string): Transform {
  switch (encoding) {
    case 'gzip':
    case 'x-gzip':
      return createGunzip()
    case 'deflate':
      return createInflate()
    case 'br':
      return createBrotliDecompress()
    default:
      throw new Error(
        `Cannot decode content stored with an unsupported encoding: ${JSON.stringify(encoding)}. ` +
          `Use asRawStream() to read the stored bytes as they are.`
      )
  }
}

/**
 * @public
 */
export class SimpleContentItem implements ContentItem {
  public encoding: string | null
  /**
   * Defaults to `size` only for UNENCODED content, where the two are the same by definition. For
   * encoded content `size` is the stored (compressed) length while `contentSize` is documented as
   * the logical one, so defaulting to it silently reported the compressed byte count under the
   * field callers use — some as `contentSize ?? size` — to bound reads. `null` is the documented
   * "unknown"; a caller that knows the real logical size passes it explicitly, as both backends do.
   */
  public contentSize: number | null

  constructor(
    private streamCreator: () => Promise<Readable>,
    public size: number | null,
    encoding: string | null,
    contentSize?: number | null
  ) {
    // Tolerates `undefined`: this is a published CJS library, so a JavaScript caller can pass fewer
    // arguments than the signature declares, and dereferencing it crashed the constructor of a
    // `@public` class with a message naming neither it nor the argument.
    this.encoding = normalizeContentEncoding(encoding)
    this.contentSize = contentSize !== undefined ? contentSize : contentCodingOf(this.encoding) === null ? size : null
  }

  static fromBuffer(buffer: Uint8Array): SimpleContentItem {
    return new SimpleContentItem(async () => bufferToStream(buffer), buffer.length, null, buffer.length)
  }

  /**
   * Gets the readable stream, uncompressed if necessary.
   */
  async asStream(): Promise<Readable> {
    const stream = await this.streamCreator()

    const coding = contentCodingOf(this.encoding)
    if (coding !== null) {
      let decoder: Transform
      try {
        decoder = createDecoderFor(coding)
      } catch (err) {
        // The source is already open; leaving it undestroyed would hold its descriptor for the life
        // of the process.
        destroyQuietly(stream)
        throw err
      }
      // `pipeline`, not `stream.pipe(decoder)`: pipe forwards neither errors nor teardown between the
      // two streams. A source that fails to open — the documented race where the file is deleted
      // between retrieve() and this call — then emits an 'error' NOBODY listens to, which CRASHES
      // the process instead of surfacing to the consumer; and a consumer that stops reading leaves
      // the source open, leaking its file descriptor. pipeline propagates both directions, so the
      // consumer sees the source's error and abandoning the returned stream destroys the source.
      // The callback is required to keep pipeline from throwing on its own; the error reaches the
      // consumer through the returned stream.
      pipeline(stream, decoder, () => undefined)
      return decoder
    }

    return stream
  }

  /**
   * Used to get the raw stream, no matter how it is stored.
   * That may imply that the stream may be compressed, if so, the
   * compression encoding should be available in "content".
   */
  async asRawStream(): Promise<Readable> {
    return await this.streamCreator()
  }
}

/**
 * @public
 */
export function bufferToStream(buffer: Uint8Array | Buffer): Readable {
  return Readable.from(Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer))
}

/**
 * Carries the standard premature-close code so cancellation handling can recognize it as
 * teardown-caused rather than a real failure.
 */
function prematureClose(): Error {
  return Object.assign(new Error('Stream closed before it ended.'), { code: 'ERR_STREAM_PREMATURE_CLOSE' })
}

/**
 * Throws when `stream` cannot supply the content of a store, because it has already errored, already
 * been consumed, or already been torn down.
 *
 * Every backend must refuse such a source rather than store what it yields, which is NOTHING. A
 * consumed stream produces zero bytes and its pipe RESOLVES, so a store handed one committed an empty
 * object and reported success — and in a content-addressed store that is permanent: `exist(id)`
 * answers `true`, so the id is never re-fetched and the real content never lands. Reachable from any
 * caller that inspects a body before storing it (a hash or size check) and from a retry that reuses
 * its source.
 *
 * `streamToBuffer` has refused these four states from the start, for exactly this reason, which is why
 * the in-memory backend rejected while the folder-based and S3 backends silently stored 0 bytes. The
 * checks live here so all three share one rule; they are synchronous, so no event can slip in between.
 *
 * @public
 */
export function assertStorableStream(stream: Readable): void {
  if (stream.errored) throw stream.errored
  // Fully consumed by someone else: there are no bytes left to collect, and storing that as empty
  // content is the silent-corruption case above.
  if (stream.readableEnded || stream.destroyed || stream.closed) throw prematureClose()
}

/**
 * @public
 */
export function streamToBuffer(stream: Readable): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    // A stream that has ALREADY settled will never emit 'end', 'error' or 'close' again, so the
    // listeners below would never fire and this promise would hang forever — a far worse failure than
    // the rejection it replaces. Shared with the streaming backends via `assertStorableStream`, whose
    // throw rejects this promise: the two must agree, or one backend stores 0 bytes for a source
    // another rejects. Checked synchronously, before any listener is attached.
    assertStorableStream(stream)
    const buffers: Uint8Array[] = []
    // Tracks settlement so the 'close' fallback below costs nothing on the graceful path: 'close'
    // always follows 'end'/'error', and building its error unconditionally captured a stack on
    // EVERY call — the dominant cost of this helper — only to reject an already-settled promise.
    let settled = false
    stream.on('error', (error) => {
      settled = true
      reject(error)
    })
    stream.on('data', (data) => {
      if (data instanceof Uint8Array || Buffer.isBuffer(data)) {
        buffers.push(data)
      } else {
        settled = true
        reject(new Error('Stream did not emit Uint8Array'))
        stream.destroy()
      }
    })
    stream.on('end', () => {
      settled = true
      resolve(Buffer.concat(buffers))
    })
    // A stream destroyed without an error emits neither 'end' nor 'error' — only 'close'. Without
    // this the returned promise would never settle. Carries the standard premature-close code so
    // cancellation handling can recognize it as teardown-caused rather than a real failure.
    stream.on('close', () => {
      if (settled) return
      reject(prematureClose())
    })
  })
}
