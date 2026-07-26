import { pipeline, Readable, Transform } from 'stream'
import { createBrotliDecompress, createGunzip, createInflate } from 'zlib'
import { ContentItem } from './types'

/**
 * Content-coding tokens that mean "these bytes are not encoded" (RFC 9110 §8.4.1). Normalized to
 * `null` so callers never have to special-case them, and so `contentSize` defaults to `size` as it
 * does for any other unencoded content instead of reporting the logical size as unknown.
 */
const IDENTITY_ENCODINGS = new Set(['', 'identity'])

/** Absorbs a torn-down stream's trailing 'error', which would otherwise be an uncaught exception. */
const ignoreStreamError = (): void => undefined

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
    // `typeof`, not `!== null`: this is a published CJS library, so a JavaScript caller can pass
    // fewer arguments than the signature declares, and `undefined.toLowerCase()` crashed the
    // constructor of a `@public` class with a message naming neither it nor the argument.
    this.encoding =
      typeof encoding === 'string' && IDENTITY_ENCODINGS.has(encoding.toLowerCase()) ? null : (encoding ?? null)
    this.contentSize = contentSize !== undefined ? contentSize : this.encoding ? null : size
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
        // of the process. Listener first: it may still emit 'error' after being destroyed.
        stream.on('error', ignoreStreamError)
        stream.destroy()
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
 * @public
 */
export function streamToBuffer(stream: Readable): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    // A stream that has ALREADY settled will never emit 'end', 'error' or 'close' again, so the
    // listeners below would never fire and this promise would hang forever. Reachable from a retry
    // that reuses a source, or a caller that inspected the body before storing it — and the failure
    // mode (a store that never settles) is far worse than the rejection it replaces. Checked
    // synchronously, before any listener is attached, so no event can slip in between.
    if (stream.errored) {
      reject(stream.errored)
      return
    }
    if (stream.readableEnded) {
      // Fully consumed by someone else: there are no bytes left to collect, and reporting that as
      // an empty buffer would silently store empty content. Premature-close is the same shape a
      // destroyed source produces, and cancellation handling already recognizes it as teardown.
      reject(prematureClose())
      return
    }
    if (stream.destroyed || stream.closed) {
      reject(prematureClose())
      return
    }
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
