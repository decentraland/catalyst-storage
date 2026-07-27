import { pipeline, Readable, Transform } from 'stream'
import { createBrotliDecompress, createGunzip, createInflate } from 'zlib'
import { ContentItem } from './types'
import { destroyQuietly, ignoreStreamError } from './stream-teardown'

/**
 * Content-coding tokens that mean "these bytes are not encoded" (RFC 9110 §8.4.1). Normalized to
 * `null` so callers never have to special-case them, and so `contentSize` defaults to `size` as it
 * does for any other unencoded content instead of reporting the logical size as unknown.
 */
const IDENTITY_ENCODINGS = new Set(['', 'identity'])

/**
 * Codings that describe the TRANSFER of the bytes rather than the content itself, and so are already
 * undone by the time a body reaches us. `aws-chunked` is written by S3 on flexible-checksum uploads,
 * frequently alongside a real coding (`gzip, aws-chunked`).
 *
 * `aws-chunked` ONLY. Bare `chunked` was here too and has been removed: it is a `Transfer-Encoding`
 * value, not a content coding, so an object whose `Content-Encoding` says `chunked` carries metadata that
 * is simply wrong. Treating it as "nothing is applied" silently accepted that and served the bytes as if
 * the header agreed; leaving it unrecognised means `asStream()` refuses it and names `asRawStream()`, which
 * is what every other coding this library cannot undo already does.
 */
const TRANSPARENT_CODINGS = new Set(['aws-chunked'])

/** Whether a single, trimmed coding token describes nothing that is still applied to these bytes. */
function isVacuousCoding(coding: string): boolean {
  const lower = coding.toLowerCase()
  return lower.length === 0 || TRANSPARENT_CODINGS.has(lower) || IDENTITY_ENCODINGS.has(lower)
}

/**
 * The value a `ContentItem` reports as its `encoding`, given a raw `Content-Encoding`.
 *
 * Drops every coding that is not still applied to the bytes being served, and reports `null` when none
 * remains. That is the SAME predicate `contentCodingOf` uses to decide whether `asStream()` decodes, which
 * is the whole point: the two answers have to agree, or a caller is told the content is encoded in a way
 * this storage has already decided needs no decoding.
 *
 * - `identity` (and an empty header) mean "not encoded", so reporting them verbatim forces every caller to
 *   special-case a value that carries no information.
 * - TRANSFER codings go too. A bare `aws-chunked` object streams plain bytes and has a known `contentSize`,
 *   yet reporting `aws-chunked` here handed callers a `Content-Encoding` to forward for content that is not
 *   encoded at all — a client receiving it either fails to decode or is misled about what it holds. And
 *   `gzip, aws-chunked` — the ordinary shape for a checksummed S3 upload — becomes `gzip`, which is exactly
 *   what a caller forwarding the header should send for the bytes `asRawStream()` yields.
 *
 * Surviving codings keep their original spelling and order, so the result stays a valid header value
 * rather than a normalized-away one.
 *
 * @public
 */
export function normalizeContentEncoding(encoding: string | null | undefined): string | null {
  if (typeof encoding !== 'string') return encoding ?? null
  const applied = encoding
    .split(',')
    .map((each) => each.trim())
    .filter((each) => !isVacuousCoding(each))
  return applied.length === 0 ? null : applied.join(', ')
}

/**
 * Every coding still applied to the stored bytes, outermost LAST, lowercased.
 *
 * `Content-Encoding` is a comma-separated LIST applied in order, so the value is not a single token:
 * treating it as one made `gzip, aws-chunked` — the ordinary shape for an S3 object uploaded with a
 * checksum — unreadable through `asStream()`, along with a bare `aws-chunked` that needs no decoding
 * at all.
 */
function appliedCodings(encoding: string | null): string[] {
  if (encoding === null) return []
  return encoding
    .split(',')
    .map((each) => each.trim().toLowerCase())
    .filter((each) => !isVacuousCoding(each))
}

/**
 * The single content coding that still has to be undone, or `null` when the bytes are already plain.
 *
 * Answers for the OUTERMOST coding, which is the only one that could be undone first. Callers use it as
 * "is anything still applied to these bytes?" — to decide whether `contentSize` is knowable, and whether a
 * byte range can address the content — and for both of those a multi-coding body answers the same as a
 * single-coding one. Deciding what to DECODE goes through `appliedCodings` instead, because a body with more
 * than one coding cannot be undone by one decoder; see `asStream`.
 */
export function contentCodingOf(encoding: string | null): string | null {
  const codings = appliedCodings(encoding)
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

    const codings = appliedCodings(this.encoding)
    if (codings.length > 0) {
      let decoder: Transform
      try {
        // MORE THAN ONE coding is refused, not partially undone. Only one decoder is applied here, so a body
        // stored as `gzip, br` would have Brotli undone and be handed back STILL GZIPPED — under a contract
        // that says this yields decompressed content, with nothing to tell the caller otherwise. Refusing is
        // the same answer this already gives for a coding it cannot decode at all, and for the same reason.
        //
        // No backend in this library writes more than one coding: the folder-based one writes `gzip` and S3
        // writes none, so this is only reachable for an object an operator or a migration put there. Chaining
        // decoders in reverse would be the alternative, but it would be a decode path with no way to produce
        // test input from the library itself and no caller asking for it.
        if (codings.length > 1) {
          throw new Error(
            `Cannot decode content stored with multiple content codings: ${JSON.stringify(this.encoding)}. ` +
              `This storage undoes at most one. Use asRawStream() to read the stored bytes as they are.`
          )
        }
        decoder = createDecoderFor(codings[0])
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

    // The UNENCODED path returns the source itself, so nothing has attached an 'error' listener to it
    // — and the documented delete-race (the file removed between retrieve() and this call) then emits
    // one NOBODY is listening to, which terminates the process by default. The encoded path above is
    // safe only incidentally, because `pipeline` keeps a listener on the source; a consumer must not
    // have to know which representation it got in order to survive the same race. A consumer that
    // attaches its own handler still receives the error — extra listeners do not displace theirs — and
    // one that uses bare `pipe()` (which forwards no errors) sees a truncated body instead of a dead
    // process, matching what the encoded path already does.
    stream.on('error', ignoreStreamError)
    return stream
  }

  /**
   * Used to get the raw stream, no matter how it is stored.
   * That may imply that the stream may be compressed, if so, the
   * compression encoding should be available in "content".
   */
  async asRawStream(): Promise<Readable> {
    const stream = await this.streamCreator()
    // Same hazard, same reason as the unencoded branch of `asStream`: this hands back an unlistened
    // source whose open(2) can still fail.
    stream.on('error', ignoreStreamError)
    return stream
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
 * been consumed (in whole OR in part), or already been torn down.
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
  // PARTIALLY consumed, which the four flags above cannot see: they describe a stream that has
  // FINISHED, and they all flip a tick later than the read that made the source unusable. A caller
  // that read even one byte — the "hash or sniff the body before storing it" pattern named above, the
  // very reason this guard exists — therefore walked straight through it, with three distinct
  // outcomes, all of which RESOLVED:
  // - an `fs.ReadStream` after `read(4)` stored the body minus its first four bytes;
  // - a push-based source after one `iterator.next()` stored ZERO bytes;
  // - a `Readable.from` source whose iterator was left suspended never settled at all, because the
  //   iterator's lingering 'readable' listener takes precedence over `pipe`, so nothing ever flowed.
  //
  // `readableDidRead` is the state Node exposes for exactly this question. It is `false` for a fresh
  // source AND for a live-but-empty one (so an empty body is still storable, as it must be), and
  // `true` the instant anything has been pulled — which is precisely when this source can no longer
  // supply the content it is being asked to store.
  //
  // KNOWN OVER-REJECTION: `readableDidRead` records that a read HAPPENED, and nothing un-records it, so a
  // caller that sniffs a head and then `unshift()`s it back — the documented way to un-consume optimistically
  // pulled data, and a source that really does still hold the whole body — is refused too. That is
  // deliberate: there is no signal that distinguishes "read and put back" from "read and kept", and the two
  // failure modes are not comparable. Refusing a restored source costs a loud, actionable rejection; storing
  // a partially consumed one silently commits wrong bytes under an id that is then never re-fetched. A
  // caller in that position should hand over a fresh source (re-open the file, or buffer the body and use
  // `bufferToStream`) rather than a rewound one.
  if (stream.readableDidRead) throw prematureClose()
  // A source in a NON-UTF8 encoding mode yields strings, and every backend turns those back into bytes as
  // utf8 (the folder-based one by piping into an `fs.WriteStream`, whose default encoding that is; S3 and the
  // in-memory backend via `Buffer.from`). For `latin1`/`hex`/`base64` that round trip is lossy, so the bytes
  // stored are not the bytes read — silent corruption under an id that is then never re-fetched, which is the
  // failure class this guard exists for. `utf8` round-trips exactly and stays allowed, as does the ordinary
  // case of no encoding set at all.
  const encoding = stream.readableEncoding
  if (encoding !== null && encoding !== 'utf8' && encoding !== 'utf-8') {
    throw new Error(
      `Cannot store a stream in '${encoding}' encoding mode: its string chunks would be re-encoded as utf8, ` +
        `which does not round-trip. Read the source in binary mode (no encoding) instead.`
    )
  }
}

/**
 * Collects a stream into a single Buffer.
 *
 * `maxBytes` bounds how much will be held in memory, rejecting (and tearing the source down) once more
 * than that has arrived. It is OPT-IN and unset by default, which is the historical behaviour: this is
 * a `@public` helper whose callers know their own content sizes, and imposing a default ceiling would
 * start rejecting bodies that store and read correctly today. Worth passing when the stream is a
 * DECODED one — `streamToBuffer(await item.asStream())` over attacker-supplied compressed content
 * inflates without limit, since the folder-based backend's `decompressMaxFileSize` bounds only the
 * range-request inflation path, not a full read.
 *
 * @public
 */
export function streamToBuffer(stream: Readable, maxBytes?: number): Promise<Buffer> {
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
    let total = 0
    stream.on('error', (error) => {
      settled = true
      reject(error)
    })
    stream.on('data', (data) => {
      let chunk: Uint8Array
      if (data instanceof Uint8Array || Buffer.isBuffer(data)) {
        chunk = data
      } else if (typeof data === 'string') {
        // A STRING-emitting source is storable content, not a caller error. A stream in encoding mode
        // (`createReadStream(p, 'utf8')`, `Readable.from(JSON.stringify(x))`, any string-mode
        // transform) is piped straight into an `fs.WriteStream` by the folder-based backend and
        // `Buffer.from`-ed by S3's head peek, so both of those STORE it — only this helper refused,
        // which meant the in-memory backend rejected a source the other two accepted. Decoded as utf8
        // to match what those backends do with the same chunk (`Writable`'s default encoding).
        chunk = Buffer.from(data)
      } else {
        settled = true
        reject(new Error('Stream did not emit Uint8Array'))
        stream.destroy()
        return
      }
      total += chunk.length
      if (maxBytes !== undefined && total > maxBytes) {
        settled = true
        reject(new Error(`Stream exceeded the maximum allowed size of ${maxBytes} bytes`))
        stream.destroy()
        return
      }
      buffers.push(chunk)
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
