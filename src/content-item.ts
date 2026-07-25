import { pipeline, Readable } from 'stream'
import { createGunzip } from 'zlib'
import { ContentItem } from './types'

/**
 * @public
 */
export class SimpleContentItem implements ContentItem {
  constructor(
    private streamCreator: () => Promise<Readable>,
    public size: number | null,
    public encoding: string | null,
    public contentSize: number | null = size
  ) {}

  static fromBuffer(buffer: Uint8Array): SimpleContentItem {
    return new SimpleContentItem(async () => bufferToStream(buffer), buffer.length, null, buffer.length)
  }

  /**
   * Gets the readable stream, uncompressed if necessary.
   */
  async asStream(): Promise<Readable> {
    const stream = await this.streamCreator()

    if (this.encoding === 'gzip') {
      const gunzip = createGunzip()
      // `pipeline`, not `stream.pipe(gunzip)`: pipe forwards neither errors nor teardown between the
      // two streams. A source that fails to open — the documented race where the file is deleted
      // between retrieve() and this call — then emits an 'error' NOBODY listens to, which CRASHES
      // the process instead of surfacing to the consumer; and a consumer that stops reading leaves
      // the source open, leaking its file descriptor. pipeline propagates both directions, so the
      // consumer sees the source's error and abandoning the returned stream destroys the source.
      // The callback is required to keep pipeline from throwing on its own; the error reaches the
      // consumer through the returned stream.
      pipeline(stream, gunzip, () => undefined)
      return gunzip
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
 * @public
 */
export function streamToBuffer(stream: Readable): Promise<Buffer> {
  return new Promise((resolve, reject) => {
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
      reject(Object.assign(new Error('Stream closed before it ended.'), { code: 'ERR_STREAM_PREMATURE_CLOSE' }))
    })
  })
}
