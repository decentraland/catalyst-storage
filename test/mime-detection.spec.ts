import { Readable } from 'stream'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { detectMimeTypeFromBuffer, FileTypeLoader, loadFileType, peekHead } from '../src/mime-detection'
import { bufferToStream, streamToBuffer } from '../src/content-item'

function createSpyLogger(): ILoggerComponent.ILogger & { warn: jest.Mock } {
  return { log: jest.fn(), debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
}

describe('peekHead', () => {
  describe('when the source delivers the whole payload as a single chunk', () => {
    let payload: Buffer
    let head: Buffer
    let bodyBytes: Buffer

    beforeEach(async () => {
      // `bufferToStream` (this library's own helper) emits one chunk, so accumulating whole chunks
      // held the entire file in memory — the exact thing streaming the body to S3 avoids.
      payload = Buffer.alloc(50_000, 3)
      const { head: peeked, body } = await peekHead(bufferToStream(payload), 4100)
      head = peeked
      bodyBytes = await streamToBuffer(body)
    })

    it('should cut the head at the requested byte count rather than at the chunk boundary', () => {
      expect(head).toHaveLength(4100)
    })

    it('should re-emit the complete payload through the body', () => {
      expect(bodyBytes).toEqual(payload)
    })
  })

  describe('when the source ends before the requested byte count', () => {
    let payload: Buffer
    let head: Buffer
    let bodyBytes: Buffer

    beforeEach(async () => {
      payload = Buffer.from('short')
      const { head: peeked, body } = await peekHead(bufferToStream(payload), 4100)
      head = peeked
      bodyBytes = await streamToBuffer(body)
    })

    it('should return only the bytes that were available', () => {
      expect(head).toEqual(payload)
    })

    it('should re-emit the payload exactly once through the body', () => {
      expect(bodyBytes).toEqual(payload)
    })
  })

  describe('when the source yields strings rather than buffers', () => {
    let head: Buffer
    let bodyBytes: Buffer

    beforeEach(async () => {
      // A non-binary stream must still be peeked and re-emitted byte-for-byte, not concatenated into
      // something the uploader would send differently.
      const stringStream = Readable.from(['hello ', 'world'])
      const { head: peeked, body } = await peekHead(stringStream, 4100)
      head = peeked
      bodyBytes = await streamToBuffer(body)
    })

    it('should return the bytes of the strings as the head', () => {
      expect(head).toEqual(Buffer.from('hello world'))
    })

    it('should re-emit the same bytes through the body', () => {
      expect(bodyBytes).toEqual(Buffer.from('hello world'))
    })
  })

  describe('when the source delivers many chunks that straddle the boundary', () => {
    let payload: Buffer
    let head: Buffer
    let bodyBytes: Buffer

    beforeEach(async () => {
      payload = Buffer.concat(Array.from({ length: 10 }, (_, index) => Buffer.alloc(1000, index)))
      const chunked = Readable.from(
        (function* () {
          for (let offset = 0; offset < payload.length; offset += 1000) {
            yield payload.subarray(offset, offset + 1000)
          }
        })()
      )
      const { head: peeked, body } = await peekHead(chunked, 4100)
      head = peeked
      bodyBytes = await streamToBuffer(body)
    })

    it('should return exactly the first 4100 bytes of the payload', () => {
      expect(head).toEqual(payload.subarray(0, 4100))
    })

    it('should re-emit the payload with the split chunk stitched back together', () => {
      expect(bodyBytes).toEqual(payload)
    })
  })
})

describe('detectMimeTypeFromBuffer', () => {
  let logger: ILoggerComponent.ILogger & { warn: jest.Mock }

  beforeEach(() => {
    logger = createSpyLogger()
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  describe('when the detector recognizes the content', () => {
    let loader: FileTypeLoader
    let detected: string

    beforeEach(async () => {
      loader = async () => ({ fileTypeFromBuffer: async () => ({ mime: 'image/png' }) })
      detected = await detectMimeTypeFromBuffer(Buffer.alloc(10_000, 1), logger, loader)
    })

    it('should return the detected MIME type', () => {
      expect(detected).toBe('image/png')
    })
  })

  describe('when the detector is given more bytes than the detection window', () => {
    let inspected: Uint8Array | undefined
    let payload: Buffer

    beforeEach(async () => {
      payload = Buffer.alloc(10_000, 1)
      const loader: FileTypeLoader = async () => ({
        fileTypeFromBuffer: async (buffer: Uint8Array) => {
          inspected = buffer
          return { mime: 'image/png' }
        }
      })
      await detectMimeTypeFromBuffer(payload, logger, loader)
    })

    it('should hand it only the first 4100 bytes', () => {
      expect(inspected).toHaveLength(4100)
    })
  })

  describe('when the content matches no known signature', () => {
    let detected: string

    beforeEach(async () => {
      const loader: FileTypeLoader = async () => ({ fileTypeFromBuffer: async () => ({ mime: undefined }) })
      detected = await detectMimeTypeFromBuffer(Buffer.from('plain text'), logger, loader)
    })

    it('should fall back to application/octet-stream', () => {
      expect(detected).toBe('application/octet-stream')
    })
  })

  describe('when the detector module cannot be loaded', () => {
    let detected: string

    beforeEach(async () => {
      // Swallowing this made every object store as application/octet-stream with nothing in the
      // logs, which is what let a broken detector hide in this repo's own test suite.
      const loader: FileTypeLoader = async () => {
        throw new Error('ESM loader unavailable')
      }
      detected = await detectMimeTypeFromBuffer(Buffer.from('anything'), logger, loader)
    })

    it('should fall back to application/octet-stream', () => {
      expect(detected).toBe('application/octet-stream')
    })

    it('should warn with the underlying reason instead of failing silently', () => {
      expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('MIME type detection failed'), {
        error: 'ESM loader unavailable'
      })
    })
  })

  describe('when the detector itself throws while inspecting', () => {
    let detected: string

    beforeEach(async () => {
      const loader: FileTypeLoader = async () => ({
        fileTypeFromBuffer: async () => {
          throw new Error('corrupt header')
        }
      })
      detected = await detectMimeTypeFromBuffer(Buffer.from('anything'), logger, loader)
    })

    it('should fall back to application/octet-stream', () => {
      expect(detected).toBe('application/octet-stream')
    })

    it('should warn with the detector error', () => {
      expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('MIME type detection failed'), {
        error: 'corrupt header'
      })
    })
  })

  describe('when the bundled loader is called more than once', () => {
    let first: Promise<unknown>
    let second: Promise<unknown>

    beforeEach(() => {
      // Memoized per process: loading `file-type` enters the ESM loader, and doing that per store was the
      // cost this memo removed. The same promise coming back is what proves it is not re-entered.
      first = loadFileType()
      second = loadFileType()
    })

    it('should hand back the same in-flight module promise', () => {
      // Identity is the whole assertion, and it is deliberately NOT awaited. Resolving it enters the real ESM
      // loader, which is what this memo exists to do once per process — and awaiting it from a test was flaky
      // (1 run in 6): an import still in flight when Jest tears the environment down fails, the same hazard
      // that makes the S3 component await its loader at construction. `loadFileType` attaches its own `catch`
      // to clear the memo, so nothing is left unhandled here.
      expect(second).toBe(first)
    })
  })
})
