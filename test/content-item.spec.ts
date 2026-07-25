import { randomBytes } from 'crypto'
import { createReadStream, mkdtempSync, rmSync, writeFileSync } from 'fs'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import { gzipSync } from 'zlib'
import { SimpleContentItem, streamToBuffer } from '../src'

describe('SimpleContentItem', () => {
  let root: string
  const content = Buffer.alloc(1 << 20, 7)

  beforeEach(() => {
    root = mkdtempSync(path.join(os.tmpdir(), 'content-item-'))
  })

  afterEach(() => {
    rmSync(root, { recursive: true, force: true })
  })

  describe('when a gzip item is read', () => {
    let bytes: Buffer

    beforeEach(async () => {
      const gzipPath = path.join(root, 'content.gzip')
      writeFileSync(gzipPath, gzipSync(content))
      const item = new SimpleContentItem(async () => createReadStream(gzipPath), null, 'gzip')
      bytes = await streamToBuffer(await item.asStream())
    })

    it('should inflate the stored bytes', () => {
      expect(bytes).toEqual(content)
    })
  })

  describe('when a gzip item source cannot be opened', () => {
    let outcome: unknown

    beforeEach(async () => {
      // The documented race: the file is deleted between retrieve() and asStream(). Inflating with
      // `pipe` left the source's 'error' unhandled, which crashes the process instead of reaching
      // the consumer — so this asserts the consumer is the one who hears about it.
      const item = new SimpleContentItem(
        async () => createReadStream(path.join(root, 'deleted-underneath-us.gzip')),
        null,
        'gzip'
      )
      outcome = await streamToBuffer(await item.asStream()).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should reject the consumer with the source error', () => {
      expect((outcome as { code?: string }).code).toEqual('ENOENT')
    })
  })

  describe('when a consumer abandons a gzip stream mid-read', () => {
    let source: Readable

    beforeEach(async () => {
      // `pipe` does not forward teardown either: abandoning the inflated stream left the source open,
      // leaking its file descriptor for as long as the process lived.
      // Incompressible and large, so the source is provably still mid-read when the consumer gives
      // up — compressible content finishes in a single read and would close on its own.
      const gzipPath = path.join(root, 'abandoned.gzip')
      writeFileSync(gzipPath, gzipSync(randomBytes(8 << 20)))
      source = createReadStream(gzipPath)
      const item = new SimpleContentItem(async () => source, null, 'gzip')
      const inflated = await item.asStream()
      await new Promise<void>((resolve) => inflated.once('data', () => resolve()))
      inflated.destroy()
      await new Promise<void>((resolve) => source.once('close', () => resolve()))
    })

    it('should destroy the source stream', () => {
      expect(source.destroyed).toEqual(true)
    })
  })

  describe('when a raw item is read', () => {
    let bytes: Buffer

    beforeEach(async () => {
      const rawPath = path.join(root, 'content')
      writeFileSync(rawPath, content)
      const item = new SimpleContentItem(async () => createReadStream(rawPath), content.length, null)
      bytes = await streamToBuffer(await item.asStream())
    })

    it('should return the stored bytes unchanged', () => {
      expect(bytes).toEqual(content)
    })
  })
})
