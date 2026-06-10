import { existsSync, mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { compressContentFile } from '../src/extras/compression'

describe('compressContentFile', () => {
  let dir: string

  beforeEach(() => {
    dir = mkdtempSync(path.join(os.tmpdir(), 'compression-'))
  })

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true })
  })

  it(`When the content compresses well, then a .gzip is produced`, async () => {
    const input = path.join(dir, 'compressible')
    await nodeFs.writeFile(input, Buffer.alloc(1000, 0))

    const result = await compressContentFile(input)

    expect(result).toBe(true)
    expect(existsSync(input + '.gzip')).toBe(true)
  })

  it(`When the content does not compress well, then the .gzip is discarded`, async () => {
    const input = path.join(dir, 'incompressible')
    await nodeFs.writeFile(input, 'a')

    const result = await compressContentFile(input)

    expect(result).toBe(false)
    expect(existsSync(input + '.gzip')).toBe(false)
  })

  it(`When the source cannot be read, then it rejects and removes the partial .gzip`, async () => {
    const input = path.join(dir, 'missing')
    // The missing source triggers a read-open error. That goes through the same catch/unlink
    // cleanup path as a mid-stream gzip/write failure, so it exercises the partial-output removal.
    // Pre-create a stale .gzip so the assertion is deterministic regardless of stream-open races:
    // the failed compression must remove it rather than leave it to shadow the (absent) source.
    await nodeFs.writeFile(input + '.gzip', 'stale')

    await expect(compressContentFile(input)).rejects.toThrow()

    expect(existsSync(input + '.gzip')).toBe(false)
  })
})
