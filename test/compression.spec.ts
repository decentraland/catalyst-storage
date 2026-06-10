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

  it(`When compression fails, then it rejects and leaves no partial .gzip behind`, async () => {
    const input = path.join(dir, 'missing')
    // Pre-create a stale .gzip so the assertion is deterministic regardless of stream-open races:
    // the failed compression must remove it rather than leave it to shadow the (absent) source.
    await nodeFs.writeFile(input + '.gzip', 'stale')

    await expect(compressContentFile(input)).rejects.toThrow()

    expect(existsSync(input + '.gzip')).toBe(false)
  })
})
