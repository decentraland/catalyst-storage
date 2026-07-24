import {
  createReadStream,
  createWriteStream,
  existsSync,
  mkdtempSync,
  promises as nodeFs,
  rmSync,
  writeFileSync
} from 'fs'
import os from 'os'
import path from 'path'
import { CompressionFileSystem, compressContentFile } from '../src/extras/compression'

/**
 * A filesystem whose methods depend on `this`, which is what a class-based adapter looks like. Any
 * method the compression pulls off the object before calling would lose its receiver and fail here.
 */
class ThisDependentFileSystem {
  readonly calls: string[] = []

  private record(method: string): void {
    // Throws if `this` is not the instance — the whole point of the double.
    this.calls.push(method)
  }

  createReadStream(target: any, options?: any) {
    this.record('createReadStream')
    return createReadStream(target, options)
  }

  createWriteStream(target: any, options?: any) {
    this.record('createWriteStream')
    return createWriteStream(target, options)
  }

  async unlink(target: any) {
    this.record('unlink')
    return nodeFs.unlink(target)
  }

  async stat(target: any) {
    this.record('stat')
    return nodeFs.stat(target)
  }

  async lstat(target: any) {
    this.record('lstat')
    return nodeFs.lstat(target)
  }
}

describe('compressContentFile', () => {
  let dir: string

  beforeEach(() => {
    dir = mkdtempSync(path.join(os.tmpdir(), 'compression-'))
  })

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true })
  })

  describe('when the filesystem adapter methods depend on `this`', () => {
    let adapter: ThisDependentFileSystem
    let outcome: unknown

    beforeEach(async () => {
      // Detaching a method into a local (`const statSize = fs.lstat ?? fs.stat`) works for an object
      // of standalone functions but breaks any adapter that carries state on the instance.
      const input = path.join(dir, 'this-bound')
      await nodeFs.writeFile(input, Buffer.alloc(4096, 3))
      adapter = new ThisDependentFileSystem()
      outcome = await compressContentFile(
        input,
        undefined,
        undefined,
        undefined,
        adapter as unknown as CompressionFileSystem
      ).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    it('should compress through the adapter', () => {
      expect(outcome).toEqual(true)
    })

    it('should measure the sizes through the adapter', () => {
      expect(adapter.calls).toContain('lstat')
    })
  })

  describe('when the adapter has no lstat', () => {
    let adapter: ThisDependentFileSystem
    let outcome: unknown

    beforeEach(async () => {
      // `lstat` is optional, so the fallback must also be called on the object.
      const input = path.join(dir, 'no-lstat')
      await nodeFs.writeFile(input, Buffer.alloc(4096, 3))
      adapter = new ThisDependentFileSystem()
      const withoutLstat = Object.assign(Object.create(Object.getPrototypeOf(adapter)), adapter, {
        lstat: undefined
      }) as unknown as CompressionFileSystem
      outcome = await compressContentFile(input, undefined, undefined, undefined, withoutLstat).then(
        (value) => value,
        (error: unknown) => error
      )
    })

    it('should compress using stat instead', () => {
      expect(outcome).toEqual(true)
    })
  })

  describe('when the adapter throws synchronously while opening the output', () => {
    let input: string
    let outcome: unknown
    let output: string

    beforeEach(async () => {
      // A custom adapter may throw where native fs would emit asynchronously. The partial output
      // must still be cleaned up rather than left behind as a canonical `.gzip` reads would prefer.
      input = path.join(dir, 'sync-throw')
      output = input + '.gzip'
      await nodeFs.writeFile(input, Buffer.alloc(4096, 3))
      const adapter = {
        createReadStream,
        createWriteStream: (target: any) => {
          // Model an adapter that creates the file and then fails.
          writeFileSync(target, Buffer.alloc(0))
          throw new Error('cannot open the output')
        },
        unlink: nodeFs.unlink,
        stat: nodeFs.stat,
        lstat: nodeFs.lstat
      } as unknown as CompressionFileSystem
      outcome = await compressContentFile(input, undefined, undefined, undefined, adapter).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should surface the construction failure', () => {
      expect((outcome as Error).message).toEqual('cannot open the output')
    })

    it('should remove the partial output', () => {
      expect(existsSync(output)).toEqual(false)
    })
  })

  describe('when the signal is already aborted', () => {
    let input: string
    let outcome: 'resolved' | unknown

    beforeEach(async () => {
      // The signal reaches the read→gzip→write pipeline itself: an abort tears the streams down
      // instead of letting the compression run to completion for a cancelled request.
      input = path.join(dir, 'aborted-compressible')
      await nodeFs.writeFile(input, Buffer.alloc(100000, 0))
      const controller = new AbortController()
      controller.abort()
      outcome = await compressContentFile(input, undefined, undefined, controller.signal).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should reject with an abort error', () => {
      expect((outcome as Error).name).toBe('AbortError')
    })

    it('should remove the partial output', () => {
      expect(existsSync(input + '.gzip')).toBe(false)
    })

    it('should leave the input intact', async () => {
      expect((await nodeFs.stat(input)).size).toBe(100000)
    })
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
