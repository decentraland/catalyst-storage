import {
  createReadStream,
  createWriteStream,
  existsSync,
  mkdtempSync,
  promises as nodeFs,
  rmSync,
  writeFileSync
} from 'fs'
import { Readable } from 'stream'
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

  describe('when the output path is the same as the input path', () => {
    let source: string

    beforeEach(() => {
      // Compressing a file onto itself would open it for reading and truncate it for writing at the
      // same time, destroying the content it is meant to preserve.
      source = path.join(dir, 'same.txt')
      writeFileSync(source, 'content worth keeping')
    })

    it('should refuse rather than destroy the source', async () => {
      await expect(compressContentFile(source, undefined, source)).rejects.toThrow(/src==dst/)
    })

    it('should leave the source intact', async () => {
      await compressContentFile(source, undefined, source).catch(() => undefined)

      expect(await nodeFs.readFile(source, 'utf8')).toBe('content worth keeping')
    })
  })

  describe('when the partial output cannot be removed after a failure', () => {
    let warnings: Array<{ message: string; context: any }>
    let source: string

    beforeEach(async () => {
      // A surviving partial `.gzip` would be preferred by reads over the real content, so the failure
      // to remove it has to be visible rather than swallowed with the compression error.
      warnings = []
      source = path.join(dir, 'input.txt')
      writeFileSync(source, 'x'.repeat(5000))
      const logger: any = {
        log: () => undefined,
        debug: () => undefined,
        info: () => undefined,
        error: () => undefined
      }
      logger.warn = (message: string, context: any) => warnings.push({ message, context })
      const failing: CompressionFileSystem = {
        // An in-memory source rather than a real file: only the failed cleanup is under test here,
        // and a real read stream's open can still land after this suite removes its temp directory,
        // emitting an unhandled ENOENT into whichever test runs next.
        createReadStream: (() => Readable.from([Buffer.alloc(64)])) as any,
        createWriteStream: (() => {
          throw Object.assign(new Error('EIO: cannot open output'), { code: 'EIO' })
        }) as any,
        unlink: async () => {
          throw Object.assign(new Error('EPERM: cannot remove'), { code: 'EPERM' })
        },
        stat: nodeFs.stat,
        lstat: nodeFs.lstat
      }
      await compressContentFile(source, logger, path.join(dir, 'out.gzip'), undefined, failing).catch(() => undefined)
    })

    it('should warn that the compressed output was left behind', () => {
      expect(warnings.map((each) => each.message)).toContainEqual(expect.stringContaining('Failed to remove'))
    })

    it('should name the unlink failure rather than the compression one', () => {
      expect(warnings[0].context.error).toContain('EPERM')
    })
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

  describe('when the source is torn down while its open is still in flight', () => {
    let heldSource: Readable

    beforeEach(async () => {
      // `createReadStream` starts an async open; the destination throwing tears the source down while
      // that open is still pending. The open then fails and the DESTROYED stream still emits 'error'
      // — with none attached that is an uncaught exception, which terminates the process by default
      // (reproduced 200/200 outside Jest). Reachable from outside: the input path is the caller's and
      // `compressContentFile` is public API.
      heldSource = new Readable({ read() {} })
      const adapter = {
        createReadStream: () => heldSource,
        createWriteStream: () => {
          throw new Error('cannot open the output')
        },
        unlink: nodeFs.unlink,
        stat: nodeFs.stat,
        lstat: nodeFs.lstat
      } as unknown as CompressionFileSystem
      await compressContentFile(path.join(dir, 'vanishes'), undefined, undefined, undefined, adapter).catch(
        () => undefined
      )
    })

    it('should leave a handler attached so the late failure cannot escape', () => {
      // `emit('error')` THROWS when nothing is listening, which is precisely how it escapes as an
      // uncaught exception in production. Asserting on a process-level handler cannot work here —
      // Jest installs its own and the observation is swallowed.
      expect(() =>
        heldSource.emit('error', Object.assign(new Error('late open failure'), { code: 'ENOENT' }))
      ).not.toThrow()
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
