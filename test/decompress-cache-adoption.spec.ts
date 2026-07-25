import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { createHash } from 'crypto'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  IContentStorageComponent,
  IFileSystemComponent
} from '../src'
import { bufferToStream } from '../src'
import { createLogComponent } from '@well-known-components/logger'

const fs = createFsComponent()

const shardOf = (id: string): string => createHash('sha1').update(id).digest('hex').substring(0, 4)

describe('decompressed cache files left by an unclean shutdown', () => {
  let root: string
  let cachedPath: string

  beforeEach(async () => {
    root = mkdtempSync(path.join(os.tmpdir(), 'cache-adoption-'))

    // A first instance stores compressed content and materializes the decompressed range cache, then
    // "dies" without stop() — so nothing evicts what it wrote and the next boot has no record of it.
    const first = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root)
    await first.storeStreamAndCompress('cached-id', bufferToStream(Buffer.alloc(20_000, 0x61)))
    await first.retrieve('cached-id', { start: 0, end: 9 })
    cachedPath = path.join(root, shardOf('cached-id'), 'cached-id')
  })

  afterEach(() => {
    rmSync(root, { recursive: true, force: true })
  })

  it('should leave the decompressed copy on disk when the process does not shut down cleanly', async () => {
    expect(await fs.existPath(cachedPath)).toBe(true)
  })

  describe('when a new instance starts over the same root', () => {
    let second: IContentStorageComponent

    beforeEach(async () => {
      second = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
        // Everything adopted is immediately over budget, so the shutdown eviction has to reclaim it.
        decompressCacheMaxSize: 1
      })
      await second.start?.({} as any)
      // The adoption sweep is detached; stop() awaits it before evicting.
      await second.stop?.()
    })

    it('should reclaim the orphaned decompressed copy instead of leaking it across restarts', async () => {
      expect(await fs.existPath(cachedPath)).toBe(false)
    })

    it('should keep the authoritative gzip representation', async () => {
      expect(await fs.existPath(cachedPath + '.gzip')).toBe(true)
    })
  })

  describe('when adoption is disabled', () => {
    let second: IContentStorageComponent

    beforeEach(async () => {
      // The sweep costs a walk of the whole tree per start, so a large store that shuts down cleanly
      // can opt out — and then must keep the orphan, not have it silently evicted anyway.
      second = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
        decompressCacheMaxSize: 1,
        adoptOrphanedDecompressedFiles: false
      })
      await second.start?.({} as any)
      await second.stop?.()
    })

    it('should leave the orphaned decompressed copy untouched', async () => {
      expect(await fs.existPath(cachedPath)).toBe(true)
    })
  })

  describe('when one entry cannot be examined during the sweep', () => {
    let second: IContentStorageComponent
    let failingFs: IFileSystemComponent
    let otherCachedPath: string

    beforeEach(async () => {
      // One damaged file must cost only its own adoption: aborting the walk would leave every later
      // orphan untracked, which is the leak the sweep exists to close.
      const first = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root)
      await first.storeStreamAndCompress('other-id', bufferToStream(Buffer.alloc(20_000, 0x62)))
      await first.retrieve('other-id', { start: 0, end: 9 })
      otherCachedPath = path.join(root, shardOf('other-id'), 'other-id')

      failingFs = {
        ...fs,
        stat: (async (target: any, ...rest: any[]) => {
          if (String(target) === cachedPath) throw Object.assign(new Error('EIO: injected'), { code: 'EIO' })
          return (fs.stat as any)(target, ...rest)
        }) as typeof fs.stat
      }
      second = await createFolderBasedFileSystemContentStorage(
        { fs: failingFs, logs: await createLogComponent({}) },
        root,
        { decompressCacheMaxSize: 1 }
      )
      await second.start?.({} as any)
      await second.stop?.()
    })

    it('should still reclaim the orphans it could examine', async () => {
      expect(await fs.existPath(otherCachedPath)).toBe(false)
    })
  })

  describe('when eviction itself fails', () => {
    let second: IContentStorageComponent
    let unhandled: unknown[]
    let warnings: string[]

    beforeEach(async () => {
      // Eviction runs from an interval callback and at shutdown. An error escaping it would be an
      // unhandled rejection, which terminates the process by default — so it must be caught and
      // logged instead.
      unhandled = []
      warnings = []
      const onUnhandled = (reason: unknown) => unhandled.push(reason)
      process.on('unhandledRejection', onUnhandled)

      const logger: any = {
        log: () => undefined,
        debug: () => undefined,
        info: () => undefined,
        error: () => undefined,
        warn: (message: string) => warnings.push(message)
      }
      // Armed only once an entry is tracked, so the failure lands inside the eviction pass rather
      // than while the cache file is being created.
      let failVerification = false
      const failingFs: IFileSystemComponent = {
        ...fs,
        // `existsForInvariant` verifies the unlink landed with a stat, and rejects on a non-miss.
        stat: (async (target: any, ...rest: any[]) => {
          if (failVerification && String(target) === cachedPath) {
            throw Object.assign(new Error('EIO: injected'), { code: 'EIO' })
          }
          return (fs.stat as any)(target, ...rest)
        }) as typeof fs.stat,
        // Reports success without removing anything, so the verification above is always reached.
        unlink: (async () => undefined) as typeof fs.unlink
      }
      // Remove the orphan so the range read has to inflate — which is what registers a tracked entry.
      await nodeFs.rm(cachedPath, { force: true })
      second = await createFolderBasedFileSystemContentStorage(
        { fs: failingFs, logs: { getLogger: () => logger } as any },
        root,
        {
          decompressCacheMaxSize: 1,
          decompressCacheTTL: 1,
          decompressCacheEvictionInterval: 20,
          adoptOrphanedDecompressedFiles: false
        }
      )
      await second.start?.({} as any)
      await second.retrieve('cached-id', { start: 0, end: 9 })
      failVerification = true
      // Let the interval fire; it is the timer callback, not stop(), that must swallow the error.
      await new Promise((resolve) => setTimeout(resolve, 120))
      failVerification = false
      await second.stop?.()
      process.off('unhandledRejection', onUnhandled)
    })

    it('should not let the failure escape as an unhandled rejection', () => {
      expect(unhandled).toEqual([])
    })

    it('should report the eviction failure', () => {
      expect(warnings.some((each) => /eviction failed/i.test(each))).toBe(true)
    })
  })

  describe('when a store promotes the path to primary content before the sweep adopts it', () => {
    let second: IContentStorageComponent

    beforeEach(async () => {
      second = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root, {
        decompressCacheMaxSize: 1
      })
      // Overwriting with incompressible content makes the raw primary and removes the gzip, so the
      // path stops being a derived cache. Adopting it anyway would hand the only copy of the new
      // content to the evictor.
      await second.storeStream('cached-id', bufferToStream(Buffer.from('now primary content')))
      await second.start?.({} as any)
      await second.stop?.()
    })

    it('should not evict the freshly stored primary content', async () => {
      expect(await nodeFs.readFile(cachedPath, 'utf8')).toBe('now primary content')
    })
  })
})
