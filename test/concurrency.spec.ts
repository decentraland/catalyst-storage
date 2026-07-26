import { mkdtempSync, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { mapWithConcurrency } from '../src/concurrency'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  IContentStorageComponent,
  IFileSystemComponent
} from '../src'
import { bufferToStream } from '../src'
import { createLogComponent } from '@well-known-components/logger'

describe('mapWithConcurrency', () => {
  describe('when more items than the limit are mapped', () => {
    let peak: number
    let results: number[]

    beforeEach(async () => {
      let inFlight = 0
      peak = 0
      results = await mapWithConcurrency(
        Array.from({ length: 50 }, (_, index) => index),
        4,
        async (item) => {
          inFlight++
          peak = Math.max(peak, inFlight)
          await new Promise((resolve) => setImmediate(resolve))
          inFlight--
          return item * 2
        }
      )
    })

    it('should never exceed the configured concurrency', () => {
      expect(peak).toBeLessThanOrEqual(4)
    })

    it('should return every result in input order', () => {
      expect(results).toEqual(Array.from({ length: 50 }, (_, index) => index * 2))
    })
  })

  describe('when the list is empty', () => {
    it('should resolve to an empty array without invoking the mapper', async () => {
      const mapper = jest.fn()

      await expect(mapWithConcurrency([], 4, mapper)).resolves.toEqual([])
      expect(mapper).not.toHaveBeenCalled()
    })
  })

  describe('when an operation fails', () => {
    let failure: Error
    let started: number
    let outcome: unknown

    beforeEach(async () => {
      failure = new Error('storage unavailable')
      started = 0
      outcome = await mapWithConcurrency(
        Array.from({ length: 40 }, (_, index) => index),
        2,
        async (item) => {
          started++
          await new Promise((resolve) => setImmediate(resolve))
          if (item === 0) throw failure
          return item
        }
      ).then(
        () => 'resolved' as const,
        (error: unknown) => error
      )
    })

    it('should reject with the first failure', () => {
      expect(outcome).toBe(failure)
    })

    it('should stop starting new work instead of running the rest of the batch against a broken storage', () => {
      expect(started).toBeLessThan(40)
    })
  })

  describe('when the limit is not a positive number', () => {
    let results: number[]

    beforeEach(async () => {
      // Spawning `Math.min(0, n)` workers would return a fully-holed array as if every item had been
      // mapped, which is a silently wrong result rather than a visible failure.
      results = await mapWithConcurrency([1, 2, 3], 0, async (item) => item + 1)
    })

    it('should still map every item rather than returning holes', () => {
      expect(results).toEqual([2, 3, 4])
    })
  })

  describe('when the limit exceeds the number of items', () => {
    let results: number[]

    beforeEach(async () => {
      results = await mapWithConcurrency([1, 2, 3], 100, async (item) => item + 1)
    })

    it('should map every item exactly once', () => {
      expect(results).toEqual([2, 3, 4])
    })
  })
})

describe('batch surfaces', () => {
  describe('when existMultiple is given far more ids than the concurrency limit', () => {
    let root: string
    let storage: IContentStorageComponent
    let peakInFlight: number

    beforeEach(async () => {
      // Unbounded `Promise.all` issued one stat per id AT ONCE, which exhausts the process
      // file-descriptor limit and fails the whole batch with EMFILE. Nothing asserted the bound, so
      // reverting the backends to Promise.all left the suite green.
      root = mkdtempSync(path.join(os.tmpdir(), 'batch-bound-'))
      const base = createFsComponent()
      let inFlight = 0
      peakInFlight = 0
      const counting: IFileSystemComponent = {
        ...base,
        stat: (async (target: any, ...rest: any[]) => {
          inFlight++
          peakInFlight = Math.max(peakInFlight, inFlight)
          try {
            return await (base.stat as any)(target, ...rest)
          } finally {
            inFlight--
          }
        }) as typeof base.stat
      }
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: counting, logs: await createLogComponent({}) },
        root
      )
      await storage.storeStream('present-id', bufferToStream(Buffer.from('x')))
      await storage.existMultiple(Array.from({ length: 800 }, (_, index) => `id-${index}`))
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should never have more probes in flight than the configured bound', () => {
      expect(peakInFlight).toBeLessThanOrEqual(64)
    })

    it('should still have issued enough concurrency to be worth batching', () => {
      expect(peakInFlight).toBeGreaterThan(1)
    })
  })
})
