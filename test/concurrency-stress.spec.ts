import { createHash } from 'crypto'
import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { gunzipSync } from 'zlib'
import { createFolderBasedFileSystemContentStorage, createFsComponent, IFileSystemComponent } from '../src'
import { bufferToStream, streamToBuffer } from '../src'
import { createLogComponent } from '@well-known-components/logger'

/**
 * Interleaves stores, deletes and range reads on the same handful of ids and checks the invariants
 * the concurrency design claims. The individual races are covered by targeted tests elsewhere; this
 * exercises them together, under contention, with eviction actively running.
 *
 * Content is self-describing — `v<6-digit version>:` followed by a run of 'a' whose LENGTH also
 * varies with the version — so no read can pass verification unless it returned exactly one complete
 * stored version. A torn write, a mixed raw/gzip state or a stale decompressed cache all fail it.
 *
 * Deliberately NOT asserted: that `size`/`contentSize` match the streamed byte count. A ContentItem's
 * metadata is measured at `retrieve()` and its stream opens later, so overwriting an id with
 * different content can legitimately produce metadata from the previous version — a documented
 * limitation of the lazy handle, see the concurrent-read contract in the README.
 */

const IDS = ['id-a', 'id-b', 'id-c']
const OPERATIONS = 400
const WORKERS = 12

const bodyFor = (version: number): Buffer =>
  Buffer.from(`v${String(version).padStart(6, '0')}:` + 'a'.repeat(2000 + (version % 7) * 250))

const versionOf = (bytes: Buffer): number | undefined => {
  const match = /^v(\d{6}):/.exec(bytes.subarray(0, 8).toString('latin1'))
  return match ? Number(match[1]) : undefined
}

/**
 * Outcomes a read racing a write is allowed to produce with NO faults injected. Deliberately narrow:
 * a shared allowlist covering the fault-injected run too would let a regression that started
 * quarantining ids, or failing deletes, pass silently on a healthy filesystem.
 */
const isTolerableFailure = (error: any): boolean =>
  error?.code === 'ENOENT' || error instanceof RangeError || error?.code === 'ERR_STREAM_PREMATURE_CLOSE'

/** Additionally tolerated once `unlink` is being made to fail: these are the designed responses. */
const isTolerableUnderInjectedFaults = (error: any): boolean =>
  isTolerableFailure(error) ||
  /mixed state that could not be repaired/.test(error?.message ?? '') ||
  /injected/.test(error?.message ?? '') ||
  /quarantined/.test(error?.message ?? '') ||
  /could not be removed/.test(error?.message ?? '') ||
  /Failed to (delete|remove)/.test(error?.message ?? '')

type StormResult = {
  corruption: string[]
  unexpectedFailures: string[]
  stagedLeaks: string[]
  intentLeaks: string[]
  mixedStates: string[]
  corruptAtRest: string[]
  duplicateIds: string[]
  injectedFaults: number
}

async function runStorm(options: { seed: number; injectFaults?: boolean }): Promise<StormResult> {
  let cleanup: () => void = () => undefined
  try {
    return await runStormIn(options, (dir) => (cleanup = () => rmSync(dir, { recursive: true, force: true })))
  } finally {
    cleanup()
  }
}

async function runStormIn(
  options: { seed: number; injectFaults?: boolean },
  onRoot: (dir: string) => void
): Promise<StormResult> {
  let seed = options.seed
  const rnd = (): number => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff
    return seed / 0x7fffffff
  }

  const result: StormResult = {
    corruption: [],
    unexpectedFailures: [],
    stagedLeaks: [],
    intentLeaks: [],
    mixedStates: [],
    corruptAtRest: [],
    duplicateIds: [],
    injectedFaults: 0
  }

  const root = mkdtempSync(path.join(os.tmpdir(), 'stress-'))
  onRoot(root)
  const base = createFsComponent()
  const component: IFileSystemComponent = { ...base }
  if (options.injectFaults) {
    // Intermittent unlink failures drive the counterpart-cleanup failure path — pending intents,
    // quarantine, read-triggered repair — which is otherwise nearly unreachable.
    component.unlink = (async (target: any) => {
      if (rnd() < 0.08) {
        result.injectedFaults++
        throw Object.assign(new Error('EPERM: injected'), { code: 'EPERM' })
      }
      return base.unlink(target)
    }) as typeof base.unlink
  }

  const storage = await createFolderBasedFileSystemContentStorage(
    { fs: component, logs: await createLogComponent({}) },
    root,
    {
      // Tiny budget and a fast tick so eviction actively races the readers and writers.
      decompressCacheMaxSize: 32 * 1024,
      decompressCacheEvictionInterval: 25,
      decompressCacheTTL: 50
    }
  )
  await storage.start?.({} as any)

  const nextVersion = new Map<string, number>(IDS.map((id) => [id, 0]))
  let issued = 0

  const worker = async (): Promise<void> => {
    while (issued < OPERATIONS) {
      issued++
      const id = IDS[Math.floor(rnd() * IDS.length)]
      const roll = rnd()
      try {
        if (roll < 0.3) {
          const version = (nextVersion.get(id) ?? 0) + 1
          nextVersion.set(id, version)
          const body = bodyFor(version)
          if (rnd() < 0.6) await storage.storeStreamAndCompress(id, bufferToStream(body))
          else await storage.storeStream(id, bufferToStream(body))
        } else if (roll < 0.4) {
          await storage.delete([id])
        } else if (roll < 0.7) {
          const item = await storage.retrieve(id)
          if (!item) continue
          const bytes = await streamToBuffer(await item.asStream())
          const version = versionOf(bytes)
          if (version === undefined || !bytes.equals(bodyFor(version))) {
            result.corruption.push(`full read of ${id} returned ${bytes.length} bytes that are not one version`)
          }
        } else {
          // Range from 0 keeps the version header inside the slice, so the expected bytes are exactly
          // determined. This is the path that materializes the decompressed range cache.
          const end = 8 + Math.floor(rnd() * 1500)
          const item = await storage.retrieve(id, { start: 0, end })
          if (!item) continue
          const bytes = await streamToBuffer(await item.asStream())
          const version = versionOf(bytes)
          if (version === undefined || !bytes.equals(bodyFor(version).subarray(0, end + 1))) {
            result.corruption.push(
              `range read of ${id} [0,${end}] returned ${bytes.length} bytes that are not one version`
            )
          }
        }
      } catch (error: any) {
        const tolerated = options.injectFaults ? isTolerableUnderInjectedFaults : isTolerableFailure
        if (!tolerated(error)) {
          result.unexpectedFailures.push(`${error?.code ?? ''} ${error?.message}`)
        }
      }
    }
  }

  await Promise.all(Array.from({ length: WORKERS }, () => worker()))
  await storage.stop?.()

  const tempEntries = await nodeFs.readdir(path.join(root, '.tmp-writes'))
  result.stagedLeaks = tempEntries.filter((entry) => /^[0-9a-f]{16}-[0-9a-f]{32}$/.test(entry))
  result.intentLeaks = tempEntries.filter((entry) => entry.endsWith('.intent'))

  for (const id of IDS) {
    const raw = path.join(root, createHash('sha1').update(id).digest('hex').substring(0, 4), id)
    const gzip = raw + '.gzip'
    const hasRaw = await base.existPath(raw)
    const hasGzip = await base.existPath(gzip)
    if (hasRaw && hasGzip) {
      // The only legal coexistence is "the raw is the decompressed cache of exactly that gzip".
      const inflated = gunzipSync(await nodeFs.readFile(gzip))
      if (!inflated.equals(await nodeFs.readFile(raw))) result.mixedStates.push(id)
    }
    if (hasRaw || hasGzip) {
      const bytes = hasGzip ? gunzipSync(await nodeFs.readFile(gzip)) : await nodeFs.readFile(raw)
      const version = versionOf(bytes)
      if (version === undefined || !bytes.equals(bodyFor(version))) result.corruptAtRest.push(id)
    }
  }

  const listed: string[] = []
  for await (const each of storage.allFileIds()) listed.push(each)
  result.duplicateIds = listed.filter((value, index) => listed.indexOf(value) !== index)

  return result
}

describe('folder-based storage under concurrent stores, deletes and range reads', () => {
  describe('when the same ids are hammered by interleaved operations', () => {
    let result: StormResult

    beforeEach(async () => {
      result = await runStorm({ seed: 20260725 })
    }, 60000)

    it('should never serve bytes that are not exactly one complete stored version', () => {
      expect(result.corruption).toEqual([])
    })

    it('should never fail an operation in a way the contract does not allow', () => {
      expect(result.unexpectedFailures).toEqual([])
    })

    it('should leave no staged temp files behind', () => {
      expect(result.stagedLeaks).toEqual([])
    })

    it('should discharge every intent journal', () => {
      expect(result.intentLeaks).toEqual([])
    })

    it('should never leave a raw that is not the decompression of its own gzip', () => {
      expect(result.mixedStates).toEqual([])
    })

    it('should leave every surviving id holding one complete version at rest', () => {
      expect(result.corruptAtRest).toEqual([])
    })

    it('should enumerate each surviving id exactly once', () => {
      expect(result.duplicateIds).toEqual([])
    })
  })

  describe('and unlink intermittently fails underneath the storage', () => {
    let result: StormResult

    beforeEach(async () => {
      // Drives the counterpart-cleanup failure path: pending intents, quarantine, read-triggered
      // repair. Leftover intents and mixed states are the CORRECT outcome here, so they are not
      // asserted — what must still hold is that nothing corrupt is ever served.
      result = await runStorm({ seed: 764311, injectFaults: true })
    }, 60000)

    it('should actually exercise the failure path', () => {
      expect(result.injectedFaults).toBeGreaterThan(0)
    })

    it('should never serve bytes that are not exactly one complete stored version', () => {
      expect(result.corruption).toEqual([])
    })

    it('should never fail an operation in a way the contract does not allow', () => {
      expect(result.unexpectedFailures).toEqual([])
    })

    it('should leave every surviving id holding one complete version at rest', () => {
      expect(result.corruptAtRest).toEqual([])
    })

    // Staged temp files ARE expected to survive here: their cleanup is the very `unlink` being made
    // to fail, and reclaiming those leftovers is what the startup orphan sweep exists for. Asserting
    // they are absent would be asserting that injected faults have no effect.
  })
})
