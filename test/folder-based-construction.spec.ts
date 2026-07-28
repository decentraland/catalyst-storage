import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
import { gzipSync } from 'zlib'
import { intentNameFor } from './file-system-utils'
import os from 'os'
import path from 'path'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  IContentStorageComponent,
  IFileSystemComponent
} from '../src'
import { createLogComponent } from '@well-known-components/logger'

describe('folder-based storage construction', () => {
  describe('when the reserved temp directory cannot be read', () => {
    let root: string
    let failingFs: IFileSystemComponent
    let readdirError: Error

    beforeEach(async () => {
      // Construction cannot know whether a pending repair exists, and reads never consult intents —
      // so starting anyway would serve a stale representation for the whole process lifetime. That is
      // exactly what reconcile() refuses to allow once it CAN read the directory, so being unable to
      // read it has to refuse too rather than silently continue.
      root = mkdtempSync(path.join(os.tmpdir(), 'unreadable-temp-'))
      readdirError = Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' })
      const realFs = createFsComponent()
      failingFs = {
        ...realFs,
        readdir: (async (target: any, ...rest: any[]) => {
          if (String(target).endsWith('.tmp-writes')) throw readdirError
          return (realFs.readdir as any)(target, ...rest)
        }) as typeof realFs.readdir
      }
    })

    afterEach(() => {
      rmSync(root, { recursive: true, force: true })
    })

    it('should refuse to start rather than run over a possibly unreconciled state', async () => {
      await expect(
        createFolderBasedFileSystemContentStorage({ fs: failingFs, logs: await createLogComponent({}) }, root)
      ).rejects.toThrow(/Refusing to start/)
    })

    it('should name the underlying filesystem problem so it can be fixed', async () => {
      await expect(
        createFolderBasedFileSystemContentStorage({ fs: failingFs, logs: await createLogComponent({}) }, root)
      ).rejects.toThrow(/permission denied/)
    })
  })

  describe('when the reserved temp directory does not exist at reconciliation time', () => {
    let root: string
    let storage: IContentStorageComponent

    beforeEach(async () => {
      // Nothing was ever staged, so there are no intents to apply — the one case where an unreadable
      // directory is not evidence of anything.
      root = mkdtempSync(path.join(os.tmpdir(), 'absent-temp-'))
      const realFs = createFsComponent()
      const missingTempFs: IFileSystemComponent = {
        ...realFs,
        readdir: (async (target: any, ...rest: any[]) => {
          if (String(target).endsWith('.tmp-writes')) {
            throw Object.assign(new Error('ENOENT: no such file or directory'), { code: 'ENOENT' })
          }
          return (realFs.readdir as any)(target, ...rest)
        }) as typeof realFs.readdir
      }
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: missingTempFs, logs: await createLogComponent({}) },
        root
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should start normally', () => {
      expect(storage).toBeDefined()
    })
  })

  describe('when an intent journal names neither a staged file nor a committed representation', () => {
    let root: string

    beforeEach(async () => {
      // Nothing on disk proves whether the commit landed, so there is no safe way to reconcile:
      // guessing either way can delete a valid representation. Construction has to refuse.
      root = mkdtempSync(path.join(os.tmpdir(), 'unprovable-intent-'))
      await nodeFs.mkdir(path.join(root, '.tmp-writes'), { recursive: true })
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', intentNameFor('some-id')),
        JSON.stringify({ op: 'raw', id: 'some-id', staged: 'deadbeefdeadbeef-00000000000000000000000000000000' })
      )
    })

    afterEach(() => {
      rmSync(root, { recursive: true, force: true })
    })

    it('should refuse to start rather than guess which representation is current', async () => {
      await expect(
        createFolderBasedFileSystemContentStorage({ fs: createFsComponent(), logs: await createLogComponent({}) }, root)
      ).rejects.toThrow(/neither its staged file nor its committed/)
    })
  })

  describe('when an intent journal body is not parseable', () => {
    let root: string
    let storage: IContentStorageComponent
    let intentPath: string

    beforeEach(async () => {
      // Intents are written before the rename, so a torn or corrupt one means the commit never
      // started: it is discarded rather than applied to whatever the id looks like now.
      root = mkdtempSync(path.join(os.tmpdir(), 'corrupt-intent-'))
      await nodeFs.mkdir(path.join(root, '.tmp-writes'), { recursive: true })
      intentPath = path.join(root, '.tmp-writes', intentNameFor('some-id'))
      await nodeFs.writeFile(intentPath, '{"op":"raw","id":"some-i')
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        root
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should start rather than refuse over an intent that never described a commit', () => {
      expect(storage).toBeDefined()
    })

    it('should discard the malformed intent so it is not reinterpreted later', async () => {
      expect(await createFsComponent().existPath(intentPath)).toBe(false)
    })
  })

  describe('when a pending intent journal is present and readable', () => {
    let root: string
    let storage: IContentStorageComponent
    let staleRawPath: string

    beforeEach(async () => {
      // A committed gzip whose stale raw counterpart survived: reconciliation must remove the raw.
      // The intent journal is named sha256(id).intent; see intentNameFor.
      root = mkdtempSync(path.join(os.tmpdir(), 'pending-intent-'))
      const shard = path.join(root, '9584')
      await nodeFs.mkdir(shard, { recursive: true })
      staleRawPath = path.join(shard, 'some-id')
      await nodeFs.writeFile(staleRawPath, 'stale raw')
      // A REAL gzip: reconciliation refuses to treat a gzip too short to be valid as a landed commit
      // (a power loss can leave the directory entry without the data), so a placeholder string would
      // be discarded instead of being reconciled.
      await nodeFs.writeFile(path.join(shard, 'some-id.gzip'), gzipSync(Buffer.from('committed gzip')))
      await nodeFs.mkdir(path.join(root, '.tmp-writes'), { recursive: true })
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', intentNameFor('some-id')),
        JSON.stringify({ op: 'gzip', id: 'some-id', staged: 'deadbeefdeadbeef-00000000000000000000000000000000' })
      )
      storage = await createFolderBasedFileSystemContentStorage(
        { fs: createFsComponent(), logs: await createLogComponent({}) },
        root
      )
    })

    afterEach(async () => {
      await storage.stop?.()
      rmSync(root, { recursive: true, force: true })
    })

    it('should remove the stale counterpart named by the intent', async () => {
      expect(await createFsComponent().existPath(staleRawPath)).toBe(false)
    })
  })
})

describe('when the reserved temp path cannot be probed at construction', () => {
  let root: string
  let failure: unknown

  beforeEach(async () => {
    // ENOENT and ENOTDIR mean "nothing is there", which is the ordinary first boot. Anything else — a
    // permission or IO fault — leaves it unknown whether the staging area is safe to use, and construction
    // must not proceed on that: the reservation checks are what stop it hiding addressable content.
    root = mkdtempSync(path.join(os.tmpdir(), 'temp-unprobeable-'))
    const base = createFsComponent()
    const tempDir = path.join(root, '.tmp-writes')
    const fs: IFileSystemComponent = {
      ...base,
      stat: (async (target: any, ...rest: any[]) => {
        if (String(target) === tempDir) {
          throw Object.assign(new Error(`EACCES: permission denied, stat '${target}'`), { code: 'EACCES' })
        }
        return (base.stat as any)(target, ...rest)
      }) as IFileSystemComponent['stat']
    }
    failure = await createFolderBasedFileSystemContentStorage({ fs, logs: await createLogComponent({}) }, root).then(
      () => undefined,
      (error) => error
    )
  })

  afterEach(() => {
    rmSync(root, { recursive: true, force: true })
  })

  it('should refuse to start rather than guess', () => {
    expect((failure as { code?: string })?.code).toBe('EACCES')
  })
})
