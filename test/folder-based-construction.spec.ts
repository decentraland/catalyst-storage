import { mkdtempSync, promises as nodeFs, rmSync } from 'fs'
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
        path.join(root, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
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
      intentPath = path.join(root, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent')
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
      // sha1('some-id') = 9584b661c135a43f2fbbe43cc5104f7bd693d048
      root = mkdtempSync(path.join(os.tmpdir(), 'pending-intent-'))
      const shard = path.join(root, '9584')
      await nodeFs.mkdir(shard, { recursive: true })
      staleRawPath = path.join(shard, 'some-id')
      await nodeFs.writeFile(staleRawPath, 'stale raw')
      await nodeFs.writeFile(path.join(shard, 'some-id.gzip'), 'committed gzip')
      await nodeFs.mkdir(path.join(root, '.tmp-writes'), { recursive: true })
      await nodeFs.writeFile(
        path.join(root, '.tmp-writes', '9584b661c135a43f2fbbe43cc5104f7bd693d048.intent'),
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
