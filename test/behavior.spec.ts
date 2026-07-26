import { mkdtempSync, rmSync } from 'fs'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import {
  createFolderBasedFileSystemContentStorage,
  createFsComponent,
  createInMemoryStorage,
  createS3BasedFileSystemContentStorage,
  FolderStorageOptions,
  IContentStorageComponent
} from '../src'
import { bufferToStream } from '../src'
import { createLogComponent } from '@well-known-components/logger'
import { createFakeS3Client } from './fake-s3-client'

const options: (undefined | FolderStorageOptions)[] = [
  undefined,
  { disablePrefixHash: true },
  { disablePrefixHash: false }
]

function createCommonSuite(components: { storage?: IContentStorageComponent }) {
  it(`Stores the files`, async () => {
    await components.storage!.storeStream('a', bufferToStream(Buffer.from('123456')))
    await components.storage!.storeStream('f/a', bufferToStream(Buffer.from('223456')))
    await components.storage!.storeStream('f/b/c/3', bufferToStream(Buffer.from('323456')))
    await components.storage!.storeStream('f/b/c/4', bufferToStream(Buffer.from('423456')))
  })

  it(`Checks that files exist`, async () => {
    expect(await components.storage!.exist('a')).toBeTruthy()
    expect(await components.storage!.exist('f/a')).toBeTruthy()
    expect(await components.storage!.exist('f/b/c/3')).toBeTruthy()
    expect(await components.storage!.exist('f/b/c/4')).toBeTruthy()
  })

  it('Finds all elements using allFileIds', async () => {
    const files: any = {}

    for await (const id of components.storage!.allFileIds('f/')) {
      files[id] = true
    }

    expect(files).toEqual({
      'f/a': true,
      'f/b/c/3': true,
      'f/b/c/4': true
    })
  })

  it(`When a signal is provided but never aborts, then the store completes normally`, async () => {
    const controller = new AbortController()

    await components.storage!.storeStream('signal/completed', bufferToStream(Buffer.from('123456')), controller.signal)

    expect(await components.storage!.exist('signal/completed')).toBe(true)
  })

  it(`When the signal is already aborted, then the store rejects with the reason and stores nothing`, async () => {
    const reason = new Error('cancelled before start')
    const controller = new AbortController()
    controller.abort(reason)

    await expect(
      components.storage!.storeStream('signal/pre-aborted', bufferToStream(Buffer.from('123456')), controller.signal)
    ).rejects.toBe(reason)
    expect(await components.storage!.exist('signal/pre-aborted')).toBe(false)
  })

  it(`When the signal aborts while the source is still streaming, then the store rejects with the reason and stores nothing`, async () => {
    const reason = new Error('cancelled mid-stream')
    const controller = new AbortController()
    const source = new Readable({ read() {} })
    source.push(Buffer.from('first-chunk'))

    const pending = components.storage!.storeStream('signal/mid-aborted', source, controller.signal)
    await new Promise<void>((resolve) => setImmediate(resolve))
    controller.abort(reason)

    await expect(pending).rejects.toBe(reason)
    expect(await components.storage!.exist('signal/mid-aborted')).toBe(false)
  })

  it(`When the signal aborts after the source was fully consumed, then the store rejects with the reason and stores nothing`, async () => {
    // Every backend must honor an abort observed once the source is consumed: destroying the source
    // no longer cancels anything, so without a checkpoint before the commit a cancelled request
    // would still store content. The 'end' listener is registered before the store consumes the
    // stream, so it aborts in the same event as the read completing.
    const reason = new Error('cancelled after the source ended')
    const controller = new AbortController()
    const source = new Readable({ read() {} })
    source.push(Buffer.from('fully-consumed-content'))
    source.push(null)
    source.on('end', () => controller.abort(reason))

    await expect(components.storage!.storeStream('signal/post-consumption', source, controller.signal)).rejects.toBe(
      reason
    )
    expect(await components.storage!.exist('signal/post-consumption')).toBe(false)
  })

  it(`When the signal aborts while a compressed store is still streaming, then it rejects with the reason and stores nothing`, async () => {
    const reason = new Error('cancelled mid-compress')
    const controller = new AbortController()
    const source = new Readable({ read() {} })
    source.push(Buffer.from('first-chunk'))

    const pending = components.storage!.storeStreamAndCompress('signal/mid-aborted-gzip', source, controller.signal)
    await new Promise<void>((resolve) => setImmediate(resolve))
    controller.abort(reason)

    await expect(pending).rejects.toBe(reason)
    expect(await components.storage!.exist('signal/mid-aborted-gzip')).toBe(false)
  })
}

describe('s3 behavior', () => {
  const components: { storage?: IContentStorageComponent } = {}

  beforeAll(async () => {
    const logs = await createLogComponent({})
    components.storage = await createS3BasedFileSystemContentStorage({ logs }, createFakeS3Client(), {
      Bucket: 'example',
      // This suite asserts storage BEHAVIOUR, not content-type detection, and the default loader
      // reaches ESM-only `file-type` through a dynamic import that Jest's module registry does not
      // own. Once any test file's environment is torn down, that import fails from every file that
      // follows — reported as `import after the Jest environment has been torn down`, which fails the
      // run even when every assertion passes. Detection itself is covered by `mime-detection.spec.ts`
      // and by the MIME suite in the S3 spec.
      fileTypeLoader: async () => ({ fileTypeFromBuffer: async () => undefined })
    })
  })

  createCommonSuite(components)
})

describe('in-memory behavior', () => {
  const components: { storage: IContentStorageComponent } = { storage: createInMemoryStorage() }

  createCommonSuite(components)
})

options.forEach((options, index) =>
  describe(`fileSystemContentStorage behavior #${index}`, () => {
    const components: { storage?: IContentStorageComponent } = {}

    const fs = createFsComponent()
    let tmpRootDir: string

    beforeAll(async () => {
      tmpRootDir = mkdtempSync(path.join(os.tmpdir(), 'content-storage-'))
      components.storage = await createFolderBasedFileSystemContentStorage(
        { fs, logs: await createLogComponent({}) },
        tmpRootDir,
        options
      )
    })

    afterAll(() => {
      rmSync(tmpRootDir, { recursive: true, force: false })
    })

    createCommonSuite(components)
  })
)
