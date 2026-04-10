import { mkdtempSync, rmSync } from 'fs'
import os from 'os'
import path from 'path'
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
import { FileSystemUtils as fsu } from './file-system-utils'
import AWSMock from 'mock-aws-s3'

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

  it.skip('Finds all elements using allFileIds', async () => {
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
}

describe('s3 behavior', () => {
  const components: { storage?: IContentStorageComponent } = {}

  beforeAll(async () => {
    const root = fsu.createTempDirectory()
    AWSMock.config.basePath = path.join(root, 'buckets') // Can configure a basePath for your local buckets
    const s3 = new AWSMock.S3({
      params: { Bucket: 'example' }
    })
    const logs = await createLogComponent({})
    components.storage = await createS3BasedFileSystemContentStorage({ logs }, s3, { Bucket: 'example' })
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
