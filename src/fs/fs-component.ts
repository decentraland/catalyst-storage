import * as fs from 'fs'
import * as fsPromises from 'fs/promises'
import { IFileSystemComponent } from './types'

async function existPath(path: string): Promise<boolean> {
  try {
    await fs.promises.access(path, fs.constants.F_OK | fs.constants.R_OK)
    return true
  } catch {
    return false
  }
}

/**
 * @public
 */
export function createFsComponent(): IFileSystemComponent {
  return {
    createReadStream: fs.createReadStream,
    createWriteStream: fs.createWriteStream,
    opendir: fsPromises.opendir,
    stat: fsPromises.stat,
    unlink: fsPromises.unlink,
    rename: fsPromises.rename,
    lstat: fsPromises.lstat,
    mkdir: fsPromises.mkdir,
    readdir: fsPromises.readdir,
    readFile: fsPromises.readFile,
    existPath
  }
}
