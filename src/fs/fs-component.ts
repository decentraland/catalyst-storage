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
 *
 * The return type RESTATES `existPath` as present, because `IFileSystemComponent` now only declares it
 * optionally (it is deprecated and this package no longer calls it — see the note there). The
 * distinction is the point: a custom adapter is no longer required to implement a method nothing uses,
 * while a caller holding the BUNDLED component can still call it, since this one demonstrably has it.
 * Without this, deprecating the member would have broken every such caller instead of only the adapters
 * that never needed it.
 */
export function createFsComponent(): IFileSystemComponent & { existPath(path: string): Promise<boolean> } {
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
