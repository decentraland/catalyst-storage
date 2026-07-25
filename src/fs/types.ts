import * as fs from 'fs'
import * as fsPromises from 'fs/promises'

/**
 * @public
 *
 * This may be moved to well-known-components in the future
 */
export type IFileSystemComponent = Pick<typeof fs, 'createReadStream'> &
  Pick<typeof fs, 'createWriteStream'> &
  Pick<typeof fsPromises, 'opendir' | 'stat' | 'unlink' | 'mkdir' | 'readdir' | 'readFile'> & {
    existPath(path: string): Promise<boolean>
    // Optional so adding it is not a breaking change for existing IFileSystemComponent implementers.
    // When present, storeStream writes atomically (temp file + rename); when absent it falls back to a
    // direct write. The bundled createFsComponent always provides it.
    rename?: typeof fsPromises.rename
    // Optional for the same compatibility reason. When present, the folder-based storage rejects a
    // symlinked reserved temp path at construction (stat follows symlinks, so staged writes and the
    // sweep would otherwise operate outside the root). The bundled createFsComponent provides it.
    lstat?: typeof fsPromises.lstat
  }
