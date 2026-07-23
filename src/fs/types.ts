import * as fs from 'fs'
import * as fsPromises from 'fs/promises'

/**
 * @public
 *
 * This may be moved to well-known-components in the future
 */
export type IFileSystemComponent = Pick<typeof fs, 'createReadStream'> &
  Pick<typeof fs, 'createWriteStream'> &
  Pick<typeof fsPromises, 'access' | 'opendir' | 'stat' | 'unlink' | 'mkdir' | 'readdir' | 'readFile'> & {
    constants: Pick<typeof fs.constants, 'F_OK' | 'R_OK'>
  } & {
    existPath(path: string): Promise<boolean>
    // Optional so adding it is not a breaking change for existing IFileSystemComponent implementers.
    // When present, storeStream writes atomically (temp file + rename); when absent it falls back to a
    // direct write. The bundled createFsComponent always provides it.
    rename?: typeof fsPromises.rename
  }
