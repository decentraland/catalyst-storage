import * as fs from 'fs'
import * as fsPromises from 'fs/promises'

/**
 * @public
 *
 * This may be moved to well-known-components in the future
 */
export type IFileSystemComponent = Pick<typeof fs, 'createReadStream'> &
  Pick<typeof fs, 'createWriteStream'> &
  Pick<typeof fsPromises, 'opendir' | 'stat' | 'unlink' | 'rename' | 'mkdir' | 'readdir' | 'readFile'> & {
    existPath(path: string): Promise<boolean>
    /**
     * Optional. When present, the folder-based storage rejects a symlinked reserved temp path at
     * construction (`stat` follows symlinks, so staged writes and the sweep would otherwise operate
     * outside the root), and measures a link rather than its target when sizing a compression. The
     * bundled `createFsComponent` provides it; without it those two checks degrade to the documented
     * weaker guarantee, which is why it stays optional where `rename` does not.
     */
    lstat?: typeof fsPromises.lstat
  }
