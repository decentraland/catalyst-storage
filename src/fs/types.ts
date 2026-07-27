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
    /**
     * @deprecated Unused, and slated for removal in the next breaking release.
     *
     * Nothing in this package calls it any more. It tests `F_OK|R_OK`, which cannot answer any question
     * this storage actually asks: it passes for a REGULAR FILE where a directory is needed, and reports a
     * present-but-unreadable file as absent. Every caller was migrated to `stat`-based probes that
     * distinguish those — `statForRead` on the read path, `existsForInvariant` on the recovery paths, and
     * `statOccupant` where a directory has to be told from a file — each of which documents the specific
     * misclassification `existPath` produced there.
     *
     * OPTIONAL rather than deleted outright, so an existing adapter keeps compiling: dropping a member
     * from this type would fail excess-property checking for any consumer that builds the component as an
     * object literal. `createFsComponent` still supplies it. Do not add new callers.
     */
    existPath?(path: string): Promise<boolean>
    /**
     * Optional. When present, the folder-based storage rejects a symlinked reserved temp path at
     * construction (`stat` follows symlinks, so staged writes and the sweep would otherwise operate
     * outside the root), and measures a link rather than its target when sizing a compression. The
     * bundled `createFsComponent` provides it; without it those two checks degrade to the documented
     * weaker guarantee, which is why it stays optional where `rename` does not.
     */
    lstat?: typeof fsPromises.lstat
  }
