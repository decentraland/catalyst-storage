import { IFileSystemComponent } from '../fs/types'

/**
 * The two filesystem primitives the recovery paths are built on. Bound to one filesystem component
 * so every caller shares the exact same semantics — the difference between them is load-bearing:
 * `existsForInvariant` must distinguish "absent" from "cannot tell", while `noFailUnlink` is
 * deliberately best-effort and callers verify the outcome themselves.
 */
export type FsInvariants = {
  /**
   * Existence check for recovery invariants. `existPath()` tests F_OK|R_OK, so a file left behind by
   * a failed unlink in an UNREADABLE state (mode/ACL damage, transient permission problem) would
   * read as absent — letting a must-succeed cleanup be falsely considered complete, and the mixed
   * state resurface later with no repair signal. Here only ENOENT/ENOTDIR/ENAMETOOLONG mean absent;
   * any other error fails the repair/commit path loudly.
   *
   * Answers for a REGULAR FILE specifically. Every path this is asked about — an id's two
   * representations, a staged write, an intent journal, the ownership marker — is one only a file
   * belongs at, and a directory occupying one is not the thing being asked about. Answering `true` for
   * it made `delete()` reject FOREVER on an id nothing was ever stored under (a nested id creates the
   * directory: `storeStream('a/b')` makes `a`, and `a.gzip/b` makes `a`'s compressed path), because the
   * unlink cannot remove a directory and the survivor was read as a failed removal — which poisoned
   * every GC batch containing that id, unrecoverably. Reads already answer "absent" for the same path.
   */
  existsForInvariant(target: string): Promise<boolean>
  /**
   * Best-effort unlink reporting whether it removed the file. Never throws: every caller that needs
   * certainty re-checks with `existsForInvariant` and decides what a survivor means.
   */
  noFailUnlink(target: string): Promise<boolean>
}

export function createFsInvariants(fs: IFileSystemComponent): FsInvariants {
  return {
    async existsForInvariant(target: string): Promise<boolean> {
      try {
        // `isFile()`, not merely "stat succeeded" — see the contract above.
        return (await fs.stat(target)).isFile()
      } catch (err: any) {
        // ENAMETOOLONG belongs with ENOENT/ENOTDIR: no file of that name CAN exist, so it is provably
        // absent rather than a storage fault. `statForRead` already classifies it that way for reads,
        // and the disagreement was observable — `exist`/`fileInfo`/`retrieve` reported a 300-character
        // id as absent while `delete()` rejected with a bare ENAMETOOLONG from this invariant, aborting
        // the whole batch and failing identically on every retry. A store of the same id failed the
        // same way from inside its commit.
        if (err?.code === 'ENOENT' || err?.code === 'ENOTDIR' || err?.code === 'ENAMETOOLONG') return false
        throw err
      }
    },
    async noFailUnlink(target: string): Promise<boolean> {
      try {
        await fs.unlink(target)
        return true
      } catch {
        return false
      }
    }
  }
}
