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
   * state resurface later with no repair signal. Here only ENOENT/ENOTDIR mean absent; any other
   * error fails the repair/commit path loudly.
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
        await fs.stat(target)
        return true
      } catch (err: any) {
        if (err?.code === 'ENOENT' || err?.code === 'ENOTDIR') return false
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
