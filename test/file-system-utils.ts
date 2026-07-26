import { createHash } from 'crypto'
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

/**
 * The filename the intent journal uses for an id. Computed rather than hardcoded so the tests keep
 * describing the invariant ("this id's journal") instead of one particular hash function.
 */
export const intentNameFor = (id: string): string => `${createHash('sha256').update(id).digest('hex')}.intent`

export class FileSystemUtils {
  static createTempDirectory(): string {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'foo-'))
  }
}
