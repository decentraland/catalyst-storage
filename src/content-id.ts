import path from 'path'
import { PathNotContainedError } from './folder-based/errors'

/** This library's suffix for an id's compressed representation. Reserved: ids may not end in it. */
export const GZIP_EXTENSION = '.gzip'

/**
 * The longest a single path segment of an id may be, in BYTES.
 *
 * 255 is `NAME_MAX` on every filesystem this library runs on (ext4, XFS, APFS, NTFS) and it counts
 * BYTES, not characters — so the limit has to be measured in utf8, or a 200-character CJK id (600
 * bytes) passes on a developer's APFS volume and is unstorable on the ext4 volume the service actually
 * runs on.
 */
const MAX_SEGMENT_BYTES = 255

/**
 * The longest an id may be IN TOTAL, in bytes, across all of its segments and separators.
 *
 * `NAME_MAX` bounds one segment; `PATH_MAX` bounds the assembled path, and nothing enforced it. An id of
 * thirty 240-byte segments passes the per-segment rule and resolves to a 7 KB path, so the folder-based
 * backend failed it at `mkdir` with a bare `ENAMETOOLONG` — the untyped-error-and-backend-disagreement
 * outcome the per-segment rule was added to remove, reached by the dimension it does not measure. The
 * in-memory backend, having no path, stored it happily and reported `exist` true.
 *
 * 1024 bytes, not `PATH_MAX` itself: the budget an id may spend has to leave room for the root directory
 * and the shard segment, whose lengths are the deployment's choice rather than the id's, and `PATH_MAX` is
 * 4096 on Linux but 1024 on macOS/BSD. Far above any real content id (a CID is ~59 bytes) while low enough
 * that root + shard + id clears the smaller ceiling for any sane root. The residue — a legitimate id under
 * this bound that a very long root pushes past `PATH_MAX` anyway — is what the folder-based backend's own
 * commit translates, since only it knows its root.
 */
const MAX_ID_BYTES = 1024

/**
 * Trailing characters some filesystems STRIP from a filename rather than preserve.
 *
 * Win32 path semantics (so NTFS, and any SMB/CIFS mount) discard trailing dots and spaces, which makes
 * `'foo.gzip '` and `'foo.gzip.'` resolve onto `foo.gzip` — the compressed representation of `foo`.
 * Folded away before the reserved-suffix comparison for exactly the reason the comparison is
 * case-insensitive: the rule has to cover every spelling the filesystem treats as the same name.
 *
 * DOTS AND SPACES ONLY. `\s` also matches tab, newline, `\v`, `\f`, NBSP and BOM, none of which any
 * filesystem folds — including them rejected perfectly addressable ids (`'foo.gzip\n'`, `'foo.gzip '`)
 * for a collision that cannot happen.
 */
const FILESYSTEM_STRIPPED_TAIL = /[. ]+$/

/**
 * A filename reduced to the form a case-folding filesystem actually compares.
 *
 * Two names with the same folded form are ONE directory entry on APFS, NTFS or an SMB/CIFS mount. Shared
 * so every rule that has to respect that folding uses the same definition: the reserved-suffix check
 * below, and the folder-based backend's id-vs-id alias check, which without it let `Foo` and `FOO` resolve
 * to the same file and silently overwrite each other's content.
 *
 * @internal
 */
export function foldFilesystemName(name: string): string {
  return name.toLowerCase().replace(FILESYSTEM_STRIPPED_TAIL, '')
}

/**
 * The separators the RUNNING platform treats as path separators.
 *
 * Not `/[/\\]/` unconditionally. On POSIX a backslash is an ORDINARY FILENAME CHARACTER — the folder-based
 * backend stores `a\..\victim` under that literal name, and its read contract pins that — so splitting on
 * it there under-counts a segment's length: `'x'.repeat(200) + '\\' + 'y'.repeat(200)` looked like two
 * 200-byte segments and was accepted, then failed the commit rename with a bare `ENAMETOOLONG`, which is
 * verbatim the outcome `assertStorableContentId` exists to remove. Derived from `path.sep` so the rule
 * matches whatever `resolveFilePath` will actually do with the id.
 */
const PATH_SEPARATORS = path.sep === '\\' ? /[/\\]/ : /\//

/**
 * The path of the compressed representation belonging to a canonical raw path.
 *
 * Exists so the suffix is written ONCE. It was spelled as a `+ '.gzip'` literal in eleven places, none
 * of which referenced `GZIP_EXTENSION` — so the constant an id is VALIDATED against and the string the
 * paths are BUILT from could drift apart, silently splitting the two halves of the same rule.
 *
 * @internal
 */
export function gzipPathOf(filePath: string): string {
  return filePath + GZIP_EXTENSION
}

/**
 * The id-shape rules every backend enforces, independent of how (or whether) an id becomes a path.
 *
 * These three are not filesystem concerns — they are properties of the id namespace itself, and a
 * backend that accepts them is addressing content the other backends cannot:
 * - an empty id names no object at all;
 * - `<id>.gzip` is the name of ANOTHER id's compressed representation, so it occupies a second id's
 *   path and makes enumeration ambiguous;
 * - a NUL byte cannot survive a round trip through any real storage.
 *
 * @internal
 */
export function assertValidContentId(id: string): void {
  if (id.length === 0) {
    throw new PathNotContainedError('The id is empty, so it does not name a stored object')
  }

  // Case-INSENSITIVE, because the collision this prevents is a filesystem one and half the
  // filesystems this library runs on fold case. On APFS, NTFS or an SMB/CIFS mount, `<id>.GZIP` IS
  // `<id>.gzip`: storing it overwrote another id's compressed representation, so that id's reads
  // failed to inflate, its `contentSize` came out of the wrong file's last four bytes, and
  // `allFileIds()` reported it once while never listing the id that had clobbered it — the exact
  // damage this rule exists to prevent, reached through the spelling it did not check. Rejecting
  // every case keeps one id namespace across every backend and filesystem, rather than one that
  // silently widens on ext4.
  if (id.toLowerCase().endsWith(GZIP_EXTENSION)) {
    throw new PathNotContainedError(
      `The id ends in ${GZIP_EXTENSION} (in any case), which names the compressed representation of ` +
        `another id: ${JSON.stringify(id)}`
    )
  }

  if (id.includes('\0')) {
    throw new PathNotContainedError(`The id contains a NUL byte, which no storage key can hold`)
  }
}

/**
 * The additional rules a STORE must enforce, beyond the ones every surface shares.
 *
 * Deliberately NOT part of `assertValidContentId`, which the READ path also runs. Both rules here concern
 * names that are unaddressable or aliasing, and the read path answers those correctly and differently on
 * purpose — an id it cannot serve is reported absent, and `delete` resolves, rather than failing a whole
 * `existMultiple` or GC batch. Enforcing them on reads is actively harmful for the aliasing rule in
 * particular: content a previous version legitimately stored (`'legacy.gzip.'`) would become unreadable
 * AND undeletable while `allFileIds()` still yielded it, so a GC sweep would enumerate the id and then
 * fail its own delete batch, forever.
 *
 * - SEGMENT LENGTH. Without it the in-memory backend ACCEPTED a 300-byte id and reported `exist` true,
 *   while the folder-based backend could not store it at all and said so with a bare `ENAMETOOLONG`
 *   rather than a `PathNotContainedError` — so a service mapping the typed error to 400 answered 500,
 *   and the two backends disagreed about which ids exist.
 * - FILESYSTEM-FOLDED RESERVED SUFFIX. `assertValidContentId` rejects `<id>.gzip` in any case; on a
 *   filesystem that strips trailing dots and spaces, `'foo.gzip '` and `'foo.gzip.'` name that same file.
 *   Refusing to CREATE those closes the collision without stranding anything already on disk.
 *
 * Internal, like the other validators (see the README's "Public surface"): a caller cannot reach it, and
 * the rules it enforces are ones the backends apply on the caller's behalf. Deliberately NOT applied by the
 * S3 backend, whose keys are documented as opaque — see `uploadTo`.
 */
export function assertStorableContentId(id: string): void {
  // Checked against the folded spelling, but only for a name being created — see above.
  if (foldFilesystemName(id).endsWith(GZIP_EXTENSION)) {
    throw new PathNotContainedError(
      `The id ends in ${GZIP_EXTENSION} once trailing dots and spaces are removed, which some filesystems ` +
        `strip — so it names the compressed representation of another id: ${JSON.stringify(id)}`
    )
  }

  // The WHOLE id, because `PATH_MAX` applies to the assembled path and the per-segment rule below
  // cannot see it: many short segments are individually fine and collectively unstorable. Checked
  // first so the message names the real problem rather than an innocent segment.
  const totalBytes = Buffer.byteLength(id, 'utf8')
  if (totalBytes > MAX_ID_BYTES) {
    throw new PathNotContainedError(
      `The id is ${totalBytes} bytes, past the ${MAX_ID_BYTES} this storage can address (the assembled ` +
        `path must stay within PATH_MAX, which is 1024 bytes on some platforms): ` +
        `${JSON.stringify(id.slice(0, 32) + '…')}`
    )
  }

  // Per SEGMENT, because that is what NAME_MAX applies to: nested ids are legal and each segment
  // becomes its own directory entry. The FINAL segment is allowed `GZIP_EXTENSION` less, since an id's
  // compressed representation is its path plus that suffix — a final segment at exactly NAME_MAX would
  // store raw and then be unstorable compressed.
  const segments = id.split(PATH_SEPARATORS)
  for (const [index, segment] of segments.entries()) {
    const isFinal = index === segments.length - 1
    const budget = isFinal ? MAX_SEGMENT_BYTES - GZIP_EXTENSION.length : MAX_SEGMENT_BYTES
    const bytes = Buffer.byteLength(segment, 'utf8')
    if (bytes > budget) {
      throw new PathNotContainedError(
        `The id has a path segment of ${bytes} bytes, past the ${budget} this storage can address ` +
          `(NAME_MAX is ${MAX_SEGMENT_BYTES} bytes${isFinal ? `, less ${GZIP_EXTENSION} for the compressed representation` : ''}): ` +
          `${JSON.stringify(segment.length > 32 ? segment.slice(0, 32) + '…' : segment)}`
      )
    }
  }
}

/**
 * A synthetic containment root. Backends with no real root still have to answer the same question —
 * "does this id name a path of its own, inside the namespace?" — and the answer does not depend on
 * which root it is asked against.
 */
const SYNTHETIC_ROOT = path.resolve(path.sep, 'catalyst-storage-id-namespace')

/**
 * Full id validation for a backend that has no filesystem root of its own.
 *
 * Adds the two path-shaped invariants the folder-based backend enforces against its real root, so a
 * service whose id handling is exercised against the in-memory backend in tests behaves the same way
 * in production against a folder-based or S3 one. Previously the in-memory backend accepted `''`,
 * `'foo.gzip'`, `'../evil'` and `'./x'`, all of which the folder-based backend rejects.
 *
 * - ALIASING: the id must resolve to exactly its own path. `path.join` normalizes, so `./victim`,
 *   `a/../victim` and `a//victim` otherwise collapse onto another id's key.
 * - CONTAINMENT: orthogonal — `../evil` resolves to exactly its own path and so round-trips
 *   cleanly, it is simply outside the namespace.
 *
 * @internal
 */
export function assertAddressableContentId(id: string): void {
  assertValidContentId(id)

  const resolved = path.normalize(path.join(SYNTHETIC_ROOT, id))
  if (path.relative(SYNTHETIC_ROOT, resolved) !== id) {
    throw new PathNotContainedError(
      `The id does not name a path of its own: ${JSON.stringify(id)} resolves onto ` +
        `${JSON.stringify(path.relative(SYNTHETIC_ROOT, resolved))}`
    )
  }
  if (!resolved.startsWith(SYNTHETIC_ROOT + path.sep)) {
    throw new PathNotContainedError(`The id names a location outside the storage namespace: ${JSON.stringify(id)}`)
  }
}
