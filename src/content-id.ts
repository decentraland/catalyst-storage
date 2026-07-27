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
 * Trailing characters some filesystems STRIP from a filename rather than preserve.
 *
 * Win32 path semantics (so NTFS, and any SMB/CIFS mount) discard trailing dots and spaces, which makes
 * `'foo.gzip '` and `'foo.gzip.'` resolve onto `foo.gzip` — the compressed representation of `foo`.
 * Folded away before the reserved-suffix comparison for exactly the reason the comparison is
 * case-insensitive: the rule has to cover every spelling the filesystem treats as the same name.
 */
const FILESYSTEM_STRIPPED_TAIL = /[.\s]+$/

/**
 * The path of the compressed representation belonging to a canonical raw path.
 *
 * Exists so the suffix is written ONCE. It was spelled as a `+ '.gzip'` literal in eleven places, none
 * of which referenced `GZIP_EXTENSION` — so the constant an id is VALIDATED against and the string the
 * paths are BUILT from could drift apart, silently splitting the two halves of the same rule.
 *
 * @public
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
 * @public
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
  if (id.toLowerCase().replace(FILESYSTEM_STRIPPED_TAIL, '').endsWith(GZIP_EXTENSION)) {
    throw new PathNotContainedError(
      `The id ends in ${GZIP_EXTENSION} (in any case, and ignoring trailing dots or whitespace, which ` +
        `some filesystems strip), which names the compressed representation of another id: ${JSON.stringify(id)}`
    )
  }

  if (id.includes('\0')) {
    throw new PathNotContainedError(`The id contains a NUL byte, which no storage key can hold`)
  }
}

/**
 * The additional rule a STORE must enforce: every path segment has to fit in a directory entry.
 *
 * Deliberately NOT part of `assertValidContentId`, because the read path answers this input correctly
 * already and differently on purpose: a name no file can have is a name nothing is stored under, so
 * `exist`/`fileInfo`/`retrieve` report it absent and `delete` resolves, rather than failing a whole
 * `existMultiple` or GC batch over an id that is provably a miss. Those answers are pinned.
 *
 * What was NOT correct is the write side. Without this rule the in-memory backend ACCEPTED a 300-byte
 * id and reported `exist` true for it, while the folder-based backend could not store it at all and
 * said so with a bare `ENAMETOOLONG` Error rather than a `PathNotContainedError` — so a service mapping
 * the typed error to 400 answered 500, and the two backends disagreed about which ids exist. This keeps
 * the namespace rule the other checks exist for: an id one backend accepts is one every backend
 * accepts.
 *
 * @public
 */
export function assertStorableContentId(id: string): void {
  // Per SEGMENT, because that is what NAME_MAX applies to: nested ids are legal and each segment
  // becomes its own directory entry. The FINAL segment is allowed `GZIP_EXTENSION` less, since an id's
  // compressed representation is its path plus that suffix — a final segment at exactly NAME_MAX would
  // store raw and then be unstorable compressed.
  const segments = id.split(/[/\\]/)
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
 * @public
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
