import path from 'path'
import { PathNotContainedError } from './folder-based/errors'

/** This library's suffix for an id's compressed representation. Reserved: ids may not end in it. */
export const GZIP_EXTENSION = '.gzip'

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

  if (id.endsWith(GZIP_EXTENSION)) {
    throw new PathNotContainedError(
      `The id ends in ${GZIP_EXTENSION}, which names the compressed representation of another id: ${JSON.stringify(id)}`
    )
  }

  if (id.includes('\0')) {
    throw new PathNotContainedError(`The id contains a NUL byte, which no storage key can hold`)
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
