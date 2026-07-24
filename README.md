# catalyst-storage

[![Coverage Status](https://coveralls.io/repos/github/decentraland/catalyst-storage/badge.svg?branch=main)](https://coveralls.io/github/decentraland/catalyst-storage?branch=main)

The Catalyst Storage Library provides multiple implementations to handle file storage for Catalyst servers. This allows users to store and retrieve content through different backends like S3, folder-based storage, or in-memory solutions. It abstracts the complexity of interacting with these systems, offering a unified API for managing file storage.

## Installation

`npm install @dcl/catalyst-storage`

## Supported storage types

- S3 Storage: Store and retrieve content from AWS S3 buckets.
- Folder-based Storage: Local file storage on disk.
- In-memory Storage: Temporary storage for testing or lightweight operations.

## Cancellation scope

`storeStream` and `storeStreamAndCompress` accept an `AbortSignal`. Cancelling stops the work and rejects with the caller's reason, and a store that already completed before observing the abort is allowed to succeed. What "nothing was stored" guarantees differs by backend, because the two commit through different machinery:

- **Folder-based — absolute.** The commit is a local `rename` this storage fully controls, with a checkpoint at every phase boundary, so a cancelled store never leaves content at a canonical path. The previous version of the id stays intact.
- **S3 — one bounded window.** The abort tears down the in-flight request itself (`PutObject`, `UploadPart`, `CompleteMultipartUpload`), so the key does not appear; a partially uploaded multipart upload is also cleaned up, and that cleanup deliberately ignores the signal that triggered it. What cannot be covered is a request S3 has **already received in full** when the abort fires — tearing down the connection does not un-send those bytes, and the service may still apply them. The residue is bounded: S3 object writes are atomic, so the key is either absent or holds the complete content (never partial or mixed), and because content is addressed by its own hash the worst outcome is the correct bytes existing under their own id after a store reported as cancelled.

## Folder-based storage: operational contract

The folder-based storage stages writes through a reserved directory to make them crash-atomic. This comes with three explicit rules:

- **One live instance per storage root, with exclusive ownership of the tree — this is a hard requirement.** The root, its shard directories and everything beneath them must be created and managed only by this storage under the service user; no other writer and **no pre-existing symlinks anywhere under the root** (reads, writes and deletes resolve paths through the OS and would follow a planted symlink outside the root — only the reserved temp path is actively checked, via `lstat`, because staging is where files are created most often). In-memory coordination (path locks, decompress-cache tracking, staged-write ownership) is also per-instance; two instances sharing a root can delete each other's staged files and race their caches.
- **Crash-atomic writes require `rename` on the filesystem component.** The bundled `createFsComponent` provides it. Custom adapters without `rename` keep working through the legacy non-atomic direct write — and get none of the staging machinery: the reserved directory, orphan sweep and crash reconciliation are neither created nor enforced, so legacy no-rename deployments (including flat-mode ids under the default reserved name) behave exactly as before. A warning is logged at construction; callers relying on atomicity should treat `rename` as a required capability.
- **Atomicity covers process crashes, not power loss.** Staged data is deliberately **not `fsync`'d** before the commit rename: an OOM-kill, eviction or crash can never leave a partial or mixed state visible, but a power loss / kernel panic between the write and the disk flush may lose the file entirely. This is a deliberate contract choice — content is content-addressed and re-downloadable, so durability past process death buys nothing worth an fsync per write.
- **Interrupted commits are reconciled at construction.** A stored id spans two possible representations (`<id>` and `<id>.gzip`); commits that transition between them journal an intent first, and a crash between the commit and its cleanup is resolved at the next construction in favor of the committed representation — reads can never prefer a stale counterpart. A repair that cannot be completed **fails construction** (reads do not consult intents, so running over an unreconciled state would serve the stale representation for the process lifetime).
- **A symlinked reserved path is rejected** at construction when the filesystem component provides `lstat` (the bundled one does); without `lstat`, the exclusive-root model is the guarantee that no symlinks exist under the root.
- **Compression uses native `fs` on local paths.** `storeStreamAndCompress` pipes through `compressContentFile`, which reads and writes via node `fs` directly — custom filesystem adapters that virtualize paths get atomic raw writes, but compressed stores require paths that are real local files.
- **One directory name under the root is reserved** (default `.tmp-writes`, configurable via `tempDirectoryName`). Ids resolving into it are rejected. With `disablePrefixHash` (flat mode) the root is the content namespace, so the factory **refuses to start** if the reserved directory pre-exists with content it cannot prove it owns — pre-existing ids there would otherwise become silently unreachable after an upgrade. To resolve: migrate those files out, configure a different `tempDirectoryName`, or restore the ownership marker if they are staging leftovers from a previous run.
