# catalyst-storage

[![Coverage Status](https://coveralls.io/repos/github/decentraland/catalyst-storage/badge.svg?branch=main)](https://coveralls.io/github/decentraland/catalyst-storage?branch=main)

The Catalyst Storage Library provides multiple implementations to handle file storage for Catalyst servers. This allows users to store and retrieve content through different backends like S3, folder-based storage, or in-memory solutions. It abstracts the complexity of interacting with these systems, offering a unified API for managing file storage.

## Installation 

`npm install @dcl/catalyst-storage` 


## Supported storage types 

- S3 Storage: Store and retrieve content from AWS S3 buckets.
- Folder-based Storage: Local file storage on disk.
- In-memory Storage: Temporary storage for testing or lightweight operations.

## Folder-based storage: operational contract

The folder-based storage stages writes through a reserved directory to make them crash-atomic. This comes with three explicit rules:

- **One live instance per storage root.** In-memory coordination (path locks, decompress-cache tracking, staged-write ownership) is per-instance; two instances sharing a root can delete each other's staged files and race their caches. Shared roots are not supported.
- **Crash-atomic writes require `rename` on the filesystem component.** The bundled `createFsComponent` provides it. Custom adapters without `rename` keep working through the legacy non-atomic direct write — and get none of the staging machinery: the reserved directory, orphan sweep and crash reconciliation are neither created nor enforced, so legacy no-rename deployments (including flat-mode ids under the default reserved name) behave exactly as before. A warning is logged at construction; callers relying on atomicity should treat `rename` as a required capability.
- **Interrupted commits are reconciled at construction.** A stored id spans two possible representations (`<id>` and `<id>.gzip`); commits that transition between them journal an intent first, and a crash between the commit and its cleanup is resolved at the next construction in favor of the committed representation — reads can never prefer a stale counterpart. A repair that cannot be completed **fails construction** (reads do not consult intents, so running over an unreconciled state would serve the stale representation for the process lifetime).
- **One directory name under the root is reserved** (default `.tmp-writes`, configurable via `tempDirectoryName`). Ids resolving into it are rejected. With `disablePrefixHash` (flat mode) the root is the content namespace, so the factory **refuses to start** if the reserved directory pre-exists with content it cannot prove it owns — pre-existing ids there would otherwise become silently unreachable after an upgrade. To resolve: migrate those files out, configure a different `tempDirectoryName`, or restore the ownership marker if they are staging leftovers from a previous run.
