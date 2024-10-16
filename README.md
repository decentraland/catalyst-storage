# catalyst-storage

[![Coverage Status](https://coveralls.io/repos/github/decentraland/catalyst-storage/badge.svg?branch=main)](https://coveralls.io/github/decentraland/catalyst-storage?branch=main)

The Catalyst Storage Library provides multiple implementations to handle file storage for Catalyst servers. This allows users to store and retrieve content through different backends like S3, folder-based storage, or in-memory solutions. It abstracts the complexity of interacting with these systems, offering a unified API for managing file storage.

## Installation 

`npm install @dcl/catalyst-storage` 


## Supported storage types 

- S3 Storage: Store and retrieve content from AWS S3 buckets.
- Folder-based Storage: Local file storage on disk.
- In-memory Storage: Temporary storage for testing or lightweight operations.