## API Report File for "@dcl/catalyst-storage"

> Do not edit this file. It is a report generated by [API Extractor](https://api-extractor.com/).

```ts

/// <reference types="node" />

import * as fs from 'fs';
import * as fsPromises from 'fs/promises';
import { IConfigComponent } from '@well-known-components/interfaces';
import { Readable } from 'stream';
import { S3 } from 'aws-sdk';

// @public (undocumented)
export type AppComponents = {
    fs: IFileSystemComponent;
    config: IConfigComponent;
};

// @public (undocumented)
export interface ContentItem {
    asRawStream(): Promise<Readable>;
    asStream(): Promise<Readable>;
    // (undocumented)
    encoding: string | null;
    // (undocumented)
    size: number | null;
}

// @public (undocumented)
export function createAwsS3BasedFileSystemContentStorage(components: Pick<AppComponents, "fs" | "config">, bucket: string): Promise<IContentStorageComponent>;

// @public (undocumented)
export function createFolderBasedFileSystemContentStorage(components: Pick<AppComponents, "fs">, root: string): Promise<FolderBasedContentStorage>;

// @public (undocumented)
export function createFsComponent(): IFileSystemComponent;

// @beta (undocumented)
export function createS3BasedFileSystemContentStorage(components: {}, s3: Pick<S3, "headObject" | "upload" | "getObject" | "deleteObjects">, options: {
    Bucket: string;
    getKey?: (hash: string) => string;
}): Promise<IContentStorageComponent>;

// @public (undocumented)
export type FolderBasedContentStorage = IContentStorageComponent & {
    allFileIds(): AsyncIterable<string>;
};

// @public (undocumented)
export type IContentStorageComponent = {
    storeStream(fileId: string, content: Readable): Promise<void>;
    storeStreamAndCompress(fileId: string, content: Readable): Promise<void>;
    delete(fileIds: string[]): Promise<void>;
    retrieve(fileId: string): Promise<ContentItem | undefined>;
    exist(fileId: string): Promise<boolean>;
    existMultiple(fileIds: string[]): Promise<Map<string, boolean>>;
};

// @public
export type IFileSystemComponent = Pick<typeof fs, "createReadStream"> & Pick<typeof fs, "createWriteStream"> & Pick<typeof fsPromises, "access" | "opendir" | "stat" | "unlink" | "mkdir" | "readdir" | "readFile"> & {
    constants: Pick<typeof fs.constants, "F_OK" | "R_OK">;
} & {
    existPath(path: string): Promise<boolean>;
};

// (No @packageDocumentation comment for this package)

```