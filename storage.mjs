import { BlobServiceClient, ContainerClient, StorageSharedKeyCredential, BlobSASPermissions } from '@azure/storage-blob';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import utility from './utility.mjs';
import { listS3Objects, deleteS3Objects } from './s3-operations.mjs';

export function storageNames(config) {
    return config.provider === 'azure' ? (config.containers || []) : (config.buckets || []);
}

export function createAzureClient(config) {
    const azure = config.azure || {};
    const sasUrl = azure.sasUrl || process.env.AZURE_STORAGE_SAS_URL;
    if (sasUrl) {
        let parsed;
        try { parsed = new URL(sasUrl); } catch { throw new Error('azure.sasUrl must be a valid Blob service or container SAS URL'); }
        if (!['https:', 'http:'].includes(parsed.protocol) || !parsed.searchParams.get('sig') || parsed.username || parsed.password || parsed.hash) {
            throw new Error('azure.sasUrl must be an HTTP(S) URL containing a SAS signature');
        }
        const path = parsed.pathname.split('/').filter(Boolean);
        if (path.length > 1 || parsed.searchParams.get('sr') === 'b') {
            throw new Error('azure.sasUrl must target the Blob service or a container, not an individual blob');
        }
        if (path.length === 0) return new BlobServiceClient(sasUrl);
        const container = new ContainerClient(sasUrl);
        return {
            getContainerClient(name) {
                if (name !== container.containerName) throw new Error('Configured container does not match the container in azure.sasUrl');
                return container;
            }
        };
    }
    const connectionString = azure.connectionString || process.env.AZURE_STORAGE_CONNECTION_STRING;
    if (connectionString) return BlobServiceClient.fromConnectionString(connectionString);
    const accountName = azure.accountName || process.env.AZURE_STORAGE_ACCOUNT;
    const accountKey = azure.accountKey || process.env.AZURE_STORAGE_KEY;
    if (!accountName || !accountKey) throw new Error('Azure requires sasUrl, a connection string, or accountName and accountKey');
    return new BlobServiceClient(azure.endpoint || `https://${accountName}.blob.core.windows.net`,
        new StorageSharedKeyCredential(accountName, accountKey));
}

export function createStorage(config, client) {
    const provider = config.provider || 'aws';
    if (!['aws', 'azure'].includes(provider)) throw new Error(`Unsupported provider: ${provider}`);
    const deleteConcurrency = config.azure?.deleteConcurrency ?? 16;
    if (provider === 'azure' && (!Number.isInteger(deleteConcurrency) || deleteConcurrency < 1 || deleteConcurrency > 128)) {
        throw new Error('azure.deleteConcurrency must be an integer from 1 to 128');
    }
    client ||= provider === 'azure' ? createAzureClient(config) : utility.createS3Client(config);
    return {
        async list(bucket, prefix = '', onProgress = () => {}) {
            if (provider === 'aws') return listS3Objects(client, bucket, prefix);
            const objects = [];
            const started = performance.now();
            let pageNumber = 0;
            const pages = client.getContainerClient(bucket).listBlobsFlat({ prefix }).byPage({ maxPageSize: 5000 });
            for await (const page of pages) {
                for (const blob of page.segment.blobItems) {
                    objects.push({ Key: blob.name, Size: blob.properties.contentLength, LastModified: blob.properties.lastModified });
                }
                onProgress({ pages: ++pageNumber, count: objects.length, elapsedMs: performance.now() - started });
            }
            return objects;
        },
        async delete(bucket, keys, onProgress = () => {}) {
            if (provider === 'aws') return deleteS3Objects(client, bucket, keys);
            const result = { successful: [], failed: [] };
            const container = client.getContainerClient(bucket);
            const failures = new Map();
            const started = performance.now();
            let next = 0, completed = 0, lastReported = started;
            const report = () => onProgress({
                completed, total: keys.length, successful: completed - failures.size,
                failed: failures.size, elapsedMs: performance.now() - started
            });
            report();
            async function worker() {
                while (next < keys.length) {
                    const index = next++;
                    const key = keys[index];
                    try {
                        await container.deleteBlob(key);
                    } catch (error) {
                        failures.set(index, { key, error: error.message, bucket });
                    }
                    completed++;
                    const now = performance.now();
                    if (completed === keys.length || now - lastReported >= 5000) {
                        lastReported = now;
                        report();
                    }
                }
            }
            await Promise.all(Array.from({ length: Math.min(deleteConcurrency, keys.length) }, () => worker()));
            // Preserve input order even when requests finish out of order.
            keys.forEach((key, index) => {
                if (failures.has(index)) result.failed.push(failures.get(index));
                else result.successful.push(key);
            });
            return result;
        },
        async signedUrl(bucket, key, expiresIn = 86400) {
            const seconds = Number(expiresIn);
            if (!key || !Number.isInteger(seconds) || seconds <= 0 || seconds > 604800) {
                throw new Error('blob is required and expiresIn must be an integer from 1 to 604800 seconds');
            }
            if (provider === 'aws') return getSignedUrl(client, new GetObjectCommand({ Bucket: bucket, Key: key }), { expiresIn: seconds });
            return client.getContainerClient(bucket).getBlobClient(key).generateSasUrl({
                permissions: BlobSASPermissions.parse('r'),
                startsOn: new Date(Date.now() - 5 * 60 * 1000),
                expiresOn: new Date(Date.now() + seconds * 1000)
            });
        },
        async close() { if (provider === 'aws') client.destroy(); }
    };
}
