import { ListObjectsV2Command, DeleteObjectsCommand } from "@aws-sdk/client-s3";
const logger = console;

async function listS3Objects(s3Client, bucketName, prefix = '') {
    const objects = [];
    let continuationToken;

    do {
        try {
            const command = new ListObjectsV2Command({
                Bucket: bucketName,
                Prefix: prefix,
                ContinuationToken: continuationToken
            });

            const response = await s3Client.send(command);

            if (response.Contents) {
                objects.push(...response.Contents);
            }

            continuationToken = response.NextContinuationToken;
        } catch (error) {
            logger.error(`Error listing objects in bucket ${bucketName}:`, error);
            throw error;
        }
    } while (continuationToken);

    return objects;
}

async function deleteS3Objects(s3Client, bucketName, keys) {
    const chunkSize = 1000;
    const chunks = [];

    for (let i = 0; i < keys.length; i += chunkSize) {
        chunks.push(keys.slice(i, i + chunkSize));
    }

    const deletionResults = {
        successful: [],
        failed: []
    };

    for (const chunk of chunks) {
        const command = new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: chunk.map(key => ({ Key: key })),
                Quiet: false
            }
        });

        try {
            const response = await s3Client.send(command);

            if (response.Deleted) {
                deletionResults.successful.push(...response.Deleted.map(obj => obj.Key));
            }
            if (response.Errors) {
                deletionResults.failed.push(...response.Errors.map(error => ({
                    key: error.Key,
                    error: error.Message,
                    bucket: bucketName
                })));
            }
        } catch (error) {
            logger.error(`Error during batch deletion in bucket ${bucketName}:`, error);
            deletionResults.failed.push(...chunk.map(key => ({
                key,
                error: error.message,
                bucket: bucketName
            })));
        }
    }

    return deletionResults;
}

export { listS3Objects, deleteS3Objects };
