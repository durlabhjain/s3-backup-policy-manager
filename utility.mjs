import {
    S3Client,
    ListObjectsV2Command,
    DeleteObjectsCommand
} from "@aws-sdk/client-s3";

export default {
    createS3Client: (config) => {
        // Initialize S3 client with configuration
        const s3Config = {
            region: config.aws.region,
            credentials: config.aws.credentials
        };

        // Add optional S3 configurations
        if (config.aws.endpoint) {
            s3Config.endpoint = config.aws.endpoint;
            s3Config.forcePathStyle = config.aws.forcePathStyle ?? true;
            s3Config.useArnRegion = config.aws.useArnRegion ?? true;
        }

        return new S3Client({
            region: 'us-east-1',
            ...s3Config
        });
    }
}