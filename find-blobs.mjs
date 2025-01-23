import utility from "./utility.mjs";
import ActionBase from "./action-base.mjs";
import {
    ListObjectsV2Command
} from "@aws-sdk/client-s3";

class FindBlobs extends ActionBase {

    results = [];

    constructor({ bucket, searchPattern, prefix = "", logger = console }) {
        super();
        this.logger = logger;
        this.bucket = bucket;
        this.searchPattern = (typeof searchPattern === 'string' && searchPattern) ? new RegExp(searchPattern) : searchPattern;
        this.prefix = prefix;
    }

    async findBlobs(client, bucket, searchPattern, prefix = '') {
        const results = [];
        let continuationToken = undefined;

        do {
            const command = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix,
                ContinuationToken: continuationToken
            });

            const response = await client.send(command);

            if (searchPattern) {
                for (const object of response.Contents || []) {
                    if (searchPattern.test(object.Key)) {
                        results.push(object.Key);
                    }
                }
            } else {
                for (const object of response.Contents || []) {
                    results.push(object.Key);
                }
            }

            continuationToken = response.IsTruncated ? response.NextContinuationToken : null;
        } while (continuationToken);

        return results;
    }

    async run(config) {
        const { bucket, searchPattern, prefix } = this;
        if (!config.buckets.includes(bucket)) {
            this.logger.info(`Skipping ${config.aws.endpoint} with buckets ${config.buckets}`);
            return;
        }
        this.logger.info(`Processing ${config.aws.endpoint} with bucket ${bucket}`);
        const client = utility.createS3Client(config);
        const results = await this.findBlobs(client, bucket, searchPattern, prefix);
        this.results.push(...results);
    }

    async cleanup() {
        for (const result of this.results) {
            console.log(result);
        }
    }
}

export default FindBlobs;