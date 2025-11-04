import utility from "./utility.mjs";
import ActionBase from "./action-base.mjs";
import fs from "fs/promises";
import {
    ListObjectsV2Command
} from "@aws-sdk/client-s3";

class FindBlobs extends ActionBase {

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
        const { searchPattern, prefix } = this;
        const client = utility.createS3Client(config);
        for (const bucket of config.buckets) {
            this.logger.info(`Processing ${config.aws.endpoint} with bucket ${bucket}`);
            const listFile = `output/${bucket}-${config.aws.endpoint.split("://")[1]}-blobs.txt`;
            const results = await this.findBlobs(client, bucket, searchPattern, prefix);
            await fs.writeFile(listFile, results.join('\n'), 'utf8');
            this.logger.info(`Wrote ${results.length} blob keys to ${listFile}`);
        }
    }

    async cleanup() {
    }
}

export default FindBlobs;