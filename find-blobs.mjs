import { createStorage, storageNames } from './storage.mjs';
import ActionBase from './action-base.mjs';
import fs from 'node:fs/promises';
import { join } from 'node:path';

class FindBlobs extends ActionBase {
    constructor({ bucket, searchPattern, pattern, prefix, logger = console } = {}) {
        super();
        this.logger = logger;
        this.bucket = bucket;
        const expression = searchPattern ?? pattern;
        this.searchPattern = typeof expression === 'string' ? new RegExp(expression) : expression;
        this.prefix = prefix;
    }

    async run(config) {
        const names = storageNames(config).filter(name => !this.bucket || name === this.bucket);
        if (!names.length) return;
        const storage = createStorage(config);
        const statistics = { targets_scanned: 0, objects_observed: 0, objects_matched: 0, bytes_observed: 0 };
        try {
            const directory = config.outputDirectory || 'output';
            await fs.mkdir(directory, { recursive: true });
            for (const bucket of names) {
                const prefix = this.prefix ?? config.prefix ?? '';
                const started = performance.now();
                this.logger.info(`Listing ${config.provider || 'aws'}/${bucket}, prefix=${JSON.stringify(prefix)}...`);
                const objects = await storage.list(bucket, prefix, progress => {
                    this.logger.info(`Listing progress: ${progress.count} files, ${progress.pages} pages, ${(progress.elapsedMs / 1000).toFixed(1)}s`);
                });
                statistics.targets_scanned++;
                statistics.objects_observed += objects.length;
                statistics.bytes_observed += objects.reduce((sum, object) => sum + (object.Size || 0), 0);
                this.logger.info(`Listing complete: ${objects.length} files in ${((performance.now() - started) / 1000).toFixed(1)}s`);
                const results = objects.filter(object => {
                    if (!this.searchPattern) return true;
                    this.searchPattern.lastIndex = 0;
                    return this.searchPattern.test(object.Key);
                }).map(object => object.Key);
                statistics.objects_matched += results.length;
                const listFile = join(directory, `${config.provider || 'aws'}-${bucket}-blobs.txt`);
                await fs.writeFile(listFile, results.join('\n'), 'utf8');
                this.logger.info(`Wrote ${results.length} blob keys to ${listFile}`);
            }
        } finally {
            await storage.close();
        }
        return { outcome: 'success', statistics };
    }
}
export default FindBlobs;
