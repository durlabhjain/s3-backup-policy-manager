import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import cron from 'node-cron';
import { BlobServiceClient } from '@azure/storage-blob';
import { main } from '../index.mjs';

test('every mode processes selected files; cron registers each schedule and rejects invalid input before registration', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'backup-config-modes-'));
    const originalCwd = process.cwd();
    process.chdir(directory);
    t.after(() => { process.chdir(originalCwd); rmSync(directory, { recursive: true, force: true }); });
    for (const method of ['log', 'info', 'table', 'debug']) t.mock.method(console, method, () => {});
    const errors = [];
    t.mock.method(console, 'error', (...args) => errors.push(args));
    const makeConfig = (prefix, expression) => ({
        provider: 'azure', containers: ['backups'], prefix, cron: expression,
        azure: { sasUrl: 'https://example.blob.core.windows.net/?sig=test' }
    });
    const first = makeConfig('first/', '0 1 * * *');
    const second = makeConfig('second/', '0 2 * * *');
    writeFileSync('config.a.json', JSON.stringify(first));
    writeFileSync('config.b.json', JSON.stringify([second]));
    const listed = [], signed = [], jobs = [];
    t.mock.method(BlobServiceClient.prototype, 'getContainerClient', name => ({
        listBlobsFlat({ prefix }) {
            listed.push(prefix);
            return { async *byPage() { yield { segment: { blobItems: [] } }; } };
        },
        getBlobClient(key) { return { async generateSasUrl() { signed.push({ name, key }); return 'signed'; } }; }
    }));
    t.mock.method(cron, 'schedule', (expression, callback) => { jobs.push({ expression, callback }); });

    await main(['mode=prune', 'config=config.*.json', 'dryRun=true']);
    assert.deepEqual(listed.splice(0), ['first/', 'second/']);
    await main(['mode=findBlobs', 'config=config.a.json,config.b.json']);
    assert.deepEqual(listed.splice(0), ['first/', 'second/']);
    assert.equal(readFileSync('output/azure-backups-blobs.txt', 'utf8'), '');
    await main(['mode=generateSignedUrls', 'config=config.a.json', 'config=config.b.json', 'bucket=backups', 'blob=backup.bak']);
    assert.deepEqual(signed, [{ name: 'backups', key: 'backup.bak' }, { name: 'backups', key: 'backup.bak' }]);
    await main(['mode=schedulePrune', 'config=config.*.json', 'config=config.a.json', 'dryRun=true']);
    assert.deepEqual(jobs.map(job => job.expression), [first.cron, second.cron]);
    assert.deepEqual(listed, []);
    for (const job of jobs) await job.callback();
    assert.deepEqual(listed.splice(0), ['first/', 'second/']);
    assert.deepEqual(errors, []);

    jobs.length = 0;
    writeFileSync('config.b.json', JSON.stringify({ ...second, cron: 'invalid' }));
    await assert.rejects(main(['mode=schedulePrune', 'config=config.*.json']), /Invalid cron/);
    assert.equal(jobs.length, 0);
    writeFileSync('config.b.json', JSON.stringify({ ...second, containers: [] }));
    await assert.rejects(main(['mode=schedulePrune', 'config=config.*.json']), /No buckets\/containers/);
    assert.equal(jobs.length, 0);
});
