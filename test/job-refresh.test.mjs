import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import cron from 'node-cron';
import { BlobServiceClient } from '@azure/storage-blob';
import { main } from '../index.mjs';

test('scheduled jobs refresh Vault storage values per run, isolate outputs and skip overlaps', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'job-refresh-')); t.after(() => rmSync(directory, { recursive: true, force: true }));
    for (const method of ['log', 'info', 'table', 'debug', 'warn', 'error']) t.mock.method(console, method, () => {});
    const sigint = new Set(process.listeners('SIGINT')), sigterm = new Set(process.listeners('SIGTERM'));
    t.after(() => {
        for (const listener of process.listeners('SIGINT')) if (!sigint.has(listener)) process.removeListener('SIGINT', listener);
        for (const listener of process.listeners('SIGTERM')) if (!sigterm.has(listener)) process.removeListener('SIGTERM', listener);
    });
    let secret = 'first', requests = 0, block = false, unblock;
    t.mock.method(globalThis, 'fetch', async () => {
        requests++;
        if (block) { block = false; await new Promise(resolve => { unblock = resolve; }); }
        return { ok: true, async json() { return { data: { data: { provider: 'azure', containers: ['backups'], azure: { sasUrl: `https://fixture.blob.core.windows.net/?sig=${secret}` } } } }; } };
    });
    const urls = [], jobs = [];
    t.mock.method(BlobServiceClient.prototype, 'getContainerClient', function () {
        urls.push(this.url); return { listBlobsFlat() { return { async *byPage() { yield { segment: { blobItems: [] } }; } }; } };
    });
    t.mock.method(cron, 'schedule', (_, callback) => { jobs.push(callback); return { stop() {} }; });
    const appPath = join(directory, 'app.json');
    writeFileSync(appPath, JSON.stringify({ logging: { file: { enabled: false } }, vault: { address: 'http://localhost:8200', token: 'fixture-token' },
        outputDirectory: directory, jobs: [{ jobId: 'test-job', cron: '0 1 * * *', settings: 'vault:job' }] }));
    await main([`appConfig=${appPath}`, 'mode=schedulePrune']);
    await jobs[0](); assert.match(urls[0], /sig=first/);
    secret = 'rotated'; await jobs[0](); assert.match(urls[1], /sig=rotated/);
    block = true; const firstRun = jobs[0](); await new Promise(resolve => setImmediate(resolve));
    const before = requests; await jobs[0](); assert.equal(requests, before);
    unblock(); await firstRun; assert.equal(urls.length, 3);
});
