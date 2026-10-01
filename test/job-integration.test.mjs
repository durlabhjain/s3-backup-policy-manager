import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import cron from 'node-cron';
import { BlobServiceClient } from '@azure/storage-blob';
import { main } from '../index.mjs';

test('SQL partial deletes reach OTLP and job-specific reports; invalid startup registers no schedules', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'sql-job-events-')); t.after(() => rmSync(directory, { recursive: true, force: true }));
    const beforeInt = new Set(process.listeners('SIGINT')), beforeTerm = new Set(process.listeners('SIGTERM'));
    t.after(() => {
        for (const listener of process.listeners('SIGINT')) if (!beforeInt.has(listener)) process.removeListener('SIGINT', listener);
        for (const listener of process.listeners('SIGTERM')) if (!beforeTerm.has(listener)) process.removeListener('SIGTERM', listener);
    });
    for (const method of ['log', 'info', 'table', 'debug', 'warn', 'error']) t.mock.method(console, method, () => {});
    const payloads = [];
    const server = createServer(async (req, res) => {
        let body = ''; for await (const chunk of req) body += chunk;
        payloads.push(JSON.parse(body)); res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}');
    });
    server.listen(0, '127.0.0.1'); await once(server, 'listening'); t.after(() => { server.closeAllConnections(); server.close(); });
    const keys = ['db_20200101_120000-Full.bak', 'db_20200102_120000-Full.bak'];
    t.mock.method(BlobServiceClient.prototype, 'getContainerClient', () => ({
        listBlobsFlat() { return { async *byPage() { yield { segment: { blobItems: keys.map(name => ({ name, properties: { contentLength: 10 } })) } }; } }; },
        async deleteBlob(name) { if (name === keys[1]) throw new Error('fixture'); }
    }));
    const jobs = []; t.mock.method(cron, 'schedule', (_, callback) => { jobs.push(callback); return { stop() {} }; });
    const app = join(directory, 'app.json'), job = join(directory, 'job.json');
    writeFileSync(app, JSON.stringify({ outputDirectory: directory, telemetry: { enabled: true,
        endpoint: `http://127.0.0.1:${server.address().port}/v1/logs`, authorization: 'Basic fixture' } }));
    const config = { jobId: 'sql', provider: 'azure', azure: { sasUrl: 'https://fixture.blob.core.windows.net/?sig=fixture' },
        containers: ['backups'], cron: '0 1 * * *', dryRun: false, deleteNonRetained: true,
        retention: { fullBackups: 0, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0, differentialBackups: 0, logBackups: 0 } };
    writeFileSync(job, JSON.stringify(config));
    await main([`appConfig=${app}`, `config=${job}`, 'mode=schedulePrune']); await jobs[0]();
    const records = payloads.flatMap(payload => payload.resourceLogs.flatMap(resource => resource.scopeLogs.flatMap(scope => scope.logRecords)));
    const events = records.map(record => Object.fromEntries(record.attributes.map(attribute => [attribute.key, Object.values(attribute.value)[0]]))).filter(record => record.event_name);
    assert.equal(events.length, 2); assert.equal(events[0].run_id, events[1].run_id);
    assert.equal(events[1].outcome, 'partial_failure'); assert.equal(Number(events[1].objects_deleted), 1); assert.equal(Number(events[1].objects_failed), 1);
    assert.equal(JSON.parse(readFileSync(join(directory, 'sql', 'azure-backups.deletion-candidates.json'))).length, 2);
    const registered = jobs.length;
    writeFileSync(job, JSON.stringify([{ ...config, jobId: 'valid' }, { ...config, jobId: 'invalid', retention: { fullBackups: -1 } }]));
    await assert.rejects(main([`appConfig=${app}`, `config=${job}`, 'mode=schedulePrune']), /Invalid retention/);
    assert.equal(jobs.length, registered);
});
