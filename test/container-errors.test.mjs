import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BlobServiceClient } from '@azure/storage-blob';
import { main } from '../index.mjs';
import { processContainers } from '../container-policy.mjs';

const rule = { prefix: 'daily-', dateFormat: 'yyyyMMdd', retentionMonths: 1 };
test('container job failures log actionable diagnostics with credentials redacted', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'container-errors-'));
    const previousExitCode = process.exitCode;
    t.after(() => { process.exitCode = previousExitCode; rmSync(directory, { recursive: true, force: true }); });
    for (const method of ['log', 'info', 'error', 'table']) t.mock.method(console, method, () => {});
    const sasUrl = 'https://fixture.blob.core.windows.net/?sig=private-signature';
    t.mock.method(BlobServiceClient.prototype, 'listContainers', async function* () {
        throw Object.assign(new Error(`Access denied: ${sasUrl}`), {
            code: 'AuthorizationPermissionMismatch', statusCode: 403, requestId: 'request-123',
            request: { headers: { authorization: 'must-not-serialize' } }
        });
    });
    const app = join(directory, 'app.json'), config = join(directory, 'job.json');
    writeFileSync(app, JSON.stringify({ logging: { file: { enabled: true, directory, retentionDays: 1 } } }));
    writeFileSync(config, JSON.stringify({ provider: 'azure', policy: 'azure-container-retention',
        azure: { sasUrl }, cleanupRules: [rule] }));
    await main([`appConfig=${app}`, `config=${config}`, 'mode=prune', 'dryRun=true']);
    const text = readFileSync(join(directory, readdirSync(directory).find(name => name.endsWith('.jsonl'))), 'utf8');
    const completed = text.trim().split('\n').map(JSON.parse).find(record => record.event_name === 'run_completed');
    assert.equal(completed.outcome, 'failed');
    assert.equal(completed.error_message, 'Access denied: [REDACTED]');
    assert.equal(completed.error_code, 'AuthorizationPermissionMismatch');
    assert.equal(completed.status_code, 403);
    assert.equal(completed.request_id, 'request-123');
    assert.match(completed.error_stack, /Access denied/);
    assert.ok(!text.includes('private-signature'));
    assert.ok(!text.includes('must-not-serialize'));
});

test('per-container inventory and deletion failures retain their reasons', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'container-operation-errors-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const messages = [];
    const client = {
        async *listContainers() { yield { name: 'daily-20200101' }; },
        getContainerClient() { return {
            async *listBlobsFlat() { throw new Error('Inventory denied'); },
            async delete() { throw new Error('Container is leased'); }
        }; }
    };
    const config = { provider: 'azure', cleanupRules: [rule], outputDirectory: directory, dryRun: false, deleteNonRetained: true };
    const options = { client, scheduled: true, output: { info() {}, error(...args) { messages.push(args.join(' ')); } } };
    for (const list of [true, false]) {
        const result = await processContainers(config, { ...options, list });
        assert.equal(result.outcome, 'partial_failure');
    }
    assert.match(messages[0], /daily-20200101.*Inventory denied/);
    assert.match(messages[1], /daily-20200101.*Container is leased/);
});
