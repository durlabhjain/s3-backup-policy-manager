import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { processContainers, parseContainerDate, subtractMonths } from '../container-policy.mjs';

const output = { table() {}, info() {}, error() {} };
test('container dates are strict UTC and month subtraction clamps like AddMonths', () => {
    assert.equal(parseContainerDate('-20240229', 'yyyyMMdd').toISOString(), '2024-02-29T00:00:00.000Z');
    assert.equal(parseContainerDate('_202402', 'yyyyMM').toISOString(), '2024-02-01T00:00:00.000Z');
    for (const value of ['20230229', '20241301', '20240132', '20240101-extra', '00000101']) assert.equal(parseContainerDate(value, 'yyyyMMdd'), null);
    assert.equal(subtractMonths(new Date('2024-05-31T12:34:56Z'), 3).toISOString(), '2024-02-29T12:34:56.000Z');
});
test('container cleanup preserves gates, skips invalid dates, counts partial deletes and inventories', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'containers-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const deleted = [];
    const config = { provider: 'azure', outputDirectory: directory, dryRun: true, deleteNonRetained: true,
        cleanupRules: [{ prefix: 'daily-', dateFormat: 'yyyyMMdd', retentionMonths: 3 }, { prefix: 'monthly-', dateFormat: 'yyyyMM', retentionMonths: 6 }] };
    const client = {
        async *listContainers() { for (const name of ['daily-20230101', 'monthly-202301', 'daily-20260101', 'daily-20230229', 'unrelated']) yield { name }; },
        getContainerClient(name) { return { async delete() { deleted.push(name); if (name.startsWith('monthly')) throw new Error(); },
            async *listBlobsFlat() { if (name.startsWith('monthly')) throw new Error(); yield { properties: { contentLength: 10 } }; } }; }
    };
    const options = { client, output, now: new Date('2026-01-01T00:00:00Z'), scheduled: true };
    let result = await processContainers(config, options);
    assert.equal(result.statistics.containers_would_delete, 2); assert.deepEqual(deleted, []);
    assert.equal(result.statistics.parse_failure_count, 1);
    result = await processContainers({ ...config, dryRun: false, deleteNonRetained: false }, options);
    assert.deepEqual(deleted, []);
    result = await processContainers({ ...config, dryRun: false }, { ...options, scheduled: false, confirm: async () => false });
    assert.equal(result.outcome, 'cancelled'); assert.deepEqual(deleted, []);
    result = await processContainers({ ...config, dryRun: false }, options);
    assert.equal(result.outcome, 'partial_failure'); assert.equal(result.statistics.containers_deleted, 1); assert.equal(result.statistics.containers_failed, 1);
    assert.deepEqual(deleted, ['daily-20230101', 'monthly-202301']);
    result = await processContainers(config, { ...options, list: true });
    assert.equal(result.statistics.blobs_observed, 4); assert.equal(result.statistics.bytes_observed, 40); assert.equal(result.statistics.containers_failed, 1);
    assert.equal(deleted.length, 2);
});
test('a failed container enumeration never deletes its partial candidates', async () => {
    const client = { async *listContainers() { yield { name: 'daily-20200101' }; throw new Error('page failed'); }, getContainerClient() { assert.fail('must not delete'); } };
    await assert.rejects(processContainers({ provider: 'azure', dryRun: false, deleteNonRetained: true,
        cleanupRules: [{ prefix: 'daily-', dateFormat: 'yyyyMMdd', retentionMonths: 3 }] }, { client, output, scheduled: true }), /page failed/);
});
