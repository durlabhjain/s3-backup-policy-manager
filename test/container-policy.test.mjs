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

test('retention review explains retained and protected containers before confirmation', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'container-review-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const tables = [], messages = [];
    const config = { provider: 'azure', outputDirectory: directory, dryRun: false, deleteNonRetained: true,
        cleanupRules: [
            { prefix: 'daily-', dateFormat: 'yyyyMMdd', retentionMonths: 3 },
            { prefix: 'daily-', dateFormat: 'yyyyMMdd', retentionMonths: 0 },
            { prefix: 'empty-', dateFormat: 'yyyyMM', retentionMonths: 6 }
        ] };
    const client = {
        async *listContainers() {
            for (const name of ['daily-20250101', 'daily-20251001', 'daily-20251201', 'daily-20250230', 'unrelated']) yield { name };
        },
        getContainerClient() { assert.fail('Declined confirmation must not delete or enumerate blobs'); }
    };
    const result = await processContainers(config, { client, now: new Date('2026-01-01T00:00:00Z'),
        output: { info(...args) { messages.push(args.join(' ')); }, table(rows) { tables.push(rows); } },
        async confirm() {
            assert.equal(tables.length, 3);
            assert.ok(messages.some(message => message.includes('Eligible: 1 | Retained: 2 | Protected: 2 | Invalid dates: 1')));
            return false;
        }
    });
    assert.equal(result.outcome, 'cancelled');
    assert.deepEqual(tables[0].map(row => row.Status), ['Eligible for deletion', 'Retained', 'Retained', 'Protected', 'Protected']);
    assert.equal(tables[0][3].Reason, 'Invalid date for matching prefixes');
    assert.equal(tables[0][4].Reason, 'No matching rule');
    assert.equal(tables[1][0].Matched, 3);
    assert.equal(tables[1][0].Eligible, 1);
    assert.equal(tables[1][0].Kept, 2);
    assert.equal(tables[1][0]['Oldest kept (UTC)'], '2025-10-01T00:00:00.000Z');
    assert.equal(tables[1][1].Matched, 0); // First matching rule owns the container.
    assert.equal(tables[1][2].Matched, 0);
    assert.ok(messages.some(message => message.includes('Deleted: 0 | Would delete: 0 | Failed: 0 | Skipped: 4')));
});

test('exact candidate list precedes confirmation and deletion audit records only successful removals', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'container-audit-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    for (const mode of ['approve', 'decline', 'dry', 'scheduled']) {
        const events = [], tables = [];
        const client = {
            url: 'https://fixture.blob.core.windows.net/?sig=secret',
            async *listContainers() { for (const name of ['daily-20200101', 'daily-20200102', 'unrelated']) yield { name }; },
            getContainerClient(name) { return { async delete() {
                events.push({ event: 'request', container_name: name });
                if (name.endsWith('02')) throw new Error('leased');
                return { requestId: 'azure-request' };
            } }; }
        };
        await processContainers({ provider: 'azure', outputDirectory: directory, dryRun: mode === 'dry', deleteNonRetained: true,
            cleanupRules: [{ prefix: 'daily-', dateFormat: 'yyyyMMdd', retentionMonths: 1 }] }, {
            client, scheduled: mode === 'scheduled', output: { info() {}, error() {}, table(rows) { tables.push(rows); },
                event(event, details) { events.push({ event, ...details }); } },
            async confirm(details) {
                assert.ok(!['scheduled', 'dry'].includes(mode));
                assert.equal(details.resourceType, 'containers');
                assert.deepEqual(tables.at(-1).map(row => row.Container), ['daily-20200101', 'daily-20200102']);
                assert.equal(events.length, 2);
                return mode === 'approve';
            }
        });
        const removed = events.filter(row => row.event === 'container_deleted');
        assert.equal(removed.length, ['approve', 'scheduled'].includes(mode) ? 1 : 0);
        if (removed.length) {
            assert.equal(removed[0].container_name, 'daily-20200101');
            assert.equal(removed[0].storage_account, 'fixture.blob.core.windows.net');
            assert.equal(removed[0].request_id, 'azure-request');
            assert.ok(Date.parse(removed[0].deleted_at));
            assert.equal(events.filter(row => row.event === 'container_deletion_failed').length, 1);
        }
        assert.ok(!JSON.stringify(events).includes('sig=secret'));
    }
});
