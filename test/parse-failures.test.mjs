import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { applyRetentionPolicy, processBackups } from '../index.mjs';

const valid = 'WHISKEY/dedicated/full/WHISKEY_dedicated_FULL_20260807_144018_1.bak';
const invalid = [
    'notes.txt',
    valid.replace('20260807', '20260230'),
    valid.replace('144018', '256099'),
    valid.replace('/full/', '/diff/'),
    valid.replace('.bak', '.trn'),
    'DB_20260807_144018-Unknown.bak'
];
const retention = { fullBackups: 0, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0, differentialBackups: 0, logBackups: 0 };

test('parse failures are reported separately and never become deletion candidates', () => {
    const errors = [];
    const result = applyRetentionPolicy([...invalid, valid].map(Key => ({ Key })), retention, { error: message => errors.push(message) });
    assert.deepEqual(result.backupsToDelete.map(b => b.key), [valid]);
    assert.deepEqual(result.parseFailures.map(b => b.key), invalid);
    assert.equal(result.summary.parseFailureCount, invalid.length);
    assert.ok(result.parseFailures.every(b => b.reason));
    assert.equal(errors.length, invalid.length);
    assert.ok(errors.every(message => message.includes('PROTECTED') && message.includes('will NOT delete')));
});

test('dry, confirmed, and scheduled runs preserve and report invalid files on both providers', async () => {
    for (const provider of ['aws', 'azure']) {
        for (const [scheduled, dryRun] of [[false, true], [false, false], [true, true], [true, false]]) {
            const deleted = [], warnings = [];
            const result = await processBackups({ provider, buckets: ['parse-test'], containers: ['parse-test'], retention, dryRun, deleteNonRetained: true }, () => ({
                async list() { return [...invalid, valid].map(Key => ({ Key })); },
                async delete(bucket, keys) { deleted.push(...keys); return { successful: keys, failed: [] }; },
                async close() {}
            }), { scheduled, confirm: async () => true, output: { log() {}, error: message => warnings.push(message) } });
            assert.deepEqual(deleted, dryRun ? [] : [valid]);
            assert.equal(result.totalSummary.parseFailureCount, invalid.length);
            assert.ok(warnings.some(message => message.includes('ATTENTION:')));
            const report = JSON.parse(readFileSync(`output/${provider}-parse-test.parse-failures.json`, 'utf8'));
            assert.deepEqual(report.map(b => b.key), invalid);
            assert.ok(report.every(b => b.reason && b.bucketName === 'parse-test'));
        }
    }
});

test('an entirely unparseable listing never prompts or deletes', async () => {
    for (const scheduled of [false, true]) {
        const result = await processBackups({ buckets: ['parse-test'], retention, dryRun: false, deleteNonRetained: true }, () => ({
            async list() { return invalid.map(Key => ({ Key })); },
            async delete() { assert.fail('Unparseable files must not be deleted'); }, async close() {}
        }), { scheduled, confirm: () => assert.fail('No valid candidates to confirm'), output: { log() {}, error() {} } });
        assert.equal(result.totalSummary.deleteCount, 0);
    }
});
