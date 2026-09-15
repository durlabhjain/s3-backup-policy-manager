import { test } from 'node:test';
import assert from 'node:assert/strict';
import { applyRetentionPolicy, processBackups } from '../index.mjs';

const policy = { fullBackups: 1, differentialBackups: 1, logBackups: 1, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0 };
const key = (server, type, day, part = '') => `${server}/db/${type}/${server}_db_${type.toUpperCase()}_2026080${day}_120000${part}.${type === 'log' ? 'trn' : 'bak'}`;

test('overview counts files per type/source and shows the oldest retained date', () => {
    const keys = [key('A', 'full', 1), key('A', 'full', 2, '_1'), key('A', 'full', 2, '_2'), key('A', 'diff', 1), key('A', 'diff', 2), key('A', 'log', 1), key('A', 'log', 2), key('B', 'full', 1)];
    const result = applyRetentionPolicy(keys.map(Key => ({ Key })), policy, console, new Date(2026, 7, 3, 12));
    const a = result.deletionOverview.find(row => row.Server === 'A');
    assert.equal(a.Database, 'db');
    assert.equal(a['FULL'], '1/3');
    assert.equal(a['DIFF'], '1/2');
    assert.equal(a['LOG'], '1/2');
    assert.equal(a['Kept'], 4);
    assert.ok(!Object.hasOwn(a, 'Oldest to delete'));
    assert.equal(a['Oldest kept'], '2026-08-02 12:00:00');
    const b = result.deletionOverview.find(row => row.Server === 'B');
    assert.equal(b['FULL'], '0/1');
    assert.equal(b['LOG'], '0/0');
    assert.ok(!Object.hasOwn(b, 'Oldest to delete'));
});

test('overview flags complete deletion and keeps parse failures outside counts', () => {
    const result = applyRetentionPolicy([{ Key: 'db_20260801_120000-Diff.bak' }, { Key: 'invalid.txt' }], { ...policy, differentialBackups: 0 }, { error() {} });
    const [row] = result.deletionOverview;
    assert.equal(row.Database, 'db');
    assert.equal(row.Server, '(legacy: unknown)');
    assert.equal(row['DIFF'], '1/1');
    assert.equal(row.Status, 'WARNING: ALL PARSED FILES SELECTED');
    assert.equal(row['Oldest kept'], '—');
    assert.equal(result.summary.parseFailureCount, 1);
});

test('table and full-deletion warning appear before confirmation', async () => {
    const events = [];
    await processBackups({ buckets: ['overview-test'], retention: { ...policy, fullBackups: 0 }, dryRun: false, deleteNonRetained: true }, () => ({
        async list() { return [{ Key: key('A', 'full', 1) }]; },
        async delete() { assert.fail('Confirmation declined'); }, async close() {}
    }), {
        output: { table(rows) { assert.equal(rows[0]['FULL'], '1/1'); assert.equal(rows[0].Source, 'overview-test/A/db'); events.push('table'); }, log(message) { if (message.startsWith('WARNING: ALL')) events.push('warning'); }, error: assert.fail },
        async confirm() { events.push('confirm'); return false; }
    });
    assert.deepEqual(events, ['table', 'warning', 'confirm']);
});


test('source includes bucket/container for modern and legacy backups', () => {
    for (const bucketName of ['aws-backups', 'azure-backups']) {
        const result = applyRetentionPolicy([
            { Key: key('A', 'full', 1), bucketName },
            { Key: 'legacy_20260801_120000-Full.bak', bucketName }
        ], policy);
        assert.deepEqual(result.deletionOverview.map(row => row.Source), [`${bucketName}/A/db`, `${bucketName}/legacy`]);
    }
});
