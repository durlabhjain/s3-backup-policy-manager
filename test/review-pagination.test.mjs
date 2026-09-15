import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processBackups, continueReview } from '../index.mjs';
import { PassThrough } from 'node:stream';
const retention = { fullBackups: 0, differentialBackups: 0, logBackups: 0, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0 };
const keys = ['A/db1', 'A/db2', 'B/db1'].map(path => `${path}/full/example_FULL_20260801_120000.bak`);

test('interactive previews pause once per server, suppress file lines and review before deletion', async () => {
    const events = [], logs = [];
    await processBackups({ buckets: ['review-test'], retention, dryRun: false, deleteNonRetained: true }, () => ({
        async list() { return keys.map(Key => ({ Key })); },
        async delete(bucket, selected) { assert.deepEqual(selected, keys); events.push('delete'); return { successful: selected, failed: [] }; }, async close() {}
    }), {
        output: { log(message) { logs.push(message); }, error: assert.fail, table(rows) { events.push(`table:${rows.length}`); } },
        async review({ group }) { events.push(`review:${group}`); return true; },
        async confirm() { events.push('confirm'); return true; }
    });
    assert.deepEqual(events, ['table:2', 'review:review-test/A', 'table:1', 'review:review-test/B', 'confirm', 'delete']);
    assert.ok(!logs.some(line => line.startsWith('Deletion candidate:')));
    assert.ok(logs.some(line => line.includes('.deletion-candidates.json')));
});

test('stopping review prevents deletion and subsequent containers even in dry runs', async () => {
    for (const dryRun of [true, false]) {
        let lists = 0, reviews = 0;
        const result = await processBackups({ buckets: ['review-test', 'never-visited'], retention, dryRun, deleteNonRetained: true }, () => ({
            async list() { lists++; return keys.map(Key => ({ Key })); },
            async delete() { assert.fail('Review stopped'); }, async close() {}
        }), { output: { log() {}, error: assert.fail }, async review() { reviews++; return false; }, confirm: () => assert.fail('Review stopped') });
        assert.equal(result.reviewStopped, true);
        assert.equal(lists, 1);
        assert.equal(reviews, 1);
    }
});

test('scheduled previews never pause for server review', async () => {
    await processBackups({ buckets: ['review-test'], retention, dryRun: true }, () => ({
        async list() { return keys.map(Key => ({ Key })); }, async close() {}
    }), { scheduled: true, output: { log() {}, error: assert.fail }, review: () => assert.fail('Scheduled mode cannot pause') });
});

test('review prompt accepts Enter, rejects q and cancels on EOF', async () => {
    for (const response of ['\n', 'q\n', null]) {
        const input = new PassThrough(), output = new PassThrough();
        input.isTTY = output.isTTY = true;
        const result = continueReview({ group: 'A' }, input, output);
        if (response === null) input.end(); else input.write(response);
        assert.equal(await result, response === '\n');
        input.destroy(); output.destroy();
    }
});

test('each server summary totals its own database rows before prompting', async () => {
    const listed = [
        ...keys,
        'A/db1/full/example_FULL_20260802_120000_1.bak',
        'A/db1/full/example_FULL_20260802_120000_2.bak',
        'A/db1/diff/example_DIFF_20260802_120000.bak',
        'A/db2/log/example_LOG_20260802_120000.trn'
    ];
    const logs = [];
    const summaries = [];
    await processBackups({ buckets: ['review-test'], retention: { ...retention, fullBackups: 1 }, dryRun: true }, () => ({
        async list() { return listed.map(Key => ({ Key })); }, async close() {}
    }), {
        output: { log(message) { logs.push(message); }, error: assert.fail },
        async review({ group, summary }) {
            assert.ok(logs.at(-1).startsWith(`Server summary for ${group}:`));
            summaries.push(summary);
            return true;
        }
    });
    assert.deepEqual(summaries, [
        { databases: 2, totalFiles: 6, deleteFiles: 3, keptFiles: 3, allSelectedDatabases: 0, FULL: '1/4', DIFF: '1/1', LOG: '1/1' },
        { databases: 1, totalFiles: 1, deleteFiles: 0, keptFiles: 1, allSelectedDatabases: 0, FULL: '0/1', DIFF: '0/0', LOG: '0/0' }
    ]);
});
