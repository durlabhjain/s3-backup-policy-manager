import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { confirmDeletion, processBackups } from '../index.mjs';

const key = 'WHISKEY/dedicated/full/WHISKEY_dedicated_FULL_20260807_144018_1.bak';
const retention = { fullBackups: 0, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0, differentialBackups: 0 };

test('terminal pruning previews before confirmation and deletes only after approval', async () => {
    for (const provider of ['aws', 'azure']) {
        for (const answer of [true, false]) {
            const events = [];
            const result = await processBackups({ provider, buckets: ['test'], containers: ['test'], retention, dryRun: false, deleteNonRetained: true }, () => ({
                async list() { return [{ Key: key }]; },
                async delete(bucket, keys) { events.push('delete'); assert.deepEqual(keys, [key]); return { successful: keys, failed: [] }; },
                async close() { events.push('close'); }
            }), {
                output: { log() {}, table(rows) { assert.equal(rows[0].Database, 'dedicated'); events.push('preview'); }, error: assert.fail },
                async confirm(details) { assert.equal(details.count, 1); assert.equal(details.provider, provider); events.push('confirm'); return answer; }
            });
            assert.deepEqual(events, answer ? ['preview', 'confirm', 'delete', 'close'] : ['preview', 'confirm', 'close']);
            assert.equal(Boolean(result.byBucket.test.deletionResult), answer);
        }
    }
});

test('dry runs, disabled deletion, and empty lists never ask or delete', async () => {
    for (const scheduled of [true, false]) {
        for (const [dryRun, deleteNonRetained, keys] of [[true, true, [key]], [false, false, [key]], [false, true, []]]) {
            await processBackups({ buckets: ['test'], retention, dryRun, deleteNonRetained }, () => ({
                async list() { return keys.map(Key => ({ Key })); },
                async delete() { assert.fail('Unexpected deletion'); }, async close() {}
            }), { scheduled, confirm: () => assert.fail('Unexpected confirmation'), output: { log() {}, error: assert.fail } });
        }
    }
});

test('confirmation rejects non-terminal input and requires the explicit DELETE response', async () => {
    assert.equal(await confirmDeletion({}, { isTTY: false }, { isTTY: true }), false);
    for (const answer of ['DELETE', 'yes', '', 'no']) {
        const input = new PassThrough();
        const output = new PassThrough();
        input.isTTY = output.isTTY = true;
        const result = confirmDeletion({ provider: 'aws', bucketName: 'test', count: 1 }, input, output);
        input.write(`${answer}\n`);
        assert.equal(await result, answer === 'DELETE');
        input.destroy(); output.destroy();
    }
});

test('EOF and Ctrl-C cancel confirmation', async () => {
    for (const cancel of ['eof', 'interrupt']) {
        const input = new PassThrough(), output = new PassThrough();
        input.isTTY = output.isTTY = true;
        const result = confirmDeletion({ provider: 'aws', bucketName: 'test', count: 1 }, input, output);
        if (cancel === 'eof') input.end(); else input.write('\x03');
        assert.equal(await result, false);
        input.destroy(); output.destroy();
    }
});
