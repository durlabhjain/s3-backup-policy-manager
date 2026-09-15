import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { createStorage } from '../storage.mjs';

for (const concurrency of [1, 3, 16]) {
    test(`Azure deletion bounds active requests at ${concurrency} and accounts for every key`, async () => {
        let active = 0, peak = 0;
        const called = [];
        const keys = Array.from({ length: 41 }, (_, i) => `blob-${i}`);
        const client = { getContainerClient() { return {
            async deleteBlob(key) {
                called.push(key);
                peak = Math.max(peak, ++active);
                await setImmediate();
                if (key === keys[0]) await setImmediate();
                active--;
                if (key === keys[0] || key === keys[20]) throw new Error('locked');
            }
        }; } };
        // Omitting the setting exercises the default of 16.
        const config = { provider: 'azure', azure: concurrency === 16 ? {} : { deleteConcurrency: concurrency } };
        const storage = createStorage(config, client);
        const progress = [];
        const result = await storage.delete('backups', keys, update => progress.push(update));
        assert.equal(peak, concurrency);
        assert.equal(active, 0);
        assert.deepEqual(called, keys);
        assert.deepEqual(result.successful, keys.filter((_, i) => i !== 0 && i !== 20));
        assert.deepEqual(result.failed, [keys[0], keys[20]].map(key => ({ key, error: 'locked', bucket: 'backups' })));
        assert.equal(progress[0].completed, 0);
        const last = progress.at(-1);
        assert.equal(last.completed, keys.length);
        assert.equal(last.total, keys.length);
        assert.equal(last.successful, 39);
        assert.equal(last.failed, 2);
        assert.ok(last.elapsedMs >= 0);
    });
}

test('Azure deletion rejects invalid concurrency before creating a client', () => {
    for (const deleteConcurrency of [0, -1, 1.5, 129, '16', NaN, Infinity]) {
        assert.throws(() => createStorage({ provider: 'azure', azure: { deleteConcurrency } }), /azure.deleteConcurrency/);
    }
});

test('Azure deletion handles empty input and all failures', async () => {
    let calls = 0;
    const storage = createStorage({ provider: 'azure' }, { getContainerClient() { return {
        async deleteBlob() { calls++; throw new Error('denied'); }
    }; } });
    assert.deepEqual(await storage.delete('backups', []), { successful: [], failed: [] });
    assert.equal(calls, 0);
    const result = await storage.delete('backups', ['a', 'b']);
    assert.equal(calls, 2);
    assert.deepEqual(result.successful, []);
    assert.deepEqual(result.failed.map(item => item.key), ['a', 'b']);
});
