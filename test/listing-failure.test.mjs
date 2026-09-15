import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStorage } from '../storage.mjs';
import { processBackups } from '../index.mjs';

test('Azure page failure never returns a partial list for pruning', async () => {
    let closed = false;
    const messages = [];
    const storage = createStorage({ provider: 'azure' }, { getContainerClient() { return {
        listBlobsFlat() { return { async *byPage() {
            yield { segment: { blobItems: [{ name: 'db_20260101_120000-Full.bak', properties: {} }] } };
            throw new Error('Listing page failed');
        } }; }
    }; } });
    storage.delete = () => assert.fail('Partial listing must never be deleted');
    storage.close = async () => { closed = true; };
    const result = await processBackups({ provider: 'azure', containers: ['failed-list-test'], dryRun: false, deleteNonRetained: true }, () => storage, {
        scheduled: true, output: { log(message) { messages.push(message); }, error() {} }
    });
    assert.equal(result.byBucket['failed-list-test'].error, 'Listing page failed');
    assert.ok(messages.some(message => message.includes('Listing progress: 1 files, 1 pages')));
    assert.ok(!messages.some(message => message.startsWith('Retention analysis complete')));
    assert.ok(closed);
});
