import { test } from 'node:test';
import assert from 'node:assert/strict';
import BackupObject from '../backup-object.mjs';
import { applyRetentionPolicy, processBackups } from '../index.mjs';
import { createStorage } from '../storage.mjs';

const key = (type, stamp = '20260807_144018', part = '', server = 'WHISKEY') =>
    `${server}/dedicated/${type}/${server}_dedicated_${type.toUpperCase()}_${stamp}${part}.${type === 'log' ? 'trn' : 'bak'}`;
const none = { fullBackups: 0, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0, differentialBackups: 0 };
const objects = keys => keys.map(Key => ({ Key, bucketName: 'test' }));

test('legacy format, folder override and strict date parsing', () => {
    assert.equal(new BackupObject('DB_20260807_144018-Full-01.BAK').part, '01');
    assert.equal(new BackupObject('SQL-Diff/DB_20260807_144018-Full.bak').type, 'diff');
    assert.throws(() => new BackupObject('DB_20260230_144018-Full.bak'));
    assert.throws(() => new BackupObject(key('full').replace('144018', '256018')));
});

test('new layout supports all supplied examples and optional prefixes', () => {
    for (const [type, stamp, part] of [['log', '20260807_152519', ''], ['full', '20260807_144018', '_1'], ['diff', '20260807_152200', '']]) {
        const backup = new BackupObject(`backups/${key(type, stamp, part)}`);
        assert.equal(backup.type, type);
        assert.equal(backup.objectName, 'backups/WHISKEY/dedicated');
        assert.equal(backup.datetime.format('HH:mm:ss'), stamp.slice(9).replace(/(..)(..)(..)/, '$1:$2:$3'));
    }
    assert.throws(() => new BackupObject(key('full').replace('/full/', '/diff/')));
    assert.throws(() => new BackupObject(key('log').replace('.trn', '.bak')));
});

test('multipart sets stay together and servers get separate limits', () => {
    const keys = [key('full', '20260806_144018'), key('full', undefined, '_1'), key('full', undefined, '_2'), key('full', undefined, '', 'OTHER')];
    const result = applyRetentionPolicy(objects(keys), { ...none, fullBackups: 1 });
    assert.deepEqual(result.backupsToDelete.map(b => b.key), [keys[0]]);
    assert.equal(result.retainedBackups.length, 3);
});

test('logs inherit differential limits with separate counters and explicit overrides', () => {
    const keys = [key('full'), key('diff'), key('log'), key('log', '20260807_152519')];
    for (const logConfig of [{}, { logBackups: null }, { logBackups: undefined }]) {
        const result = applyRetentionPolicy(objects(keys), { ...none, differentialBackups: 1, ...logConfig }, console, new Date(2026, 7, 8, 15));
        assert.deepEqual(new Set(result.retainedBackups), new Set([keys[3]]));
    }
    const explicit = applyRetentionPolicy(objects(keys), { ...none, logBackups: 2, differentialBackups: 2 }, console, new Date(2026, 7, 8, 15));
    assert.deepEqual(new Set(explicit.retainedBackups), new Set(keys.slice(1)));
    const zero = applyRetentionPolicy(objects(keys), { ...none, logBackups: 0, differentialBackups: 2 }, console, new Date(2026, 7, 8, 15));
    assert.deepEqual(zero.retainedBackups, [keys[1]]);
    assert.deepEqual(applyRetentionPolicy(objects(keys), none).retainedBackups, []);
    assert.throws(() => applyRetentionPolicy([], { logBackups: -1 }));
});

test('logs default to seven days when both limits are omitted', () => {
    const keys = Array.from({ length: 8 }, (_, i) => key('log', `2026080${i + 1}_144018`));
    const result = applyRetentionPolicy(objects(keys), {}, console, new Date(2026, 7, 9, 14, 40, 18));
    assert.equal(result.retainedBackups.length, 7);
    assert.deepEqual(result.backupsToDelete.map(backup => backup.key), [keys[0]]);
});

test('weekly keys distinguish different weeks', () => {
    assert.notEqual(new BackupObject(key('full')).getWeekKey(), new BackupObject(key('full', '20260817_144018')).getWeekKey());
});

test('Azure adapter lists all iterator results, captures delete failures, signs read URLs', async () => {
    let options;
    const client = { getContainerClient(name) {
        assert.equal(name, 'backups');
        return {
            listBlobsFlat({ prefix }) { assert.equal(prefix, 'WHISKEY/'); return { async *byPage({ maxPageSize }) { assert.equal(maxPageSize, 5000); for (const name of ['one', 'two']) yield { segment: { blobItems: [{ name, properties: { contentLength: 12 } }] } }; } }; },
            async deleteBlob(name) { if (name === 'two') throw new Error('locked'); },
            getBlobClient(name) { assert.equal(name, 'one'); return { async generateSasUrl(value) { options = value; return 'signed'; } }; }
        };
    } };
    const storage = createStorage({ provider: 'azure' }, client);
    const progress = [];
    assert.deepEqual((await storage.list('backups', 'WHISKEY/', update => progress.push(update))).map(o => o.Key), ['one', 'two']);
    assert.deepEqual(progress.map(({pages, count}) => ({pages, count})), [{pages:1, count:1}, {pages:2, count:2}]);
    const deleted = await storage.delete('backups', ['one', 'two']);
    assert.deepEqual(deleted.successful, ['one']);
    assert.equal(deleted.failed[0].error, 'locked');
    assert.equal(await storage.signedUrl('backups', 'one', '3600'), 'signed');
    assert.equal(options.permissions.toString(), 'r');
    await assert.rejects(storage.signedUrl('backups', 'one', 'bad'));
});

test('S3 adapter paginates and batches deletions at 1000', async () => {
    let pages = 0;
    const sizes = [];
    const storage = createStorage({}, { async send(command) {
        if (command.input.Delete) {
            sizes.push(command.input.Delete.Objects.length);
            return { Deleted: command.input.Delete.Objects };
        }
        pages++;
        return { Contents: [{ Key: String(pages) }], NextContinuationToken: pages === 1 ? 'next' : undefined };
    }, destroy() {} });
    assert.equal((await storage.list('test')).length, 2);
    assert.equal((await storage.delete('test', Array.from({ length: 1001 }, (_, i) => String(i)))).successful.length, 1001);
    assert.deepEqual(sizes, [1000, 1]);
});

test('both providers require both deletion flags, skip invalid objects and close clients', async () => {
    for (const provider of ['aws', 'azure']) {
        for (const [dryRun, deleteNonRetained] of [[true, true], [false, false], [false, true]]) {
            let deletes = 0, closed = false;
            const result = await processBackups({ provider, buckets: ['test'], containers: ['test'], retention: none, dryRun, deleteNonRetained }, () => ({
                async list() { return objects([key('full'), 'unrelated.txt']); },
                async delete(bucket, keys) { deletes++; assert.deepEqual(keys, [key('full')]); return { successful: keys, failed: [] }; },
                async close() { closed = true; }
            }), { scheduled: true });
            assert.equal(deletes, !dryRun && deleteNonRetained ? 1 : 0);
            assert.equal(result.totalSummary.deleteCount, 1);
            assert.ok(closed);
        }
    }
});

test('Azure SDK generates a SAS locally with account-key credentials', async () => {
    const storage = createStorage({ provider: 'azure', azure: {
        connectionString: `DefaultEndpointsProtocol=https;AccountName=example;AccountKey=${Buffer.alloc(32, 1).toString('base64')};EndpointSuffix=core.windows.net`
    } });
    const url = new URL(await storage.signedUrl('backups', key('full'), 3600));
    assert.equal(url.hostname, 'example.blob.core.windows.net');
    assert.equal(url.searchParams.get('sp'), 'r');
    assert.ok(url.searchParams.get('sig'));
});
