import { test } from 'node:test';
import assert from 'node:assert/strict';
import { applyRetentionPolicy } from '../index.mjs';
const zero = { fullBackups: 0, yearlyBackups: 0, monthlyBackups: 0, weeklyBackups: 0, differentialBackups: 0, logBackups: 0 };
const now = new Date(2026, 8, 14, 12);
const object = (type, date, time = '120000', part = '') => ({ Key: `S/db/${type}/S_db_${type.toUpperCase()}_${date}_${time}${part}.${type === 'log' ? 'trn' : 'bak'}` });
const run = (files, policy) => applyRetentionPolicy(files, { ...zero, ...policy }, console, now);

test('DIFF and LOG keep every file in seven days, including cutoff and multipart files', () => {
    for (const type of ['diff', 'log']) {
        const files = [object(type, '20260907', '115959'), object(type, '20260907'), ...Array.from({length: 20}, (_, i) => object(type, '20260914', `10${String(i).padStart(2, '0')}00`)), object(type, '20260914', '110000', '_1'), object(type, '20260914', '110000', '_2')];
        const result = run(files, { differentialBackups: 7, logBackups: null });
        assert.deepEqual(result.backupsToDelete.map(b => b.key), [files[0].Key]);
        assert.equal(result.retainedBackups.length, files.length - 1);
    }
});

test('old DIFF and LOG files expire even when fewer than the configured number exist', () => {
    const result = run([object('diff', '20260801'), object('log', '20260801')], { differentialBackups: 7, logBackups: 7 });
    assert.equal(result.retainedBackups.length, 0);
});

test('monthly and yearly tiers select calendar representatives only within recent periods', () => {
    const files = [object('full', '20240801'), object('full', '20250901'), object('full', '20251001'), object('full', '20251002'), object('full', '20260101'), object('full', '20260901')];
    assert.deepEqual(run(files, { monthlyBackups: 12 }).retainedBackups, [files[2].Key, files[4].Key, files[5].Key]);
    assert.deepEqual(run(files, { yearlyBackups: 1 }).retainedBackups, [files[4].Key]);
    assert.deepEqual(run(files, { yearlyBackups: 2 }).retainedBackups, [files[1].Key, files[4].Key]);
});

test('weekly tier selects at most one full per rolling week in the last 28 days', () => {
    const files = ['20260817', '20260818', '20260825', '20260901', '20260908', '20260914', '20260816'].map(date => object('full', date));
    assert.deepEqual(run(files, { weeklyBackups: 4 }).retainedBackups, [files[5].Key, files[3].Key, files[2].Key, files[1].Key]);
});

test('full minimum preserves old backups outside all calendar windows', () => {
    const files = [object('full', '20240101'), object('full', '20240201')];
    assert.deepEqual(run(files, { fullBackups: 1, monthlyBackups: 12, yearlyBackups: 1, weeklyBackups: 4 }).retainedBackups, [files[1].Key]);
});
