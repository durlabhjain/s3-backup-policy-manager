import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadConfig, parseCliArgs, resolveConfigs } from '../index.mjs';

test('CLI parses values containing equals and rejects malformed arguments', () => {
    assert.deepEqual(parseCliArgs(['config=/tmp/my config=x.json', 'dryRun=true']), {
        config: '/tmp/my config=x.json', dryRun: 'true'
    });
    assert.throws(() => parseCliArgs(['dryRun']), /key=value/);
});

test('selected config replaces implicit files and CLI dryRun overrides every entry', t => {
    const directory = mkdtempSync(join(tmpdir(), 'backup-config-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const config = join(directory, 'selected.json');
    writeFileSync(config, JSON.stringify([{ dryRun: false, buckets: ['first'] }, { dryRun: true, buckets: ['second'] }]));
    assert.equal(loadConfig(config).length, 2);
    assert.deepEqual(resolveConfigs({ config }).map(c => c.dryRun), [false, true]);
    assert.deepEqual(resolveConfigs({ config, dryRun: 'true' }).map(c => c.dryRun), [true, true]);
    assert.deepEqual(resolveConfigs({ config, dryRun: 'false' }).map(c => c.dryRun), [false, false]);
    for (const value of ['TRUE', 'yes', '', '0']) {
        assert.throws(() => resolveConfigs({ config, dryRun: value }), /dryRun must/);
    }
    writeFileSync(config, JSON.stringify({ buckets: ['isolated'] }));
    const [single] = resolveConfigs({ config });
    assert.deepEqual(single.buckets, ['isolated']);
    assert.equal(single.dryRun, true);
    assert.equal(single.deleteNonRetained, false);
    assert.throws(() => loadConfig(join(directory, 'missing.json')));
    assert.throws(() => loadConfig(''), /file path/);
    writeFileSync(config, '{invalid');
    assert.throws(() => resolveConfigs({ config }));
    writeFileSync(config, 'null');
    assert.throws(() => resolveConfigs({ config }), /Configuration must/);
});
