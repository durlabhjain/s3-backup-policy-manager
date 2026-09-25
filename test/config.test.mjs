import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync, mkdirSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadConfig, parseCliArgs, resolveConfigs, resolveConfigFiles } from '../index.mjs';

test('CLI parses values containing equals and rejects malformed arguments', () => {
    assert.deepEqual(parseCliArgs(['config=/tmp/my config=x.json', 'dryRun=true']), {
        config: '/tmp/my config=x.json', dryRun: 'true'
    });
    assert.throws(() => parseCliArgs(['dryRun']), /key=value/);
});

test('config lists, repeated arguments and globs preserve order and deduplicate files', t => {
    const directory = mkdtempSync(join(tmpdir(), 'backup-config-list-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const first = join(directory, 'a.json'), second = join(directory, 'b.json');
    writeFileSync(first, JSON.stringify({ buckets: ['a'], dryRun: false }));
    writeFileSync(second, JSON.stringify([{ buckets: ['b'] }, { buckets: ['c'] }]));
    mkdirSync(join(directory, 'directory.json'));
    const alias = join(directory, 'alias.json');
    symlinkSync(first, alias);
    const args = parseCliArgs([`config=${second},${first}`, `config=${directory}/*.json`, `config=${alias}`, 'dryRun=true']);
    assert.equal(args.config.length, 3);
    assert.deepEqual(resolveConfigs(args).map(c => c.buckets), [['b'], ['c'], ['a']]);
    assert.ok(resolveConfigs(args).every(c => c.dryRun));
    assert.deepEqual(resolveConfigs({ config: `${directory}/[ab].json` }).map(c => c.buckets), [['a'], ['b'], ['c']]);
    assert.deepEqual(resolveConfigs({ config: `${directory}/{a,b}.json` }).map(c => c.buckets), [['a'], ['b'], ['c']]);
    for (const config of ['', [], `${first},`, `${first},${directory}/missing*.json`, join(directory, 'directory.json')]) {
        assert.throws(() => resolveConfigs({ config }), /config|matched/);
    }
    const literal = join(directory, 'config with spaces,equals=.json');
    writeFileSync(literal, JSON.stringify({ buckets: ['literal'] }));
    assert.deepEqual(resolveConfigs({ config: literal })[0].buckets, ['literal']);
    assert.equal(resolveConfigFiles(`${directory}/**/a.json`).length, 1);
    writeFileSync(second, '[]');
    assert.throws(() => resolveConfigs({ config: [first, second] }), /b\.json.*nonempty/);
    writeFileSync(second, '{broken');
    assert.throws(() => resolveConfigs({ config: [first, second] }), /Unable to load config.*b\.json/);
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
