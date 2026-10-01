import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { resolveVault, expandSettings } from '../vault.mjs';
import { applicationDefaults, mergeConfig } from '../application-config.mjs';

test('Vault supports KV1/KV2, whole sections, nested references, caching and fresh values per call', async () => {
    let value = 'first', reads = 0;
    const fetchImpl = async (url, options) => {
        reads++; assert.equal(options.headers['X-Vault-Token'], 'test-token'); assert.equal(options.redirect, 'error');
        assert.match(url, /\/v1\/kv\/data\/jobs\/prod$/);
        return { ok: true, async json() { return { data: { data: { sasUrl: value, nested: 'vault:jobs/prod:sasUrl' } } }; } };
    };
    const settings = { address: 'http://127.0.0.1:8200', environment: 'prod' }, env = { VAULT_TOKEN: 'test-token' };
    const source = { azure: 'vault:jobs/${environment}', token: 'vault:jobs/prod:sasUrl' };
    let config = await resolveVault(source, settings, { fetchImpl, env });
    assert.equal(config.azure.sasUrl, 'first'); assert.equal(config.azure.nested, 'first'); assert.equal(reads, 1);
    value = 'second'; config = await resolveVault(source, settings, { fetchImpl, env });
    assert.equal(config.token, 'second'); assert.equal(reads, 2);
    const v1 = await resolveVault('vault:job:field', { ...settings, kvVersion: 1 }, { env,
        fetchImpl: async url => { assert.match(url, /\/kv\/job$/); return { ok: true, async json() { return { data: { field: 42 } }; } }; } });
    assert.equal(v1, 42);
});
test('Vault token files are reread and failures never expose token or response contents', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'vault-token-')); t.after(() => rmSync(directory, { recursive: true, force: true }));
    const tokenFile = join(directory, 'token'); let token = 'first-secret'; writeFileSync(tokenFile, token);
    const settings = { address: 'http://localhost:8200', tokenFile }, source = 'vault:job:key';
    const fetchImpl = async (_, options) => { assert.equal(options.headers['X-Vault-Token'], token); return { ok: false }; };
    await assert.rejects(resolveVault(source, settings, { fetchImpl, env: {} }), error => !error.message.includes(token));
    token = 'rotated-secret'; writeFileSync(tokenFile, token);
    await assert.rejects(resolveVault(source, settings, { fetchImpl, env: {} }), /Vault lookup failed/);
    await assert.rejects(resolveVault(source, { address: 'https://vault.example' }, { env: {} }), /token is required/);
    await assert.rejects(resolveVault(source, { address: 'http://remote.example', token }, { env: {} }), /HTTPS/);
    await assert.rejects(resolveVault('vault:../job:key', { ...settings, token }, { env: {} }), /Invalid Vault path/);
    const cyclical = async () => ({ ok: true, async json() { return { data: { data: { key: source } } }; } });
    await assert.rejects(resolveVault(source, settings, { fetchImpl: cyclical, env: {} }), /Cyclic/);
});
test('application defaults cannot authorize deletion and nested settings merge without concatenating arrays', () => {
    assert.throws(() => applicationDefaults({ jobDefaults: { dryRun: false } }), /per job/);
    const result = mergeConfig({ azure: { endpoint: 'endpoint', accountName: 'name' }, containers: ['old'] }, { azure: { accountKey: 'secret' }, containers: ['new'] });
    assert.equal(result.azure.accountName, 'name'); assert.deepEqual(result.containers, ['new']);
    assert.equal(expandSettings({ settings: { jobId: 'job', dryRun: false }, dryRun: true }).dryRun, true);
});
