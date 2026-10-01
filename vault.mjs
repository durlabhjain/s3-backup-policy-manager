import { readFileSync } from 'node:fs';
import { mergeConfig } from './application-config.mjs';
// Secret references are resolved in memory before any storage client is created.
export async function resolveVault(config, settings = {}, { fetchImpl = fetch, env = process.env } = {}) {
    const cache = new Map();
    let token;
    let address;
    function bootstrap() {
        const tokenFile = env.VAULT_TOKEN_FILE || settings.tokenFile;
        try { token = env.VAULT_TOKEN || (tokenFile ? readFileSync(tokenFile, 'utf8').trim() : settings.token); }
        catch { throw new Error('Vault token file could not be read'); }
        try { address = new URL(env.VAULT_ADDR || settings.address); } catch { throw new Error('Vault address is required'); }
        if ((address.protocol !== 'https:' && !(address.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(address.hostname))) ||
            address.username || address.password || address.search || address.hash) throw new Error('Vault address must be HTTPS (loopback HTTP allowed)');
        if (!token || token.startsWith('vault:')) throw new Error('Vault token is required; supply VAULT_TOKEN');
        if (![1, 2].includes(settings.kvVersion ?? 2)) throw new Error('Vault kvVersion must be 1 or 2');
    }
    const encode = path => path.split('/').map(part => {
        if (!part || part === '.' || part === '..') throw new Error('Invalid Vault path');
        return encodeURIComponent(part);
    }).join('/');
    async function expand(value, active = new Set(), depth = 0) {
        if (depth > 64) throw new Error('Vault reference nesting exceeds limit');
        if (Array.isArray(value)) return Promise.all(value.map(item => expand(item, active, depth + 1)));
        if (value && typeof value === 'object') {
            const result = Object.create(null);
            for (const [key, item] of Object.entries(value)) {
                if (['__proto__', 'constructor', 'prototype'].includes(key)) throw new Error('Unsafe configuration key');
                result[key] = await expand(item, active, depth + 1);
            }
            return result;
        }
        if (typeof value !== 'string' || !value.startsWith('vault:')) return value;
        if (!address) bootstrap();
        let reference = value.slice(6);
        if (/\$\{environment\}/i.test(reference)) {
            if (!settings.environment) throw new Error('Vault environment is required');
            reference = reference.replace(/\$\{environment\}/gi, settings.environment);
        }
        if (active.has(reference)) throw new Error('Cyclic Vault reference');
        const colon = reference.indexOf(':');
        const path = (colon < 0 ? reference : reference.slice(0, colon)).replace(/^\/+|\/+$/g, '');
        const key = colon < 0 ? undefined : reference.slice(colon + 1);
        if (key === '') throw new Error('Invalid Vault secret key');
        const engine = encode(settings.engine || 'kv');
        const encodedPath = encode(path);
        if (!cache.has(path)) {
            cache.set(path, (async () => {
                let response;
                try {
                    response = await fetchImpl(`${address.href.replace(/\/$/, '')}/v1/${engine}/${(settings.kvVersion ?? 2) === 2 ? 'data/' : ''}${encodedPath}`, {
                        headers: { 'X-Vault-Token': token }, redirect: 'error', signal: AbortSignal.timeout(5000)
                    });
                    if (!response.ok) throw new Error();
                    const body = await response.json();
                    const secret = (settings.kvVersion ?? 2) === 2 ? body.data?.data : body.data;
                    if (!secret || typeof secret !== 'object' || Array.isArray(secret)) throw new Error();
                    return secret;
                } catch { throw new Error('Vault lookup failed (response and credentials redacted)'); }
            })());
        }
        const secret = await cache.get(path);
        const selected = key === undefined ? secret : Object.hasOwn(secret, key) ? secret[key] : undefined;
        if (selected === undefined || selected === null) throw new Error('Vault secret key missing or null');
        let decoded = selected;
        if (typeof selected === 'string' && /^[\s]*[\[{]/.test(selected)) {
            try { decoded = JSON.parse(selected); } catch { /* Keep ordinary string secrets unchanged. */ }
        }
        return expand(decoded, new Set([...active, reference]), depth + 1);
    }
    return expand(config);
}

// Allows the entire application/job body to live in Vault, with explicit local overrides.
export function expandSettings(config) {
    if (!config || typeof config !== 'object' || Array.isArray(config)) throw new Error('Resolved config must be an object');
    const { settings, ...overrides } = config;
    if (settings === undefined) return overrides;
    if (!settings || typeof settings !== 'object' || Array.isArray(settings)) throw new Error('Resolved settings must be an object');
    return mergeConfig(settings, overrides);
}
