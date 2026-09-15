import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createAzureClient } from '../storage.mjs';
const query = 'sv=2025-01-05&sp=rld&sig=fake%2Bsignature%3D';

test('service SAS URL is preserved when constructing container/blob clients', () => {
    const client = createAzureClient({ azure: { sasUrl: `https://example.blob.core.windows.net/?${query}`, connectionString: 'ignored' } });
    const url = new URL(client.getContainerClient('backups').getBlobClient('server/db/file.bak').url);
    assert.equal(url.pathname, '/backups/server/db/file.bak');
    assert.equal(url.searchParams.get('sig'), 'fake+signature=');
});

test('container SAS URL avoids duplicating the container path and rejects other containers', () => {
    const client = createAzureClient({ azure: { sasUrl: `https://example.blob.core.windows.net/backups?sr=c&${query}` } });
    const url = new URL(client.getContainerClient('backups').getBlobClient('file.bak').url);
    assert.equal(url.pathname, '/backups/file.bak');
    assert.equal(url.searchParams.get('sig'), 'fake+signature=');
    assert.throws(() => client.getContainerClient('other'), /does not match/);
});

test('invalid and individual-blob SAS URLs fail without leaking the supplied URL', () => {
    for (const sasUrl of ['invalid-secret', 'https://example.blob.core.windows.net/', `https://example.blob.core.windows.net/backups/file?${query}`, `https://example.blob.core.windows.net/backups?sr=b&${query}`]) {
        assert.throws(() => createAzureClient({ azure: { sasUrl } }), error => {
            assert.ok(!error.message.includes(sasUrl));
            assert.ok(!error.message.includes('fake'));
            return true;
        });
    }
});
