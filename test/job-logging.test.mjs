import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, writeFileSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JobLogging, secretRedactor } from '../job-logging.mjs';

const quiet = { info() {}, error() {}, warn() {}, debug() {}, log() {}, table() {} };
test('UTC daily rotation retains only application files and redacts secrets', async t => {
    const directory = mkdtempSync(join(tmpdir(), 'job-logs-')); t.after(() => rmSync(directory, { recursive: true, force: true }));
    const old = join(directory, 's3-backup-policy-manager-2026-01-01.jsonl'); writeFileSync(old, 'old');
    writeFileSync(join(directory, 'unrelated-2026-01-01.jsonl'), 'preserve');
    let now = new Date('2026-01-03T23:59:59Z');
    const logging = new JobLogging({ logging: { level: 'error', file: { enabled: true, directory, retentionDays: 2 } } }, {
        consoleOutput: quiet, clock: () => now, redact: secretRedactor({ azure: { sasUrl: 'supersecret' } })
    });
    const logger = logging.logger({ job_id: 'job' });
    logger.event('run_started', {}); logger.error('Credential=%s', 'supersecret');
    assert.equal(existsSync(old), false); assert.equal(existsSync(join(directory, 'unrelated-2026-01-01.jsonl')), true);
    const text = readFileSync(join(directory, 's3-backup-policy-manager-2026-01-03.jsonl'), 'utf8');
    assert.match(text, /run_started/); assert.match(text, /REDACTED/); assert.ok(!text.includes('supersecret'));
    now = new Date('2026-01-04T00:00:00Z'); logger.event('run_completed', { outcome: 'success' });
    assert.ok(existsSync(join(directory, 's3-backup-policy-manager-2026-01-04.jsonl')));
    await logging.close();
});
test('standard OTLP exporter sends real lifecycle payloads and auth; receiver rejection does not throw', async t => {
    const received = [];
    let reject = false;
    const server = createServer(async (req, res) => {
        let body = ''; for await (const chunk of req) body += chunk;
        received.push({ authorization: req.headers.authorization, stream: req.headers['stream-name'], body: JSON.parse(body) });
        res.writeHead(reject ? 401 : 200, { 'content-type': 'application/json' }); res.end('{}');
    });
    server.listen(0, '127.0.0.1'); await once(server, 'listening'); t.after(() => { server.closeAllConnections(); server.close(); });
    const app = { telemetry: { enabled: true, endpoint: `http://127.0.0.1:${server.address().port}/v1/logs`, authorization: 'Basic fixture', stream: 'jobs', timeoutSeconds: 1 } };
    let logging = new JobLogging(app, { consoleOutput: quiet });
    const logger = logging.logger({ run_id: 'run', job_id: 'job', mode: 'dry_run' });
    logger.event('run_started', {}); logger.event('run_completed', { outcome: 'partial_failure', containers_deleted: 1, containers_failed: 1 });
    await logging.close();
    assert.equal(received[0].authorization, 'Basic fixture'); assert.equal(received[0].stream, 'jobs');
    const records = received.flatMap(request => request.body.resourceLogs.flatMap(resource => resource.scopeLogs.flatMap(scope => scope.logRecords)));
    assert.equal(records.length, 2);
    const attrs = Object.fromEntries(records[1].attributes.map(attribute => [attribute.key, Object.values(attribute.value)[0]]));
    assert.equal(attrs.event_name, 'run_completed'); assert.equal(attrs.outcome, 'partial_failure'); assert.equal(attrs.run_id, 'run');
    assert.equal(Number(attrs.containers_deleted), 1);
    reject = true; logging = new JobLogging(app, { consoleOutput: quiet }); logging.logger({}).event('run_completed', { outcome: 'success' });
    await assert.doesNotReject(logging.close());
});

test('a stalled receiver has a bounded request timeout and does not fail completed work', async t => {
    const server = createServer(() => {});
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    t.after(() => { server.closeAllConnections(); server.close(); });
    const logging = new JobLogging({ telemetry: { enabled: true, endpoint: `http://127.0.0.1:${server.address().port}/v1/logs`, authorization: 'Basic fixture', timeoutSeconds: 1 } }, { consoleOutput: quiet });
    logging.logger({}).event('run_completed', { outcome: 'success' });
    const started = performance.now(); await assert.doesNotReject(logging.close());
    assert.ok(performance.now() - started < 4000);
});
