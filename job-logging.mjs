import { mkdirSync, readdirSync, lstatSync, unlinkSync, appendFileSync } from 'node:fs';
import { join } from 'node:path';
import { format } from 'node:util';
import { LoggerProvider, BatchLogRecordProcessor } from '@opentelemetry/sdk-logs';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';

const levels = { debug: 5, info: 9, log: 9, warn: 13, error: 17 };
// Select diagnostic fields instead of serializing SDK requests and credentials.
export function errorDetails(error) {
    const details = { error_type: error?.name || 'Error', error_message: error?.message || String(error) };
    for (const [field, value] of Object.entries({ error_stack: error?.stack, error_code: error?.code,
        status_code: error?.statusCode ?? error?.$metadata?.httpStatusCode,
        request_id: error?.requestId ?? error?.details?.requestId ?? error?.$metadata?.requestId })) {
        if (typeof value === 'string' || typeof value === 'number') details[field] = value;
    }
    return details;
}

export function validateLogging(app) {
    const file = app.logging?.file || {}, telemetry = app.telemetry || {};
    if (app.logging?.level && !['debug', 'info', 'warn', 'error'].includes(app.logging.level)) throw new Error('Invalid logging level');
    if (file.enabled !== undefined && typeof file.enabled !== 'boolean') throw new Error('File logging enabled must be boolean');
    if (file.enabled && (typeof file.directory !== 'string' || !file.directory || !Number.isInteger(file.retentionDays) || file.retentionDays < 1 || file.retentionDays > 36500))
        throw new Error('File logging requires directory and retentionDays (1..36500)');
    if (telemetry.enabled !== undefined && typeof telemetry.enabled !== 'boolean') throw new Error('Telemetry enabled must be boolean');
    if (!telemetry.enabled) return;
    let url; try { url = new URL(telemetry.endpoint); } catch { throw new Error('Telemetry endpoint is required'); }
    if ((url.protocol !== 'https:' && !(url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))) || url.username || url.password || url.search || url.hash)
        throw new Error('Telemetry endpoint must be HTTPS (loopback HTTP allowed)');
    if (typeof telemetry.authorization !== 'string' || !telemetry.authorization || /[\r\n]/.test(telemetry.authorization)) throw new Error('Telemetry authorization header is required');
    if (!/^[A-Za-z0-9_]+$/.test(telemetry.stream || 'backup_jobs')) throw new Error('Invalid telemetry stream');
    if (!Number.isInteger(telemetry.timeoutSeconds ?? 5) || (telemetry.timeoutSeconds ?? 5) < 1 || (telemetry.timeoutSeconds ?? 5) > 30) throw new Error('Telemetry timeoutSeconds must be 1..30');
}

export function secretRedactor(configs) {
    const secrets = new Set();
    function collect(value, key = '') {
        if (typeof value === 'string' && /key|token|authorization|connectionString|sasUrl|password|secret/i.test(key) && value) secrets.add(value);
        else if (value && typeof value === 'object') for (const [name, item] of Object.entries(value)) collect(item, name);
    }
    collect(configs);
    for (const [key, value] of Object.entries(process.env)) if (/^(VAULT_TOKEN|AZURE_STORAGE_(KEY|CONNECTION_STRING|SAS_URL)|AWS_(SECRET_ACCESS_KEY|SESSION_TOKEN|ACCESS_KEY_ID))$/.test(key) && value) secrets.add(value);
    return text => {
        for (const secret of [...secrets].sort((a, b) => b.length - a.length)) text = text.replaceAll(secret, '[REDACTED]');
        return text.replace(/([?&](?:sig|token|X-Amz-Signature)=)[^\s&'"<>]+/gi, '$1[REDACTED]');
    };
}

export class JobLogging {
    constructor(app, { clock = () => new Date(), redact = value => value, consoleOutput = console } = {}) {
        validateLogging(app);
        this.app = app; this.clock = clock;
        const redactApp = secretRedactor(app);
        this.redact = value => redactApp(redact(value)); this.console = consoleOutput;
        this.minimum = levels[app.logging?.level || 'info'];
        this.file = app.logging?.file || {}; this.day = null;
        if (this.file.enabled) mkdirSync(this.file.directory, { recursive: true });
        const telemetry = app.telemetry || {};
        if (telemetry.enabled) {
            const exporter = new OTLPLogExporter({
                url: telemetry.endpoint, headers: { Authorization: telemetry.authorization, 'stream-name': telemetry.stream || 'backup_jobs' },
                timeoutMillis: (telemetry.timeoutSeconds ?? 5) * 1000
            });
            const exportRecords = exporter.export.bind(exporter);
            exporter.export = (records, callback) => exportRecords(records, result => {
                if (result.code !== 0) {
                    this.reportFailure('Telemetry export failed', result.error || new Error(`Exporter result code: ${result.code}`));
                }
                callback(result);
            });
            this.provider = new LoggerProvider({ resource: resourceFromAttributes({
                'service.name': telemetry.serviceName || 's3-backup-policy-manager',
                'deployment.environment.name': app.environment || 'development'
            }), processors: [new BatchLogRecordProcessor({ exporter, maxQueueSize: 2048, maxExportBatchSize: 512, exportTimeoutMillis: (telemetry.timeoutSeconds ?? 5) * 1000 })] });
            this.otel = this.provider.getLogger('policy-jobs');
        }
    }
    writeFile(record) {
        if (!this.file.enabled) return;
        try {
            const day = record.timestamp.slice(0, 10);
            if (this.day !== day) {
                const cutoff = new Date(`${day}T00:00:00Z`); cutoff.setUTCDate(cutoff.getUTCDate() - this.file.retentionDays + 1);
                for (const name of readdirSync(this.file.directory)) {
                    const match = /^s3-backup-policy-manager-(\d{4}-\d{2}-\d{2})\.jsonl$/.exec(name);
                    const path = join(this.file.directory, name);
                    const date = match ? new Date(`${match[1]}T00:00:00Z`) : null;
                    if (date && Number.isFinite(date.valueOf()) && date.toISOString().slice(0, 10) === match[1] &&
                        match[1] < cutoff.toISOString().slice(0, 10) && lstatSync(path).isFile()) unlinkSync(path);
                }
                this.day = day;
            }
            const path = join(this.file.directory, `s3-backup-policy-manager-${day}.jsonl`);
            appendFileSync(path, JSON.stringify(record) + '\n');
        } catch (error) { this.reportFailure('Daily file logging failed', error, false); }
    }
    reportFailure(message, error, persist = true) {
        const details = JSON.parse(this.redact(JSON.stringify(errorDetails(error))));
        this.console.error(message, details);
        // Never recurse into a failed file sink or send exporter errors back to OTLP.
        if (persist) this.writeFile({ timestamp: this.clock().toISOString(), level: 'error', message, ...details });
    }
    logger(identity) {
        const sanitize = value => {
            if (typeof value === 'string') return this.redact(value);
            if (Array.isArray(value)) return value.map(sanitize);
            if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, sanitize(item)]));
            return value;
        };
        const write = (level, message, attributes = {}) => {
            if (levels[level] < this.minimum && !attributes.event_name) return;
            const safeAttributes = sanitize({ ...identity, ...attributes });
            const record = { timestamp: this.clock().toISOString(), level, message: this.redact(message), ...safeAttributes };
            this.console[level === 'log' ? 'log' : level]?.(record.message, safeAttributes);
            this.writeFile(record);
            this.otel?.emit({ severityNumber: levels[level], severityText: level.toUpperCase(), body: record.message, attributes: safeAttributes });
        };
        const result = {};
        for (const level of Object.keys(levels)) result[level] = (...args) => write(level, format(...args));
        result.table = rows => { const safe = sanitize(rows); this.console.table?.(safe); write('info', 'Review table', { rows: JSON.stringify(safe) }); };
        result.event = (name, attributes) => write(name === 'run_completed' && ['failed', 'partial_failure'].includes(attributes.outcome) ? 'error' : 'info', name, { event_name: name, ...attributes });
        return result;
    }
    async close() {
        if (!this.provider) return;
        try { await this.provider.shutdown(); }
        catch (error) { this.reportFailure('Telemetry shutdown failed', error); }
    }
}
