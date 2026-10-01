import { createAzureClient } from './storage.mjs';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export function validateContainerPolicy(config) {
    if (config.provider !== 'azure') throw new Error('azure-container-retention requires provider=azure');
    if (!Array.isArray(config.cleanupRules) || !config.cleanupRules.length) throw new Error('cleanupRules must be a nonempty array');
    for (const rule of config.cleanupRules) {
        if (typeof rule.prefix !== 'string' || !rule.prefix || !['yyyyMMdd', 'yyyyMM'].includes(rule.dateFormat) ||
            !Number.isInteger(rule.retentionMonths) || rule.retentionMonths < 0 || rule.retentionMonths > 1200)
            throw new Error('Invalid container cleanup rule: require prefix, dateFormat and retentionMonths (0..1200)');
    }
}

export function parseContainerDate(suffix, format) {
    suffix = suffix.replace(/^[-_]/, '');
    if (!(format === 'yyyyMM' ? /^\d{6}$/ : /^\d{8}$/).test(suffix)) return null;
    const year = Number(suffix.slice(0, 4)), month = Number(suffix.slice(4, 6)), day = format === 'yyyyMM' ? 1 : Number(suffix.slice(6, 8));
    if (year < 1 || month < 1 || month > 12 || day < 1) return null;
    const date = new Date(0);
    date.setUTCFullYear(year, month - 1, day); date.setUTCHours(0, 0, 0, 0);
    return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day ? date : null;
}
export function subtractMonths(now, months) {
    const result = new Date(now);
    const day = result.getUTCDate(); result.setUTCDate(1); result.setUTCMonth(result.getUTCMonth() - months);
    const last = new Date(result); last.setUTCMonth(last.getUTCMonth() + 1); last.setUTCDate(0);
    result.setUTCDate(Math.min(day, last.getUTCDate())); return result;
}

export async function processContainers(config, { client = createAzureClient(config), output = console,
    now = new Date(), scheduled = false, confirm = async () => false, list = false } = {}) {
    validateContainerPolicy(config);
    if (typeof client.listContainers !== 'function') throw new Error('Container cleanup requires account-level Azure credentials, not a container SAS');
    const statistics = { containers_scanned: 0, containers_eligible: 0, containers_deleted: 0,
        containers_would_delete: 0, containers_failed: 0, containers_skipped: 0, parse_failure_count: 0,
        blobs_observed: 0, bytes_observed: 0 };
    const candidates = [], inventory = [];
    // Complete enumeration before any delete: a failed page never authorizes a partial cleanup.
    for await (const container of client.listContainers()) {
        statistics.containers_scanned++;
        if (list) {
            try {
                let count = 0, bytes = 0;
                for await (const blob of client.getContainerClient(container.name).listBlobsFlat()) {
                    count++; bytes += blob.properties.contentLength || 0;
                }
                inventory.push({ name: container.name, blobCount: count, totalBytes: bytes });
                statistics.blobs_observed += count; statistics.bytes_observed += bytes;
            } catch { statistics.containers_failed++; inventory.push({ name: container.name, failed: true }); }
            continue;
        }
        let matched = false;
        for (const rule of config.cleanupRules) {
            if (!container.name.startsWith(rule.prefix)) continue;
            const date = parseContainerDate(container.name.slice(rule.prefix.length), rule.dateFormat);
            if (!date) continue;
            matched = true;
            if (date < subtractMonths(now, rule.retentionMonths)) candidates.push({ name: container.name, date: date.toISOString(), rule: rule.description || rule.prefix });
            break; // First successfully parsed rule wins, matching the original tool.
        }
        if (!matched && config.cleanupRules.some(rule => container.name.startsWith(rule.prefix))) statistics.parse_failure_count++;
    }
    statistics.containers_eligible = candidates.length;
    statistics.containers_skipped = list ? 0 : statistics.containers_scanned - candidates.length;
    const directory = config.outputDirectory || 'output'; mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, list ? 'container-inventory.json' : 'container-deletion-candidates.json'), JSON.stringify(list ? inventory : candidates, null, 2));
    output.table?.(list ? inventory : candidates);
    let cancelled = false;
    if (!list && candidates.length) {
        if (config.dryRun !== false || config.deleteNonRetained !== true) statistics.containers_would_delete = candidates.length;
        else if (scheduled || await confirm({ provider: 'azure', bucketName: 'entire containers in account', count: candidates.length }) === true) {
            for (const candidate of candidates) {
                try { await client.getContainerClient(candidate.name).delete(); statistics.containers_deleted++; }
                catch { statistics.containers_failed++; output.error(`Container deletion failed: ${candidate.name}`); }
            }
        } else cancelled = true;
    }
    const outcome = cancelled ? 'cancelled' : statistics.containers_failed ? 'partial_failure' : 'success';
    output.info('Container summary', statistics);
    return { statistics, outcome, reviewStopped: cancelled };
}
