import { createAzureClient } from './storage.mjs';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { errorDetails } from './job-logging.mjs';

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
    const candidates = [], inventory = [], overview = [];
    const ruleSummaries = config.cleanupRules.map(rule => ({
        Rule: rule.description || rule.prefix, Prefix: rule.prefix,
        'Retention months': rule.retentionMonths, 'Cutoff (UTC)': subtractMonths(now, rule.retentionMonths).toISOString(),
        Matched: 0, Eligible: 0, Kept: 0, 'Oldest kept (UTC)': '—'
    }));
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
            } catch (error) {
                statistics.containers_failed++; inventory.push({ name: container.name, failed: true });
                output.error('Container inventory failed: %s %s', container.name, JSON.stringify(errorDetails(error)));
            }
            continue;
        }
        let matched = false;
        for (const [index, rule] of config.cleanupRules.entries()) {
            if (!container.name.startsWith(rule.prefix)) continue;
            const date = parseContainerDate(container.name.slice(rule.prefix.length), rule.dateFormat);
            if (!date) continue;
            matched = true;
            const summary = ruleSummaries[index];
            const eligible = date < subtractMonths(now, rule.retentionMonths);
            const dateText = date.toISOString();
            summary.Matched++;
            if (eligible) {
                summary.Eligible++;
                candidates.push({ name: container.name, date: dateText, rule: rule.description || rule.prefix });
            } else {
                summary.Kept++;
                if (summary['Oldest kept (UTC)'] === '—' || dateText < summary['Oldest kept (UTC)']) summary['Oldest kept (UTC)'] = dateText;
            }
            overview.push({ Container: container.name, 'Date (UTC)': dateText, Rule: summary.Rule,
                'Retention months': rule.retentionMonths, 'Cutoff (UTC)': summary['Cutoff (UTC)'],
                Status: eligible ? 'Eligible for deletion' : 'Retained',
                Reason: eligible ? 'Date before cutoff' : 'Date on or after cutoff' });
            break; // First successfully parsed rule wins, matching the original tool.
        }
        if (!matched) {
            const invalidDate = config.cleanupRules.some(rule => container.name.startsWith(rule.prefix));
            if (invalidDate) statistics.parse_failure_count++;
            overview.push({ Container: container.name, 'Date (UTC)': '—', Rule: '—', 'Retention months': '—',
                'Cutoff (UTC)': '—', Status: 'Protected', Reason: invalidDate ? 'Invalid date for matching prefixes' : 'No matching rule' });
        }
    }
    statistics.containers_eligible = candidates.length;
    statistics.containers_skipped = list ? 0 : statistics.containers_scanned - candidates.length;
    const directory = config.outputDirectory || 'output'; mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, list ? 'container-inventory.json' : 'container-deletion-candidates.json'), JSON.stringify(list ? inventory : candidates, null, 2));
    const showTable = rows => output.table ? output.table(rows) : output.info(JSON.stringify(rows, null, 2));
    if (list) showTable(inventory);
    else {
        const report = join(directory, 'container-retention-overview.json');
        writeFileSync(report, JSON.stringify({ containers: overview, rules: ruleSummaries }, null, 2));
        output.info('Container retention overview (dates from container names; counts are containers)');
        showTable(overview);
        output.info('Retention summary by rule');
        showTable(ruleSummaries);
        const kept = ruleSummaries.reduce((sum, row) => sum + row.Kept, 0);
        output.info(`Container review: ${statistics.containers_scanned} scanned | Eligible: ${candidates.length} | Retained: ${kept} | Protected: ${statistics.containers_skipped - kept} | Invalid dates: ${statistics.parse_failure_count}`);
        output.info(`Full retention overview: ${report}`);
    }
    let cancelled = false;
    if (!list && candidates.length) {
        output.info(`Containers selected for deletion (${candidates.length}); deleting a container removes ALL its contents:`);
        showTable(candidates.map(candidate => ({ Container: candidate.name, 'Date (UTC)': candidate.date, Rule: candidate.rule })));
        // Lifecycle events persist even when the configured minimum log level is error.
        const audit = (event, candidate, extra = {}) => {
            const details = { container_name: candidate.name, container_date: candidate.date, rule: candidate.rule, ...extra };
            if (typeof client.url === 'string') {
                try { const url = new URL(client.url); details.storage_account = url.hostname; } catch { /* Custom clients may have no URL. */ }
            }
            if (output.event) output.event(event, details);
            else output.info(event, details);
        };
        for (const candidate of candidates) audit('container_deletion_candidate', candidate);
        if (config.dryRun !== false || config.deleteNonRetained !== true) statistics.containers_would_delete = candidates.length;
        else if (scheduled || await confirm({ provider: 'azure', bucketName: 'entire containers in account', count: candidates.length, resourceType: 'containers' }) === true) {
            for (const candidate of candidates) {
                let response;
                try { response = await client.getContainerClient(candidate.name).delete(); }
                catch (error) {
                    statistics.containers_failed++;
                    output.error('Container deletion failed: %s %s', candidate.name, JSON.stringify(errorDetails(error)));
                    audit('container_deletion_failed', candidate, { outcome: 'failed', ...errorDetails(error) });
                    continue;
                }
                statistics.containers_deleted++;
                audit('container_deleted', candidate, { outcome: 'success', deleted_at: new Date().toISOString(),
                    ...(response?.requestId ? { request_id: response.requestId } : {}) });
            }
        } else cancelled = true;
    }
    const outcome = cancelled ? 'cancelled' : statistics.containers_failed ? 'partial_failure' : 'success';
    output.info('Container summary', statistics);
    if (!list) output.info(`Container result (${outcome}): Eligible: ${statistics.containers_eligible} | Deleted: ${statistics.containers_deleted} | Would delete: ${statistics.containers_would_delete} | Failed: ${statistics.containers_failed} | Skipped: ${statistics.containers_skipped}`);
    return { statistics, outcome, reviewStopped: cancelled };
}
