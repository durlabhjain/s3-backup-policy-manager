import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';
import { mkdirSync, globSync, statSync, realpathSync } from 'node:fs';
import { createStorage, storageNames } from './storage.mjs';
import { listS3Objects, deleteS3Objects } from './s3-operations.mjs';
import dayjs from 'dayjs';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';
import { readFileSync } from 'fs';
import { writeFileSync } from "fs";
import { existsSync } from 'fs';
import cron from 'node-cron';
import BackupObject from "./backup-object.mjs";
import FindBlobs from "./find-blobs.mjs";
import ActionBase from "./action-base.mjs";
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { loadApplicationConfig, mergeConfig, applicationDefaults } from './application-config.mjs';
import { resolveVault, expandSettings } from './vault.mjs';
import { JobLogging, secretRedactor, validateLogging, errorDetails } from './job-logging.mjs';
import { processContainers, validateContainerPolicy } from './container-policy.mjs';

const debug = false;

const logger = console;

dayjs.extend(weekOfYear);

const defaultConfig = {
    provider: "aws",
    cron: "* */4 * * *",
    aws: {
        credentials: {
            accessKeyId: '',
            secretAccessKey: ''
        },
        region: 'us-east-1',
        endpoint: null,
        forcePathStyle: false,
        useArnRegion: true
    },
    buckets: [],
    prefix: '',
    retention: {
        yearlyBackups: 1,
        monthlyBackups: 12,
        weeklyBackups: 4,
        differentialBackups: 7,
        logBackups: null, // Inherit differentialBackups unless explicitly configured.
        fullBackups: 1
    },
    dryRun: true,
    deleteNonRetained: false
};

function loadConfig(selectedPath) {
    // An explicit file replaces both implicit config files and must exist.
    if (selectedPath !== undefined) {
        if (!selectedPath) throw new Error('config must specify a file path');
        try { return JSON.parse(readFileSync(selectedPath, 'utf8')); } catch { throw new Error('Invalid configuration JSON'); }
    }
    let config = {};

    // Load base config
    const configPath = './config.json';
    if (existsSync(configPath)) {
        let configFile; try { configFile = JSON.parse(readFileSync(configPath, 'utf8')); } catch { throw new Error('Invalid configuration JSON'); }
        config = { ...config, ...configFile };
    }

    // Load local config overrides
    const localConfigPath = './config.local.json';
    if (existsSync(localConfigPath)) {
        let localConfigFile; try { localConfigFile = JSON.parse(readFileSync(localConfigPath, 'utf8')); } catch { throw new Error('Invalid local configuration JSON'); }
        if (Array.isArray(localConfigFile)) {
            config = localConfigFile.map(localConfigEntry => {
                return { ...config, ...localConfigEntry }
            });
        } else {
            config = { ...config, ...localConfigFile };
        }
    }

    return config;
}

function groupBackupsByObject(backupObjects) {
    // First group by object name
    const byObjectName = new Map();

    backupObjects.forEach(backup => {
        if (!byObjectName.has(backup.objectName)) {
            byObjectName.set(backup.objectName, []);
        }
        byObjectName.get(backup.objectName).push(backup);
    });

    // For each object name, group by backup ID (to keep parts together)
    const result = new Map();

    byObjectName.forEach((backups, objectName) => {
        const backupGroups = new Map();

        backups.forEach(backup => {
            if (!backupGroups.has(backup.backupId)) {
                backupGroups.set(backup.backupId, []);
            }
            backupGroups.get(backup.backupId).push(backup);
        });

        result.set(objectName, Array.from(backupGroups.values()));
    });

    return result;
}

function applyRetentionPolicy(backups, retentionConfig = {}, output = logger, now = new Date()) {
    retentionConfig = { ...defaultConfig.retention, ...retentionConfig };
    retentionConfig.logBackups ??= retentionConfig.differentialBackups;
    for (const [name, limit] of Object.entries(retentionConfig)) {
        if (!Number.isInteger(limit) || limit < 0) throw new Error(`Invalid retention limit: ${name}`);
    }
    const reference = dayjs(now);
    if (!reference.isValid()) throw new Error('Invalid retention reference time');
    const dayMs = 24 * 60 * 60 * 1000;
    const withinDays = (backup, days) => days > 0 &&
        backup.datetime.valueOf() >= reference.valueOf() - days * dayMs;
    const parseFailures = [];
    const backupObjects = backups
        .map(obj => {
            try {
                return new BackupObject(obj.Key, obj.bucketName);
            } catch (e) {
                const failure = { key: obj?.Key ?? null, bucketName: obj?.bucketName ?? null, reason: e.message };
                parseFailures.push(failure);
                (output.warn || output.error).call(output, `PROTECTED — PARSE FAILED (will NOT delete): ${JSON.stringify(failure)}`);
                return null;
            }
        })
        .filter(Boolean);

    // Group backups by object name and then by backup ID
    const groupedBackups = groupBackupsByObject(backupObjects);
    const retainedBackups = new Set();
    const deletionOverview = [];
    const retentionSummary = {
        totalBackups: 0,
        retainedCount: 0,
        deleteCount: 0,
        parseFailureCount: parseFailures.length,
        byObject: Object.create(null)
    };

    // Process each object's backups separately
    groupedBackups.forEach((backupGroups, objectName) => {
        // Sort backup groups by date (oldest first)
        const sortedBackupGroups = backupGroups
            .sort((a, b) => {
                return a[0].datetime.unix() - b[0].datetime.unix();
            });

        const objectRetained = new Set();
        const objectSummary = {
            totalBackups: sortedBackupGroups.length,
            yearlyBackups: 0,
            monthlyBackups: 0,
            weeklyBackups: 0,
            differentialBackups: 0,
            logBackups: 0,
            fullBackups: 0  // New: Track retained full backups
        };

        const keep = group => group.forEach(part => {
            objectRetained.add(part.key);
            retainedBackups.add(part.key);
        });
        const reversedGroups = [...sortedBackupGroups].reverse();
        const fullGroups = sortedBackupGroups.filter(group => group[0].isFullBackup);

        // Minimum newest full sets, regardless of age.
        for (const group of [...fullGroups].reverse().slice(0, retentionConfig.fullBackups)) {
            keep(group);
            objectSummary.fullBackups++;
        }

        // Calendar year/month representatives; tiers may share the same full set.
        for (const [field, unit, periodKey] of [
            ['yearlyBackups', 'year', backup => String(backup.year)],
            ['monthlyBackups', 'month', backup => backup.getMonthKey()]
        ]) {
            const periods = retentionConfig[field];
            if (!periods) continue;
            const cutoff = reference.startOf(unit).subtract(periods - 1, unit);
            const seen = new Set();
            for (const group of fullGroups) {
                const backup = group[0];
                if (backup.datetime.isBefore(cutoff) || backup.datetime.isAfter(reference)) continue;
                const key = periodKey(backup);
                if (seen.has(key)) continue;
                seen.add(key);
                keep(group);
                objectSummary[field]++;
            }
        }

        // One newest full set in each rolling seven-day interval.
        const weeks = new Set();
        for (const group of reversedGroups) {
            const backup = group[0];
            const age = reference.valueOf() - backup.datetime.valueOf();
            if (backup.isFullBackup && retentionConfig.weeklyBackups > 0 &&
                age >= 0 && age <= retentionConfig.weeklyBackups * 7 * dayMs) {
                const week = Math.min(Math.floor(age / (7 * dayMs)), retentionConfig.weeklyBackups - 1);
                if (!weeks.has(week)) {
                    weeks.add(week);
                    keep(group);
                    objectSummary.weeklyBackups++;
                }
            }
            if (backup.type === 'diff' && withinDays(backup, retentionConfig.differentialBackups)) {
                keep(group);
                objectSummary.differentialBackups++;
            }
            if (backup.type === 'log' && withinDays(backup, retentionConfig.logBackups)) {
                keep(group);
                objectSummary.logBackups++;
            }
        }

        // Update summary for this object
        const files = backupGroups.flat();
        const filesByKey = new Map(files.map(backup => [backup.key, backup]));
        objectSummary.retainedCount = objectRetained.size;
        objectSummary.deleteCount = files.length - objectRetained.size;

        // Add more detailed information about retained backups
        objectSummary.retainedBackups = Array.from(objectRetained).map(key => {
            const backup = filesByKey.get(key);
            return {
                key: backup.key,
                date: backup.datetime.format('YYYY-MM-DD HH:mm:ss'),
                type: backup.type,
                part: backup.part
            };
        });

        const deleting = files.filter(backup => !objectRetained.has(backup.key));
        const keeping = files.filter(backup => objectRetained.has(backup.key));
        const oldest = items => items.length
            ? items.reduce((first, backup) => backup.datetime.isBefore(first.datetime) ? backup : first)
                .datetime.format('YYYY-MM-DD HH:mm:ss')
            : '—';
        const counts = type => `${deleting.filter(backup => backup.type === type).length}/${files.filter(backup => backup.type === type).length}`;
        deletionOverview.push({
            Source: [files[0].bucketName, objectName].filter(Boolean).join('/'),
            Server: files[0].server || '(legacy: unknown)',
            Database: files[0].database || objectName,
            'FULL': counts('full'),
            'DIFF': counts('diff'),
            'LOG': counts('log'),
            'Kept': keeping.length,
            'Oldest kept': oldest(keeping),
            Status: keeping.length === 0 ? 'WARNING: ALL PARSED FILES SELECTED' : 'Files retained'
        });

        retentionSummary.byObject[objectName] = objectSummary;
        retentionSummary.totalBackups += objectSummary.totalBackups;
        retentionSummary.retainedCount += objectSummary.retainedCount;
        retentionSummary.deleteCount += objectSummary.deleteCount;
    });

    // Calculate backups to delete
    const backupsToDelete = backupObjects
        .filter(backup => !retainedBackups.has(backup.key))
        .map(backup => ({
            key: backup.key,
            bucketName: backup.bucketName,
            date: backup.datetime.format('YYYY-MM-DD HH:mm:ss'),
            type: backup.type
        }));

    return {
        retainedBackups: Array.from(retainedBackups),
        backupsToDelete,
        parseFailures,
        deletionOverview,
        summary: retentionSummary
    };
}

async function confirmDeletion({ provider, bucketName, count, resourceType = 'files' }, input = process.stdin, output = process.stdout) {
    // Never accept piped input as authorization to delete backups.
    if (!input.isTTY || !output.isTTY) return false;
    const terminal = createInterface({ input, output });
    return new Promise(resolve => {
        terminal.once('close', () => resolve(false));
        terminal.once('SIGINT', () => terminal.close());
        const prompt = resourceType === 'containers'
            ? `Delete ${count} listed Azure containers and ALL their contents? Type DELETE to confirm: `
            : `Delete ${count} listed files from ${provider}/${bucketName}? Type DELETE to confirm: `;
        terminal.question(prompt, answer => {
            resolve(answer.trim() === 'DELETE');
            terminal.close();
        });
    });
}

async function continueReview({ group }, input = process.stdin, output = process.stdout) {
    // Redirected previews need no pagination; deletion still requires a real terminal.
    if (!input.isTTY || !output.isTTY) return true;
    const terminal = createInterface({ input, output });
    return new Promise(resolve => {
        terminal.once('close', () => resolve(false));
        terminal.once('SIGINT', () => terminal.close());
        terminal.question(`Reviewed ${group}. Press Enter to continue, or type q to stop: `, answer => {
            resolve(answer.trim() === '');
            terminal.close();
        });
    });
}

async function showDeletionOverview(rows, { scheduled, output, review }) {
    const groups = new Map();
    for (const row of rows) {
        // Keep prefixed servers distinct. Legacy sources have no reliable server identity.
        const group = row.Server === '(legacy: unknown)' ? '(legacy layout)' : row.Source.split('/').slice(0, -1).join('/');
        if (!groups.has(group)) groups.set(group, []);
        groups.get(group).push(row);
    }
    for (const [group, entries] of groups) {
        output.log(`Server/main folder: ${group}`);
        if (output.table) output.table(entries);
        else output.log(JSON.stringify(entries, null, 2));
        for (const row of entries) {
            if (row['Kept'] === 0) {
                output.log(`WARNING: ALL parsed backup files selected for deletion for ${JSON.stringify(row.Source)}. Review retention limits before proceeding.`);
            }
        }
        const summary = {
            databases: entries.length, totalFiles: 0, deleteFiles: 0,
            keptFiles: entries.reduce((sum, row) => sum + row.Kept, 0),
            allSelectedDatabases: entries.filter(row => row.Kept === 0).length
        };
        for (const type of ['FULL', 'DIFF', 'LOG']) {
            let deleted = 0, total = 0;
            for (const row of entries) {
                const [deleteCount, totalCount] = row[type].split('/').map(Number);
                deleted += deleteCount;
                total += totalCount;
            }
            summary[type] = `${deleted}/${total}`;
            summary.deleteFiles += deleted;
            summary.totalFiles += total;
        }
        output.log(`Server summary for ${group}: ${summary.databases} databases | ${summary.totalFiles} parsed files | Delete: ${summary.deleteFiles} | Kept: ${summary.keptFiles} | FULL: ${summary.FULL} | DIFF: ${summary.DIFF} | LOG: ${summary.LOG} (delete/total) | All files selected: ${summary.allSelectedDatabases} databases`);
        if (!scheduled && await review({ group, rows: entries, summary }) !== true) return false;
    }
    return true;
}

async function processBackups(config, storageFactory = createStorage, {
    scheduled = false, confirm = confirmDeletion, output = logger, review = continueReview
} = {}) {
    const storage = storageFactory(config);

    const allResults = {
        byBucket: {},
        totalSummary: {
            totalBackups: 0,
            retainedCount: 0,
            deleteCount: 0,
            parseFailureCount: 0
        }
    };

    try {
        const outputDirectory = config.outputDirectory || 'output';
        mkdirSync(outputDirectory, { recursive: true });
        for (const bucketName of storageNames(config)) {
            output.log(`\nProcessing bucket: ${bucketName}`);

            try {
                const listFilename = join(outputDirectory, `${config.provider || "aws"}-${bucketName}.list.json`);
                let objects;
                if (debug === true && existsSync(listFilename)) {
                    objects = JSON.parse(readFileSync(listFilename));
                } else {
                    const listingStarted = performance.now();
                    output.log(`Listing ${config.provider || 'aws'}/${bucketName}, prefix=${JSON.stringify(config.prefix || '')}...`);
                    objects = await storage.list(bucketName, config.prefix, progress => {
                        output.log(`Listing progress: ${progress.count} files, ${progress.pages} pages, ${(progress.elapsedMs / 1000).toFixed(1)}s`);
                    });
                    output.log(`Listing complete: ${objects.length} files in ${((performance.now() - listingStarted) / 1000).toFixed(1)}s`);
                    writeFileSync(listFilename, JSON.stringify(objects));
                }
                objects.forEach(obj => obj.bucketName = bucketName);

                const analysisStarted = performance.now();
                const result = applyRetentionPolicy(objects, config.retention, output);
                output.log(`Retention analysis complete in ${((performance.now() - analysisStarted) / 1000).toFixed(1)}s`);
                const parseReport = join(outputDirectory, `${config.provider || 'aws'}-${bucketName}.parse-failures.json`);
                writeFileSync(parseReport, JSON.stringify(result.parseFailures, null, 2));
                if (result.parseFailures.length) {
                    output.error(`ATTENTION: ${result.parseFailures.length} file(s) could not be parsed and are PROTECTED from deletion. Review ${parseReport}`);
                }

                output.log(`\nRetention Policy Summary for ${bucketName}:`, {
                    totalBackupSets: result.summary.totalBackups,
                    retainedFiles: result.summary.retainedCount,
                    deletionCandidates: result.summary.deleteCount,
                    protectedParseFailures: result.summary.parseFailureCount
                });
                output.log('Deletion overview: counts are files to delete / total parsed files (multipart files count separately).');
                const candidateReport = join(outputDirectory, `${config.provider || 'aws'}-${bucketName}.deletion-candidates.json`);
                writeFileSync(candidateReport, JSON.stringify(result.backupsToDelete, null, 2));
                output.log(`Full deletion candidate list: ${candidateReport}`);
                const reviewed = await showDeletionOverview(result.deletionOverview, { scheduled, output, review });
                if (!reviewed) {
                    output.log('Review stopped; no files deleted from this bucket/container.');
                    allResults.byBucket[bucketName] = result;
                    for (const field of Object.keys(allResults.totalSummary)) {
                        allResults.totalSummary[field] += result.summary[field];
                    }
                    allResults.reviewStopped = true;
                    return allResults;
                }

                if (result.backupsToDelete.length > 0) {
                    output.log(`\nBackups to delete in ${bucketName}: ${result.backupsToDelete.length}`);

                    for (const backup of scheduled ? result.backupsToDelete : []) {
                        output.log(`Deletion candidate: ${JSON.stringify({
                            provider: config.provider || 'aws', bucket: bucketName,
                            key: backup.key, type: backup.type, date: backup.date
                        })}`);
                    }

                    let approved = false;
                    if (config.dryRun !== false || config.deleteNonRetained !== true) {
                        output.log('Preview only: deletion requires dryRun=false and deleteNonRetained=true.');
                    } else {
                        approved = scheduled === true || await confirm({
                            provider: config.provider || 'aws', bucketName,
                            count: result.backupsToDelete.length
                        }) === true;
                        if (!approved) output.log('Deletion not confirmed; no files deleted from this bucket/container.');
                    }
                    if (approved) {
                        output.log(`\nDeleting non-retained backups from ${bucketName}...`);
                        const deletionResult = await storage.delete(
                            bucketName,
                            result.backupsToDelete.map(b => b.key),
                            ({ completed, total, successful, failed, elapsedMs }) => {
                                const seconds = elapsedMs / 1000;
                                const rate = seconds > 0 ? completed / seconds : 0;
                                output.log(`Deletion progress for ${bucketName}: ${completed}/${total} completed | ${successful} deleted | ${failed} failed | ${seconds.toFixed(1)}s | ${rate.toFixed(1)} blobs/s`);
                            }
                        );

                        result.deletionResult = deletionResult;
                        if (deletionResult.failed.length) {
                            output.error(`Failed deletions in ${bucketName}:`, deletionResult.failed);
                        }
                    }
                }

                allResults.byBucket[bucketName] = result;
                allResults.totalSummary.totalBackups += result.summary.totalBackups;
                allResults.totalSummary.retainedCount += result.summary.retainedCount;
                allResults.totalSummary.deleteCount += result.summary.deleteCount;
                allResults.totalSummary.parseFailureCount += result.summary.parseFailureCount;

            } catch (error) {
                output.error('Error processing bucket/container %s: %s', bucketName, JSON.stringify(errorDetails(error)));
                allResults.byBucket[bucketName] = { error: error.message };
            }
        }
    } finally {
        // Ensure client is properly closed
        await storage.close();
    }

    return allResults;
}

async function prune(config, options) {
    const output = options?.output || logger;
    const finalTable = [];

    const results = await processBackups(config, createStorage, options);

    output.log('\nTotal Summary:', results.totalSummary);

    if (config.dryRun && config.deleteNonRetained) {
        output.log('\nTo perform actual deletions, set dryRun: false in your config');
    }

    for (const bucket in results.byBucket) {
        if (!results.byBucket[bucket].summary) continue;
        const { byObject } = results.byBucket[bucket].summary;
        for (const db in byObject) {
            const summary = byObject[db];
            summary.retainedBackups.forEach(backup => {
                finalTable.push({
                    bucket,
                    db,
                    key: backup.key
                })
            });
        }
    }

    const statistics = { objects_eligible: results.totalSummary.deleteCount, objects_deleted: 0,
        objects_scanned: results.totalSummary.retainedCount + results.totalSummary.deleteCount + results.totalSummary.parseFailureCount,
        objects_retained: results.totalSummary.retainedCount, objects_would_delete: 0, objects_failed: 0, targets_failed: 0, parse_failure_count: results.totalSummary.parseFailureCount };
    for (const result of Object.values(results.byBucket)) {
        if (result.error) statistics.targets_failed++;
        statistics.objects_deleted += result.deletionResult?.successful.length || 0;
        statistics.objects_failed += result.deletionResult?.failed.length || 0;
        if (config.dryRun !== false || config.deleteNonRetained !== true) statistics.objects_would_delete += result.summary?.deleteCount || 0;
    }
    const cancelled = results.reviewStopped || (config.dryRun === false && config.deleteNonRetained === true &&
        Object.values(results.byBucket).some(result => result.backupsToDelete?.length && !result.deletionResult));
    return { finalTable, reviewStopped: results.reviewStopped, statistics,
        outcome: statistics.targets_failed === Object.keys(results.byBucket).length && statistics.targets_failed ? 'failed' :
            statistics.targets_failed || statistics.objects_failed ? 'partial_failure' : cancelled ? 'cancelled' : 'success' };
}

class PruneOnce extends ActionBase {
    async run(config) {
        return await prune(config);
    }
}

class GenerateSignedUrls extends ActionBase {
    async run(config, args) {
        const { bucket, blob, expiresIn = 24 * 60 * 60 } = args;
        if (!storageNames(config).includes(bucket)) return;
        const storage = createStorage(config);
        try {
            logger.info("Signed URL for download:", await storage.signedUrl(bucket, blob, expiresIn));
        } finally {
            await storage.close();
        }

    }
}

const modes = {
    prune: PruneOnce,
    schedulePrune: PruneOnce,
    findBlobs: FindBlobs,
    generateSignedUrls: GenerateSignedUrls
};

function parseCliArgs(argv) {
    const args = {};
    for (const arg of argv) {
        const separator = arg.indexOf('=');
        if (separator < 1) throw new Error(`Expected key=value argument: ${arg}`);
        const key = arg.slice(0, separator), value = arg.slice(separator + 1);
        if (key === 'config' && args.config !== undefined) {
            args.config = [args.config, value].flat();
        } else {
            Object.defineProperty(args, key, { value, enumerable: true, configurable: true, writable: true });
        }
    }
    return args;
}

function resolveConfigFiles(selection) {
    const files = new Set();
    for (const value of [selection].flat()) {
        if (typeof value !== 'string' || !value.trim()) throw new Error('config must specify a file path or pattern');
        // Existing literal paths may contain commas or glob metacharacters.
        const selectors = [];
        if (existsSync(value)) selectors.push(value);
        else {
            let start = 0, depth = 0;
            for (let i = 0; i < value.length; i++) {
                if ('{[('.includes(value[i])) depth++;
                if ('}])'.includes(value[i])) depth--;
                if (value[i] === ',' && depth === 0) {
                    selectors.push(value.slice(start, i).trim());
                    start = i + 1;
                }
            }
            selectors.push(value.slice(start).trim());
        }
        for (const selector of selectors) {
            if (!selector) throw new Error('config must specify a file path or pattern for every list entry');
            const matches = existsSync(selector) ? [selector] : globSync(selector).sort();
            const matchedFiles = matches.filter(path => statSync(path).isFile());
            if (!matchedFiles.length) throw new Error(`No config files matched: ${selector}`);
            for (const path of matchedFiles) files.add(realpathSync(path));
        }
    }
    if (!files.size) throw new Error('config must specify at least one file path or pattern');
    return [...files];
}

function resolveConfigs(args, jobDefaults, raw = false) {
    const overrides = {};
    if (args.dryRun !== undefined) {
        if (!['true', 'false'].includes(args.dryRun)) {
            throw new Error('dryRun must be true or false');
        }
        overrides.dryRun = args.dryRun === 'true';
    }
    const loaded = args.config === undefined ? loadConfig() : resolveConfigFiles(args.config).flatMap(path => {
        try {
            const value = loadConfig(path);
            const entries = Array.isArray(value) ? value : [value];
            if (!entries.length || entries.some(entry => !entry || typeof entry !== 'object' || Array.isArray(entry))) {
                throw new Error('Configuration must be an object or a nonempty array of objects');
            }
            return entries;
        } catch (error) {
            throw new Error(`Unable to load config ${path}: ${error.message}`, { cause: error });
        }
    });
    return (Array.isArray(loaded) ? loaded : [loaded]).map(config => {
        if (!config || typeof config !== 'object' || Array.isArray(config)) {
            throw new Error('Configuration must be an object or an array of objects');
        }
        if (raw) return { ...config, ...overrides };
        return jobDefaults === undefined ? { ...defaultConfig, ...config, ...overrides } :
            { ...mergeConfig(mergeConfig(defaultConfig, jobDefaults), config), ...overrides };
    });
}

async function main(argv = process.argv.slice(2)) {
    const args = parseCliArgs(argv);
    if (args.dryRun !== undefined && !['true', 'false'].includes(args.dryRun)) throw new Error('dryRun must be true or false');

    const { mode = "findBlobs", ...options } = args;

    if (!modes[mode] && mode !== 'listContainers') {
        throw new Error(`Invalid mode: ${mode}`);
    }

    const rawApp = loadApplicationConfig(args.appConfig, args.appConfig !== undefined);
    if (JSON.stringify(rawApp.vault || {}).includes('vault:')) throw new Error('Vault bootstrap settings cannot contain secret references');
    const app = applicationDefaults(expandSettings(await resolveVault(rawApp, rawApp.vault, { deferJobs: true })));
    validateLogging(app);
    const applicationJobs = args.config === undefined && app.jobs !== undefined;
    const sources = applicationJobs ? rawApp.jobs || app.jobs : resolveConfigs(args, undefined, true);
    if (!Array.isArray(sources) || !sources.length) throw new Error('Jobs must be a nonempty array');
    const resolveJob = async (source, currentApp) => {
        const resolved = expandSettings(await resolveVault(source, rawApp.vault));
        // Preserve legacy shallow merges unless application defaults or Vault settings are used.
        const config = source.settings !== undefined || Object.keys(currentApp.jobDefaults || {}).length ?
            mergeConfig(mergeConfig(defaultConfig, currentApp.jobDefaults), resolved) : { ...defaultConfig, ...resolved };
        if (args.dryRun !== undefined) config.dryRun = args.dryRun === 'true';
        return config;
    };
    const configs = [];
    for (const source of sources) configs.push(await resolveJob(source, app));
    function validateBackupJob(config) {
        if (!['aws', 'azure'].includes(config.provider)) throw new Error('Unsupported provider');
        const targets = storageNames(config);
        if (!Array.isArray(targets) || !targets.length) throw new Error('No buckets/containers configured. Please check your config files.');
        if (targets.some(target => typeof target !== 'string' || !target || /[\\/]/.test(target))) throw new Error('Invalid bucket/container name');
        const retention = { ...defaultConfig.retention, ...config.retention };
        retention.logBackups ??= retention.differentialBackups;
        if (Object.values(retention).some(value => !Number.isInteger(value) || value < 0)) throw new Error('Invalid retention limit');
    }
    const ids = new Set();

    for (const [index, config] of configs.entries()) {
        const explicitId = config.jobId;
        config.jobId ||= `legacy-${config.provider}-${index + 1}`;
        if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/.test(config.jobId) || ids.has(config.jobId)) throw new Error('jobId must be unique and contain only letters, digits, underscore or dash');
        ids.add(config.jobId);
        config.policy ||= 'sql-backup-retention';
        if (!['sql-backup-retention', 'azure-container-retention'].includes(config.policy)) throw new Error('Invalid job policy');
        if (typeof config.dryRun !== 'boolean' || typeof config.deleteNonRetained !== 'boolean') throw new Error('Job deletion settings must be booleans');
        config.outputDirectory = explicitId ? join(app.outputDirectory || 'output', config.jobId) : (app.outputDirectory || 'output');
        if (config.policy === 'azure-container-retention') {
            validateContainerPolicy(config);
            if (!['prune', 'schedulePrune', 'listContainers'].includes(mode)) throw new Error('Container policy supports prune, schedulePrune and listContainers');
        } else if (mode === 'listContainers') throw new Error('listContainers requires a container retention job');
        else validateBackupJob(config);
        if (mode === 'schedulePrune' && !cron.validate(config.cron)) {
            throw new Error(`Invalid cron schedule: ${config.cron}`);
        }
    }
    const active = new Map();
    const tasks = [];
    const run = config => {
        if (active.has(config.jobId)) {
            console.warn('Skipped overlapping scheduled execution:', config.jobId);
            return Promise.resolve();
        }
        const execution = (async () => {
            const runId = randomUUID(), started = performance.now();
            let currentApp, currentConfig, logging;
            try {
                currentApp = applicationDefaults(expandSettings(await resolveVault(rawApp, rawApp.vault, { deferJobs: true })));
                currentConfig = await resolveJob(applicationJobs ? (rawApp.jobs || currentApp.jobs)[configs.indexOf(config)] : sources[configs.indexOf(config)], currentApp);
                if ((currentConfig.jobId || config.jobId) !== config.jobId || (currentConfig.policy || 'sql-backup-retention') !== config.policy || currentConfig.provider !== config.provider)
                    throw new Error('Job identity, provider or policy changed; restart required');
                if (typeof currentConfig.dryRun !== 'boolean' || typeof currentConfig.deleteNonRetained !== 'boolean') throw new Error('Job deletion settings must be booleans');
                currentConfig.jobId = config.jobId; currentConfig.policy = config.policy; currentConfig.outputDirectory = config.outputDirectory;
                if (currentConfig.policy === 'azure-container-retention') validateContainerPolicy(currentConfig);
                else validateBackupJob(currentConfig);
                logging = new JobLogging(currentApp, { redact: secretRedactor([currentApp, currentConfig]) });
            } catch (error) {
                // Vault failures cannot export using credentials that could not be resolved.
                const bootstrap = new JobLogging({ logging: app.logging, telemetry: { enabled: false } }, { redact: secretRedactor([rawApp, app, sources, currentApp, currentConfig]) });
                bootstrap.logger({ job_id: config.jobId, run_id: runId }).event('run_completed', { outcome: 'failed', mode: 'startup', ...errorDetails(error), duration_ms: Math.round(performance.now() - started) });
                await bootstrap.close();
                if (mode !== 'schedulePrune') process.exitCode = 1;
                return { outcome: 'failed' };
            }
            const identity = { run_id: runId, job_id: config.jobId, environment: currentConfig.environment || currentApp.environment || 'development',
                mode: mode === 'listContainers' ? 'list' : ['prune', 'schedulePrune'].includes(mode) ? currentConfig.dryRun !== false || currentConfig.deleteNonRetained !== true ? 'dry_run' : 'execute' : mode,
                policy: config.policy, provider: config.provider, host_name: hostname() };
            const output = logging.logger(identity);
            output.event('run_started', { started_at: new Date().toISOString() });
            let result, failure;
            try {
                if (config.policy === 'azure-container-retention') result = await processContainers(currentConfig, {
                    output, scheduled: mode === 'schedulePrune', confirm: confirmDeletion, list: mode === 'listContainers'
                });
                else if (['prune', 'schedulePrune'].includes(mode)) result = await prune(currentConfig, { output, scheduled: mode === 'schedulePrune' });
                else {
                    const action = new modes[mode]({ ...options, logger: output });
                    try { result = await action.run(currentConfig, args); } finally { await action.cleanup(); }
                }
                result ||= { outcome: 'success', statistics: {} };
            } catch (error) { failure = errorDetails(error); result = { outcome: 'failed', statistics: {} }; output.error('Job failed: %s', JSON.stringify(failure)); }
            finally {
                output.event('run_completed', { outcome: result.outcome || 'success', duration_ms: Math.round(performance.now() - started),
                    ...result.statistics, ...failure });
                await logging.close();
            }
            if (mode !== 'schedulePrune' && ['failed', 'partial_failure'].includes(result.outcome)) process.exitCode = 1;
            return result;
        })();
        active.set(config.jobId, execution);
        return execution.finally(() => active.delete(config.jobId));
    };
    let shutdown;
    try {
        if (mode === 'schedulePrune') {
            for (const config of configs) tasks.push(cron.schedule(config.cron, () => run(config)));
            shutdown = async () => {
                for (const task of tasks) task?.stop();
                await Promise.allSettled([...active.values()]);
                process.removeListener('SIGINT', shutdown); process.removeListener('SIGTERM', shutdown);
            };
            process.once('SIGINT', shutdown); process.once('SIGTERM', shutdown);
            return;
        }
        for (const config of configs) {
            const result = await run(config);
            if (result?.reviewStopped) break;
        }
    } finally {
        if (mode === 'schedulePrune' && !shutdown) for (const task of tasks) task?.stop();
    }
}

export {
    loadConfig,
    parseCliArgs,
    resolveConfigs,
    resolveConfigFiles,
    BackupObject,
    applyRetentionPolicy,
    listS3Objects,
    deleteS3Objects,
    processBackups,
    confirmDeletion,
    continueReview,
    main
};

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main().catch(error => { console.error(error); process.exitCode = 1; });
}
