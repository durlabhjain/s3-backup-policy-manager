import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';
import { mkdirSync } from 'node:fs';
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
        return JSON.parse(readFileSync(selectedPath, 'utf8'));
    }
    let config = {};

    // Load base config
    const configPath = './config.json';
    if (existsSync(configPath)) {
        const configFile = JSON.parse(readFileSync(configPath, 'utf8'));
        config = { ...config, ...configFile };
    }

    // Load local config overrides
    const localConfigPath = './config.local.json';
    if (existsSync(localConfigPath)) {
        const localConfigFile = JSON.parse(readFileSync(localConfigPath, 'utf8'));
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
                output.error(`PROTECTED — PARSE FAILED (will NOT delete): ${JSON.stringify(failure)}`);
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

async function confirmDeletion({ provider, bucketName, count }, input = process.stdin, output = process.stdout) {
    // Never accept piped input as authorization to delete backups.
    if (!input.isTTY || !output.isTTY) return false;
    const terminal = createInterface({ input, output });
    return new Promise(resolve => {
        terminal.once('close', () => resolve(false));
        terminal.once('SIGINT', () => terminal.close());
        terminal.question(`Delete ${count} listed files from ${provider}/${bucketName}? Type DELETE to confirm: `, answer => {
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
        mkdirSync("output", { recursive: true });
        for (const bucketName of storageNames(config)) {
            output.log(`\nProcessing bucket: ${bucketName}`);

            try {
                const listFilename = `output/${config.provider || "aws"}-${bucketName}.list.json`;
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
                const parseReport = `output/${config.provider || 'aws'}-${bucketName}.parse-failures.json`;
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
                const candidateReport = `output/${config.provider || 'aws'}-${bucketName}.deletion-candidates.json`;
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
                output.error(`Error processing bucket ${bucketName}:`, error);
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
    const finalTable = [];

    const results = await processBackups(config, createStorage, options);

    logger.log('\nTotal Summary:', results.totalSummary);

    if (config.dryRun && config.deleteNonRetained) {
        logger.log('\nTo perform actual deletions, set dryRun: false in your config');
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

    return { finalTable, reviewStopped: results.reviewStopped };
}

class PruneBackup extends ActionBase {
    async run(config) {
        logger.info('Configuration loaded:', {
            provider: config.provider, containers: storageNames(config),
            prefix: config.prefix, retention: config.retention,
            dryRun: config.dryRun, deleteNonRetained: config.deleteNonRetained
        });

        cron.schedule(config.cron, async () => {
            const title = `${config.provider} - ${storageNames(config).join(',')}`
            logger.info(`Running ${title}....`);
            if (config.dryRun) {
                logger.debug('DRY RUN MODE - No deletions will be performed');
            }
            try {
                await prune(config, { scheduled: true });
            } catch (err) {
                logger.error(err);
            }
        });
    }
};

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
    schedulePrune: PruneBackup,
    findBlobs: FindBlobs,
    generateSignedUrls: GenerateSignedUrls
};

function parseCliArgs(argv) {
    return Object.fromEntries(argv.map(arg => {
        const separator = arg.indexOf('=');
        if (separator < 1) throw new Error(`Expected key=value argument: ${arg}`);
        return [arg.slice(0, separator), arg.slice(separator + 1)];
    }));
}

function resolveConfigs(args) {
    const overrides = {};
    if (args.dryRun !== undefined) {
        if (!['true', 'false'].includes(args.dryRun)) {
            throw new Error('dryRun must be true or false');
        }
        overrides.dryRun = args.dryRun === 'true';
    }
    const loaded = loadConfig(args.config);
    return (Array.isArray(loaded) ? loaded : [loaded]).map(config => {
        if (!config || typeof config !== 'object' || Array.isArray(config)) {
            throw new Error('Configuration must be an object or an array of objects');
        }
        return { ...defaultConfig, ...config, ...overrides };
    });
}

async function main() {
    const args = parseCliArgs(process.argv.slice(2));

    const { mode = "findBlobs", ...options } = args;

    if (!modes[mode]) {
        throw new Error(`Invalid mode: ${mode}`);
    }

    const configs = resolveConfigs(args);

    const action = new modes[mode](options);

    for (const config of configs) {
        if (storageNames(config).length === 0) {
            throw new Error('No buckets/containers configured. Please check your config files.');
        }
        const result = await action.run(config, args);
        if (result?.reviewStopped) break;
    }

    await action.cleanup();
}

export {
    loadConfig,
    parseCliArgs,
    resolveConfigs,
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
