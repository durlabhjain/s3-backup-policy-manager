import {
    S3Client,
    ListObjectsV2Command,
    DeleteObjectsCommand,
    GetObjectCommand,
    PutObjectCommand
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import dayjs from 'dayjs';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';
import { readFileSync } from 'fs';
import { writeFileSync } from "fs";
import { existsSync } from 'fs';
import ObjectsToCsv from "objects-to-csv";
import cron from 'node-cron';
import BackupObject from "./backup-object.mjs";
import FindBlobs from "./find-blobs.mjs";
import ActionBase from "./action-base.mjs";
import utility from "./utility.mjs";

const debug = false;

const logger = console;

dayjs.extend(weekOfYear);

const defaultConfig = {
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
        fullBackups: 1
    },
    dryRun: true,
    deleteNonRetained: false
};

function loadConfig() {
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

async function listS3Objects(s3Client, bucketName, prefix = '') {
    const objects = [];
    let continuationToken;

    do {
        try {
            const command = new ListObjectsV2Command({
                Bucket: bucketName,
                Prefix: prefix,
                ContinuationToken: continuationToken
            });

            const response = await s3Client.send(command);

            if (response.Contents) {
                objects.push(...response.Contents);
            }

            continuationToken = response.NextContinuationToken;
        } catch (error) {
            logger.error(`Error listing objects in bucket ${bucketName}:`, error);
            throw error;
        }
    } while (continuationToken);

    return objects;
}

async function deleteS3Objects(s3Client, bucketName, keys) {
    const chunkSize = 1000;
    const chunks = [];

    for (let i = 0; i < keys.length; i += chunkSize) {
        chunks.push(keys.slice(i, i + chunkSize));
    }

    const deletionResults = {
        successful: [],
        failed: []
    };

    for (const chunk of chunks) {
        const command = new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: chunk.map(key => ({ Key: key })),
                Quiet: false
            }
        });

        try {
            const response = await s3Client.send(command);

            if (response.Deleted) {
                deletionResults.successful.push(...response.Deleted.map(obj => obj.Key));
            }
            if (response.Errors) {
                deletionResults.failed.push(...response.Errors.map(error => ({
                    key: error.Key,
                    error: error.Message,
                    bucket: bucketName
                })));
            }
        } catch (error) {
            logger.error(`Error during batch deletion in bucket ${bucketName}:`, error);
            deletionResults.failed.push(...chunk.map(key => ({
                key,
                error: error.message,
                bucket: bucketName
            })));
        }
    }

    return deletionResults;
}

function applyRetentionPolicy(backups, retentionConfig) {
    const backupObjects = backups
        .map(obj => {
            try {
                return new BackupObject(obj.Key, obj.bucketName);
            } catch (e) {
                console.warn(`Skipping invalid backup: ${obj.Key}`);
                return null;
            }
        })
        .filter(Boolean);

    // Group backups by object name and then by backup ID
    const groupedBackups = groupBackupsByObject(backupObjects);
    const retainedBackups = new Set();
    const retentionSummary = {
        totalBackups: 0,
        retainedCount: 0,
        deleteCount: 0,
        byObject: {}
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
            fullBackups: 0  // New: Track retained full backups
        };

        const retainedYears = new Set();
        const retainedMonths = new Set();
        const retainedWeeks = new Set();
        let diffCount = 0;
        let fullCount = 0;  // New: Track count of retained full backups

        // First pass: Retain newest full backups up to the limit
        const reversedGroups = [...sortedBackupGroups].reverse();
        reversedGroups.forEach(backupGroup => {
            const backup = backupGroup[0];
            if (backup.isFullBackup && fullCount < retentionConfig.fullBackups) {
                backupGroup.forEach(part => {
                    objectRetained.add(part.key);
                    retainedBackups.add(part.key);
                });
                fullCount++;
                objectSummary.fullBackups++;
            }
        });

        // Second pass: Process yearly/monthly retention for remaining full backups
        sortedBackupGroups.forEach(backupGroup => {
            const backup = backupGroup[0];
            if (backup.isFullBackup && !objectRetained.has(backup.key)) {
                let shouldRetain = false;

                // 1. Yearly retention
                if (!retainedYears.has(backup.year) &&
                    retainedYears.size < retentionConfig.yearlyBackups) {
                    retainedYears.add(backup.year);
                    objectSummary.yearlyBackups++;
                    shouldRetain = true;
                }

                // 2. Monthly retention
                const monthKey = backup.getMonthKey();
                if (!retainedMonths.has(monthKey) &&
                    retainedMonths.size < retentionConfig.monthlyBackups) {
                    retainedMonths.add(monthKey);
                    objectSummary.monthlyBackups++;
                    shouldRetain = true;
                }

                if (shouldRetain) {
                    // Retain all parts of this backup
                    backupGroup.forEach(part => {
                        objectRetained.add(part.key);
                        retainedBackups.add(part.key);
                    });
                }
            }
        });

        // Third pass: Process weekly backups and differential backups
        reversedGroups.forEach(backupGroup => {
            const backup = backupGroup[0];
            let shouldRetain = false;
            if (backup.isFullBackup && !objectRetained.has(backup.key)) {
                // 3. Weekly retention
                const weekKey = backup.getWeekKey();
                if (!retainedWeeks.has(weekKey) &&
                    retainedWeeks.size < retentionConfig.weeklyBackups) {
                    retainedWeeks.add(weekKey);
                    objectSummary.weeklyBackups++;
                    shouldRetain = true;
                }
            }

            if (!backup.isFullBackup && diffCount < retentionConfig.differentialBackups) {
                shouldRetain = true;
                diffCount++;
                objectSummary.differentialBackups++;
            }

            if (shouldRetain) {
                // Retain all parts of this backup
                backupGroup.forEach(part => {
                    objectRetained.add(part.key);
                    retainedBackups.add(part.key);
                });
            }
        });


        // Update summary for this object
        objectSummary.retainedCount = objectRetained.size;
        objectSummary.deleteCount = backupObjects
            .filter(b => b.objectName === objectName)
            .length - objectRetained.size;

        // Add more detailed information about retained backups
        objectSummary.retainedBackups = Array.from(objectRetained).map(key => {
            const backup = backupObjects.find(b => b.key === key);
            return {
                key: backup.key,
                date: backup.datetime.format('YYYY-MM-DD HH:mm:ss'),
                type: backup.type,
                part: backup.part
            };
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
        summary: retentionSummary
    };
}

async function processBackups(config) {
    const s3Client = utility.createS3Client(config);

    const allResults = {
        byBucket: {},
        totalSummary: {
            totalBackups: 0,
            retainedCount: 0,
            deleteCount: 0
        }
    };

    try {
        for (const bucketName of config.buckets) {
            console.log(`\nProcessing bucket: ${bucketName}`);

            try {
                const listFilename = `output/${bucketName}.list.json`;
                let objects;
                if (debug === true && existsSync(listFilename)) {
                    objects = JSON.parse(readFileSync(listFilename));
                } else {
                    objects = await listS3Objects(s3Client, bucketName, config.prefix);
                    writeFileSync(listFilename, JSON.stringify(objects));
                }
                objects.forEach(obj => obj.bucketName = bucketName);

                const result = applyRetentionPolicy(objects, config.retention);

                console.log(`\nRetention Policy Summary for ${bucketName}:`, result.summary);

                if (result.backupsToDelete.length > 0) {
                    console.log(`\nBackups to delete in ${bucketName}: ${result.backupsToDelete.length}`);

                    if (config.deleteNonRetained && !config.dryRun) {
                        console.log(`\nDeleting non-retained backups from ${bucketName}...`);
                        const deletionResult = await deleteS3Objects(
                            s3Client,
                            bucketName,
                            result.backupsToDelete.map(b => b.key)
                        );

                        result.deletionResult = deletionResult;
                    }
                }

                allResults.byBucket[bucketName] = result;
                allResults.totalSummary.totalBackups += result.summary.totalBackups;
                allResults.totalSummary.retainedCount += result.summary.retainedCount;
                allResults.totalSummary.deleteCount += result.summary.deleteCount;

            } catch (error) {
                console.error(`Error processing bucket ${bucketName}:`, error);
                allResults.byBucket[bucketName] = { error: error.message };
            }
        }
    } finally {
        // Ensure client is properly closed
        await s3Client.destroy();
    }

    return allResults;
}

async function prune(config) {
    const finalTable = [];

    const results = await processBackups(config);

    logger.log('\nTotal Summary:', results.totalSummary);

    if (config.dryRun && config.deleteNonRetained) {
        logger.log('\nTo perform actual deletions, set dryRun: false in your config');
    }

    for (const bucket in results.byBucket) {
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

    return finalTable;
}

class PruneBackup extends ActionBase {
    async run(config) {
        logger.info('Configuration loaded:', {
            ...config,
            aws: {
                ...config.aws,
                credentials: {
                    accessKeyId: '***',
                    secretAccessKey: '***'
                }
            }
        });

        cron.schedule(config.cron, () => {
            const title = `${config.aws.endpoint} - ${config.buckets.join(',')}`
            logger.info(`Running ${title}....`);
            if (config.dryRun) {
                logger.debug('DRY RUN MODE - No deletions will be performed');
            }
            try {
                prune(config);
            } catch (err) {
                logger.error(err);
            }
        });
    }
};

class GenerateSignedUrls extends ActionBase {
    async run(config, args) {
        const { bucket, blob, expiresIn = 24 * 60 * 60 } = args;
        if (!config.buckets.includes(bucket)) {
            logger.info(`Bucket ${bucket} not found in ${config.aws.endpoint}`);
            return;
        }

        const s3Client = utility.createS3Client(config);
        const getObjectParams = {
            Bucket: bucket,
            Key: blob, // The path to your object in S3
        };
        const getCommand = new GetObjectCommand(getObjectParams);
        const signedUrl = await getSignedUrl(s3Client, getCommand, { expiresIn });
        logger.info("Signed URL for download:", signedUrl);

    }
}

const modes = {
    schedulePrune: PruneBackup,
    findBlobs: FindBlobs,
    generateSignedUrls: GenerateSignedUrls
};

async function main() {
    // convert arguments to an object
    const args = process.argv.slice(2).reduce((acc, arg) => {
        const [key, value] = arg.split('=');
        acc[key] = value;
        return acc;
    }, {});

    const { mode = "schedulePrune", ...options } = args;

    if (!modes[mode]) {
        throw new Error(`Invalid mode: ${mode}`);
    }

    let configs = loadConfig();

    if (!Array.isArray(configs)) {
        configs = [configs];
    }

    const action = new modes[mode](options);

    for (let config of configs) {
        config = { ...defaultConfig, ...config };
        if (!config.buckets || config.buckets.length === 0) {
            throw new Error('No buckets configured. Please check your config files.');
        }
        await action.run(config, args);
    }

    await action.cleanup();
}

export {
    loadConfig,
    BackupObject,
    applyRetentionPolicy,
    listS3Objects,
    deleteS3Objects,
    processBackups,
    main
};

main();
//findBlobs('sql-indexing', /.*Full\/USA.*-01.BAK/, '');
//const csv = new ObjectsToCsv(final.finalTable);
//await csv.toDisk('./output/backup-list.csv');
//console.table(final.finalTable);