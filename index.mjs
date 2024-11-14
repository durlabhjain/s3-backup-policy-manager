import {
    S3Client,
    ListObjectsV2Command,
    DeleteObjectsCommand
} from "@aws-sdk/client-s3";
import dayjs from 'dayjs';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';
import { readFileSync } from 'fs';
import { writeFileSync } from "fs";
import { existsSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const debug = false;

dayjs.extend(weekOfYear);

// Get current file's directory (ESM equivalent of __dirname)
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function loadConfig() {
    const defaultConfig = {
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
            differentialBackups: 7
        },
        dryRun: true,
        deleteNonRetained: false
    };

    let config = { ...defaultConfig };

    // Load base config
    const configPath = join(__dirname, 'config.json');
    if (existsSync(configPath)) {
        const configFile = JSON.parse(readFileSync(configPath, 'utf8'));
        config = { ...config, ...configFile };
    }

    // Load local config overrides
    const localConfigPath = join(__dirname, 'config.local.json');
    if (existsSync(localConfigPath)) {
        const localConfigFile = JSON.parse(readFileSync(localConfigPath, 'utf8'));
        config = { ...config, ...localConfigFile };
    }

    return config;
}

class BackupObject {
    constructor(key, bucketName) {
        const parts = key.split('/');
        const filename = parts[1] || parts[0];
        const matches = filename.match(/(.+)_(\d{8})_(\d{6})-(\w+)(?:-(\d+))?\..*$/);

        if (!matches) throw new Error(`Invalid backup name format: ${key}`);

        this.key = key;
        this.bucketName = bucketName;
        this.objectName = matches[1];        // e.g., "AnalysisCompanyMaster"
        const dateTimeStr = `${matches[2]}${matches[3]}`;
        this.datetime = dayjs(dateTimeStr, 'YYYYMMDDHHMMSS');
        if (!this.datetime.isValid()) {
            throw new Error(`Invalid date/time in backup name: ${dateTimeStr}`);
        }
        this.date = matches[2];
        this.time = matches[3];
        this.type = parts[0].split("-")[1].toLowerCase();
        this.part = matches[5] || '1';       // If no part number, assume it's single file
        this.backupId = `${this.objectName}_${this.date}_${this.time}`; // Unique identifier for this backup
        this.year = this.datetime.year();
        this.month = this.datetime.month() + 1; // dayjs months are 0-based
        this.week = this.datetime.week();
        this.isFullBackup = this.type === 'full';
    }

    // Helper method to get formatted date strings
    getMonthKey() {
        return this.datetime.format('YYYY-MM');
    }

    getWeekKey() {
        return this.datetime.format('YYYY-[W]WW');
    }
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
            console.error(`Error listing objects in bucket ${bucketName}:`, error);
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
            console.error(`Error during batch deletion in bucket ${bucketName}:`, error);
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
            differentialBackups: 0
        };

        // Track what we've retained
        const retainedYears = new Set();
        const retainedMonths = new Set();
        const retainedWeeks = new Set();
        let diffCount = 0;

        // Process full backups first for year/ month (retain oldest)
        sortedBackupGroups.forEach(backupGroup => {
            const backup = backupGroup[0]; // Use first part for metadata
            if (backup.isFullBackup) {
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

        // Process latest weekly backups
        sortedBackupGroups.reverse().forEach(backupGroup => {
            const backup = backupGroup[0]; // Use first part for metadata
            let shouldRetain = false;
            if (backup.isFullBackup) {
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
    // Initialize S3 client with configuration
    const s3Config = {
        region: config.aws.region,
        credentials: config.aws.credentials
    };

    // Add optional S3 configurations
    if (config.aws.endpoint) {
        s3Config.endpoint = config.aws.endpoint;
        s3Config.forcePathStyle = config.aws.forcePathStyle ?? true;
        s3Config.useArnRegion = config.aws.useArnRegion ?? true;
    }

    const s3Client = new S3Client(s3Config);

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
                const listFilename = `${bucketName}.list.json`;
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

async function main() {
    try {
        const config = loadConfig();

        if (!config.buckets || config.buckets.length === 0) {
            throw new Error('No buckets configured. Please check your config files.');
        }

        console.log('Configuration loaded:', {
            ...config,
            aws: {
                ...config.aws,
                credentials: {
                    accessKeyId: '***',
                    secretAccessKey: '***'
                }
            }
        });

        if (config.dryRun) {
            console.log('\nDRY RUN MODE - No deletions will be performed');
        }

        const results = await processBackups(config);

        console.log('\nTotal Summary:', results.totalSummary);

        if (config.dryRun && config.deleteNonRetained) {
            console.log('\nTo perform actual deletions, set dryRun: false in your config');
        }

        return results;

    } catch (error) {
        console.error('Error in main process:', error);
        throw error;
    }
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