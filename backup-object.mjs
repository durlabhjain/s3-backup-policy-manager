import dayjs from 'dayjs';

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

export default BackupObject;