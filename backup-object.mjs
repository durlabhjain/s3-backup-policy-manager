import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat.js';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';

dayjs.extend(customParseFormat);
dayjs.extend(weekOfYear);
const BackupTypes = ['full', 'diff', 'log'];

class BackupObject {
    constructor(key, bucketName) {
        const parts = key.split('/');
        const filename = parts.at(-1);
        const modern = filename.match(/^(.+)_(FULL|DIFF|LOG)_(\d{8})_(\d{6})(?:_(\d+))?\.(bak|trn)$/i);
        const legacy = filename.match(/^(.+)_(\d{8})_(\d{6})-(\w+)(?:-(\d+))?\.[^/]+$/);
        if (!modern && !legacy) throw new Error(`Invalid backup name format: ${key}`);

        this.key = key;
        this.bucketName = bucketName;
        if (modern) {
            if (parts.length < 4) throw new Error(`Expected server/database/type/file: ${key}`);
            this.server = parts.at(-4);
            this.database = parts.at(-3);
            this.type = modern[2].toLowerCase();
            if (parts.at(-2).toLowerCase() !== this.type ||
                (this.type === 'log' ? modern[6].toLowerCase() !== 'trn' : modern[6].toLowerCase() !== 'bak')) {
                throw new Error(`Backup folder/type/extension mismatch: ${key}`);
            }
            // Include any leading prefix as well as the server to isolate backup sets.
            this.objectName = parts.slice(0, -2).join('/');
            this.date = modern[3];
            this.time = modern[4];
            this.part = modern[5] || '1';
        } else {
            this.objectName = legacy[1];
            this.date = legacy[2];
            this.time = legacy[3];
            const folderType = parts.at(-2)?.split('-')[1]?.toLowerCase();
            this.type = BackupTypes.includes(folderType) ? folderType : legacy[4].toLowerCase();
            this.part = legacy[5] || '1';
        }
        if (!BackupTypes.includes(this.type)) throw new Error(`Invalid backup type: ${this.type}`);
        this.datetime = dayjs(`${this.date}${this.time}`, 'YYYYMMDDHHmmss', true);
        if (!this.datetime.isValid()) throw new Error(`Invalid backup date/time: ${key}`);
        this.backupId = `${this.objectName}_${this.type}_${this.date}_${this.time}`;
        this.year = this.datetime.year();
        this.month = this.datetime.month() + 1;
        this.week = this.datetime.week();
        this.isFullBackup = this.type === 'full';
    }

    getMonthKey() { return this.datetime.format('YYYY-MM'); }
    getWeekKey() {
        const weekYear = this.month === 12 && this.week === 1 ? this.year + 1 : this.year;
        return `${weekYear}-W${String(this.week).padStart(2, '0')}`;
    }
}

export default BackupObject;
