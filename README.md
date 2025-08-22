## s3-backup-policy-manager

A small Node.js utility to inspect S3 buckets containing backup files and apply a retention policy. It can run scheduled pruning (dry-run or actual deletion), search for backup blobs matching a pattern, and generate pre-signed download URLs for objects.

This repository is driven by `index.mjs` and supports three main modes:
- `schedulePrune` — evaluate backups and optionally delete non-retained objects according to the configured retention policy (cron-capable).
- `findBlobs` — search a bucket for object keys matching a regex (helper for building lists or debugging).
- `generateSignedUrls` — create a pre-signed URL for a specific object.

## Quick start

Requirements:
- Node.js (v16+ recommended)
- npm/yarn

Install dependencies:

```bash
npm install
```

Configuration is driven by `config.json` and optional `config.local.json` (local overrides). The app supports either a single config object or an array of configs defined in `config.local.json`.

## Configuration (defaults)

A minimal example config shows the available options and defaults used by the code:

```json
{
  "cron": "* */4 * * *",
  "aws": {
    "credentials": { "accessKeyId": "", "secretAccessKey": "" },
    "region": "us-east-1",
    "endpoint": null,
    "forcePathStyle": false,
    "useArnRegion": true
  },
  "buckets": [],
  "prefix": "",
  "retention": {
    "yearlyBackups": 1,
    "monthlyBackups": 12,
    "weeklyBackups": 4,
    "differentialBackups": 7,
    "fullBackups": 1
  },
  "dryRun": true,
  "deleteNonRetained": false
}
```

- `buckets`: array of bucket names to process.
- `prefix`: optional object key prefix to restrict the listing.
- `retention`: retention policy numeric limits.
- `dryRun`: when `true`, no deletions are performed.
- `deleteNonRetained`: when `true` the app will attempt deletions (subject to `dryRun`).

Place your real credentials in `config.local.json` (or provide proper environment credentials) and ensure the buckets list is configured.

## Modes & Usage

The script reads CLI args formatted as `key=value`. Any unknown keys are available to modes via the `args` object.

1) schedulePrune (default)

- Purpose: Evaluate all configured buckets, apply the retention policy and optionally delete non-retained objects.
- CLI:

```bash
# Dry run (default behavior driven by config)
node index.mjs mode=schedulePrune

# Force actual deletions if configured
node index.mjs mode=schedulePrune
```

Behavior: When run, `schedulePrune` will schedule the prune task using `cron` per the `cron` config value. It logs a retention summary per bucket and writes a listing file to `output/<bucket>.list.json` when it enumerates objects.

2) findBlobs

- Purpose: Search for object keys that match a provided regex pattern inside a bucket. Useful for locating particular backup sets.
- CLI (example):

```bash
node index.mjs mode=findBlobs bucket=your-bucket pattern='.*Full/USA.*-01.BAK'
```

Note: `findBlobs` accepts arguments passed through the `args` object; consult `find-blobs.mjs` for exact names (typically `bucket`, `pattern`, and an optional `prefix`).

3) generateSignedUrls

- Purpose: Create a pre-signed download URL for a specified object.
- CLI example:

```bash
node index.mjs mode=generateSignedUrls bucket=your-bucket blob=path/to/object.ext expiresIn=3600
```

- `expiresIn` is in seconds (defaults to 24*60*60 in the code).

## Outputs

- `output/<bucket>.list.json` — cached listing of objects for each bucket scanned (created when listing in non-debug mode).
- `output/backup-list.csv` — optional CSV when you choose to export retained objects (some helper code exists in repo comments).

## Safety & notes

- The tool defaults to `dryRun: true`. To actually delete objects set `dryRun: false` and `deleteNonRetained: true` in your config.
- The retention logic groups multi-part backups by a `backupId` and retains by year/month/week/differential/full backups according to the `retention` configuration.

## Troubleshooting

- AWS permissions: Ensure the credentials used have ListObjects, GetObject and DeleteObjects permissions for the target buckets.
- If listings are empty, check `prefix` and the bucket names.
- For S3-compatible endpoints (minio, etc.), configure `aws.endpoint` and `forcePathStyle` in the config.

## Development notes

- Main script: `index.mjs`
- Core helpers: `backup-object.mjs`, `find-blobs.mjs`, `action-base.mjs`, `utility.mjs`.
- The code uses the AWS SDK v3 (@aws-sdk/client-s3) and `@aws-sdk/s3-request-presigner` for signed URLs.

## Example: run once (macOS / zsh)

```bash
# With a local config.json already in place
node index.mjs mode=schedulePrune

# Generate a signed URL for testing
node index.mjs mode=generateSignedUrls bucket=my-bucket blob='backups/db/Full/2025-08-01.bak' expiresIn=3600
```

## Next steps / improvements

- Add unit tests around `applyRetentionPolicy` (edge cases: incomplete multi-part backups, timezone handling).
- Add CLI help text and argument validation.
- Optionally support dry-run output files showing exactly which keys would be deleted.

## License

MIT
