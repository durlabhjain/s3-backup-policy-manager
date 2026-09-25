# s3-backup-policy-manager

Inspect SQL backup files, apply retention rules, search object names, and generate download URLs on AWS S3 (including S3-compatible services) or Azure Blob Storage.

Requires Node.js 22+ and npm or Yarn. Install with `npm install`; run tests with `npm test`.

## Supported backup paths

Both patterns are detected automatically on either provider. Paths use `/`; backup types are case-insensitive.

### Existing format

```text
[folder/]<database>_<YYYYMMDD>_<HHmmss>-<type>[-<part>].<extension>

AnalysisCompanyMaster_20260807_144018-Full-01.BAK
SQL-Full/AnalysisCompanyMaster_20260807_144018-Full-01.BAK
SQL-Diff/AnalysisCompanyMaster_20260807_152200-Diff.BAK
```

The database name may contain underscores. The part number is optional and defaults to `1`. Historically a folder whose second hyphen-separated component is `Full` or `Diff` overrides the filename type; this behavior is preserved (also accepting `Log`). Otherwise the filename supplies the type. Legacy backups are grouped by the database name in the filename, so that name must uniquely identify a backup source within the bucket/container. Leading path prefixes are accepted; the last path component supplies the filename.

### Server/database/type folders

```text
[prefix/]<server>/<database>/<full|diff|log>/<server>_<database>_<FULL|DIFF|LOG>_<YYYYMMDD>_<HHmmss>[_<part>].<bak|trn>

WHISKEY/dedicated/log/WHISKEY_dedicated_LOG_20260807_152519.trn
WHISKEY/dedicated/full/WHISKEY_dedicated_FULL_20260807_144018_1.bak
WHISKEY/dedicated/diff/WHISKEY_dedicated_DIFF_20260807_152200.bak
```

Full and differential files use `.bak`; log files use `.trn`. Folder type and filename type must agree. Server/database identity comes from the folders, allowing underscores in either name; the filename prefix is descriptive and is not validated against those folders. Optional leading prefixes are included in the identity, so separate server/database paths have independent retention limits. Multipart files with the same identity, type, and timestamp are retained or deleted together. The tool cannot verify that all parts of a set are present.

Dates are parsed strictly as local time with a 24-hour clock. Invalid filenames, dates, and mismatched folder types are excluded from deletion in every pruning mode. Each failure emits a `PROTECTED — PARSE FAILED (will NOT delete)` error with the full key and reason. Per-container and overall summaries include `parseFailureCount`, and details are saved to `output/<provider>-<bucket-or-container>.parse-failures.json` (overwritten on each successful scan, including an empty array when no failures occur). Protected files are separate from the retained/deleted backup counts. Valid files in the same bucket/container are still evaluated normally.

## Configuration

`config.json` supplies the base configuration. The gitignored `config.local.json` can contain either an overriding object or an array of configurations (for example, separate AWS and Azure accounts). Overrides are shallow; provide complete nested credential/settings objects when overriding them. Omitted retention fields receive the defaults below.

```json
{
  "provider": "aws",
  "cron": "* */4 * * *",
  "aws": {
    "region": "us-east-1",
    "endpoint": null,
    "forcePathStyle": false,
    "useArnRegion": true
  },
  "buckets": ["backup-bucket"],
  "prefix": "",
  "retention": {
    "yearlyBackups": 1,
    "monthlyBackups": 12,
    "weeklyBackups": 4,
    "differentialBackups": 7,
    "fullBackups": 1,
    "logBackups": null
  },
  "dryRun": true,
  "deleteNonRetained": false
}
```

### General settings

| Setting | Default | Meaning |
|---|---|---|
| `provider` | `"aws"` | `"aws"` for S3/S3-compatible storage or `"azure"` for Blob Storage. |
| `buckets` | `[]` | S3 buckets to scan; unused for Azure. |
| `containers` | `[]` | Azure containers to scan; unused for AWS. Must be explicitly configured even with a container SAS URL. |
| `prefix` | `""` | Case-sensitive, literal beginning of an object key, applied by the storage service. Empty scans the entire bucket/container. No wildcard or regex matching. Example: `"coolr-dev/"`. A mismatched prefix returns no files. |
| `cron` | `"* */4 * * *"` | Schedule used only by `schedulePrune`, in the process's local timezone. This default runs every minute during every fourth hour; `"0 */4 * * *"` runs once every four hours. |
| `dryRun` | `true` | Preview only when true. Actual deletion requires the boolean `false`, not the string `"false"`. CLI `dryRun=` takes precedence. |
| `deleteNonRetained` | `false` | Enables deletion only when the boolean `true` and `dryRun` is false. Interactive deletion still requires confirmation. |
| `retention` | See below | Per-source retention policy. Missing fields get built-in defaults, including when a local override replaces the retention object. |

Defaults above are built into the code; values in your selected config files can override them. Normal loading is built-in defaults → `config.json` → `config.local.json` → supported CLI overrides. Nested file overrides are shallow. An explicit `config=...` skips both implicit files and uses built-in defaults → each selected configuration → CLI overrides. Selected files are processed independently, not merged together. Keep config files with credentials out of commits.

### AWS settings

| Setting | Meaning |
|---|---|
| `aws.region` | Signing region; built-in default `us-east-1`. Include it when supplying an `aws` object. |
| `aws.credentials.accessKeyId` / `secretAccessKey` | Explicit credentials. An absent/empty access key selects the SDK credential chain instead. |
| `aws.credentials.sessionToken` | Optional token for temporary credentials. |
| `aws.endpoint` | Optional S3-compatible endpoint URL; `null` uses AWS endpoints. |
| `aws.forcePathStyle` | Controls path-style addressing when a custom endpoint is set. Built-in default false; when omitted from a replacement `aws` object with an endpoint, the helper uses true. |
| `aws.useArnRegion` | Controls ARN-region handling with a custom endpoint; defaults to true. |

`provider` defaults to `aws`, preserving existing configurations. AWS credentials use the SDK credential chain when `aws.credentials` is absent or empty. Alternatively supply `accessKeyId`, `secretAccessKey`, and an optional `sessionToken` in `aws.credentials`. S3-compatible services can use `aws.endpoint` and `forcePathStyle`.

For Azure, use containers instead of buckets:

```json
{
  "provider": "azure",
  "azure": {},
  "containers": ["sql-backups"],
  "prefix": "WHISKEY/",
  "dryRun": true,
  "deleteNonRetained": false
}
```

To use a SAS URL directly, set `azure.sasUrl` (or `AZURE_STORAGE_SAS_URL`):

```json
{
  "provider": "azure",
  "azure": {
    "sasUrl": "https://youraccount.blob.core.windows.net/sql-backups?sv=...&sp=rl&sr=c&sig=..."
  },
  "containers": ["sql-backups"],
  "dryRun": true,
  "deleteNonRetained": false
}
```

Both Blob service URLs (`https://account.blob.core.windows.net/?...`) and container URLs are supported. For a container URL, `containers` must name that same container. Individual blob URLs are not supported for pruning. The SAS must be unexpired and authorize listing; deletion additionally requires delete permission. SAS authentication cannot generate new signed download URLs with this tool. `azure.sasUrl` / `AZURE_STORAGE_SAS_URL` take precedence over connection strings and account keys. This URL contains credentials; keep it out of source control.

Set `AZURE_STORAGE_CONNECTION_STRING` to an account-key connection string. Alternatively configure `azure.accountName` and `azure.accountKey`, or environment variables `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`. An optional `azure.endpoint` overrides the Blob service URL with account-name/key authentication. A connection string takes precedence; `azure.connectionString` can also be set in the local config. Do not commit credentials.

### Azure settings and credential precedence

| Setting | Environment fallback | Meaning |
|---|---|---|
| `azure.sasUrl` | `AZURE_STORAGE_SAS_URL` | Full service/container URL including SAS query. Highest-priority authentication option. |
| `azure.connectionString` | `AZURE_STORAGE_CONNECTION_STRING` | Used if no SAS URL is present. Account-key or SAS connection strings are accepted. |
| `azure.accountName` | `AZURE_STORAGE_ACCOUNT` | Used with account key if neither SAS URL nor connection string is present. |
| `azure.accountKey` | `AZURE_STORAGE_KEY` | Storage account access key. |
| `azure.endpoint` | None | Optional Blob service URL for account-name/key authentication only. Defaults to `https://<accountName>.blob.core.windows.net`. |
| `azure.deleteConcurrency` | None | Maximum simultaneous blob deletions; defaults to `16`. Must be an integer from `1` to `128`; use `1` for sequential deletion. |

For each credential field, a nonempty config value takes precedence over its environment fallback. A SAS URL from the environment still takes precedence over a connection string in the file. An environment connection string likewise takes precedence over account-name/key fields. There are no listing concurrency, page-size, or timeout settings in the config; SDK retry behavior applies.

Azure deletes use a bounded worker pool so each request no longer waits for the previous blob to finish. Progress reports completed requests, successful deletions, failures, elapsed time, and average blobs/second at startup, on request completion roughly every five seconds, and at completion. SDK retries can delay progress updates. Each blob's result is still recorded individually. Actual throughput depends on request latency and service throttling; lower `azure.deleteConcurrency` if necessary.

Azure listing and deletion also work with an appropriately authorized SAS connection string; generating new download SAS URLs requires account-key credentials. Azure snapshots, versions, and soft-deleted blobs are not enumerated or purged. A blob with snapshots or an immutability restriction may fail deletion and is reported individually. SDK reference: [Azure Blob Storage for JavaScript](https://learn.microsoft.com/en-us/javascript/api/overview/azure/storage-blob-readme?view=azure-node-latest).

## Retention behavior

Retention is evaluated relative to the current run time, using timestamps in filenames (local time). All parts of a selected backup set are kept together. Per database/source:

| Setting | Default | Unit and exact behavior |
|---|---|---|
| `fullBackups` | `1` | **Backup sets:** minimum newest full sets kept, regardless of age. Other tiers may retain additional full sets. |
| `yearlyBackups` | `1` | **Calendar years:** earliest available full set per year, covering the current year and N−1 preceding years. `1` means the current calendar year, not the last 365 days. |
| `monthlyBackups` | `12` | **Calendar months:** earliest available full set per month, covering this month and N−1 preceding months. |
| `weeklyBackups` | `4` | **Rolling weeks:** newest available full set in each seven-day interval ending at run time. `4` spans 28 days, not necessarily a calendar month. |
| `differentialBackups` | `7` | **Days:** all differential sets in the last N × 24 hours, with no file-count limit. |
| `logBackups` | `null` | **Days:** all transaction log sets in the last N × 24 hours. Missing/null inherits the numeric `differentialBackups` value. |

All numeric limits must be nonnegative integers. Only `logBackups` also accepts `null`. For a run at September 14, 2026, 12:00 local time: monthly `12` starts October 1, 2025; yearly `1` starts January 1, 2026; weekly `4` starts August 17, 2026, 12:00; daily `7` starts September 7, 2026, 12:00. A newer full selected by `fullBackups` does not prevent retaining an earlier monthly/yearly representative.

Daily limits use exact 24-hour intervals; a backup exactly at the cutoff is retained. Future-dated DIFF/LOG files are also kept when their limit is positive, rather than treated as expired. `0` disables that tier; for DIFF/LOG it permits every file of that type to be pruned. Calendar tiers can select the same full set, so their counts are not additive. No tier can guarantee a backup exists for a period with missing backups.

Retention does not inspect SQL backup headers or LSNs and does not validate restore chains or ensure retained DIFF/LOG files have their required FULL/log dependencies. Configure limits accordingly. Summaries count `totalBackups` as sets and retained/deleted counts as individual files.

Deletion requires **both** `dryRun: false` and `deleteNonRetained: true` in every mode. Before confirmation or deletion, a `console.table` overview shows every recognized source with bucket/container-qualified Source, Server, Database, FULL/DIFF/LOG counts, Kept, Oldest kept, and Status. The FULL/DIFF/LOG values use delete/total notation. Counts are individual files (including each multipart file), not backup sets. Rows with no retained files are marked `WARNING: ALL PARSED FILES SELECTED` and receive a separate console warning. This warning does not block deletion; the normal mode/settings/confirmation rules still apply. Parse failures remain protected and are excluded from these counts because their database/type cannot be reliably identified.

The full deletion candidate list is saved to `output/<provider>-<bucket-or-container>.deletion-candidates.json` on every scan, including dry runs. Interactive pruning suppresses individual candidate lines to keep the console readable. Scheduled pruning also prints individual candidates.

- `mode=prune` runs once immediately. In a terminal it displays one server/main-folder table at a time, containing all that server's database rows. A server summary follows each table, showing database count, total parsed files, files to delete/keep, FULL/DIFF/LOG delete/total counts, and how many databases have all files selected. Parse failures remain excluded from these totals and separately reported. Press Enter after each summary to continue; `q`, EOF, or Ctrl-C stops the run before deleting anything in the current bucket/container. This review pause also applies to dry runs. Leading prefixes keep servers distinct; legacy layouts without reliable server identity appear together under `(legacy layout)`. After all tables for a bucket/container are reviewed, type `DELETE` to authorize that bucket/container's candidates when deletion is enabled. Continuing the review is not deletion approval. Earlier completed bucket/container deletions are not undone by stopping later. Redirected previews do not pause, and non-interactive or piped input cannot authorize deletion.
- `mode=schedulePrune` runs on cron ticks without confirmation. When both deletion settings permit it, it automatically attempts deletion of the listed candidates. Dry-run and disabled-deletion settings still prevent deletion.

A preview-only run pauses for review in a terminal but never asks for deletion confirmation. `findBlobs` remains the default mode and does not evaluate retention or delete files. Mode selection is explicit: launching `schedulePrune` from a terminal still selects scheduled behavior.

## Modes

CLI arguments use `key=value`. `bucket` identifies either an S3 bucket or an Azure container, depending on the configuration.

```bash
# Default mode: search/list keys. pattern is an alias for searchPattern.
node index.mjs mode=findBlobs bucket=sql-backups searchPattern='.*FULL.*' prefix=WHISKEY/

# Run once, preview candidates, and exit without deleting.
node index.mjs mode=prune config=./config.tls.json dryRun=true

# Run once and require terminal confirmation before deletion.
# Also requires deleteNonRetained: true in the configuration file.
node index.mjs mode=prune config=./config.tls.json dryRun=false

# Schedule retention evaluation and force dry-run mode.
# Runs on cron ticks, not immediately.
node index.mjs mode=schedulePrune dryRun=true

# Use a different configuration file and force dry-run mode.
node index.mjs mode=schedulePrune config=./config.azure.json dryRun=true

# Generate a read-only download URL (expiry in seconds, 1–604800; default 86400).
node index.mjs mode=generateSignedUrls bucket=sql-backups blob=WHISKEY/dedicated/full/WHISKEY_dedicated_FULL_20260807_144018_1.bak expiresIn=3600
```

### Selecting multiple config files (all modes)

`config=` accepts a path, a wildcard pattern, a comma-separated list, or repeated arguments. These work with `prune`, `schedulePrune`, `findBlobs`, and `generateSignedUrls`:

```bash
# Schedule every matching config using its own cron expression
node index.mjs mode=schedulePrune 'config=./config.*.json' dryRun=true

# Select an explicit list
node index.mjs mode=schedulePrune 'config=./config.tls.json,./config.coolr.json' dryRun=true

# Repeat config= to combine files and patterns
node index.mjs mode=prune config=./config.tls.json 'config=./configs/*.json' dryRun=true

# Search all selected configurations
node index.mjs mode=findBlobs 'config=./configs/**/*.json' searchPattern='.*FULL.*'

# Generate a URL for each selected config containing the requested bucket/container
node index.mjs mode=generateSignedUrls 'config=./configs/*.json' bucket=sql-backups blob=backup.bak
```

Quote wildcard arguments so the application expands them, and quote arguments containing spaces. Paths are relative to the current working directory; absolute paths also work. Wildcards use [Node.js glob syntax](https://nodejs.org/api/fs.html#fsglobsyncpattern-options), including `*`, `?`, and recursive `**`. Use repeated `config=` arguments for literal filenames containing commas.

Selections are processed in argument/list order, with each wildcard's matches sorted by path. Files are deduplicated by their real path, including overlapping patterns and symlinks. Each file can contain one configuration object or a nonempty array; entries retain their array order. Every entry receives the built-in defaults and supported CLI overrides such as `dryRun=true` independently.

Explicit selections replace the usual `config.json` / `config.local.json` loading. Every selector must match at least one file; missing files, unmatched patterns, malformed JSON, and invalid object/array shapes fail before any mode starts. All configurations must specify buckets/containers, and cron mode validates all schedules before registering jobs. Without `config=`, the usual implicit loading remains in effect.

One-shot modes process configurations sequentially. Cron mode loads files once at startup and registers a separate job per entry using its `cron` expression (or the built-in default); restart to pick up changed files or new wildcard matches. Existing output filenames still apply, so configurations targeting the same provider and bucket/container may overwrite each other's report files.

`dryRun=true` or `dryRun=false` overrides the file setting for every configuration in the run. If omitted, the file setting is used (default `true`). Other values are rejected. `dryRun=false` still requires `deleteNonRetained: true` in the file before deletion can occur.

Search uses all configured buckets/containers when `bucket` is omitted and defaults to the configured prefix. Both pruning modes use all configured buckets/containers and the file-configured prefix; CLI `bucket`, `prefix`, and regex options only filter `findBlobs` (`bucket` also identifies the target for signed URLs). The default cron expression runs every minute during each fourth hour; use `0 */4 * * *` for once every four hours.

Listings and search results are written to `output/<provider>-<bucket-or-container>.list.json` and `output/<provider>-<bucket-or-container>-blobs.txt`. Configurations using the same provider/container name overwrite the same report files. The directory is created automatically.

## Listing performance and troubleshooting

Azure enumeration reads pages of up to 5,000 blobs and reports file count, page count, and elapsed time after every page. Both pruning and search print listing start/completion messages. Pruning separately prints retention-analysis time. This distinguishes waiting for storage from local parsing/analysis. The previous iterator already used the SDK's default pagination; explicit page iteration is primarily for progress visibility. [Azure listing documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-list-javascript).

- Use `prefix` to limit the remote scan to the intended server/root, for example `"coolr-dev/"`. Do not use a type-only prefix for pruning if you expect it to evaluate all backup types for a database.
- Search regexes run locally after enumeration, so they do not reduce listing requests. A prefix does.
- Pages require continuation tokens from earlier responses; they are read sequentially. No blob contents or per-blob properties requests are needed for listing.
- A slow first page can be network/service latency or SDK retries; progress appears only when a page returns. An empty listing commonly means a mismatched prefix, not an empty container.
- Retention uses indexed lookups for retained files to avoid repeatedly scanning the entire listing. Listing data and generated reports are still held in memory; very large scans can consume substantial memory and disk-write time.
- Listings are fetched afresh in normal operation. The `.list.json` report is not a persistent listing cache. A failed page aborts that container's scan before retention or deletion; a partial listing is not used to prune backups.

## Known retention limitations

- FULL, DIFF, and LOG limits are independent. Required dependencies are not automatically protected; filename parsing alone does not establish a valid restore chain.
- Yearly/monthly tiers select calendar representatives; weekly tiers use rolling seven-day intervals. These tiers do not retain every full backup in those periods.
- A multipart set is grouped by its filename timestamp; completeness is not verified. A malformed part is protected individually, but the tool cannot establish its relationship to other parts.
- Legacy filenames are grouped by database name, so identically named databases from different sources in one bucket/container share retention limits. Use the server/database layout to separate them.
- Scheduled runs have no overlap lock, and deletes address object keys without checking whether content changed after listing. Avoid overlapping processes and reuse of backup keys during pruning.

## Development

- `backup-object.mjs`: filename parsing and backup identity.
- `index.mjs`: configuration, retention, scheduling, and CLI.
- `storage.mjs`: common AWS/Azure listing, deletion, and signing interface.
- `s3-operations.mjs`: paginated S3 listing and batch deletion.
- `find-blobs.mjs`: provider-independent search.

Tests use local fixtures and fake storage clients; they do not access or delete cloud backups.
