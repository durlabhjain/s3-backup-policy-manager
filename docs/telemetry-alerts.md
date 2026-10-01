# Receiver alerts and migration acceptance

Alerts belong to the OTLP receiver. Enable export only after supplying the real full OpenObserve OTLP/HTTP JSON logs endpoint, stream and authorization header. Keep telemetry credentials in Vault. The endpoint must be HTTPS; only loopback HTTP is accepted for fixtures. TLS certificate checks remain enabled.

Before configuring alerts, run a dry-run fixture and inspect the actual ingested record. Confirm OpenObserve's mapping of OTLP attributes to searchable fields: `event_name`, `job_id`, `environment`, `mode`, `run_id`, `outcome`, `duration_ms` and numeric statistics. Resource metadata includes service name and deployment environment. If your receiver nests/prefixes attribute names, adapt queries to those names; do not assume the examples below work unchanged.

## Completion alerts

After field normalization, a completion filter can use:

```sql
SELECT * FROM backup_jobs
WHERE event_name = 'run_completed'
  AND job_id = 'production-job'
  AND environment = 'production'
  AND mode = 'execute'
  AND outcome IN ('failed', 'partial_failure')
```

Use a short lookback that exceeds export delay and the alert evaluation interval. Deduplicate on `(job_id, environment, mode, run_id)` so repeated evaluations do not resend the same completion. The alert title should include job/outcome; the body should include run ID, duration, successful/failed counts and protected parse failures. Optional success notifications use the same filter with `outcome = 'success'`. A cancellation is distinct from success/failure and may merit a separate notification if production executions should always complete.

For SQL jobs, `objects_eligible` counts candidates; `objects_deleted` counts confirmed successful requests; `objects_failed` and `targets_failed` indicate partial work. For container jobs, use `containers_deleted`, `containers_failed` and `containers_eligible`. `blobs_observed` and `bytes_observed` are measured only during inventory, not bytes reclaimed by container deletion. Do not add object and container counts into one unit.

## Missing completion alerts

Scope absence monitoring by `(job_id, environment, mode)`, normally `mode = 'execute'`. Manual dry runs must not reset the production completion clock. For a daily job, an illustrative threshold is 26 hours, adjusted for its schedule, worst expected runtime, export delay and grace period. Use the receiver's silence/absence facility if available. A SQL-based example after field normalization is:

```sql
SELECT count(*) AS completion_count FROM backup_jobs
WHERE event_name = 'run_completed'
  AND job_id = 'production-job'
  AND environment = 'production'
  AND mode = 'execute'
```

Evaluate over the configured expected-completion lookback and alert on `completion_count = 0`. Verify the receiver evaluates and triggers even when the stream/query has no rows. A grouped `MAX(timestamp)` query often cannot detect a job that never emitted any record; maintain the expected job inventory in receiver configuration. Choose a cooldown to avoid repeated absence emails, and reset it after a valid completion. A failed terminal event proves the job ran but still needs a failure alert; absence monitoring alone is insufficient.

## Deployment acceptance

1. Compare dry-run candidates with the existing tools using the same fixed reference time/account. Test daily/monthly names, invalid dates, month ends and overlapping prefixes. Confirm SQL policy behavior remains unchanged.
2. Verify only intended account targets and container prefixes are configured. Container deletion removes all contents. Interactive deletion needs `DELETE`; cron requires both per-job deletion flags.
3. Verify one start/completion pair and accurate outcome/counts for success, partial failure, dry run, inventory, cancellation and fatal storage errors.
4. Rotate Vault storage credentials and telemetry authorization between scheduled runs. The next run must use the new values without a restart; a revoked/missing value must prevent storage access. Verify Vault Agent refreshes its token file and the application's policy can read only required paths.
5. Reject/stall the receiver and confirm cleanup outcomes remain accurate and export completes within bounded request deadlines. Verify local daily logs retain the terminal summary, do not contain secrets, rotate at UTC midnight and preserve unrelated files.
6. Test actual email delivery, deduplication, absence alerts with an empty stream, and recovery/cooldown behavior. This repository documents alert recipes; it does not create or activate receiver alerts.
7. Deploy one scheduler instance, avoid overlapping external schedulers, and verify service SIGTERM allows in-flight storage operations and telemetry shutdown to finish.

The automated suite uses fake storage clients and loopback receivers; it does not delete production storage or verify production OpenObserve field mapping/email delivery. Switch the old scheduler only after deployment acceptance, then retire the duplicate implementation.
