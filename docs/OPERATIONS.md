# Operations and rollout

## Settings

`CCD Agent Sync Settings` contains:

- global enable and coordination switches;
- maximum parallel Master ingestions;
- ingestion and post-processing lease TTLs;
- default/maximum fast insert batch sizes;
- post-processing batch size and queue.

Each `CCD Registration` contains:

- optional stable source override;
- legacy/fast mode and batch override;
- read-only runtime state and progress.

Start conservatively. Two parallel ingestions and a 500-row insert batch are
development defaults, not universal production values.

## Canary rollout

1. Back up the site database.
2. Run the installer across every backend, queue, scheduler, and frontend
   runtime. Separate Docker containers may have separate app volumes.
3. Keep all registrations in legacy mode and validate the settings/state UI.
4. Verify the selected registration's source ID against existing CCD Master
   rows before enabling fast mode.
5. Enable one noncritical or development source.
6. Force a controlled full sync and record ingestion throughput.
7. Wait for state `Succeeded`; also review Error Log, queue failures, portal
   coverage, and Unified Person integrity.
8. Compare source counts and a deterministic sample of mapped fields.
9. Increase global capacity or enable another source only after the canary is
   clean.

## Client agent cutover

Stop the old parent process, watchdog, and detached job daemon before starting
the new agent. Install the new files in the same agent directory if the intent
is to reuse the existing `daemon_logs/*_delta_cache.json` checkpoints. Do not
start from a new empty directory for an existing CCD Master source; a missing
checkpoint requires an explicit reconciliation procedure.

Run `agent/setup_windows.bat` for the normal curl/PowerShell-fallback path, or
`agent/setup_windows_no_powershell.bat` for the CMD/curl-only path. Both install
checksum-pinned assets and store the URL, integration account, site, and
Socket.IO namespace in Windows Credential Manager. Then start `Run_Agent.bat`.
Confirm the log shows the governed stable source ID and central configuration.

Do not delete a delta cache merely to test connectivity. Cache absence is not
authorization to delete, retire, or rebuild an existing CCD Master source.

## Governed clear-and-reinsert test

Use `SYNC_TO_CCD_MASTER_BULK` for this test; the legacy macro intentionally
refuses to bootstrap a missing cache because it cannot prove an empty target
under a central lease.

1. Deploy the server modules and enable central synchronization and central
   coordination. Keep the selected registration in `Fast Bulk Insert` mode.
2. Install and run the new agent once against the still-active registration.
   This upgrades an existing version-1 delta cache to the registration-scoped
   version-2 format. This migration is recommended but not required for an
   amended registration.
3. Stop that source's agent/job daemon.
4. In Desk, run **Actions → Cancel with Identity Retirement** and complete its
   fingerprint and reason confirmation. Do not replace this with an agent API
   call or manual SQL deletion.
5. Amend the cancelled registration, submit the amended document, and retain
   the governed stable source key. The amended document name is the new
   registration revision.
6. On the amended draft, select `Windows - Central Sync` or, for a computer
   without PowerShell, `Windows - Central Sync (No PowerShell)`. Submit it and
   use **Agent Information → Click to download**. Install into the existing
   agent folder so logs/caches are preserved, then start the agent. The agent
   discovers the new submitted registration by physical hostname; the server
   rejects cancelled registrations.
7. The agent takes the source lease, verifies that CCD Master has no row for
   the stable source, and performs a non-destructive full insert. It does not
   clear Master and it does not call the identity-retirement service.

If the server finds rows for that source while the cache is absent or belongs
to another registration revision, the run stops before mutation and Desk shows
`Reconciliation Required`. Investigate the source/cache identity instead of
deleting the cache again.

## Deployment command

```bash
./deployment/install_runtime.sh \
  --site frontend \
  --prefix frappe_docker \
  --restart-runtime
```

Omit `--restart-runtime` for the first installation if controlled restarts are
handled separately. Use it for upgrades where a worker may already have the
module imported.

## Validation

Compile/generate checks:

```bash
python3 -m py_compile agent/agent_program.py server/*.py
```

The transactional smoke test requires a development registration and rolls all
SQL changes back:

```text
CCD_TEST_REGISTRATION=<development-registration>
```

The queue-handoff smoke requires both `CCD_TEST_REGISTRATION` and
`CCD_TEST_SOURCE_ID`. It inserts no Master row; it verifies that the long worker
can complete an empty post-processing run.

## Failure handling

- `Waiting`: inspect the same source for an active post-process and check global
  slot capacity. Do not raise capacity merely to bypass a source lease.
- `Failed` with lease expiry: inspect worker availability and Error Log. The TTL
  makes the source recoverable, but determine why heartbeats stopped first.
- insert HTTP ambiguity: the agent reconciles source keys before retrying; lease
  acquisition also reuses the same token.
- downstream errors: keep fast mode limited to the canary, resolve the logged
  record failures, then rerun. The delta checkpoint preserves confirmed rows.
- `Reconciliation Required`: read **Latest Error**. Missing/mismatched cache
  with a populated source and client-side source-key deletions are deliberately
  fail-closed; no Master clearing or deletion was attempted.
- unsupported autoname: use legacy mode until a reviewed allocator is added for
  that exact naming rule.

## Production promotion

Use environment-specific settings rather than changing code. Re-run the same
idempotent installer in production, leave the global switch off, validate the
UI/schema, then follow the canary process. Never copy site configuration,
credentials, logs, delta caches, or client extracts into this repository.
