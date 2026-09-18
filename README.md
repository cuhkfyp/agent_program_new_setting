# CCD Agent Central Sync

This repository contains an opt-in CCD Master ingestion path for the existing
database agent. It solves two separate problems without changing the current
`agent_bulk_sync` Server Script or existing Frappe hooks:

- agents on different computers coordinate through a central Redis lease;
- new CCD Master rows use one validated SQL bulk insert per batch instead of a
  `Document.insert()` call for every row.

Existing registrations remain on `Legacy Document Insert` until a System
Manager changes that registration to `Fast Bulk Insert`.

## Important behavior

- A physical host and a source database are different identities. A host may
  run any number of submitted CCD Registration records/databases.
- Source identity is resolved in this order: `Agent Sync Source ID`, governed
  `Stable CCD Source Key`, complete CCD Registration name. Numeric suffixes are
  never blindly removed.
- The agent computes its delta before requesting capacity. A zero-change run
  does not take a global slot.
- The global slot is released when ingestion finishes. A per-source lease stays
  active while identity and portal work runs on the long queue, preventing a
  second mutation of the same source.
- While that background work is active, the Agent Sync tab reports both the
  processed count and total Master rows for the run (for example,
  `25,900 / 92,068`) and remains in `Post-processing` until completion.
- Fast mode is limited to CCD Master's current static format naming rule. An
  unsupported naming rule fails closed and must use legacy mode.
- A missing or revision-mismatched Master cache never authorizes a clear. The
  bulk macro takes the per-source central lease, verifies that the source is
  empty, and then performs inserts only. A populated source is shown as
  `Reconciliation Required` in the registration's Agent Sync tab.
- A missing CCD Registration cache also never authorizes a clear. Both its
  bulk and legacy paths verify that the generated target is empty before a
  full insert. A populated target fails closed as `Reconciliation Required`,
  preserving the governed deletion-reason and identity-retirement controls.
- Job actions are fail-closed within each registration/database daemon. If the
  CCD Registration action logs an error, the remaining actions are skipped, so
  CCD Master cannot continue for that database during the failed cycle. Other
  databases on the same host retain their independent jobs and locks.
- Agent delta detection never physically deletes CCD Master rows. A removed
  source key fails closed as `Reconciliation Required`; governed registration
  cancellation and identity retirement remain the authority for source
  deletion.
- Mapped `Date` and `Datetime` values that are null, empty, or whitespace-only
  are sent as database `NULL`, preventing strict-SQL failures during CCD-REG
  updates and CCD Master ingestion. Other field types retain their established
  conversion behavior.
- The two namespaced Central Sync OS selections refresh their copied installer
  before draft saves and submission. Runtime installation also repairs stale
  copies on active/draft Central Sync registrations without altering legacy or
  colleague-owned OS templates.

## Repository layout

- `agent/agent_program.py` — sanitized cross-platform agent with central
  coordination and a legacy fallback.
- `agent/setup_windows.bat` — normal Windows installer; it prefers `curl.exe`
  and can use Windows PowerShell as its download fallback.
- `agent/setup_windows_no_powershell.bat` — CMD/curl-only installer for older
  managed computers that cannot run PowerShell.
- `agent/configure_agent.py` — stores endpoint/account values in the OS
  credential store without embedding them in source or generated launchers.
- `server/api_agent_sync.py` — authenticated lease, bulk insert, state, and
  post-processing API.
- `server/agent_sync_setup.py` — idempotent DocType/custom-field installer.
- `deployment/install_runtime.sh` — parameterized Docker deployment for all
  Frappe runtime containers.
- `tests/` — transactional and queue-handoff smoke tests.
- `docs/ARCHITECTURE.md` and `docs/OPERATIONS.md` — design and rollout details.

## Development installation

```bash
./deployment/install_runtime.sh --site frontend --prefix frappe_docker
```

The installer creates `CCD Agent Sync Settings` and an `Agent Sync` tab on
`CCD Registration`. It also adds two namespaced `CCD Agent OS Batch Template`
records—standard Windows and CMD/curl-only—without replacing the existing
`Windows 11`, Ubuntu, or colleague-owned templates. It deliberately leaves the
global switch unchanged.

In Desk:

1. Open **CCD Agent Sync Settings**.
2. Enable **Central Agent Sync** and **Central Coordination**.
3. Set global capacity and batch sizes for the environment.
4. Open one CCD Registration and verify **Agent Sync Source ID** against its
   existing `ccd_reg_source` values.
5. Change only that registration to **Fast Bulk Insert**.

Run a full sync for one canary database and watch its Agent Sync tab. Do not
promote more registrations until both ingestion and post-processing finish.

## Agent configuration

No deployment URL, username, password, cookie, or client database credential is
stored in this repository. On a normal Windows computer, run:

```text
agent\setup_windows.bat
```

For an older computer that cannot run PowerShell, provide `curl.exe` in the
installer directory or Windows `PATH`, then run:

```text
agent\setup_windows_no_powershell.bat
```

Both installers can install Python, download the same checksum-pinned agent
assets, install dependencies, reuse or replace existing Windows Credential
Manager values, and create `Run_Agent.bat`. Neither installer removes
`daemon_logs` or delta-cache files. `CCD_AGENT_ARTIFACT_BASE_URL` may be set by
the deployment environment to use an approved internal artifact origin instead
of the repository default.

The agent logs in over outbound HTTPS and opens a direct outbound WebSocket over
the same ERPNext endpoint. No inbound port or listener is required on the client
computer.

Alternatively, configure the endpoint/account through environment variables:

```text
CCD_ERPNEXT_URL=https://erp.example.org
CCD_ERPNEXT_USER=ccd-agent@example.org
CCD_SITE_NAME=frontend
```

`configure_agent.py` stores `erpnext_url`, `erpnext_user`, `erpnext_pass`, site,
and Socket.IO namespace in the OS keyring under service `ccd_agent`. Client
database passwords are still retrieved through the existing authorized ERPNext
method and are never printed by this version.

## Compatibility and rollback

Fast mode is per registration. To roll back a source, change **CCD Master Sync
Mode** to `Legacy Document Insert`; the agent uses the existing Server Script
route on its next changed run. Disabling **Central Agent Sync** globally also
returns every registration to legacy compatibility mode.

The new server files have unique names and require no `hooks.py` edit. They do
not import or replace `api_identity_retirement.py`, its cancellation hooks, or
the **Cancel with Identity Retirement** action. The installer only owns fields
prefixed `agent_sync_` plus the dedicated settings DocType and index.
