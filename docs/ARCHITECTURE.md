# Architecture

## Why the old path was slow

The old endpoint accepted a list but executed `frappe.new_doc(...).insert()` for
every item. Each Master insert therefore ran naming, validation, document hooks,
identity processing, portal indexing, and database commits. Small HTTP batches
did not make this a database bulk operation.

The local shared file lock added a second delay: it covered login, client reads,
mapping, and delta calculation, and it could coordinate only daemons sharing one
filesystem. An unchanged source could wait behind a large source even though it
had nothing to write, while agents on other computers were not coordinated.

## Coordination model

```text
client database -> map + compute delta (no lease)
                          |
                          +-- zero delta -> report state -> done
                          |
                          +-- changed -> acquire per-source lease
                                         + global bounded slot
                                                |
                                                v
                                           ingest changes
                                                |
                              release global slot, retain source lease
                                                |
                                                v
                                  deferred identity + portal work
                                                |
                                                v
                                      release per-source lease
```

Redis Lua scripts make acquire, renew, defer, and release atomic. Leases have
TTLs so a dead agent or worker cannot hold a source forever. Acquire is
idempotent with a client-generated token, which covers an ambiguous HTTP retry.

The global capacity is configurable. Per-source concurrency is always one,
regardless of capacity or how many hosts run agents.

## Source identity

`physical_hostname` is used for discovery and observability, not ownership.
Ownership uses a stable source ID:

1. explicit `agent_sync_source_id` override;
2. governed `ccd_stable_source_key`;
3. complete registration document name.

This supports multiple databases on one computer and submitted registration
revisions without merging unrelated sources.

## Fast insert boundary

`fast_insert_master_batch` requires the integration role, an active lease, the
matching registration/source/run, an enabled per-registration mode, known
scalar metadata fields, and nonempty unique source keys.

It preserves:

- the established `format:...{####}` naming series by reserving a locked range;
- owner/timestamp/docstatus metadata;
- source and run ownership;
- the active deterministic `match_ct` Before Save behavior;
- retry idempotency for already-present source keys.

It deliberately bypasses per-document events during ingestion. The run marker
then drives bounded background batches that execute the existing Unified Person
and identity update handlers in document-event order. The trusted portal adapter
runs after those batches. The registration stays `Post-processing` and keeps its
per-source lease until all downstream work finishes.

Deletes and updates continue through the existing source-scoped API. The new
route accelerates the initial/new-row bottleneck without replacing colleague
logic for those operations.

## State model

Each registration exposes:

- `Idle`, `Waiting`, `Ingesting`, `Post-processing`, `Succeeded`,
  `Completed with errors`, or `Failed`;
- run ID, physical host, client database, timestamps, row progress, result, and
  latest error.

Global controls live in the Single DocType `CCD Agent Sync Settings`.
