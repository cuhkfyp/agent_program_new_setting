"""Non-destructive live smoke test for lease transfer to the long worker."""

from __future__ import annotations

import uuid
import os

import frappe

from db_connector import api_agent_sync


registration = os.environ.get("CCD_TEST_REGISTRATION", "").strip()
source_id = os.environ.get("CCD_TEST_SOURCE_ID", "").strip()
if not registration or not source_id:
    raise RuntimeError("CCD_TEST_REGISTRATION and CCD_TEST_SOURCE_ID are required")
run_id = f"postprocess-smoke-{uuid.uuid4().hex}"
lease = api_agent_sync.acquire_sync_lease(
    registration=registration,
    source_id=source_id,
    run_id=run_id,
    physical_hostname="transactional-smoke-host",
    database_name="transactional-smoke-db",
)
assert lease["acquired"], lease
result = api_agent_sync.finish_sync(
    registration=registration,
    source_id=source_id,
    lease_token=str(lease["lease_token"]),
    run_id=run_id,
    full_sync=0,
    changed_source_keys=[],
    deleted_source_keys=[],
)
frappe.db.commit()
print({"run_id": run_id, "finish": result})
