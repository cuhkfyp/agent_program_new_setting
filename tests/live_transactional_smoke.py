"""Transactional development-site smoke test; all SQL changes are rolled back."""

from __future__ import annotations

import uuid
import os

import frappe

from db_connector import api_agent_sync


def run(registration: str) -> dict[str, object]:
    lease: dict[str, object] | None = None
    source = ""
    run_id = f"smoke-{uuid.uuid4().hex}"
    source_key = f"__agent_sync_smoke__{uuid.uuid4().hex}"
    before_count = frappe.db.count("CCD Master")
    try:
        frappe.db.set_single_value("CCD Agent Sync Settings", "enabled", 1)
        frappe.db.set_single_value(
            "CCD Agent Sync Settings", "coordination_enabled", 1
        )
        frappe.db.set_value(
            "CCD Registration",
            registration,
            "agent_sync_mode",
            api_agent_sync.FAST_MODE,
            update_modified=False,
        )
        registration_doc = frappe.get_doc("CCD Registration", registration)
        source = api_agent_sync._effective_source(registration_doc)
        lease = api_agent_sync.acquire_sync_lease(
            registration=registration,
            source_id=source,
            run_id=run_id,
            physical_hostname="transactional-smoke-host",
            database_name="transactional-smoke-db",
        )
        assert lease["acquired"], lease
        result = api_agent_sync.fast_insert_master_batch(
            registration=registration,
            source_id=source,
            lease_token=str(lease["lease_token"]),
            run_id=run_id,
            rows=[
                {
                    "ccd_reg_source": source,
                    "ccd_source_key": source_key,
                }
            ],
        )
        assert result["inserted"] == 1, result
        inserted = frappe.db.get_value(
            "CCD Master",
            {"ccd_reg_source": source, "ccd_source_key": source_key},
            ["name", "agent_sync_run_id", "match_ct"],
            as_dict=True,
        )
        assert inserted, "bulk row not found inside the transaction"
        assert inserted.agent_sync_run_id == run_id
        assert int(inserted.match_ct or 0) == 0
        assert frappe.db.count("CCD Master") == before_count + 1
        return {
            "status": "ok",
            "source_id": source,
            "inserted_name": inserted.name,
            "rolled_back": True,
        }
    finally:
        if lease and lease.get("lease_token") and source:
            try:
                api_agent_sync.release_sync_lease(
                    registration=registration,
                    source_id=source,
                    lease_token=str(lease["lease_token"]),
                    run_id=run_id,
                )
            except Exception:
                pass
        frappe.db.rollback()
        frappe.clear_cache(doctype="CCD Agent Sync Settings")
        frappe.clear_cache(doctype="CCD Registration")
        assert frappe.db.count("CCD Master") == before_count


if __name__ == "__main__":
    registration_name = os.environ.get("CCD_TEST_REGISTRATION", "").strip()
    if not registration_name:
        raise RuntimeError("CCD_TEST_REGISTRATION is required")
    print(run(registration_name))
