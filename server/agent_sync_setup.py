"""Idempotent UI/schema setup for centralized CCD agent synchronization."""

from __future__ import annotations

from typing import Any

import frappe


SETTINGS_DOCTYPE = "CCD Agent Sync Settings"


SETTINGS_FIELDS: list[dict[str, Any]] = [
    {
        "fieldname": "master_sync_section",
        "fieldtype": "Section Break",
        "label": "CCD Master Synchronization",
    },
    {
        "fieldname": "enabled",
        "fieldtype": "Check",
        "label": "Enable Central Agent Sync",
        "default": "0",
        "description": "Master switch. Registrations stay on the legacy path while disabled.",
    },
    {
        "fieldname": "coordination_enabled",
        "fieldtype": "Check",
        "label": "Enable Central Coordination",
        "default": "1",
        "description": "Coordinate agents across different hosts through Redis leases.",
    },
    {
        "fieldname": "limits_column",
        "fieldtype": "Column Break",
    },
    {
        "fieldname": "max_parallel_master_syncs",
        "fieldtype": "Int",
        "label": "Maximum Parallel Master Ingestions",
        "default": "2",
        "description": "Global capacity across all hosts. Per-source exclusion always remains one.",
    },
    {
        "fieldname": "lease_seconds",
        "fieldtype": "Int",
        "label": "Ingestion Lease Seconds",
        "default": "1800",
    },
    {
        "fieldname": "postprocess_lease_seconds",
        "fieldtype": "Int",
        "label": "Post-processing Lease Seconds",
        "default": "86400",
    },
    {
        "fieldname": "batch_section",
        "fieldtype": "Section Break",
        "label": "Batch and Queue Settings",
    },
    {
        "fieldname": "default_batch_size",
        "fieldtype": "Int",
        "label": "Default Fast Insert Batch Size",
        "default": "500",
    },
    {
        "fieldname": "maximum_batch_size",
        "fieldtype": "Int",
        "label": "Maximum Fast Insert Batch Size",
        "default": "1000",
    },
    {
        "fieldname": "postprocess_column",
        "fieldtype": "Column Break",
    },
    {
        "fieldname": "postprocess_batch_size",
        "fieldtype": "Int",
        "label": "Post-processing Batch Size",
        "default": "100",
    },
    {
        "fieldname": "postprocess_queue",
        "fieldtype": "Select",
        "label": "Post-processing Queue",
        "options": "long\ndefault\nshort",
        "default": "long",
    },
]


def _create_or_upgrade_settings_doctype() -> dict[str, Any]:
    created = not frappe.db.exists("DocType", SETTINGS_DOCTYPE)
    if created:
        frappe.get_doc(
            {
                "doctype": "DocType",
                "name": SETTINGS_DOCTYPE,
                "module": "Db Connector",
                "custom": 1,
                "issingle": 1,
                "track_changes": 1,
                "fields": SETTINGS_FIELDS,
                "permissions": [
                    {
                        "role": "System Manager",
                        "read": 1,
                        "write": 1,
                        "create": 1,
                        "print": 1,
                        "email": 1,
                    }
                ],
            }
        ).insert(ignore_permissions=True)
    else:
        doctype = frappe.get_doc("DocType", SETTINGS_DOCTYPE)
        existing = {field.fieldname for field in doctype.fields}
        changed = False
        for field in SETTINGS_FIELDS:
            if field["fieldname"] not in existing:
                doctype.append("fields", field)
                changed = True
        if changed:
            doctype.save(ignore_permissions=True)
    frappe.clear_cache(doctype=SETTINGS_DOCTYPE)
    return {"created": created, "name": SETTINGS_DOCTYPE}


def _registration_insert_after() -> str:
    meta = frappe.get_meta("CCD Registration")
    fieldnames = {field.fieldname for field in meta.fields}
    for candidate in (
        "custom_service_start_date",
        "service_name",
        "ccd_reg_doctype",
        "agent_status",
    ):
        if candidate in fieldnames:
            return candidate
    return meta.fields[-1].fieldname if meta.fields else ""


def _custom_fields() -> dict[str, list[dict[str, Any]]]:
    insert_after = _registration_insert_after()
    common = {"allow_on_submit": 1, "no_copy": 1}
    return {
        "CCD Registration": [
            {
                "fieldname": "agent_sync_tab",
                "fieldtype": "Tab Break",
                "label": "Agent Sync",
                "insert_after": insert_after,
                **common,
            },
            {
                "fieldname": "agent_sync_settings_section",
                "fieldtype": "Section Break",
                "label": "Database Identity and Mode",
                "insert_after": "agent_sync_tab",
                **common,
            },
            {
                "fieldname": "agent_sync_source_id",
                "fieldtype": "Data",
                "label": "Agent Sync Source ID",
                "insert_after": "agent_sync_settings_section",
                "description": (
                    "Optional override for this database identity. Leave blank to use "
                    "Stable CCD Source Key, then the complete Registration name."
                ),
                **common,
            },
            {
                "fieldname": "agent_sync_mode",
                "fieldtype": "Select",
                "label": "CCD Master Sync Mode",
                "options": "Legacy Document Insert\nFast Bulk Insert",
                "default": "Legacy Document Insert",
                "insert_after": "agent_sync_source_id",
                "description": "Fast mode is opt-in for each database.",
                **common,
            },
            {
                "fieldname": "agent_sync_batch_size",
                "fieldtype": "Int",
                "label": "Fast Insert Batch Size Override",
                "default": "0",
                "insert_after": "agent_sync_mode",
                "description": "Zero uses the global setting.",
                **common,
            },
            {
                "fieldname": "agent_sync_state_section",
                "fieldtype": "Section Break",
                "label": "Current State",
                "insert_after": "agent_sync_batch_size",
                **common,
            },
            {
                "fieldname": "agent_sync_state",
                "fieldtype": "Select",
                "label": "Sync State",
                "options": (
                    "Idle\nWaiting\nIngesting\nPost-processing\nSucceeded\n"
                    "Completed with errors\nReconciliation Required\nFailed"
                ),
                "default": "Idle",
                "read_only": 1,
                "insert_after": "agent_sync_state_section",
                **common,
            },
            {
                "fieldname": "agent_sync_run_id",
                "fieldtype": "Data",
                "label": "Current Run ID",
                "read_only": 1,
                "insert_after": "agent_sync_state",
                **common,
            },
            {
                "fieldname": "agent_sync_rows_processed",
                "fieldtype": "Int",
                "label": "Rows Processed",
                "read_only": 1,
                "insert_after": "agent_sync_run_id",
                **common,
            },
            {
                "fieldname": "agent_sync_state_column",
                "fieldtype": "Column Break",
                "insert_after": "agent_sync_rows_processed",
                **common,
            },
            {
                "fieldname": "agent_sync_active_host",
                "fieldtype": "Data",
                "label": "Active Physical Host",
                "read_only": 1,
                "insert_after": "agent_sync_state_column",
                **common,
            },
            {
                "fieldname": "agent_sync_active_database",
                "fieldtype": "Data",
                "label": "Active Client Database",
                "read_only": 1,
                "insert_after": "agent_sync_active_host",
                **common,
            },
            {
                "fieldname": "agent_sync_started_at",
                "fieldtype": "Datetime",
                "label": "Started At",
                "read_only": 1,
                "insert_after": "agent_sync_active_database",
                **common,
            },
            {
                "fieldname": "agent_sync_heartbeat_at",
                "fieldtype": "Datetime",
                "label": "Last Heartbeat",
                "read_only": 1,
                "insert_after": "agent_sync_started_at",
                **common,
            },
            {
                "fieldname": "agent_sync_finished_at",
                "fieldtype": "Datetime",
                "label": "Finished At",
                "read_only": 1,
                "insert_after": "agent_sync_heartbeat_at",
                **common,
            },
            {
                "fieldname": "agent_sync_result_section",
                "fieldtype": "Section Break",
                "label": "Latest Result",
                "insert_after": "agent_sync_finished_at",
                **common,
            },
            {
                "fieldname": "agent_sync_last_result",
                "fieldtype": "Small Text",
                "label": "Latest Result",
                "read_only": 1,
                "insert_after": "agent_sync_result_section",
                **common,
            },
            {
                "fieldname": "agent_sync_last_error",
                "fieldtype": "Small Text",
                "label": "Latest Error",
                "read_only": 1,
                "insert_after": "agent_sync_last_result",
                **common,
            },
        ],
        "CCD Master": [
            {
                "fieldname": "agent_sync_run_id",
                "fieldtype": "Data",
                "label": "Agent Sync Run ID",
                "read_only": 1,
                "hidden": 1,
                "no_copy": 1,
                "insert_after": "ccd_source_key",
            }
        ],
    }


def _initialize_defaults() -> None:
    defaults = {
        "enabled": 0,
        "coordination_enabled": 1,
        "max_parallel_master_syncs": 2,
        "lease_seconds": 1800,
        "postprocess_lease_seconds": 86400,
        "default_batch_size": 500,
        "maximum_batch_size": 1000,
        "postprocess_batch_size": 100,
        "postprocess_queue": "long",
    }
    for fieldname, value in defaults.items():
        current = frappe.db.get_single_value(SETTINGS_DOCTYPE, fieldname)
        is_uninitialized = current in (None, "") or (
            fieldname != "enabled" and current == 0
        )
        if is_uninitialized:
            frappe.db.set_single_value(SETTINGS_DOCTYPE, fieldname, value)


def _add_indexes() -> list[str]:
    indexes = []
    index_name = "idx_ccd_master_agent_sync_run"
    if not frappe.db.has_index("tabCCD Master", index_name):
        frappe.db.add_index(
            "CCD Master",
            ["ccd_reg_source", "agent_sync_run_id", "name"],
            index_name=index_name,
        )
        indexes.append(index_name)
    return indexes


@frappe.whitelist()
def install() -> dict[str, Any]:
    if "System Manager" not in set(frappe.get_roles()):
        frappe.throw("System Manager role is required", frappe.PermissionError)
    settings = _create_or_upgrade_settings_doctype()
    from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

    create_custom_fields(_custom_fields(), update=True)
    _initialize_defaults()
    indexes = _add_indexes()
    frappe.clear_cache(doctype="CCD Registration")
    frappe.clear_cache(doctype="CCD Master")
    return {
        "settings": settings,
        "custom_fields": [
            "CCD Registration-agent_sync_tab",
            "CCD Registration-agent_sync_source_id",
            "CCD Registration-agent_sync_mode",
            "CCD Registration-agent_sync_state",
            "CCD Master-agent_sync_run_id",
        ],
        "indexes_added": indexes,
        "enabled": bool(frappe.db.get_single_value(SETTINGS_DOCTYPE, "enabled")),
    }
