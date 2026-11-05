"""Idempotent UI/schema setup for centralized CCD agent synchronization."""

from __future__ import annotations

from pathlib import Path
from textwrap import indent
from typing import Any

import frappe


SETTINGS_DOCTYPE = "CCD Agent Sync Settings"
AGENT_TEMPLATE_DOCTYPE = "CCD Agent OS Batch Template"
AGENT_TEMPLATE_ASSETS = {
    "Windows - Central Sync": "setup_windows.bat",
    "Windows - Central Sync (No PowerShell)": "setup_windows_no_powershell.bat",
}
AGENT_TEMPLATE_GUARD_EVENTS = {
    "CCD Central Agent Template Before Save": "Before Save",
    "CCD Central Agent Template Before Submit": "Before Submit",
}
AGENT_TEMPLATE_GUARD_SCRIPT = """central_agent_templates = (
    "Windows - Central Sync",
    "Windows - Central Sync (No PowerShell)",
)

if doc.get("agent_os") in central_agent_templates:
    central_template = frappe.get_doc(
        "CCD Agent OS Batch Template", doc.get("agent_os")
    )
    central_template_content = central_template.get("os_template") or ""
    if not central_template_content:
        frappe.throw("The selected Central Sync agent template is empty")
    doc.agent_installation = central_template_content
"""

LEGACY_AGENT_BULK_SYNC_SCRIPT = "Agent bulk sync"
AGENT_V3_FENCE_MARKER = "# CCD Agent V3 governed staging fence"

REGISTRATION_VALIDATION_BODY = """physical_hostname = str(doc.get("physical_hostname") or "").strip()
database_name = str(doc.get("db_database") or "").strip()
table_name = str(doc.get("ccd_table") or "").strip()
register_name = str(doc.get("ccd_reg_doctype") or "").strip()

if not physical_hostname:
    frappe.throw("Physical Hostname is required before saving CCD Registration")

if not register_name.startswith("CCD-REG-"):
    frappe.throw("CCD Register Name must start with CCD-REG-")

register_suffix = register_name[len("CCD-REG-"):]
allowed_characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
invalid_character = ""
for character in register_suffix:
    if character not in allowed_characters:
        invalid_character = character
        break
if not register_suffix or invalid_character:
    frappe.throw(
        "CCD Register Name may contain only letters, numbers, underscore, and hyphen after CCD-REG-"
    )

table_component = table_name.rsplit(".", 1)[-1]
if table_name == "*** Customize ***":
    table_component = ""

register_name_lower = register_name.lower()
source_components = (
    ("Physical Hostname", physical_hostname),
    ("Database name", database_name),
    ("CCD table", table_component),
)
matched_components = []
for label, value in source_components:
    if value and value.lower() in register_name_lower:
        matched_components.append(label)

if len(matched_components) < 2:
    matched_text = ", ".join(matched_components) or "none"
    frappe.throw(
        "CCD Register Name must contain at least two source components: "
        "Physical Hostname, Database name, and CCD table. "
        "Matched components: " + matched_text
    )
"""

REGISTRATION_VALIDATION_SERVER_SCRIPTS = {
    "CCD Registration Routing Before Save": {
        "doctype_event": "Before Save",
        "script": "if doc.docstatus == 0:\n" + indent(REGISTRATION_VALIDATION_BODY, "    "),
    },
    "CCD Registration Routing Before Submit": {
        "doctype_event": "Before Submit",
        "script": REGISTRATION_VALIDATION_BODY,
    },
}

REGISTRATION_VALIDATION_CLIENT_SCRIPT_NAME = "CCD Registration Routing Validation"
REGISTRATION_VALIDATION_CLIENT_SCRIPT = r"""function ccd_registration_validate_routing(frm) {
    if (frm.doc.docstatus !== 0) {
        return;
    }

    const physical_hostname = String(frm.doc.physical_hostname || '').trim();
    const database_name = String(frm.doc.db_database || '').trim();
    const table_name = String(frm.doc.ccd_table || '').trim();
    const register_name = String(frm.doc.ccd_reg_doctype || '').trim();

    if (!physical_hostname) {
        frappe.throw(__('Physical Hostname is required before saving CCD Registration'));
    }
    if (!register_name.startsWith('CCD-REG-')) {
        frappe.throw(__('CCD Register Name must start with CCD-REG-'));
    }

    const register_suffix = register_name.slice('CCD-REG-'.length);
    if (!register_suffix || !/^[A-Za-z0-9_-]+$/.test(register_suffix)) {
        frappe.throw(__('CCD Register Name may contain only letters, numbers, underscore, and hyphen after CCD-REG-'));
    }

    let table_component = table_name.split('.').pop() || '';
    if (table_name === '*** Customize ***') {
        table_component = '';
    }
    const register_name_lower = register_name.toLowerCase();
    const source_components = [
        ['Physical Hostname', physical_hostname],
        ['Database name', database_name],
        ['CCD table', table_component]
    ];
    const matched_components = source_components
        .filter((entry) => entry[1] && register_name_lower.includes(entry[1].toLowerCase()))
        .map((entry) => entry[0]);

    if (matched_components.length < 2) {
        frappe.throw(__(
            'CCD Register Name must contain at least two source components: Physical Hostname, Database name, and CCD table. Matched components: {0}',
            [matched_components.join(', ') || 'none']
        ));
    }
}

frappe.ui.form.on('CCD Registration', {
    setup(frm) {
        frm.set_df_property('physical_hostname', 'reqd', 1);
        frm.set_df_property('ccd_reg_doctype', 'reqd', 1);
    },
    refresh(frm) {
        frm.set_df_property('physical_hostname', 'reqd', 1);
        frm.set_df_property('ccd_reg_doctype', 'reqd', 1);
    },
    validate(frm) {
        ccd_registration_validate_routing(frm);
    }
});
"""

AGENT_LOCALHOST_FIELD = {
    "fieldname": "agent_localhost",
    "label": "Use localhost instead of the server URL",
    "fieldtype": "Check",
    "default": "0",
    "insert_after": "agent_tx_type",
    "allow_on_submit": 1,
    "description": (
        "When enabled, the installed agent connects to 127.0.0.1. "
        "Desk connection tests continue to use Server/URL."
    ),
}


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
        "fieldname": "enforce_agent_v3_staging_fence",
        "fieldtype": "Check",
        "label": "Require Agent V3 for CCD Registration Staging",
        "default": "0",
        "description": (
            "Transition switch. Leave disabled while any V2 client remains. "
            "After every client machine is on Agent V3, enable this to reject "
            "the legacy unfenced CCD-REG write endpoint."
        ),
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
    {
        "fieldname": "agent_mail_section",
        "fieldtype": "Section Break",
        "label": "Agent Failure Email Defaults",
        "description": (
            "Default [MAIL] values generated for CCD client agents. Existing "
            "registration-specific [MAIL] values remain authoritative."
        ),
    },
    {
        "fieldname": "agent_mail_smtp_server",
        "fieldtype": "Data",
        "label": "SMTP Server",
        "default": "smtp.gmail.com",
    },
    {
        "fieldname": "agent_mail_smtp_port",
        "fieldtype": "Int",
        "label": "SMTP Port",
        "default": "587",
    },
    {
        "fieldname": "agent_mail_smtp_user",
        "fieldtype": "Data",
        "label": "SMTP User",
        "default": "ben.kong@rehabsociety.org.hk",
    },
    {
        "fieldname": "agent_mail_column",
        "fieldtype": "Column Break",
    },
    {
        "fieldname": "agent_mail_smtp_password",
        "fieldtype": "Password",
        "label": "SMTP App Password",
        "description": "Stored encrypted; initially seeded from the established Ben agent config.",
    },
    {
        "fieldname": "agent_mail_notify_email",
        "fieldtype": "Data",
        "label": "Notify Email",
        "default": "ben.kong@rehabsociety.org.hk",
    },
]


LEGACY_AFTER_SUBMIT_SCRIPT = "CCD Registration After Submit V2"
LEGACY_AFTER_SUBMIT_COMMENT = """# Comment by Ben (2026-10-30):
# This Server Script is intentionally disabled. It mixed the original staging
# DocType/workspace workflow with experimental custom-SQL configuration code.
# The complete original workflow plus guarded custom-SQL, three sync actions,
# and [MAIL] generation now lives in managed application code:
# db_connector.ccd_registration_provisioning
# The prior source remains below and in Version history for audit/reference.

"""


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
                "fieldname": "agent_retirement_safety_section",
                "fieldtype": "Section Break",
                "label": "Governed Retirement Safety",
                "insert_after": "agent_sync_batch_size",
                "description": (
                    "A System Manager must stop the client agent and place a "
                    "server-side hold before retiring this source."
                ),
                **common,
            },
            {
                "fieldname": "agent_retirement_hold",
                "fieldtype": "Check",
                "label": "Agent Retirement Hold",
                "default": "0",
                "read_only": 1,
                "insert_after": "agent_retirement_safety_section",
                "description": (
                    "Blocks new central-sync leases and agent writes while the "
                    "governed source-retirement operation is prepared."
                ),
                **common,
            },
            {
                "fieldname": "agent_retirement_hold_at",
                "fieldtype": "Datetime",
                "label": "Last Retirement Hold At",
                "read_only": 1,
                "insert_after": "agent_retirement_hold",
                **common,
            },
            {
                "fieldname": "agent_retirement_hold_column",
                "fieldtype": "Column Break",
                "insert_after": "agent_retirement_hold_at",
                **common,
            },
            {
                "fieldname": "agent_retirement_hold_by",
                "fieldtype": "Link",
                "options": "User",
                "label": "Last Retirement Hold By",
                "read_only": 1,
                "insert_after": "agent_retirement_hold_column",
                **common,
            },
            {
                "fieldname": "agent_retirement_hold_note",
                "fieldtype": "Small Text",
                "label": "Latest Retirement Hold Event",
                "read_only": 1,
                "insert_after": "agent_retirement_hold_by",
                **common,
            },
            {
                "fieldname": "agent_sync_state_section",
                "fieldtype": "Section Break",
                "label": "Current State",
                "insert_after": "agent_retirement_hold_note",
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
            {
                "fieldname": "agent_baseline_adoption_section",
                "fieldtype": "Section Break",
                "label": "Governed Existing Master Baseline Adoption",
                "insert_after": "agent_sync_last_error",
                "description": (
                    "Manager-authorized, zero-write recovery for an exact existing "
                    "CCD Master population when the current client cache is missing."
                ),
                **common,
            },
            {
                "fieldname": "agent_baseline_adoption_state",
                "fieldtype": "Select",
                "label": "Baseline Adoption State",
                "options": "Not Requested\nAuthorized\nIn Progress\nCompleted\nFailed",
                "default": "Not Requested",
                "read_only": 1,
                "insert_after": "agent_baseline_adoption_section",
                **common,
            },
            {
                "fieldname": "agent_baseline_adoption_expected_count",
                "fieldtype": "Int",
                "label": "Expected Existing Rows",
                "read_only": 1,
                "insert_after": "agent_baseline_adoption_state",
                **common,
            },
            {
                "fieldname": "agent_baseline_staging_progress",
                "fieldtype": "Int",
                "label": "Staging Rows Confirmed",
                "read_only": 1,
                "insert_after": "agent_baseline_adoption_expected_count",
                **common,
            },
            {
                "fieldname": "agent_baseline_adoption_progress",
                "fieldtype": "Int",
                "label": "Master Rows Confirmed",
                "read_only": 1,
                "insert_after": "agent_baseline_staging_progress",
                **common,
            },
            {
                "fieldname": "agent_baseline_adoption_column",
                "fieldtype": "Column Break",
                "insert_after": "agent_baseline_adoption_progress",
                **common,
            },
            {
                "fieldname": "agent_baseline_adoption_event",
                "fieldtype": "Small Text",
                "label": "Latest Baseline Adoption Event",
                "read_only": 1,
                "insert_after": "agent_baseline_adoption_column",
                **common,
            },
            {
                "fieldname": "agent_baseline_adoption_payload",
                "fieldtype": "Long Text",
                "label": "Baseline Adoption Authorization Payload",
                "read_only": 1,
                "hidden": 1,
                "insert_after": "agent_baseline_adoption_event",
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
        "enforce_agent_v3_staging_fence": 0,
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


def _initialize_agent_mail_defaults() -> dict[str, Any]:
    """Seed encrypted mail defaults from the established live Ben config."""
    from db_connector.ccd_registration_provisioning import (
        mail_defaults_from_existing_registration,
    )

    settings = frappe.get_single(SETTINGS_DOCTYPE)
    existing_password = str(
        settings.get_password("agent_mail_smtp_password", raise_exception=False) or ""
    )
    legacy = mail_defaults_from_existing_registration()
    defaults = {
        "agent_mail_smtp_server": "smtp.gmail.com",
        "agent_mail_smtp_port": 587,
        "agent_mail_smtp_user": "ben.kong@rehabsociety.org.hk",
        "agent_mail_notify_email": "ben.kong@rehabsociety.org.hk",
    }
    mapping = {
        "agent_mail_smtp_server": "SMTP_SERVER",
        "agent_mail_smtp_port": "SMTP_PORT",
        "agent_mail_smtp_user": "SMTP_USER",
        "agent_mail_smtp_password": "SMTP_PASS",
        "agent_mail_notify_email": "NOTIFY_EMAIL",
    }
    changed_fields = []
    for fieldname, mail_key in mapping.items():
        current = (
            existing_password
            if fieldname == "agent_mail_smtp_password"
            else settings.get(fieldname)
        )
        if current not in (None, "", 0):
            continue
        value = legacy.get(mail_key) or defaults.get(fieldname)
        if value in (None, ""):
            continue
        settings.set(fieldname, value)
        changed_fields.append(fieldname)
    if changed_fields:
        settings.save(ignore_permissions=True)
    return {
        "updated": bool(changed_fields),
        "fields": changed_fields,
        "password_available": bool(
            existing_password or legacy.get("SMTP_PASS")
        ),
    }


def _disable_legacy_registration_after_submit() -> dict[str, Any]:
    """Retain the colleague script for audit while preventing double writes."""
    if not frappe.db.exists("Server Script", LEGACY_AFTER_SUBMIT_SCRIPT):
        return {"name": LEGACY_AFTER_SUBMIT_SCRIPT, "found": False, "updated": False}
    script = frappe.get_doc("Server Script", LEGACY_AFTER_SUBMIT_SCRIPT)
    changed = False
    if not str(script.get("script") or "").startswith("# Comment by Ben (2026-10-30):"):
        script.script = LEGACY_AFTER_SUBMIT_COMMENT + str(script.get("script") or "")
        changed = True
    if not script.get("disabled"):
        script.disabled = 1
        changed = True
    if changed:
        script.save(ignore_permissions=True)
    return {"name": script.name, "found": True, "updated": changed, "disabled": 1}


def _install_agent_templates() -> list[dict[str, Any]]:
    """Upsert only namespaced templates; preserve all existing colleague templates."""
    if not frappe.db.exists("DocType", AGENT_TEMPLATE_DOCTYPE):
        return []
    asset_directory = Path(__file__).with_name("agent_assets")
    results = []
    for template_name, filename in AGENT_TEMPLATE_ASSETS.items():
        asset_path = asset_directory / filename
        if not asset_path.is_file():
            frappe.throw(f"Required agent template asset is missing: {filename}")
        content = asset_path.read_text(encoding="utf-8")
        created = not frappe.db.exists(AGENT_TEMPLATE_DOCTYPE, template_name)
        if created:
            frappe.get_doc(
                {
                    "doctype": AGENT_TEMPLATE_DOCTYPE,
                    "name": template_name,
                    "agent_os": template_name,
                    "os_template": content,
                }
            ).insert(ignore_permissions=True)
            changed = True
        else:
            template = frappe.get_doc(AGENT_TEMPLATE_DOCTYPE, template_name)
            changed = bool(
                str(template.get("agent_os") or "") != template_name
                or str(template.get("os_template") or "") != content
            )
            if changed:
                template.agent_os = template_name
                template.os_template = content
                template.save(ignore_permissions=True)
        results.append(
            {"name": template_name, "created": created, "updated": changed}
        )
    return results


def _install_agent_template_guards() -> list[dict[str, Any]]:
    """Refresh only namespaced Central Sync installers during document saves."""
    results = []
    for script_name, event in AGENT_TEMPLATE_GUARD_EVENTS.items():
        values = {
            "script_type": "DocType Event",
            "reference_doctype": "CCD Registration",
            "doctype_event": event,
            "script": AGENT_TEMPLATE_GUARD_SCRIPT,
            "disabled": 0,
        }
        created = not frappe.db.exists("Server Script", script_name)
        if created:
            frappe.get_doc(
                {"doctype": "Server Script", "name": script_name, **values}
            ).insert(ignore_permissions=True)
            changed = True
        else:
            server_script = frappe.get_doc("Server Script", script_name)
            changed = any(server_script.get(key) != value for key, value in values.items())
            if changed:
                server_script.update(values)
                server_script.save(ignore_permissions=True)
        results.append({"name": script_name, "created": created, "updated": changed})
    return results


def _install_legacy_agent_v3_guard() -> dict[str, Any]:
    """Place a default-off V3 transition guard in the legacy API script.

    The original script body remains intact. While the setting is disabled this
    call is a no-op, so existing V2 machines keep working during the rollout.
    """
    if not frappe.db.exists("Server Script", LEGACY_AGENT_BULK_SYNC_SCRIPT):
        return {
            "name": LEGACY_AGENT_BULK_SYNC_SCRIPT,
            "found": False,
            "updated": False,
        }
    server_script = frappe.get_doc("Server Script", LEGACY_AGENT_BULK_SYNC_SCRIPT)
    source = str(server_script.get("script") or "")
    if AGENT_V3_FENCE_MARKER in source:
        return {"name": server_script.name, "found": True, "updated": False}

    lines = source.splitlines()
    assignment_index = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip().startswith("doctype")
            and 'data.get("doctype"' in line
        ),
        None,
    )
    if assignment_index is None:
        frappe.throw(
            "The Agent bulk sync Server Script has an unknown layout; the V3 "
            "transition guard was not installed"
        )
    indent_prefix = lines[assignment_index][
        : len(lines[assignment_index]) - len(lines[assignment_index].lstrip())
    ]
    guard_lines = [
        f"{indent_prefix}{AGENT_V3_FENCE_MARKER}",
        f"{indent_prefix}frappe.call(",
        f'{indent_prefix}    "db_connector.api_agent_sync.guard_legacy_agent_bulk_sync",',
        f"{indent_prefix}    doctype=doctype,",
        f"{indent_prefix})",
    ]
    lines[assignment_index + 1 : assignment_index + 1] = guard_lines
    server_script.script = "\n".join(lines)
    server_script.save(ignore_permissions=True)
    return {"name": server_script.name, "found": True, "updated": True}


def _install_registration_validation_scripts() -> dict[str, Any]:
    """Install isolated client/server guards for registration routing fields."""
    server_results = []
    for script_name, definition in REGISTRATION_VALIDATION_SERVER_SCRIPTS.items():
        values = {
            "script_type": "DocType Event",
            "reference_doctype": "CCD Registration",
            "doctype_event": definition["doctype_event"],
            "script": definition["script"],
            "disabled": 0,
        }
        created = not frappe.db.exists("Server Script", script_name)
        if created:
            frappe.get_doc(
                {"doctype": "Server Script", "name": script_name, **values}
            ).insert(ignore_permissions=True)
            changed = True
        else:
            server_script = frappe.get_doc("Server Script", script_name)
            changed = any(
                server_script.get(key) != value for key, value in values.items()
            )
            if changed:
                server_script.update(values)
                server_script.save(ignore_permissions=True)
        server_results.append(
            {"name": script_name, "created": created, "updated": changed}
        )

    client_values = {
        "dt": "CCD Registration",
        "view": "Form",
        "enabled": 1,
        "script": REGISTRATION_VALIDATION_CLIENT_SCRIPT,
    }
    client_created = not frappe.db.exists(
        "Client Script", REGISTRATION_VALIDATION_CLIENT_SCRIPT_NAME
    )
    if client_created:
        frappe.get_doc(
            {
                "doctype": "Client Script",
                "name": REGISTRATION_VALIDATION_CLIENT_SCRIPT_NAME,
                **client_values,
            }
        ).insert(ignore_permissions=True)
        client_changed = True
    else:
        client_script = frappe.get_doc(
            "Client Script", REGISTRATION_VALIDATION_CLIENT_SCRIPT_NAME
        )
        client_changed = any(
            client_script.get(key) != value
            for key, value in client_values.items()
        )
        if client_changed:
            client_script.update(client_values)
            client_script.save(ignore_permissions=True)

    return {
        "server_scripts": server_results,
        "client_script": {
            "name": REGISTRATION_VALIDATION_CLIENT_SCRIPT_NAME,
            "created": client_created,
            "updated": client_changed,
        },
    }


def _ensure_agent_localhost_field() -> dict[str, Any]:
    """Preserve the supervisor's field and make the agent-only switch reusable."""
    row_name = frappe.db.get_value(
        "DocField",
        {
            "parent": "CCD Registration",
            "parenttype": "DocType",
            "fieldname": AGENT_LOCALHOST_FIELD["fieldname"],
        },
        "name",
    )
    if row_name:
        current = frappe.db.get_value(
            "DocField",
            row_name,
            ["label", "fieldtype", "allow_on_submit", "description"],
            as_dict=True,
        )
        values = {
            "label": AGENT_LOCALHOST_FIELD["label"],
            "fieldtype": AGENT_LOCALHOST_FIELD["fieldtype"],
            "allow_on_submit": AGENT_LOCALHOST_FIELD["allow_on_submit"],
            "description": AGENT_LOCALHOST_FIELD["description"],
        }
        changed = any(current.get(key) != value for key, value in values.items())
        if changed:
            frappe.db.set_value(
                "DocField", row_name, values, update_modified=False
            )
        return {
            "created": False,
            "updated": changed,
            "fieldname": AGENT_LOCALHOST_FIELD["fieldname"],
        }

    doctype = frappe.get_doc("DocType", "CCD Registration")
    doctype.append("fields", AGENT_LOCALHOST_FIELD)
    doctype.save(ignore_permissions=True)
    return {
        "created": True,
        "updated": True,
        "fieldname": AGENT_LOCALHOST_FIELD["fieldname"],
    }


def _refresh_active_agent_installations() -> list[str]:
    """Repair stale copied installers without touching legacy OS selections."""
    templates = {
        name: frappe.db.get_value(AGENT_TEMPLATE_DOCTYPE, name, "os_template") or ""
        for name in AGENT_TEMPLATE_ASSETS
    }
    registrations = frappe.get_all(
        "CCD Registration",
        filters={
            "docstatus": ["in", [0, 1]],
            "agent_os": ["in", list(AGENT_TEMPLATE_ASSETS)],
        },
        fields=["name", "agent_os", "agent_installation"],
        limit_page_length=0,
    )
    updated = []
    for registration in registrations:
        expected = templates.get(registration.agent_os, "")
        if expected and str(registration.agent_installation or "") != expected:
            frappe.db.set_value(
                "CCD Registration",
                registration.name,
                "agent_installation",
                expected,
                update_modified=False,
            )
            updated.append(registration.name)
    return updated


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


def _apply_setup() -> dict[str, Any]:
    settings = _create_or_upgrade_settings_doctype()
    from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

    create_custom_fields(_custom_fields(), update=True)
    agent_localhost_field = _ensure_agent_localhost_field()
    _initialize_defaults()
    agent_mail_defaults = _initialize_agent_mail_defaults()
    agent_templates = _install_agent_templates()
    agent_template_guards = _install_agent_template_guards()
    legacy_agent_v3_guard = _install_legacy_agent_v3_guard()
    registration_validation = _install_registration_validation_scripts()
    refreshed_agent_installations = _refresh_active_agent_installations()
    legacy_after_submit = _disable_legacy_registration_after_submit()
    from db_connector.api_agent_sync import repair_inherited_amended_sync_state

    inherited_sync_state_repair = repair_inherited_amended_sync_state()
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
        "agent_localhost_field": agent_localhost_field,
        "agent_mail_defaults": agent_mail_defaults,
        "agent_templates": agent_templates,
        "agent_template_guards": agent_template_guards,
        "legacy_agent_v3_guard": legacy_agent_v3_guard,
        "registration_validation": registration_validation,
        "refreshed_agent_installations": refreshed_agent_installations,
        "legacy_after_submit": legacy_after_submit,
        "inherited_sync_state_repair": inherited_sync_state_repair,
        "indexes_added": indexes,
        "enabled": bool(frappe.db.get_single_value(SETTINGS_DOCTYPE, "enabled")),
    }


@frappe.whitelist()
def install() -> dict[str, Any]:
    if "System Manager" not in set(frappe.get_roles()):
        frappe.throw("System Manager role is required", frappe.PermissionError)
    return _apply_setup()


def after_migrate() -> dict[str, Any]:
    """Keep registration provisioning metadata consistent after deployment."""
    return _apply_setup()
