"""Bounded, observable CCD Master ingestion for database agents.

This module is intentionally standalone.  Installing it does not modify Frappe
hooks, scheduler entries, or the existing ``agent_bulk_sync`` Server Script.
Registrations remain on the legacy path until ``agent_sync_mode`` is explicitly
set to ``Fast Bulk Insert``.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from typing import Any, Iterable

import frappe
from frappe.utils import cint, now_datetime


SETTINGS_DOCTYPE = "CCD Agent Sync Settings"
REGISTRATION_DOCTYPE = "CCD Registration"
MASTER_DOCTYPE = "CCD Master"
FAST_MODE = "Fast Bulk Insert"
LEGACY_MODE = "Legacy Document Insert"
ACTIVE_STATES = {"Waiting", "Ingesting", "Post-processing"}
ALLOWED_QUEUES = {"short", "default", "long"}
STATIC_FORMAT_RE = re.compile(r"^format:([^{}]*)\{(#+)\}([^{}]*)$")
RECONCILIATION_REASONS = {
    "cache_missing_source_populated": (
        "Local CCD Master cache is missing, but this source already has Master "
        "rows. Automatic clearing is disabled; operator reconciliation is required."
    ),
    "cache_scope_mismatch_source_populated": (
        "Local CCD Master cache belongs to another registration revision, but this "
        "source still has Master rows. Operator reconciliation is required."
    ),
    "source_deletions_detected": (
        "The client database no longer contains one or more cached Master source "
        "keys. Automatic Master deletion is disabled; use the governed identity "
        "retirement workflow or reconcile the source explicitly."
    ),
    "safe_bootstrap_unavailable": (
        "A safe empty-source bootstrap could not be verified through central "
        "coordination. No CCD Master rows were changed."
    ),
}

ACQUIRE_LUA = """
local source_key = KEYS[1]
local slots_key = KEYS[2]
local now = tonumber(ARGV[1])
local expires = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local token = ARGV[4]
redis.call('ZREMRANGEBYSCORE', slots_key, '-inf', now)
if redis.call('EXISTS', source_key) == 1 then
  if redis.call('GET', source_key) == token then
    redis.call('EXPIRE', source_key, expires - now)
    redis.call('ZADD', slots_key, expires, token)
    return {1, 'already_acquired', expires - now}
  end
  return {0, 'source_busy', redis.call('TTL', source_key)}
end
if redis.call('ZCARD', slots_key) >= capacity then
  local oldest = redis.call('ZRANGE', slots_key, 0, 0, 'WITHSCORES')
  local retry_after = 5
  if oldest[2] then retry_after = math.max(1, tonumber(oldest[2]) - now) end
  return {0, 'capacity_full', retry_after}
end
redis.call('SET', source_key, token, 'EX', expires - now, 'NX')
redis.call('ZADD', slots_key, expires, token)
return {1, 'acquired', expires - now}
"""

RENEW_LUA = """
local source_key = KEYS[1]
local slots_key = KEYS[2]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local expires = tonumber(ARGV[3])
if redis.call('GET', source_key) ~= token then return 0 end
redis.call('EXPIRE', source_key, expires - now)
redis.call('ZADD', slots_key, expires, token)
return 1
"""

RELEASE_LUA = """
local source_key = KEYS[1]
local slots_key = KEYS[2]
local token = ARGV[1]
if redis.call('GET', source_key) ~= token then return 0 end
redis.call('DEL', source_key)
redis.call('ZREM', slots_key, token)
return 1
"""

DEFER_LUA = """
local source_key = KEYS[1]
local slots_key = KEYS[2]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
if redis.call('GET', source_key) ~= token then return 0 end
redis.call('EXPIRE', source_key, ttl)
redis.call('ZREM', slots_key, token)
return 1
"""


def _require_writer() -> None:
    roles = set(frappe.get_roles())
    if "System Manager" not in roles and "ccd-user" not in roles:
        frappe.throw(
            "ccd-user or System Manager role is required", frappe.PermissionError
        )


def _settings() -> Any:
    if not frappe.db.exists("DocType", SETTINGS_DOCTYPE):
        return frappe._dict(
            enabled=0,
            coordination_enabled=0,
            max_parallel_master_syncs=1,
            lease_seconds=1800,
            postprocess_lease_seconds=86400,
            default_batch_size=500,
            maximum_batch_size=1000,
            postprocess_batch_size=100,
            postprocess_queue="long",
        )
    return frappe.get_single(SETTINGS_DOCTYPE)


def _registration(registration: str) -> Any:
    if not registration or not frappe.db.exists(REGISTRATION_DOCTYPE, registration):
        frappe.throw("A valid CCD Registration is required", frappe.DoesNotExistError)
    doc = frappe.get_doc(REGISTRATION_DOCTYPE, registration)
    if cint(doc.docstatus) != 1:
        frappe.throw(
            "An active submitted CCD Registration is required",
            frappe.PermissionError,
        )
    return doc


def _effective_source(registration_doc: Any) -> str:
    # Prefer the governed stable key created by the registration lifecycle.
    # Do not infer identity by trimming numeric suffixes: two databases may
    # legitimately have names ending in -1/-2 on the same physical host.
    return str(
        registration_doc.get("agent_sync_source_id")
        or registration_doc.get("ccd_stable_source_key")
        or registration_doc.name
    ).strip()


def _validate_source(registration_doc: Any, source_id: str) -> str:
    expected = _effective_source(registration_doc)
    if str(source_id or "").strip() != expected:
        frappe.throw(
            f"Source ID does not match CCD Registration {registration_doc.name}",
            frappe.PermissionError,
        )
    return expected


def _source_digest(source_id: str) -> str:
    return hashlib.sha256(source_id.encode("utf-8")).hexdigest()


def _sync_generation(registration_doc: Any, source_id: str) -> str:
    """Identify one registration revision without changing governed source identity."""
    material = f"{source_id}\x1f{registration_doc.name}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _lease_keys(source_id: str) -> tuple[bytes, bytes]:
    cache = frappe.cache
    source_key = cache.make_key(f"ccd_agent_sync:source:{_source_digest(source_id)}")
    slots_key = cache.make_key("ccd_agent_sync:master_slots")
    return source_key, slots_key


def _decode_redis_result(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _has_lease(source_id: str, token: str) -> bool:
    source_key, _ = _lease_keys(source_id)
    current = frappe.cache.get(source_key)
    return _decode_redis_result(current) == token


def _require_lease(source_id: str, token: str) -> None:
    if not token or not _has_lease(source_id, token):
        frappe.throw("The CCD Master sync lease is missing or expired", frappe.PermissionError)


def _state_fields() -> set[str]:
    if not frappe.db.exists("DocType", REGISTRATION_DOCTYPE):
        return set()
    return {field.fieldname for field in frappe.get_meta(REGISTRATION_DOCTYPE).fields}


def _set_state(registration: str, **values: Any) -> None:
    fields = _state_fields()
    safe = {
        fieldname: value
        for fieldname, value in values.items()
        if fieldname in fields
    }
    if safe:
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            registration,
            safe,
            update_modified=False,
        )


def _config(registration_doc: Any) -> dict[str, Any]:
    settings = _settings()
    mode = str(registration_doc.get("agent_sync_mode") or LEGACY_MODE)
    default_batch = max(1, cint(settings.get("default_batch_size") or 500))
    max_batch = max(1, cint(settings.get("maximum_batch_size") or 1000))
    override = cint(registration_doc.get("agent_sync_batch_size") or 0)
    batch_size = min(max_batch, max(1, override or default_batch))
    enabled = bool(cint(settings.get("enabled") or 0))
    return {
        "enabled": enabled,
        "coordination_enabled": enabled
        and bool(cint(settings.get("coordination_enabled") or 0)),
        "mode": mode,
        "fast_insert_enabled": enabled and mode == FAST_MODE,
        "batch_size": batch_size,
        "lease_seconds": max(60, cint(settings.get("lease_seconds") or 1800)),
        "postprocess_lease_seconds": max(
            3600, cint(settings.get("postprocess_lease_seconds") or 86400)
        ),
        "max_parallel_master_syncs": max(
            1, cint(settings.get("max_parallel_master_syncs") or 1)
        ),
        "postprocess_batch_size": max(
            1, min(1000, cint(settings.get("postprocess_batch_size") or 100))
        ),
        "postprocess_queue": (
            str(settings.get("postprocess_queue") or "long")
            if str(settings.get("postprocess_queue") or "long") in ALLOWED_QUEUES
            else "long"
        ),
    }


@frappe.whitelist()
def get_sync_config(
    registration: str,
    source_id: str,
    physical_hostname: str = "",
    database_name: str = "",
) -> dict[str, Any]:
    """Return effective settings without exposing database credentials."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    config = _config(doc)
    config.update(
        {
            "registration": doc.name,
            "amended_from": str(doc.get("amended_from") or ""),
            "source_id": source,
            "sync_generation": _sync_generation(doc, source),
            "physical_hostname": str(physical_hostname or ""),
            "database_name": str(database_name or ""),
        }
    )
    return config


@frappe.whitelist(methods=["POST"])
def acquire_sync_lease(
    registration: str,
    source_id: str,
    run_id: str,
    physical_hostname: str = "",
    database_name: str = "",
    lease_token: str = "",
) -> dict[str, Any]:
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    config = _config(doc)
    if not config["coordination_enabled"]:
        return {"acquired": False, "reason": "coordination_disabled"}
    run_id = str(run_id or "").strip()
    if not run_id or len(run_id) > 140:
        frappe.throw("A valid run_id is required", frappe.ValidationError)

    token = str(lease_token or "").strip() or uuid.uuid4().hex
    if len(token) > 128 or not token.replace("-", "").isalnum():
        frappe.throw("lease_token contains invalid characters", frappe.ValidationError)
    now = int(time.time())
    expires = now + config["lease_seconds"]
    source_key, slots_key = _lease_keys(source)
    raw = frappe.cache.eval(
        ACQUIRE_LUA,
        2,
        source_key,
        slots_key,
        now,
        expires,
        config["max_parallel_master_syncs"],
        token,
    )
    acquired = bool(int(raw[0]))
    reason = str(_decode_redis_result(raw[1]))
    retry_after = max(1, cint(raw[2] or 5))
    if acquired:
        _set_state(
            doc.name,
            agent_sync_state="Ingesting",
            agent_sync_run_id=run_id,
            agent_sync_active_host=str(physical_hostname or ""),
            agent_sync_active_database=str(database_name or ""),
            agent_sync_started_at=now_datetime(),
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_finished_at=None,
            agent_sync_rows_processed=0,
            agent_sync_last_error=None,
            agent_sync_last_result=None,
        )
    elif reason == "capacity_full":
        _set_state(
            doc.name,
            agent_sync_state="Waiting",
            agent_sync_run_id=run_id,
            agent_sync_active_host=str(physical_hostname or ""),
            agent_sync_active_database=str(database_name or ""),
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_last_result="Waiting for a global CCD Master ingestion slot",
        )
    return {
        "acquired": acquired,
        "reason": reason,
        "retry_after": min(retry_after, 30),
        "lease_token": token if acquired else "",
        "lease_seconds": config["lease_seconds"],
    }


@frappe.whitelist(methods=["POST"])
def inspect_master_source_state(
    registration: str,
    source_id: str,
    lease_token: str,
    run_id: str,
) -> dict[str, Any]:
    """Read the source state while its central lease prevents bootstrap races."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    _require_lease(source, str(lease_token or ""))
    active_run_id = str(doc.get("agent_sync_run_id") or "")
    if active_run_id and active_run_id != str(run_id or ""):
        frappe.throw("run_id does not match the active sync", frappe.PermissionError)
    has_master_rows = bool(
        frappe.db.exists(MASTER_DOCTYPE, {"ccd_reg_source": source})
    )
    return {
        "registration": doc.name,
        "source_id": source,
        "sync_generation": _sync_generation(doc, source),
        "master_has_rows": has_master_rows,
        "source_state": "Populated" if has_master_rows else "Empty",
    }


@frappe.whitelist(methods=["POST"])
def heartbeat_sync_lease(
    registration: str, source_id: str, lease_token: str, run_id: str
) -> dict[str, Any]:
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    config = _config(doc)
    now = int(time.time())
    expires = now + config["lease_seconds"]
    source_key, slots_key = _lease_keys(source)
    renewed = bool(
        int(
            frappe.cache.eval(
                RENEW_LUA,
                2,
                source_key,
                slots_key,
                str(lease_token or ""),
                now,
                expires,
            )
        )
    )
    if renewed:
        _set_state(
            doc.name,
            agent_sync_state="Ingesting",
            agent_sync_run_id=str(run_id or ""),
            agent_sync_heartbeat_at=now_datetime(),
        )
    return {"renewed": renewed, "lease_seconds": config["lease_seconds"]}


@frappe.whitelist(methods=["POST"])
def release_sync_lease(
    registration: str,
    source_id: str,
    lease_token: str,
    run_id: str = "",
    error: str = "",
) -> dict[str, Any]:
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    source_key, slots_key = _lease_keys(source)
    released = bool(
        int(
            frappe.cache.eval(
                RELEASE_LUA,
                2,
                source_key,
                slots_key,
                str(lease_token or ""),
            )
        )
    )
    if released:
        error_text = str(error or "")[:1000]
        final_state = (
            "Reconciliation Required"
            if error_text.startswith("Reconciliation Required")
            else ("Failed" if error_text else "Idle")
        )
        _set_state(
            doc.name,
            agent_sync_state=final_state,
            agent_sync_run_id=str(run_id or ""),
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_finished_at=now_datetime(),
            agent_sync_last_error=error_text or None,
        )
    return {"released": released}


@frappe.whitelist(methods=["POST"])
def complete_legacy_sync(
    registration: str,
    source_id: str,
    lease_token: str,
    run_id: str,
    result: str = "",
) -> dict[str, Any]:
    """Complete a coordinated legacy run whose normal document hooks already ran."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    _require_lease(source, str(lease_token or ""))
    source_key, slots_key = _lease_keys(source)
    released = bool(
        int(
            frappe.cache.eval(
                RELEASE_LUA,
                2,
                source_key,
                slots_key,
                str(lease_token or ""),
            )
        )
    )
    if released:
        _set_state(
            doc.name,
            agent_sync_state="Succeeded",
            agent_sync_run_id=str(run_id or ""),
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_finished_at=now_datetime(),
            agent_sync_last_error=None,
            agent_sync_last_result=str(result or "Legacy sync completed")[:2000],
        )
    return {"released": released, "state": "Succeeded" if released else "Failed"}


@frappe.whitelist(methods=["POST"])
def report_no_changes(
    registration: str,
    source_id: str,
    physical_hostname: str = "",
    database_name: str = "",
) -> dict[str, Any]:
    """Expose a successful zero-delta check without consuming an ingestion slot."""
    _require_writer()
    doc = _registration(registration)
    _validate_source(doc, source_id)
    _set_state(
        doc.name,
        agent_sync_state="Succeeded",
        agent_sync_active_host=str(physical_hostname or ""),
        agent_sync_active_database=str(database_name or ""),
        agent_sync_heartbeat_at=now_datetime(),
        agent_sync_finished_at=now_datetime(),
        agent_sync_rows_processed=0,
        agent_sync_last_error=None,
        agent_sync_last_result="No CCD Master changes detected; no capacity slot used",
    )
    return {"state": "Succeeded", "changed": 0}


@frappe.whitelist(methods=["POST"])
def report_reconciliation_required(
    registration: str,
    source_id: str,
    reason_code: str,
    physical_hostname: str = "",
    database_name: str = "",
) -> dict[str, Any]:
    """Expose a fail-closed agent decision without mutating CCD Master rows."""
    _require_writer()
    doc = _registration(registration)
    _validate_source(doc, source_id)
    code = str(reason_code or "").strip()
    if code not in RECONCILIATION_REASONS:
        frappe.throw("Unknown reconciliation reason", frappe.ValidationError)
    message = RECONCILIATION_REASONS[code]
    _set_state(
        doc.name,
        agent_sync_state="Reconciliation Required",
        agent_sync_active_host=str(physical_hostname or ""),
        agent_sync_active_database=str(database_name or ""),
        agent_sync_heartbeat_at=now_datetime(),
        agent_sync_finished_at=now_datetime(),
        agent_sync_last_error=message[:1000],
        agent_sync_last_result=(
            f"No automatic CCD Master deletion or clearing was performed ({code})"
        )[:2000],
    )
    return {"state": "Reconciliation Required", "reason_code": code}


def _json_list(value: Any, label: str) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            frappe.throw(f"{label} must contain valid JSON: {exc}")
    if not isinstance(value, list):
        frappe.throw(f"{label} must be a JSON list", frappe.ValidationError)
    return value


def _validated_rows(
    raw_rows: Any, source_id: str, run_id: str, maximum: int
) -> list[dict[str, Any]]:
    rows = _json_list(raw_rows, "rows")
    if not rows or len(rows) > maximum:
        frappe.throw(
            f"rows must contain between 1 and {maximum} records",
            frappe.ValidationError,
        )
    meta = frappe.get_meta(MASTER_DOCTYPE)
    scalar_fields = {
        field.fieldname
        for field in meta.fields
        if field.fieldtype not in {"Table", "Table MultiSelect", "Section Break", "Column Break", "Tab Break", "HTML", "Button", "Heading", "Fold"}
    }
    protected = {
        "doctype",
        "name",
        "owner",
        "creation",
        "modified",
        "modified_by",
        "docstatus",
        "idx",
        "parent",
        "parentfield",
        "parenttype",
        "agent_sync_run_id",
    }
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for position, value in enumerate(rows, start=1):
        if not isinstance(value, dict):
            frappe.throw(f"rows[{position}] must be an object", frappe.ValidationError)
        unknown = sorted(set(map(str, value)) - scalar_fields - protected)
        if unknown:
            frappe.throw(
                f"Invalid CCD Master field(s): {', '.join(unknown[:5])}",
                frappe.ValidationError,
            )
        row = {
            str(fieldname): fieldvalue
            for fieldname, fieldvalue in value.items()
            if str(fieldname) in scalar_fields and str(fieldname) not in protected
        }
        if str(row.get("ccd_reg_source") or "") != source_id:
            frappe.throw(
                f"rows[{position}].ccd_reg_source does not match the lease source",
                frappe.PermissionError,
            )
        source_key = str(row.get("ccd_source_key") or "")
        if not source_key:
            frappe.throw(
                f"rows[{position}].ccd_source_key is required", frappe.ValidationError
            )
        if source_key in seen:
            frappe.throw(
                f"Duplicate ccd_source_key in batch: {source_key}",
                frappe.ValidationError,
            )
        seen.add(source_key)
        if "agent_sync_run_id" in scalar_fields:
            row["agent_sync_run_id"] = run_id
        # Preserve the active CCD Master Before Save Server Script.  Agent
        # payloads contain no match_table children, so its deterministic result
        # is zero without invoking a document event per row.
        if "match_ct" in scalar_fields and "match_ct" not in row:
            row["match_ct"] = 0
        output.append(row)
    return output


def _existing_source_keys(source_id: str, keys: list[str]) -> set[str]:
    existing: set[str] = set()
    for offset in range(0, len(keys), 500):
        existing.update(
            str(value)
            for value in frappe.get_all(
                MASTER_DOCTYPE,
                filters={
                    "ccd_reg_source": source_id,
                    "ccd_source_key": ["in", keys[offset : offset + 500]],
                },
                pluck="ccd_source_key",
                limit_page_length=1000,
            )
        )
    return existing


def _reserve_static_format_names(count: int) -> list[str]:
    meta = frappe.get_meta(MASTER_DOCTYPE)
    match = STATIC_FORMAT_RE.fullmatch(str(meta.autoname or ""))
    if not match:
        frappe.throw(
            "Fast Bulk Insert supports one static format series such as "
            "format:HKSR{#######}; use Legacy Document Insert for this DocType naming rule",
            frappe.ValidationError,
        )
    prefix, hashes, suffix = match.groups()
    # Frappe's format autoname evaluates only the braced numeric component via
    # parse_naming_series, so the tabSeries key is the empty prefix here.
    current = frappe.db.sql(
        "SELECT `current` FROM `tabSeries` WHERE `name`=%s FOR UPDATE",
        ("",),
    )
    if current:
        first = cint(current[0][0]) + 1
        frappe.db.sql(
            "UPDATE `tabSeries` SET `current`=`current`+%s WHERE `name`=%s",
            (count, ""),
        )
    else:
        first = 1
        frappe.db.sql(
            "INSERT INTO `tabSeries` (`name`, `current`) VALUES (%s, %s)",
            ("", count),
        )
    width = len(hashes)
    return [f"{prefix}{number:0{width}d}{suffix}" for number in range(first, first + count)]


def _bulk_insert_rows(rows: list[dict[str, Any]]) -> list[str]:
    names = _reserve_static_format_names(len(rows))
    now = now_datetime()
    user = frappe.session.user
    all_fields = sorted({fieldname for row in rows for fieldname in row})
    fields = [
        "name",
        "owner",
        "creation",
        "modified",
        "modified_by",
        "docstatus",
        "idx",
        *all_fields,
    ]
    values = []
    for name, row in zip(names, rows):
        values.append(
            [name, user, now, now, user, 0, 0]
            + [row.get(fieldname) for fieldname in all_fields]
        )
    frappe.db.bulk_insert(MASTER_DOCTYPE, fields, values, chunk_size=len(values))
    return names


@frappe.whitelist(methods=["POST"])
def fast_insert_master_batch(
    registration: str,
    source_id: str,
    lease_token: str,
    run_id: str,
    rows: Any,
) -> dict[str, Any]:
    """Insert one validated batch without running per-document hooks."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    config = _config(doc)
    if not config["fast_insert_enabled"]:
        frappe.throw(
            "Fast Bulk Insert is not enabled for this CCD Registration",
            frappe.PermissionError,
        )
    _require_lease(source, str(lease_token or ""))
    run_id = str(run_id or "").strip()
    if run_id != str(doc.get("agent_sync_run_id") or ""):
        frappe.throw("run_id does not own the active registration state")
    validated = _validated_rows(rows, source, run_id, config["batch_size"])
    keys = [str(row["ccd_source_key"]) for row in validated]
    existing = _existing_source_keys(source, keys)
    pending = [row for row in validated if str(row["ccd_source_key"]) not in existing]
    names = _bulk_insert_rows(pending) if pending else []
    previous = cint(doc.get("agent_sync_rows_processed") or 0)
    _set_state(
        doc.name,
        agent_sync_state="Ingesting",
        agent_sync_heartbeat_at=now_datetime(),
        agent_sync_rows_processed=previous + len(pending),
        agent_sync_last_result=(
            f"Ingested {previous + len(pending)} row(s); "
            f"confirmed {len(existing)} retry row(s) in latest batch"
        ),
    )
    return {
        "inserted": len(pending),
        "confirmed_existing": len(existing),
        "accepted": len(validated),
        "names": names,
        "errors": [],
    }


def _enqueue_postprocess(
    *,
    registration: str,
    source_id: str,
    run_id: str,
    lease_token: str,
    full_sync: bool,
    changed_source_keys: list[str],
    deleted_source_keys: list[str],
    total: int = 0,
    cursor: str = "",
    processed: int = 0,
    error_count: int = 0,
) -> None:
    config = _config(_registration(registration))
    job_suffix = hashlib.sha256(f"{run_id}:{cursor}".encode()).hexdigest()[:16]
    frappe.enqueue(
        "db_connector.api_agent_sync.run_postprocess_batch",
        queue=config["postprocess_queue"],
        timeout=21600,
        enqueue_after_commit=True,
        job_id=f"ccd-agent-postprocess-{job_suffix}",
        registration=registration,
        source_id=source_id,
        run_id=run_id,
        lease_token=lease_token,
        full_sync=bool(full_sync),
        changed_source_keys=changed_source_keys,
        deleted_source_keys=deleted_source_keys,
        total=cint(total),
        cursor=cursor,
        processed=processed,
        error_count=error_count,
    )


@frappe.whitelist(methods=["POST"])
def finish_sync(
    registration: str,
    source_id: str,
    lease_token: str,
    run_id: str,
    full_sync: int | str = 0,
    changed_source_keys: Any = None,
    deleted_source_keys: Any = None,
) -> dict[str, Any]:
    """Release global capacity and keep the per-source lease for post-processing."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    _require_lease(source, str(lease_token or ""))
    if str(run_id or "") != str(doc.get("agent_sync_run_id") or ""):
        frappe.throw("run_id does not own the active registration state")
    changed = [str(value) for value in _json_list(changed_source_keys or [], "changed_source_keys") if str(value)]
    deleted = [str(value) for value in _json_list(deleted_source_keys or [], "deleted_source_keys") if str(value)]
    if len(changed) > 10000 or len(deleted) > 10000:
        frappe.throw("Incremental post-processing is limited to 10,000 source keys")
    config = _config(doc)
    source_key, slots_key = _lease_keys(source)
    deferred = bool(
        int(
            frappe.cache.eval(
                DEFER_LUA,
                2,
                source_key,
                slots_key,
                str(lease_token or ""),
                config["postprocess_lease_seconds"],
            )
        )
    )
    if not deferred:
        frappe.throw("Unable to transfer the sync lease to post-processing")
    postprocess_total = frappe.db.count(
        MASTER_DOCTYPE,
        filters={
            "ccd_reg_source": source,
            "agent_sync_run_id": str(run_id),
        },
    )
    _set_state(
        doc.name,
        agent_sync_state="Post-processing",
        agent_sync_heartbeat_at=now_datetime(),
        agent_sync_rows_processed=0,
        agent_sync_last_result=(
            "Ingestion complete; downstream processing queued "
            f"(0 / {postprocess_total:,} Master row(s))"
        ),
    )
    _enqueue_postprocess(
        registration=doc.name,
        source_id=source,
        run_id=str(run_id),
        lease_token=str(lease_token),
        full_sync=bool(cint(full_sync)),
        changed_source_keys=sorted(set(changed)),
        deleted_source_keys=sorted(set(deleted)),
        total=postprocess_total,
    )
    return {
        "queued": True,
        "state": "Post-processing",
        "total": postprocess_total,
    }


def _renew_postprocess_lease(source_id: str, token: str, ttl: int) -> bool:
    source_key, _ = _lease_keys(source_id)
    if _decode_redis_result(frappe.cache.get(source_key)) != token:
        return False
    return bool(frappe.cache.expire(source_key, ttl))


def _release_postprocess_lease(source_id: str, token: str) -> None:
    source_key, slots_key = _lease_keys(source_id)
    frappe.cache.eval(RELEASE_LUA, 2, source_key, slots_key, token)


def _run_downstream_record_hooks(names: Iterable[str]) -> int:
    try:
        from db_connector.api_unified_person import ensure_unified_person_after_insert
        from db_connector.api_identity_resolution import handle_ccd_master_update
    except (ImportError, AttributeError):
        return 0
    errors = 0
    for position, name in enumerate(names):
        savepoint = f"agent_postprocess_{position}"
        frappe.db.savepoint(savepoint)
        try:
            document = frappe.get_doc(MASTER_DOCTYPE, name)
            ensure_unified_person_after_insert(document)
            # Document.insert normally invokes on_update after after_insert.
            # Preserve that order for governed identity revalidation.
            handle_ccd_master_update(document)
        except Exception:
            frappe.db.rollback(save_point=savepoint)
            errors += 1
            frappe.log_error(
                frappe.get_traceback(),
                f"CCD Agent post-process failed for {name}"[:140],
            )
    return errors


def _run_portal_adapter(
    source_id: str,
    *,
    full_sync: bool,
    changed_source_keys: list[str],
    deleted_source_keys: list[str],
) -> Any:
    try:
        from ccd_portal.sync import after_agent_sync
    except (ImportError, AttributeError):
        return {"status": "not_installed"}
    return after_agent_sync(
        source=source_id,
        source_keys=None if full_sync else changed_source_keys,
        deleted_source_keys=deleted_source_keys,
        full_sync=full_sync,
    )


def run_postprocess_batch(
    registration: str,
    source_id: str,
    run_id: str,
    lease_token: str,
    full_sync: bool,
    changed_source_keys: list[str],
    deleted_source_keys: list[str],
    total: int = 0,
    cursor: str = "",
    processed: int = 0,
    error_count: int = 0,
) -> dict[str, Any]:
    """Restartable chained job for work deliberately skipped by bulk SQL insert."""
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    config = _config(doc)
    if not _renew_postprocess_lease(
        source, lease_token, config["postprocess_lease_seconds"]
    ):
        _set_state(
            registration,
            agent_sync_state="Failed",
            agent_sync_finished_at=now_datetime(),
            agent_sync_last_error="Post-processing lease expired",
        )
        return {"status": "lease_expired"}

    filters: dict[str, Any] = {
        "ccd_reg_source": source,
        "agent_sync_run_id": run_id,
        "name": [">", cursor or ""],
    }
    postprocess_total = cint(total)
    if postprocess_total <= 0:
        # Compatibility with a queued job created before total progress was
        # added. The composite run index keeps this one-time count bounded.
        postprocess_total = frappe.db.count(
            MASTER_DOCTYPE,
            filters={
                "ccd_reg_source": source,
                "agent_sync_run_id": run_id,
            },
        )
    names = frappe.get_all(
        MASTER_DOCTYPE,
        filters=filters,
        pluck="name",
        order_by="name asc",
        limit_page_length=config["postprocess_batch_size"],
    )
    if names:
        batch_errors = _run_downstream_record_hooks(names)
        processed = cint(processed) + len(names)
        error_count = cint(error_count) + batch_errors
        _set_state(
            registration,
            agent_sync_state="Post-processing",
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_rows_processed=processed,
            agent_sync_last_result=(
                f"Post-processing progress: {processed:,} / "
                f"{postprocess_total:,} Master row(s); {error_count:,} error(s)"
            ),
        )
        _enqueue_postprocess(
            registration=registration,
            source_id=source,
            run_id=run_id,
            lease_token=lease_token,
            full_sync=bool(full_sync),
            changed_source_keys=list(changed_source_keys or []),
            deleted_source_keys=list(deleted_source_keys or []),
            total=postprocess_total,
            cursor=str(names[-1]),
            processed=processed,
            error_count=error_count,
        )
        return {
            "status": "continuing",
            "processed": processed,
            "total": postprocess_total,
            "errors": error_count,
        }

    try:
        portal_result = _run_portal_adapter(
            source,
            full_sync=bool(full_sync),
            changed_source_keys=list(changed_source_keys or []),
            deleted_source_keys=list(deleted_source_keys or []),
        )
        final_state = "Succeeded" if not error_count else "Completed with errors"
        _set_state(
            registration,
            agent_sync_state=final_state,
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_finished_at=now_datetime(),
            agent_sync_last_error=(
                f"{error_count} downstream record(s) failed; see Error Log"
                if error_count
                else None
            ),
            agent_sync_last_result=json.dumps(
                {
                    "postprocessed": cint(processed),
                    "total": postprocess_total,
                    "errors": cint(error_count),
                    "portal": portal_result,
                },
                ensure_ascii=False,
                default=str,
            )[:2000],
        )
        return {
            "status": final_state,
            "processed": cint(processed),
            "total": postprocess_total,
            "errors": cint(error_count),
            "portal": portal_result,
        }
    except Exception:
        _set_state(
            registration,
            agent_sync_state="Failed",
            agent_sync_finished_at=now_datetime(),
            agent_sync_last_error=frappe.get_traceback()[-1000:],
        )
        raise
    finally:
        _release_postprocess_lease(source, lease_token)
