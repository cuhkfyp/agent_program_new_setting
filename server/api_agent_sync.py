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
from datetime import date as DateValue
from datetime import datetime as DateTimeValue
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import frappe
from frappe.utils import cint, getdate, get_datetime, now_datetime


SETTINGS_DOCTYPE = "CCD Agent Sync Settings"
REGISTRATION_DOCTYPE = "CCD Registration"
MASTER_DOCTYPE = "CCD Master"
FAST_MODE = "Fast Bulk Insert"
LEGACY_MODE = "Legacy Document Insert"
ACTIVE_STATES = {"Waiting", "Ingesting", "Post-processing"}
REGISTRATION_RETIREMENT_HOLD_FIELD = "agent_retirement_hold"
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
BASELINE_ADOPTION_AUTHORIZED = "Authorized"
BASELINE_ADOPTION_IN_PROGRESS = "In Progress"
BASELINE_ADOPTION_COMPLETED = "Completed"
BASELINE_ADOPTION_ACTIVE_STATES = {
    BASELINE_ADOPTION_AUTHORIZED,
    BASELINE_ADOPTION_IN_PROGRESS,
}
BASELINE_ADOPTION_PROGRESS_TTL = 7 * 24 * 60 * 60
BASELINE_ADOPTION_CONFIRMATION_PREFIX = "ADOPT EXISTING MASTER BASELINE: "

ACQUIRE_LUA = """
local source_key = KEYS[1]
local slots_key = KEYS[2]
local retirement_hold_key = KEYS[3]
local now = tonumber(ARGV[1])
local expires = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local token = ARGV[4]
redis.call('ZREMRANGEBYSCORE', slots_key, '-inf', now)
if redis.call('EXISTS', retirement_hold_key) == 1 then
  return {0, 'retirement_hold', 30}
end
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

BASELINE_PROGRESS_LUA = """
local progress_key = KEYS[1]
local ttl = tonumber(ARGV[1])
for index = 2, #ARGV do
  redis.call('SADD', progress_key, ARGV[index])
end
redis.call('EXPIRE', progress_key, ttl)
return redis.call('SCARD', progress_key)
"""

BASELINE_PROGRESS_COUNT_LUA = """
return redis.call('SCARD', KEYS[1])
"""

STAGING_BASELINE_PROGRESS_LUA = """
local progress_key = KEYS[1]
local ttl = tonumber(ARGV[1])
for index = 2, #ARGV, 2 do
  redis.call('HSET', progress_key, ARGV[index], ARGV[index + 1])
end
redis.call('EXPIRE', progress_key, ttl)
return redis.call('HLEN', progress_key)
"""

STAGING_BASELINE_PROGRESS_COUNT_LUA = """
return redis.call('HLEN', KEYS[1])
"""

STAGING_BASELINE_PROGRESS_VALUES_LUA = """
return redis.call('HGETALL', KEYS[1])
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


def _registration(
    registration: str, *, allow_retirement_hold: bool = False
) -> Any:
    if not registration or not frappe.db.exists(REGISTRATION_DOCTYPE, registration):
        frappe.throw("A valid CCD Registration is required", frappe.DoesNotExistError)
    doc = frappe.get_doc(REGISTRATION_DOCTYPE, registration)
    if cint(doc.docstatus) != 1:
        frappe.throw(
            "An active submitted CCD Registration is required",
            frappe.PermissionError,
        )
    if (
        not allow_retirement_hold
        and cint(doc.get(REGISTRATION_RETIREMENT_HOLD_FIELD))
    ):
        frappe.throw(
            "CCD Registration is under a governed retirement hold. "
            "Agent synchronization is disabled until the hold is released or "
            "the registration is cancelled.",
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


def _validate_sync_generation(
    registration_doc: Any,
    source_id: str,
    sync_generation: str,
    *,
    required: bool = True,
) -> str:
    """Fence one client daemon to the exact submitted Registration revision."""
    expected = _sync_generation(registration_doc, source_id)
    supplied = str(sync_generation or "").strip()
    if required and not supplied:
        frappe.throw(
            "Agent V3 synchronization generation is required",
            frappe.PermissionError,
        )
    if supplied and supplied != expected:
        frappe.throw(
            "The CCD Registration synchronization generation is stale; stop this "
            "daemon and rediscover the active submitted Registration.",
            frappe.PermissionError,
        )
    return expected


def _staging_scope(
    registration: str,
    source_id: str,
    sync_generation: str,
    *,
    lock: bool = False,
) -> tuple[Any, str, str, str]:
    """Resolve and fence one generated staging target.

    Mutations lock the Registration row for the request transaction. A manager
    placing a retirement hold therefore cannot complete until the in-flight
    staging batch has finished, and every later batch observes the hold.
    """
    if lock:
        locked = frappe.db.sql(
            "SELECT name FROM `tabCCD Registration` WHERE name=%s FOR UPDATE",
            (str(registration or ""),),
        )
        if not locked:
            frappe.throw("A valid CCD Registration is required", frappe.DoesNotExistError)
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    hold_owner = str(
        _decode_redis_result(frappe.cache.get(_retirement_hold_key(source))) or ""
    )
    if hold_owner:
        frappe.throw(
            "CCD Registration is under a governed retirement hold. Agent staging "
            "synchronization is disabled.",
            frappe.PermissionError,
        )
    generation = _validate_sync_generation(doc, source, sync_generation)
    staging_doctype = str(doc.get("ccd_reg_doctype") or "").strip()
    if not staging_doctype or not frappe.db.exists("DocType", staging_doctype):
        frappe.throw(
            "The generated CCD staging DocType is unavailable for this Registration",
            frappe.PermissionError,
        )
    return doc, source, generation, staging_doctype


def _staging_batch_rows(
    registration: Any,
    rows: Any,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Validate one bounded staging payload without accepting internal fields."""
    raw_rows = _json_list(rows, "rows")
    maximum = max(1, cint(_settings().get("maximum_batch_size") or 1000))
    if not raw_rows or len(raw_rows) > maximum:
        frappe.throw(
            f"Staging batches require 1 to {maximum} rows",
            frappe.ValidationError,
        )
    field_types = _staging_baseline_fields(registration)
    allowed = set(field_types)
    validated: list[dict[str, Any]] = []
    seen: set[str] = set()
    invalid: list[str] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            invalid.append("<non-object>")
            continue
        incoming = {str(key).lower(): value for key, value in raw_row.items()}
        unknown = sorted(set(incoming) - allowed)
        key = str(incoming.get("ccd_source_key") or "").strip()
        if not key:
            invalid.append("<blank>:ccd_source_key")
        elif key in seen:
            invalid.append(f"{key}:duplicate")
        elif unknown:
            invalid.append(f"{key}:unauthorized " + ",".join(unknown[:5]))
        seen.add(key)
        incoming["ccd_source_key"] = key
        validated.append(incoming)
        if len(invalid) >= 5:
            break
    if invalid or len(validated) != len(raw_rows):
        frappe.throw(
            "Invalid governed staging batch: " + "; ".join(invalid[:5]),
            frappe.ValidationError,
        )
    return validated, field_types


def _staging_rows_match(
    incoming: dict[str, Any],
    stored: Any,
    field_types: dict[str, str],
) -> bool:
    return all(
        _normalize_baseline_value(field_types[fieldname], value)
        == _normalize_baseline_value(field_types[fieldname], stored.get(fieldname))
        for fieldname, value in incoming.items()
    )


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


def _retirement_hold_key(source_id: str) -> bytes:
    return frappe.cache.make_key(
        f"ccd_agent_sync:retirement_hold:{_source_digest(source_id)}"
    )


def source_sync_lease_owner(source_id: str) -> str:
    """Return the current central-sync lease token without exposing Redis details."""
    source_key, _ = _lease_keys(str(source_id or "").strip())
    return str(_decode_redis_result(frappe.cache.get(source_key)) or "")


def _require_manager() -> None:
    if "System Manager" not in set(frappe.get_roles()):
        frappe.throw("System Manager role is required", frappe.PermissionError)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _ordered_value_fingerprint(values: Iterable[Any]) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = str(value or "").encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _baseline_mapping_snapshot(registration: Any) -> dict[str, Any]:
    rows = frappe.get_all(
        "CCD Field Match",
        filters={"parent": registration.name},
        fields=[
            "idx",
            "ccd_fieldname",
            "sys_fieldname",
            "fieldtype",
            "assignment",
        ],
        order_by="idx asc",
        limit_page_length=10_000,
    )
    payload = [
        {
            "idx": cint(row.idx),
            "ccd_fieldname": str(row.ccd_fieldname or ""),
            "sys_fieldname": str(row.sys_fieldname or ""),
            "fieldtype": str(row.fieldtype or ""),
            "assignment": str(row.assignment or ""),
        }
        for row in rows
    ]
    return {
        "mapping_count": len(payload),
        "mapped_fields": sorted(
            {
                str(row["sys_fieldname"] or "").split(":", 1)[0].strip()
                for row in payload
                if str(row["sys_fieldname"] or "").split(":", 1)[0].strip()
            }
        ),
        "mapping_fingerprint": hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest(),
    }


def _baseline_population_snapshot(registration: Any, source_id: str) -> dict[str, Any]:
    staging_doctype = str(registration.get("ccd_reg_doctype") or "").strip()
    if not staging_doctype or not frappe.db.exists("DocType", staging_doctype):
        frappe.throw("The generated CCD staging DocType is unavailable")
    staging_meta = frappe.get_meta(staging_doctype)
    if not staging_meta.has_field("ccd_source_key"):
        frappe.throw("The generated CCD staging DocType has no source key")

    staging_keys = [
        str(value or "")
        for value in frappe.get_all(
            staging_doctype,
            pluck="ccd_source_key",
            order_by="ccd_source_key asc",
            limit_page_length=1_000_000,
        )
    ]
    master_keys = [
        str(value or "")
        for value in frappe.get_all(
            MASTER_DOCTYPE,
            filters={"ccd_reg_source": source_id},
            pluck="ccd_source_key",
            order_by="ccd_source_key asc",
            limit_page_length=1_000_000,
        )
    ]
    blank_staging_keys = sum(not value for value in staging_keys)
    blank_master_keys = sum(not value for value in master_keys)
    duplicate_staging_keys = len(staging_keys) - len(set(staging_keys))
    duplicate_master_keys = len(master_keys) - len(set(master_keys))
    exact_key_match = staging_keys == master_keys
    mapping = _baseline_mapping_snapshot(registration)
    return {
        "registration": str(registration.name),
        "source_id": source_id,
        "staging_doctype": staging_doctype,
        "staging_count": len(staging_keys),
        "master_count": len(master_keys),
        "blank_staging_keys": blank_staging_keys,
        "blank_master_keys": blank_master_keys,
        "duplicate_staging_keys": duplicate_staging_keys,
        "duplicate_master_keys": duplicate_master_keys,
        "exact_key_match": exact_key_match,
        "source_key_fingerprint": _ordered_value_fingerprint(staging_keys),
        "sync_generation": _sync_generation(registration, source_id),
        **mapping,
    }


def _validated_baseline_population(registration: Any, source_id: str) -> dict[str, Any]:
    snapshot = _baseline_population_snapshot(registration, source_id)
    if not snapshot["staging_count"] or not snapshot["master_count"]:
        frappe.throw("Baseline adoption requires populated staging and Master sources")
    if snapshot["staging_count"] != snapshot["master_count"]:
        frappe.throw("Staging and Master row counts do not match")
    if not snapshot["exact_key_match"]:
        frappe.throw("Staging and Master source keys do not match exactly")
    if any(
        snapshot[fieldname]
        for fieldname in (
            "blank_staging_keys",
            "blank_master_keys",
            "duplicate_staging_keys",
            "duplicate_master_keys",
        )
    ):
        frappe.throw("Baseline adoption requires unique, non-blank source keys")
    if not snapshot["mapping_count"]:
        frappe.throw("Baseline adoption requires an active field mapping")
    return snapshot


def _baseline_scope_fingerprint(snapshot: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(snapshot).encode("utf-8")).hexdigest()


def _baseline_adoption_payload(registration: Any) -> dict[str, Any]:
    raw = registration.get("agent_baseline_adoption_payload")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _active_baseline_adoption(
    registration: Any,
    source_id: str,
    *,
    run_id: str = "",
) -> dict[str, Any]:
    state = str(registration.get("agent_baseline_adoption_state") or "")
    if state not in BASELINE_ADOPTION_ACTIVE_STATES:
        return {}
    payload = _baseline_adoption_payload(registration)
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    if (
        str(payload.get("registration") or "") != str(registration.name)
        or str(snapshot.get("source_id") or "") != source_id
        or str(snapshot.get("sync_generation") or "")
        != _sync_generation(registration, source_id)
    ):
        return {}
    authorized_run = str(payload.get("run_id") or "")
    if run_id and authorized_run and authorized_run != str(run_id):
        return {}
    return payload


def _baseline_progress_key(authorization_id: str) -> bytes:
    return frappe.cache.make_key(
        f"ccd_agent_sync:baseline_adoption:{str(authorization_id or '').strip()}"
    )


def _staging_baseline_progress_key(authorization_id: str) -> bytes:
    return frappe.cache.make_key(
        "ccd_agent_sync:staging_baseline_adoption:"
        f"{str(authorization_id or '').strip()}"
    )


def _record_baseline_progress(authorization_id: str, keys: list[str]) -> int:
    if not authorization_id or not keys:
        return 0
    return cint(
        frappe.cache.eval(
            BASELINE_PROGRESS_LUA,
            1,
            _baseline_progress_key(authorization_id),
            BASELINE_ADOPTION_PROGRESS_TTL,
            *keys,
        )
    )


def _baseline_progress_count(authorization_id: str) -> int:
    if not authorization_id:
        return 0
    return cint(
        frappe.cache.eval(
            BASELINE_PROGRESS_COUNT_LUA,
            1,
            _baseline_progress_key(authorization_id),
        )
        or 0
    )


def _record_staging_baseline_progress(
    authorization_id: str, row_hashes: dict[str, str]
) -> int:
    if not authorization_id or not row_hashes:
        return 0
    arguments: list[Any] = [BASELINE_ADOPTION_PROGRESS_TTL]
    for key, row_hash in row_hashes.items():
        arguments.extend([key, row_hash])
    return cint(
        frappe.cache.eval(
            STAGING_BASELINE_PROGRESS_LUA,
            1,
            _staging_baseline_progress_key(authorization_id),
            *arguments,
        )
    )


def _staging_baseline_progress_count(authorization_id: str) -> int:
    if not authorization_id:
        return 0
    return cint(
        frappe.cache.eval(
            STAGING_BASELINE_PROGRESS_COUNT_LUA,
            1,
            _staging_baseline_progress_key(authorization_id),
        )
        or 0
    )


def _staging_baseline_progress_hashes(
    authorization_id: str,
) -> dict[str, str]:
    if not authorization_id:
        return {}
    values = frappe.cache.eval(
        STAGING_BASELINE_PROGRESS_VALUES_LUA,
        1,
        _staging_baseline_progress_key(authorization_id),
    )
    output: dict[str, str] = {}
    for index in range(0, len(values or []), 2):
        output[str(_decode_redis_result(values[index]) or "")] = str(
            _decode_redis_result(values[index + 1]) or ""
        )
    return output


def place_source_retirement_hold(source_id: str, registration: str) -> None:
    """Block a new central-sync lease before the database hold is committed."""
    source = str(source_id or "").strip()
    owner = str(registration or "").strip()
    if not source or not owner:
        frappe.throw("A source and registration are required for a retirement hold")
    frappe.cache.set(_retirement_hold_key(source), owner)


def clear_source_retirement_hold(source_id: str, registration: str) -> None:
    """Clear only the marker owned by this registration revision."""
    key = _retirement_hold_key(str(source_id or "").strip())
    current = str(_decode_redis_result(frappe.cache.get(key)) or "")
    if current == str(registration or "").strip():
        frappe.cache.delete(key)


AMENDED_OPERATIONAL_DEFAULTS: dict[str, Any] = {
    "agent_sync_state": "Idle",
    "agent_sync_run_id": None,
    "agent_sync_rows_processed": 0,
    "agent_sync_active_host": None,
    "agent_sync_active_database": None,
    "agent_sync_started_at": None,
    "agent_sync_heartbeat_at": None,
    "agent_sync_finished_at": None,
    "agent_sync_last_result": None,
    "agent_sync_last_error": None,
    "agent_status": "",
    "agent_ip": None,
    "ws_log": None,
    "last_heartbeat": None,
    "total_number_record": None,
    "agent_cpu": None,
    "agent_retirement_hold": 0,
    "agent_retirement_hold_at": None,
    "agent_retirement_hold_by": None,
    "agent_retirement_hold_note": None,
    "agent_baseline_adoption_state": "Not Requested",
    "agent_baseline_adoption_expected_count": 0,
    "agent_baseline_staging_progress": 0,
    "agent_baseline_adoption_progress": 0,
    "agent_baseline_adoption_payload": None,
    "agent_baseline_adoption_event": None,
}


def reset_amended_registration_operational_state(
    registration: Any, method: str | None = None
) -> None:
    """Do not copy runtime success/heartbeat state into an amended revision."""
    if not str(registration.get("amended_from") or "").strip():
        return
    fields = {field.fieldname for field in frappe.get_meta(REGISTRATION_DOCTYPE).fields}
    for fieldname, value in AMENDED_OPERATIONAL_DEFAULTS.items():
        if fieldname in fields:
            registration.set(fieldname, value)
    if "log_info" in fields:
        registration.set("log_info", [])


def repair_inherited_amended_sync_state() -> dict[str, Any]:
    """Repair revisions whose displayed sync timestamps predate their creation."""
    meta = frappe.get_meta(REGISTRATION_DOCTYPE)
    fields = {field.fieldname for field in meta.fields}
    timestamp_fields = [
        fieldname
        for fieldname in (
            "agent_sync_started_at",
            "agent_sync_heartbeat_at",
            "agent_sync_finished_at",
            "last_heartbeat",
        )
        if fieldname in fields
    ]
    if not timestamp_fields:
        return {"updated": 0, "registrations": []}
    rows = frappe.get_all(
        REGISTRATION_DOCTYPE,
        filters={
            "docstatus": ["in", [0, 1]],
            "amended_from": ["is", "set"],
        },
        fields=["name", "creation", *timestamp_fields],
        limit_page_length=100_000,
    )
    values = {
        fieldname: value
        for fieldname, value in AMENDED_OPERATIONAL_DEFAULTS.items()
        if fieldname in fields
    }
    updated: list[str] = []
    for row in rows:
        created_at = get_datetime(row.creation)
        inherited = any(
            row.get(fieldname)
            and get_datetime(row.get(fieldname)) < created_at
            for fieldname in timestamp_fields
        )
        if not inherited:
            continue
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            row.name,
            values,
            update_modified=False,
        )
        updated.append(str(row.name))
    return {"updated": len(updated), "registrations": updated}


def reject_master_write_during_registration_retirement(
    master: Any, method: str | None = None
) -> None:
    """Freeze document-based Master writes while a source is being retired."""
    source = str(master.get("ccd_reg_source") or "").strip()
    if not source or not frappe.get_meta(REGISTRATION_DOCTYPE).has_field(
        REGISTRATION_RETIREMENT_HOLD_FIELD
    ):
        return
    held = frappe.get_all(
        REGISTRATION_DOCTYPE,
        filters={"docstatus": 1, REGISTRATION_RETIREMENT_HOLD_FIELD: 1},
        fields=["name", "ccd_stable_source_key", "agent_sync_source_id"],
        limit_page_length=100_000,
    )
    owner = next(
        (
            row.name
            for row in held
            if source
            == str(
                row.get("agent_sync_source_id")
                or row.get("ccd_stable_source_key")
                or row.name
            ).strip()
        ),
        "",
    )
    if owner:
        frappe.throw(
            f"CCD Master source {source} is frozen by the governed retirement "
            f"hold on Registration {owner}.",
            frappe.PermissionError,
        )


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
def guard_legacy_agent_bulk_sync(doctype: str = "") -> dict[str, Any]:
    """Reject only legacy CCD-REG writes after the V3 rollout is activated.

    This method is called by the existing ``agent_bulk_sync`` Server Script.
    It deliberately remains a no-op by default so V2 clients can operate while
    client machines are upgraded. Agent V3 never uses that endpoint for staging.
    """
    _require_writer()
    target = str(doctype or "").strip()
    strict = bool(
        cint(_settings().get("enforce_agent_v3_staging_fence") or 0)
    )
    legacy_staging_target = target.upper().startswith("CCD-REG-")
    if strict and legacy_staging_target:
        frappe.throw(
            "This site requires CCD Agent V3 for Registration staging. Stop the "
            "legacy daemon, install the current governed agent, and restart it.",
            frappe.PermissionError,
        )
    return {
        "allowed": True,
        "strict": strict,
        "legacy_staging_target": legacy_staging_target,
    }


@frappe.whitelist()
def get_sync_config(
    registration: str,
    source_id: str,
    physical_hostname: str = "",
    database_name: str = "",
    sync_generation: str = "",
) -> dict[str, Any]:
    """Return effective settings without exposing database credentials."""
    _require_writer()
    doc = _registration(registration)
    if str(doc.get("agent_baseline_adoption_state") or "") == "Failed":
        frappe.throw(
            "The governed existing-baseline adoption failed. A System Manager "
            "must investigate and authorize a fresh exact scope before this "
            "registration can synchronize again.",
            frappe.PermissionError,
        )
    source = _validate_source(doc, source_id)
    generation = _validate_sync_generation(
        doc,
        source,
        sync_generation,
        required=False,
    )
    config = _config(doc)
    config.update(
        {
            "registration": doc.name,
            "amended_from": str(doc.get("amended_from") or ""),
            "source_id": source,
            "sync_generation": generation,
            "agent_protocol": 3,
            "governed_staging": True,
            "staging_doctype": str(doc.get("ccd_reg_doctype") or ""),
            "physical_hostname": str(physical_hostname or ""),
            "database_name": str(database_name or ""),
        }
    )
    return config


@frappe.whitelist()
def inspect_registration_target(
    registration: str,
    source_id: str,
    sync_generation: str,
) -> dict[str, Any]:
    """Inspect only the staging DocType owned by this active revision."""
    _require_writer()
    _doc, source, generation, staging_doctype = _staging_scope(
        registration, source_id, sync_generation
    )
    count = cint(frappe.db.count(staging_doctype))
    return {
        "registration": str(registration),
        "source_id": source,
        "sync_generation": generation,
        "staging_doctype": staging_doctype,
        "target_has_rows": count > 0,
        "count": count,
    }


@frappe.whitelist()
def get_registration_rows(
    registration: str,
    source_id: str,
    sync_generation: str,
    source_keys: Any,
    fields: Any,
) -> dict[str, Any]:
    """Return a bounded, generation-fenced staging comparison batch."""
    _require_writer()
    doc, source, generation, staging_doctype = _staging_scope(
        registration, source_id, sync_generation
    )
    keys = [str(key or "").strip() for key in _json_list(source_keys, "source_keys")]
    maximum = max(1, cint(_settings().get("maximum_batch_size") or 1000))
    if not keys or len(keys) > maximum or any(not key for key in keys):
        frappe.throw(
            f"source_keys must contain 1 to {maximum} non-blank values",
            frappe.ValidationError,
        )
    requested = [str(field or "").strip().lower() for field in _json_list(fields, "fields")]
    allowed = set(_staging_baseline_fields(doc)) | {"name"}
    invalid = sorted(set(requested) - allowed)
    if invalid:
        frappe.throw(
            "Unauthorized staging fields: " + ", ".join(invalid[:5]),
            frappe.PermissionError,
        )
    output_fields = list(dict.fromkeys(["name", "ccd_source_key", *requested]))
    rows = frappe.get_all(
        staging_doctype,
        filters={"ccd_source_key": ["in", list(dict.fromkeys(keys))]},
        fields=output_fields,
        limit_page_length=len(keys) + 1,
    )
    returned_keys: set[str] = set()
    duplicate_keys: set[str] = set()
    for row in rows:
        returned_key = str(row.get("ccd_source_key") or "").strip()
        if returned_key in returned_keys:
            duplicate_keys.add(returned_key)
        returned_keys.add(returned_key)
    if duplicate_keys:
        frappe.throw(
            "Governed staging comparison found duplicate source keys: "
            + ", ".join(sorted(duplicate_keys)[:5]),
            frappe.ValidationError,
        )
    return {
        "registration": str(registration),
        "source_id": source,
        "sync_generation": generation,
        "rows": [dict(row) for row in rows],
    }


@frappe.whitelist(methods=["POST"])
def insert_registration_batch(
    registration: str,
    source_id: str,
    sync_generation: str,
    rows: Any,
) -> dict[str, Any]:
    """Insert or exactly confirm one active-revision staging batch."""
    _require_writer()
    doc, source, generation, staging_doctype = _staging_scope(
        registration, source_id, sync_generation, lock=True
    )
    validated, field_types = _staging_batch_rows(doc, rows)
    explicit_fields = sorted({field for row in validated for field in row})
    keys = [row["ccd_source_key"] for row in validated]
    existing_rows = frappe.get_all(
        staging_doctype,
        filters={"ccd_source_key": ["in", keys]},
        fields=["name", *explicit_fields],
        limit_page_length=len(keys) + 1,
    )
    existing_by_key: dict[str, list[Any]] = {}
    for existing in existing_rows:
        existing_by_key.setdefault(
            str(existing.get("ccd_source_key") or "").strip(), []
        ).append(existing)
    inserted = 0
    confirmed_existing = 0
    errors: list[str] = []
    for position, row in enumerate(validated):
        key = row["ccd_source_key"]
        existing = existing_by_key.get(key, [])
        if len(existing) > 1:
            errors.append(f"{key}:duplicate existing staging rows")
            continue
        if existing:
            if _staging_rows_match(row, existing[0], field_types):
                confirmed_existing += 1
            else:
                errors.append(f"{key}:existing staging values differ")
            continue
        savepoint = f"agent_staging_insert_{position}"
        frappe.db.savepoint(savepoint)
        try:
            staging_row = frappe.new_doc(staging_doctype)
            staging_row.update(row)
            staging_row.insert(ignore_permissions=True)
            inserted += 1
        except Exception as exc:
            frappe.db.rollback(save_point=savepoint)
            errors.append(f"{key}:{str(exc)[:180]}")
    return {
        "registration": str(registration),
        "source_id": source,
        "sync_generation": generation,
        "inserted": inserted,
        "confirmed_existing": confirmed_existing,
        "accepted": inserted + confirmed_existing,
        "errors": errors,
    }


@frappe.whitelist(methods=["POST"])
def update_registration_batch(
    registration: str,
    source_id: str,
    sync_generation: str,
    rows: Any,
) -> dict[str, Any]:
    """Update only rows in the exact active-revision staging DocType."""
    _require_writer()
    doc, source, generation, staging_doctype = _staging_scope(
        registration, source_id, sync_generation, lock=True
    )
    validated, field_types = _staging_batch_rows(doc, rows)
    updated = 0
    confirmed_existing = 0
    errors: list[str] = []
    for row in validated:
        key = row["ccd_source_key"]
        fields = {field: value for field, value in row.items() if field != "ccd_source_key"}
        existing = frappe.get_all(
            staging_doctype,
            filters={"ccd_source_key": key},
            fields=["name", "ccd_source_key", *sorted(fields)],
            limit_page_length=2,
        )
        if len(existing) != 1:
            errors.append(
                f"{key}:" + ("missing" if not existing else "duplicate existing rows")
            )
            continue
        if _staging_rows_match(row, existing[0], field_types):
            confirmed_existing += 1
            continue
        frappe.db.set_value(
            staging_doctype,
            existing[0].name,
            fields,
        )
        updated += 1
    return {
        "registration": str(registration),
        "source_id": source,
        "sync_generation": generation,
        "updated": updated,
        "confirmed_existing": confirmed_existing,
        "accepted": updated + confirmed_existing,
        "errors": errors,
    }


@frappe.whitelist(methods=["POST"])
def delete_registration_source_keys(
    registration: str,
    source_id: str,
    sync_generation: str,
    source_keys: Any,
) -> dict[str, Any]:
    """Delete a bounded key set only from the active revision's staging target."""
    _require_writer()
    _doc, source, generation, staging_doctype = _staging_scope(
        registration, source_id, sync_generation, lock=True
    )
    keys = [str(key or "").strip() for key in _json_list(source_keys, "source_keys")]
    maximum = max(1, cint(_settings().get("maximum_batch_size") or 1000))
    if not keys or len(keys) > maximum or any(not key for key in keys):
        frappe.throw(
            f"source_keys must contain 1 to {maximum} non-blank values",
            frappe.ValidationError,
        )
    rows = frappe.get_all(
        staging_doctype,
        filters={"ccd_source_key": ["in", list(dict.fromkeys(keys))]},
        fields=["name", "ccd_source_key"],
        limit_page_length=len(keys) + 1,
    )
    by_key: dict[str, list[str]] = {}
    for row in rows:
        by_key.setdefault(str(row.ccd_source_key or "").strip(), []).append(row.name)
    deleted = 0
    confirmed_absent = 0
    errors: list[str] = []
    for key in dict.fromkeys(keys):
        names = by_key.get(key, [])
        if not names:
            confirmed_absent += 1
            continue
        if len(names) > 1:
            errors.append(f"{key}:duplicate existing rows")
            continue
        frappe.delete_doc(staging_doctype, names[0], ignore_permissions=True)
        deleted += 1
    return {
        "registration": str(registration),
        "source_id": source,
        "sync_generation": generation,
        "deleted": deleted,
        "confirmed_absent": confirmed_absent,
        "accepted": deleted + confirmed_absent,
        "errors": errors,
    }


@frappe.whitelist()
def preview_master_baseline_adoption(registration: str) -> dict[str, Any]:
    """Prove that staging and an existing Master source have the same key scope."""
    _require_manager()
    doc = _registration(registration)
    if cint(doc.get(REGISTRATION_RETIREMENT_HOLD_FIELD)):
        frappe.throw("Release the retirement hold before baseline adoption")
    source = _effective_source(doc)
    if source_sync_lease_owner(source):
        frappe.throw("Stop the client agent and wait for its source lease to drain")
    if str(doc.get("agent_sync_state") or "") != "Reconciliation Required":
        frappe.throw(
            "Baseline adoption is available only for a Reconciliation Required source"
        )
    snapshot = _validated_baseline_population(doc, source)
    scope_fingerprint = _baseline_scope_fingerprint(snapshot)
    return {
        "zero_write": True,
        "registration": str(doc.name),
        "source_id": source,
        "staging_count": snapshot["staging_count"],
        "master_count": snapshot["master_count"],
        "mapping_count": snapshot["mapping_count"],
        "source_key_fingerprint": snapshot["source_key_fingerprint"],
        "mapping_fingerprint": snapshot["mapping_fingerprint"],
        "scope_fingerprint": scope_fingerprint,
        "required_confirmation": (
            f"{BASELINE_ADOPTION_CONFIRMATION_PREFIX}{scope_fingerprint}"
        ),
        "next_step": (
            "Authorize this exact scope, then start only the current client agent. "
            "The agent will rebuild its local cache by confirming exact existing "
            "Master rows; no Master insert, update, or deletion is allowed."
        ),
    }


@frappe.whitelist(methods=["POST"])
def authorize_master_baseline_adoption(
    registration: str,
    scope_fingerprint: str,
    confirmation: str,
    reason: str,
) -> dict[str, Any]:
    """Authorize one exact, no-write cache-baseline adoption from Desk."""
    _require_manager()
    reason_text = str(reason or "").strip()
    if not reason_text:
        frappe.throw("A baseline-adoption reason is required")
    locked = frappe.db.sql(
        "SELECT docstatus FROM `tabCCD Registration` WHERE name=%s FOR UPDATE",
        (registration,),
    )
    if not locked or cint(locked[0][0]) != 1:
        frappe.throw("An active submitted CCD Registration is required")
    doc = _registration(registration)
    source = _effective_source(doc)
    if source_sync_lease_owner(source):
        frappe.throw("The source lease is active; keep the client agent stopped")
    if str(doc.get("agent_sync_state") or "") != "Reconciliation Required":
        frappe.throw(
            "Baseline adoption is available only for a Reconciliation Required source"
        )
    snapshot = _validated_baseline_population(doc, source)
    actual_fingerprint = _baseline_scope_fingerprint(snapshot)
    supplied_fingerprint = str(scope_fingerprint or "").strip()
    if supplied_fingerprint != actual_fingerprint:
        frappe.throw("The source population changed; run a fresh preview")
    expected_confirmation = (
        f"{BASELINE_ADOPTION_CONFIRMATION_PREFIX}{actual_fingerprint}"
    )
    if str(confirmation or "").strip() != expected_confirmation:
        frappe.throw("Type the exact displayed baseline-adoption confirmation")
    authorization_id = uuid.uuid4().hex
    payload = {
        "version": 1,
        "authorization_id": authorization_id,
        "registration": str(doc.name),
        "scope_fingerprint": actual_fingerprint,
        "snapshot": snapshot,
        "authorized_at": str(now_datetime()),
        "authorized_by": str(frappe.session.user),
        "reason": reason_text,
        "run_id": "",
    }
    progress_key = _baseline_progress_key(authorization_id)
    staging_progress_key = _staging_baseline_progress_key(authorization_id)
    frappe.cache.delete(progress_key)
    frappe.cache.delete(staging_progress_key)
    event = (
        f"Authorized by {frappe.session.user} at {now_datetime()}: "
        f"{snapshot['master_count']:,} exact existing rows; {reason_text}"
    )
    frappe.db.set_value(
        REGISTRATION_DOCTYPE,
        doc.name,
        {
            "agent_baseline_adoption_state": BASELINE_ADOPTION_AUTHORIZED,
            "agent_baseline_adoption_expected_count": snapshot["master_count"],
            "agent_baseline_staging_progress": 0,
            "agent_baseline_adoption_progress": 0,
            "agent_baseline_adoption_payload": _canonical_json(payload),
            "agent_baseline_adoption_event": event[:1000],
            "agent_sync_rows_processed": 0,
            "agent_sync_last_result": (
                "Zero-write staging and Master baseline adoption authorized. "
                "Start only the current client agent to rebuild its scoped caches."
            ),
            "agent_sync_last_error": None,
        },
        update_modified=False,
    )
    return {
        "status": BASELINE_ADOPTION_AUTHORIZED,
        "registration": str(doc.name),
        "source_id": source,
        "expected_count": snapshot["master_count"],
        "scope_fingerprint": actual_fingerprint,
        "authorization_id": authorization_id,
        "writes_allowed": {"insert": 0, "update": 0, "delete": 0},
    }


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
    retirement_hold_key = _retirement_hold_key(source)
    raw = frappe.cache.eval(
        ACQUIRE_LUA,
        3,
        source_key,
        slots_key,
        retirement_hold_key,
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
    adoption = _active_baseline_adoption(doc, source)
    if adoption and has_master_rows:
        snapshot = _validated_baseline_population(doc, source)
        if _baseline_scope_fingerprint(snapshot) != str(
            adoption.get("scope_fingerprint") or ""
        ):
            frappe.throw(
                "The authorized baseline population changed; run a fresh preview"
            )
        authorization_id = str(adoption.get("authorization_id") or "")
        expected = cint(snapshot.get("staging_count") or 0)
        staging_progress = _staging_baseline_progress_count(authorization_id)
        if not expected or staging_progress != expected:
            frappe.throw(
                "The exact staging baseline must be confirmed before CCD Master "
                f"adoption: confirmed {staging_progress:,} / {expected:,} rows"
            )
        if not str(adoption.get("staging_completed_at") or ""):
            frappe.throw(
                "The exact staging baseline has not completed its final stability check"
            )
        if _staging_baseline_value_fingerprint(doc) != str(
            adoption.get("staging_value_fingerprint") or ""
        ):
            frappe.throw(
                "The staging population changed after baseline confirmation; "
                "run a fresh governed preview"
            )
        adoption["run_id"] = str(run_id or "")
        adoption["started_at"] = str(now_datetime())
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            doc.name,
            {
                "agent_baseline_adoption_state": BASELINE_ADOPTION_IN_PROGRESS,
                "agent_baseline_adoption_payload": _canonical_json(adoption),
                "agent_baseline_adoption_event": (
                    f"No-write cache baseline adoption started by agent run "
                    f"{str(run_id or '')[:140]}"
                ),
            },
            update_modified=False,
        )
        return {
            "registration": doc.name,
            "source_id": source,
            "sync_generation": _sync_generation(doc, source),
            # The current agent treats an empty source as the only safe reason
            # to build a new cache. During an explicitly authorized adoption,
            # every subsequent batch is instead forced to match existing rows
            # exactly and is prohibited from writing.
            "master_has_rows": False,
            "actual_master_has_rows": True,
            "source_state": "Authorized Existing Baseline Adoption",
            "baseline_adoption_authorized": True,
            "expected_count": snapshot["master_count"],
        }
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
    # A run that acquired its lease immediately before a manager placed the
    # retirement hold must still be able to release that lease. All write and
    # renewal endpoints remain blocked by the hold.
    doc = _registration(registration, allow_retirement_hold=True)
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
        adoption = _active_baseline_adoption(doc, source)
        adoption_failed = bool(adoption.get("failed"))
        final_state = (
            "Reconciliation Required"
            if adoption_failed
            or error_text.startswith("Reconciliation Required")
            else ("Failed" if error_text else "Idle")
        )
        values: dict[str, Any] = {
            "agent_sync_state": final_state,
            "agent_sync_run_id": str(run_id or ""),
            "agent_sync_heartbeat_at": now_datetime(),
            "agent_sync_finished_at": now_datetime(),
            "agent_sync_last_error": error_text or None,
        }
        if adoption_failed:
            values.update(
                {
                    "agent_baseline_adoption_state": "Failed",
                    "agent_baseline_adoption_event": str(
                        adoption.get("failure")
                        or "Exact existing-baseline confirmation failed"
                    )[:1000],
                }
            )
        _set_state(doc.name, **values)
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
    source = _validate_source(doc, source_id)
    baseline_state = str(doc.get("agent_baseline_adoption_state") or "")
    if baseline_state == "Failed":
        frappe.throw(
            "The governed existing-baseline adoption failed and blocks zero-delta "
            "completion until a fresh manager authorization",
            frappe.PermissionError,
        )
    adoption = _active_baseline_adoption(doc, source)
    if adoption:
        snapshot = _validated_baseline_population(doc, source)
        if _baseline_scope_fingerprint(snapshot) != str(
            adoption.get("scope_fingerprint") or ""
        ):
            frappe.throw(
                "The authorized baseline population changed before zero-delta "
                "completion"
            )
        expected = cint(snapshot.get("master_count") or 0)
        progress = _baseline_progress_count(
            str(adoption.get("authorization_id") or "")
        )
        if not expected or progress != expected:
            frappe.throw(
                f"Existing-baseline adoption remains incomplete: confirmed "
                f"{progress:,} / {expected:,} rows",
                frappe.PermissionError,
            )
        finished_at = now_datetime()
        result = (
            f"Recovered completed no-write baseline confirmation after an "
            f"interruption: {expected:,} exact existing row(s), "
            f"{cint(snapshot.get('mapping_count'))} mapped fields; zero Master writes"
        )
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            doc.name,
            {
                "agent_sync_state": "Succeeded",
                "agent_sync_active_host": str(physical_hostname or ""),
                "agent_sync_active_database": str(database_name or ""),
                "agent_sync_heartbeat_at": finished_at,
                "agent_sync_finished_at": finished_at,
                "agent_sync_rows_processed": expected,
                "agent_sync_last_error": None,
                "agent_sync_last_result": result,
                "agent_baseline_adoption_state": BASELINE_ADOPTION_COMPLETED,
                "agent_baseline_adoption_expected_count": expected,
                "agent_baseline_adoption_progress": progress,
                "agent_baseline_adoption_event": result[:1000],
            },
            update_modified=False,
        )
        return {
            "state": "Succeeded",
            "changed": 0,
            "baseline_adoption": True,
            "confirmed_existing": progress,
        }
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


def _normalize_baseline_value(fieldtype: str, value: Any) -> str:
    normalized_type = str(fieldtype or "Data").strip().lower()
    if normalized_type == "check":
        if isinstance(value, bool):
            return "1" if value else "0"
        normalized = str(value or "").strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return "1"
        if normalized in {"", "0", "false", "no", "off", "none"}:
            return "0"
    if normalized_type in {"int", "long int", "float", "currency", "percent"}:
        normalized = str(value if value not in (None, "") else 0).strip()
        try:
            decimal_value = Decimal(normalized)
        except (InvalidOperation, ValueError):
            return normalized
        if not decimal_value:
            return "0"
        return format(decimal_value.normalize(), "f")
    if value is None:
        return ""
    normalized = str(value)
    if normalized_type == "date":
        if not normalized.strip():
            return ""
        # ``frappe.utils.getdate("0001-01-01")`` returns ``None`` even though
        # MariaDB returns the same stored value as ``datetime.date(1, 1, 1)``.
        # SQL Server sources can legitimately contain that sentinel.  Parse a
        # strict ISO date prefix first so text and database-native values have
        # identical canonical forms without treating different dates as equal.
        if isinstance(value, DateTimeValue):
            return value.date().isoformat()
        if isinstance(value, DateValue):
            return value.isoformat()
        iso_match = re.match(
            r"^(\d{4})-(\d{2})-(\d{2})(?:[ T].*)?$", normalized.strip()
        )
        if iso_match:
            try:
                return DateValue(
                    *(int(part) for part in iso_match.groups())
                ).isoformat()
            except ValueError:
                pass
        parsed = getdate(value)
        return parsed.isoformat() if parsed else ""
    if normalized_type == "datetime":
        if not normalized.strip():
            return ""
        return str(get_datetime(value))
    return normalized


def _staging_baseline_fields(registration: Any) -> dict[str, str]:
    """Return governed staging field types keyed by their actual fieldnames."""
    staging_doctype = str(registration.get("ccd_reg_doctype") or "").strip()
    if not staging_doctype or not frappe.db.exists("DocType", staging_doctype):
        frappe.throw("The generated CCD staging DocType is unavailable")
    meta = frappe.get_meta(staging_doctype)
    scalar_types = {
        str(field.fieldname).lower(): str(field.fieldtype or "Data")
        for field in meta.fields
        if field.fieldtype
        not in {
            "Table",
            "Table MultiSelect",
            "Section Break",
            "Column Break",
            "Tab Break",
            "HTML",
            "Button",
            "Heading",
            "Fold",
        }
    }
    mapping_rows = frappe.get_all(
        "CCD Field Match",
        filters={"parent": registration.name},
        fields=["ccd_fieldname", "fieldtype"],
        order_by="idx asc",
        limit_page_length=10_000,
    )
    field_types: dict[str, str] = {}
    for row in mapping_rows:
        fieldname = str(row.ccd_fieldname or "").strip().lower()
        if not fieldname or fieldname not in scalar_types:
            continue
        field_types[fieldname] = str(
            row.fieldtype or scalar_types.get(fieldname) or "Data"
        )
    if "ccd_source_key" not in scalar_types:
        frappe.throw("The generated CCD staging DocType has no source key")
    field_types["ccd_source_key"] = scalar_types["ccd_source_key"]
    if len(field_types) <= 1:
        frappe.throw("Baseline adoption requires active staging field mappings")
    return field_types


def _confirm_existing_staging_baseline_rows(
    registration: Any,
    raw_rows: list[dict[str, Any]],
) -> dict[str, str]:
    """Require every mapped client value to equal its existing staging row."""
    staging_doctype = str(registration.get("ccd_reg_doctype") or "").strip()
    field_types = _staging_baseline_fields(registration)
    compare_fields = sorted(field_types)
    normalized_rows: list[dict[str, Any]] = []
    keys: list[str] = []
    seen: set[str] = set()
    invalid: list[str] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            invalid.append("<non-object>")
            continue
        incoming = {str(key).lower(): value for key, value in raw_row.items()}
        key = str(incoming.get("ccd_source_key") or "").strip()
        missing = [fieldname for fieldname in compare_fields if fieldname not in incoming]
        if not key:
            invalid.append("<blank>:ccd_source_key")
        elif key in seen:
            invalid.append(f"{key}:duplicate")
        elif missing:
            invalid.append(f"{key}:missing " + ",".join(missing[:5]))
        seen.add(key)
        keys.append(key)
        normalized_rows.append(incoming)
        if len(invalid) >= 5:
            break
    if invalid or len(normalized_rows) != len(raw_rows):
        frappe.throw(
            "Staging baseline adoption requires unique complete rows: "
            + "; ".join(invalid[:5]),
            frappe.ValidationError,
        )
    stored_rows = frappe.get_all(
        staging_doctype,
        filters={"ccd_source_key": ["in", keys]},
        fields=["name", *compare_fields],
        limit_page_length=len(keys),
    )
    stored_by_key = {
        str(row.get("ccd_source_key") or "").strip(): row for row in stored_rows
    }
    mismatches: list[str] = []
    for incoming in normalized_rows:
        key = str(incoming.get("ccd_source_key") or "").strip()
        stored = stored_by_key.get(key)
        if not stored:
            mismatches.append(f"{key}:missing")
            continue
        for fieldname in compare_fields:
            if _normalize_baseline_value(
                field_types[fieldname], incoming.get(fieldname)
            ) != _normalize_baseline_value(
                field_types[fieldname], stored.get(fieldname)
            ):
                mismatches.append(f"{key}:{fieldname}")
                break
        if len(mismatches) >= 5:
            break
    if mismatches:
        frappe.throw(
            "Staging baseline adoption found missing or different existing rows: "
            + ", ".join(mismatches),
            frappe.ValidationError,
        )
    return {
        str(incoming.get("ccd_source_key") or "").strip(): hashlib.sha256(
            _canonical_json(
                [
                    [
                        fieldname,
                        _normalize_baseline_value(
                            field_types[fieldname], incoming.get(fieldname)
                        ),
                    ]
                    for fieldname in compare_fields
                ]
            ).encode("utf-8")
        ).hexdigest()
        for incoming in normalized_rows
    }


def _current_staging_baseline_hashes(registration: Any) -> dict[str, str]:
    staging_doctype = str(registration.get("ccd_reg_doctype") or "").strip()
    field_types = _staging_baseline_fields(registration)
    compare_fields = sorted(field_types)
    rows = frappe.get_all(
        staging_doctype,
        fields=compare_fields,
        order_by="ccd_source_key asc",
        limit_page_length=1_000_000,
    )
    output: dict[str, str] = {}
    for row in rows:
        key = str(row.get("ccd_source_key") or "").strip()
        if not key or key in output:
            frappe.throw(
                "Staging baseline verification requires unique non-blank source keys"
            )
        output[key] = hashlib.sha256(
            _canonical_json(
                [
                    [
                        fieldname,
                        _normalize_baseline_value(
                            field_types[fieldname], row.get(fieldname)
                        ),
                    ]
                    for fieldname in compare_fields
                ]
            ).encode("utf-8")
        ).hexdigest()
    return output


def _staging_baseline_value_fingerprint(registration: Any) -> str:
    return hashlib.sha256(
        _canonical_json(
            sorted(_current_staging_baseline_hashes(registration).items())
        ).encode("utf-8")
    ).hexdigest()


@frappe.whitelist(methods=["POST"])
def confirm_registration_baseline_batch(
    registration: str,
    source_id: str,
    rows: Any,
    sync_generation: str = "",
) -> dict[str, Any]:
    """Confirm an exact existing staging batch without writing any server row."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    _validate_sync_generation(
        doc, source, sync_generation, required=bool(sync_generation)
    )
    adoption = _active_baseline_adoption(doc, source)
    if not adoption:
        frappe.throw("No active governed baseline adoption is authorized")
    snapshot = adoption.get("snapshot") or {}
    current_mapping = _baseline_mapping_snapshot(doc)
    if str(current_mapping.get("mapping_fingerprint") or "") != str(
        snapshot.get("mapping_fingerprint") or ""
    ):
        frappe.throw("The authorized field mapping changed; run a fresh preview")
    raw_rows = _json_list(rows, "rows")
    maximum = max(1, cint(_settings().get("maximum_batch_size") or 1000))
    if not raw_rows or len(raw_rows) > maximum:
        frappe.throw(
            f"Staging baseline batches require 1 to {maximum} rows",
            frappe.ValidationError,
        )
    authorization_id = str(adoption.get("authorization_id") or "")
    expected = cint(snapshot.get("staging_count") or 0)
    try:
        confirmed_hashes = _confirm_existing_staging_baseline_rows(doc, raw_rows)
    except Exception as exc:
        failure = (
            "Existing staging-baseline adoption rejected an unequal or incomplete "
            f"batch: {str(exc)}"
        )[:1000]
        adoption["failed"] = True
        adoption["failed_at"] = str(now_datetime())
        adoption["failure"] = failure
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            doc.name,
            {
                "agent_baseline_adoption_state": "Failed",
                "agent_baseline_adoption_payload": _canonical_json(adoption),
                "agent_baseline_adoption_event": failure,
                "agent_sync_last_error": failure,
            },
            update_modified=False,
        )
        return {
            "confirmed_existing": 0,
            "accepted": 0,
            "errors": [failure],
            "baseline_adoption": True,
            "staging_progress": _staging_baseline_progress_count(authorization_id),
            "staging_expected": expected,
            "complete": False,
            "writes_allowed": {"insert": 0, "update": 0, "delete": 0},
        }
    progress = _record_staging_baseline_progress(
        authorization_id, confirmed_hashes
    )
    if not expected or progress > expected:
        frappe.throw("Staging baseline-adoption progress exceeded its authorized scope")
    event = (
        f"Confirmed {progress:,} / {expected:,} exact existing staging rows; "
        "zero staging writes"
    )
    frappe.db.set_value(
        REGISTRATION_DOCTYPE,
        doc.name,
        {
            "agent_baseline_adoption_state": BASELINE_ADOPTION_IN_PROGRESS,
            "agent_baseline_staging_progress": progress,
            "agent_baseline_adoption_event": event[:1000],
            "agent_sync_last_result": event[:2000],
            "agent_sync_last_error": None,
        },
        update_modified=False,
    )
    return {
        "confirmed_existing": len(confirmed_hashes),
        "accepted": len(confirmed_hashes),
        "errors": [],
        "baseline_adoption": True,
        "staging_progress": progress,
        "staging_expected": expected,
        "ready_to_finalize": progress == expected,
        "complete": False,
        "writes_allowed": {"insert": 0, "update": 0, "delete": 0},
    }


@frappe.whitelist(methods=["POST"])
def complete_registration_baseline_adoption(
    registration: str,
    source_id: str,
    sync_generation: str = "",
) -> dict[str, Any]:
    """Finalize exact staging confirmation before a local cache is created."""
    _require_writer()
    doc = _registration(registration)
    source = _validate_source(doc, source_id)
    _validate_sync_generation(
        doc, source, sync_generation, required=bool(sync_generation)
    )
    adoption = _active_baseline_adoption(doc, source)
    if not adoption:
        frappe.throw("No active governed baseline adoption is authorized")
    snapshot = _validated_baseline_population(doc, source)
    if _baseline_scope_fingerprint(snapshot) != str(
        adoption.get("scope_fingerprint") or ""
    ):
        frappe.throw("The authorized baseline population changed; run a fresh preview")
    authorization_id = str(adoption.get("authorization_id") or "")
    expected = cint(snapshot.get("staging_count") or 0)
    progress_hashes = _staging_baseline_progress_hashes(authorization_id)
    if not expected or len(progress_hashes) != expected:
        frappe.throw(
            f"Staging baseline adoption is incomplete: confirmed "
            f"{len(progress_hashes):,} / {expected:,} exact rows"
        )
    current_hashes = _current_staging_baseline_hashes(doc)
    if current_hashes != progress_hashes:
        different = sorted(
            key
            for key in set(current_hashes) | set(progress_hashes)
            if current_hashes.get(key) != progress_hashes.get(key)
        )
        failure = (
            "Existing staging-baseline adoption changed during confirmation: "
            + ", ".join(different[:5])
        )[:1000]
        adoption["failed"] = True
        adoption["failed_at"] = str(now_datetime())
        adoption["failure"] = failure
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            doc.name,
            {
                "agent_baseline_adoption_state": "Failed",
                "agent_baseline_adoption_payload": _canonical_json(adoption),
                "agent_baseline_adoption_event": failure,
                "agent_sync_last_error": failure,
            },
            update_modified=False,
        )
        return {
            "complete": False,
            "errors": [failure],
            "staging_progress": len(progress_hashes),
            "staging_expected": expected,
            "writes_allowed": {"insert": 0, "update": 0, "delete": 0},
        }
    value_fingerprint = hashlib.sha256(
        _canonical_json(sorted(current_hashes.items())).encode("utf-8")
    ).hexdigest()
    adoption["staging_completed_at"] = str(now_datetime())
    adoption["staging_value_fingerprint"] = value_fingerprint
    event = (
        f"Completed zero-write staging baseline: confirmed {expected:,} exact "
        "existing rows; local cache creation authorized"
    )
    frappe.db.set_value(
        REGISTRATION_DOCTYPE,
        doc.name,
        {
            "agent_baseline_adoption_state": BASELINE_ADOPTION_IN_PROGRESS,
            "agent_baseline_staging_progress": expected,
            "agent_baseline_adoption_payload": _canonical_json(adoption),
            "agent_baseline_adoption_event": event[:1000],
            "agent_sync_last_result": event[:2000],
            "agent_sync_last_error": None,
        },
        update_modified=False,
    )
    return {
        "complete": True,
        "errors": [],
        "staging_progress": expected,
        "staging_expected": expected,
        "staging_value_fingerprint": value_fingerprint,
        "writes_allowed": {"insert": 0, "update": 0, "delete": 0},
    }


def _confirm_existing_baseline_rows(
    raw_rows: list[dict[str, Any]],
    validated_rows: list[dict[str, Any]],
    source_id: str,
    required_fields: list[str],
) -> list[str]:
    """Reject an adoption batch unless every sent field matches an existing row."""
    meta = frappe.get_meta(MASTER_DOCTYPE)
    scalar_fields = {
        field.fieldname
        for field in meta.fields
        if field.fieldtype
        not in {
            "Table",
            "Table MultiSelect",
            "Section Break",
            "Column Break",
            "Tab Break",
            "HTML",
            "Button",
            "Heading",
            "Fold",
        }
    }
    internal_fields = {
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
        "match_ct",
    }
    explicit_fields = sorted(
        {
            str(fieldname)
            for raw_row in raw_rows
            for fieldname in raw_row
            if str(fieldname) in scalar_fields
            and str(fieldname) not in internal_fields
        }
    )
    required = sorted(
        {
            "ccd_reg_source",
            "ccd_source_key",
            *(
                str(fieldname).strip()
                for fieldname in required_fields
                if str(fieldname).strip() in scalar_fields
                and str(fieldname).strip() not in internal_fields
            ),
        }
    )
    missing_by_row: list[str] = []
    for raw_row in raw_rows:
        missing = [fieldname for fieldname in required if fieldname not in raw_row]
        if missing:
            missing_by_row.append(
                f"{str(raw_row.get('ccd_source_key') or '<blank>')}:"
                + ",".join(missing[:5])
            )
        if len(missing_by_row) >= 5:
            break
    if missing_by_row:
        frappe.throw(
            "Baseline adoption requires every mapped field in every row: "
            + "; ".join(missing_by_row),
            frappe.ValidationError,
        )
    compare_fields = sorted(set(explicit_fields) | set(required))
    keys = [str(row.get("ccd_source_key") or "") for row in validated_rows]
    stored_rows = frappe.get_all(
        MASTER_DOCTYPE,
        filters={
            "ccd_reg_source": source_id,
            "ccd_source_key": ["in", keys],
        },
        fields=["name", *compare_fields],
        limit_page_length=len(keys),
    )
    stored_by_key = {
        str(row.ccd_source_key or ""): row
        for row in stored_rows
    }
    mismatches: list[str] = []
    for incoming in validated_rows:
        key = str(incoming.get("ccd_source_key") or "")
        stored = stored_by_key.get(key)
        if not stored:
            mismatches.append(f"{key}:missing")
            continue
        for fieldname in compare_fields:
            field = meta.get_field(fieldname)
            fieldtype = str(field.fieldtype if field else "Data")
            if _normalize_baseline_value(
                fieldtype, incoming.get(fieldname)
            ) != _normalize_baseline_value(fieldtype, stored.get(fieldname)):
                mismatches.append(f"{key}:{fieldname}")
                break
        if len(mismatches) >= 5:
            break
    if mismatches:
        frappe.throw(
            "Baseline adoption found missing or different existing rows: "
            + ", ".join(mismatches),
            frappe.ValidationError,
        )
    return keys


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
    raw_rows = _json_list(rows, "rows")
    validated = _validated_rows(raw_rows, source, run_id, config["batch_size"])
    keys = [str(row["ccd_source_key"]) for row in validated]
    adoption = _active_baseline_adoption(doc, source, run_id=run_id)
    if adoption:
        authorization_id = str(adoption.get("authorization_id") or "")
        expected = cint(
            (adoption.get("snapshot") or {}).get("master_count") or 0
        )
        if adoption.get("failed"):
            return {
                "inserted": 0,
                "confirmed_existing": 0,
                "accepted": 0,
                "names": [],
                "errors": [
                    "Existing-baseline adoption previously failed; no Master "
                    "rows were written"
                ],
                "baseline_adoption": True,
                "baseline_progress": _baseline_progress_count(authorization_id),
                "baseline_expected": expected,
            }
        try:
            confirmed_keys = _confirm_existing_baseline_rows(
                raw_rows,
                validated,
                source,
                list((adoption.get("snapshot") or {}).get("mapped_fields") or []),
            )
        except Exception as exc:
            failure = (
                "Existing-baseline adoption rejected an unequal or incomplete "
                f"batch: {str(exc)}"
            )[:1000]
            adoption["failed"] = True
            adoption["failed_at"] = str(now_datetime())
            adoption["failure"] = failure
            frappe.db.set_value(
                REGISTRATION_DOCTYPE,
                doc.name,
                {
                    "agent_baseline_adoption_payload": _canonical_json(adoption),
                    "agent_baseline_adoption_event": failure,
                    "agent_sync_last_error": failure,
                },
                update_modified=False,
            )
            return {
                "inserted": 0,
                "confirmed_existing": 0,
                "accepted": 0,
                "names": [],
                "errors": [failure],
                "baseline_adoption": True,
                "baseline_progress": _baseline_progress_count(authorization_id),
                "baseline_expected": expected,
            }
        progress = _record_baseline_progress(authorization_id, confirmed_keys)
        if not expected or progress > expected:
            frappe.throw("Baseline-adoption progress exceeded its authorized scope")
        _set_state(
            doc.name,
            agent_sync_state="Ingesting",
            agent_sync_heartbeat_at=now_datetime(),
            agent_sync_rows_processed=progress,
            agent_sync_last_result=(
                f"Confirmed {progress:,} / {expected:,} exact existing Master "
                "row(s); no identity writes performed"
            ),
        )
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            doc.name,
            {
                "agent_baseline_adoption_state": BASELINE_ADOPTION_IN_PROGRESS,
                "agent_baseline_adoption_progress": progress,
                "agent_baseline_adoption_event": (
                    f"Confirmed {progress:,} / {expected:,} exact existing rows"
                ),
            },
            update_modified=False,
        )
        return {
            "inserted": 0,
            "confirmed_existing": len(confirmed_keys),
            "accepted": len(validated),
            "names": [],
            "errors": [],
            "baseline_adoption": True,
            "baseline_progress": progress,
            "baseline_expected": expected,
        }
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
    chain_nonce: str = "",
) -> None:
    config = _config(_registration(registration))
    job_material = f"{run_id}:{cursor}"
    if chain_nonce:
        job_material = f"{job_material}:{chain_nonce}"
    job_suffix = hashlib.sha256(job_material.encode()).hexdigest()[:16]
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
        chain_nonce=str(chain_nonce or ""),
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
    adoption = _active_baseline_adoption(doc, source, run_id=str(run_id or ""))
    if adoption:
        snapshot = _validated_baseline_population(doc, source)
        actual_fingerprint = _baseline_scope_fingerprint(snapshot)
        if actual_fingerprint != str(adoption.get("scope_fingerprint") or ""):
            frappe.throw(
                "The authorized baseline population changed before completion"
            )
        authorization_id = str(adoption.get("authorization_id") or "")
        expected = cint(snapshot.get("master_count") or 0)
        progress = _baseline_progress_count(authorization_id)
        if not expected or progress != expected:
            frappe.throw(
                f"Baseline adoption is incomplete: confirmed {progress:,} / "
                f"{expected:,} exact existing rows"
            )
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
        if not released:
            frappe.throw("Unable to release the completed baseline-adoption lease")
        finished_at = now_datetime()
        result = (
            f"Completed no-write Master baseline adoption: confirmed "
            f"{expected:,} exact existing row(s) across "
            f"{cint(snapshot.get('mapping_count'))} mapped fields; inserted 0, "
            "updated 0, deleted 0"
        )
        frappe.db.set_value(
            REGISTRATION_DOCTYPE,
            doc.name,
            {
                "agent_sync_state": "Succeeded",
                "agent_sync_run_id": str(run_id or ""),
                "agent_sync_heartbeat_at": finished_at,
                "agent_sync_finished_at": finished_at,
                "agent_sync_rows_processed": expected,
                "agent_sync_last_error": None,
                "agent_sync_last_result": result,
                "agent_baseline_adoption_state": BASELINE_ADOPTION_COMPLETED,
                "agent_baseline_adoption_expected_count": expected,
                "agent_baseline_adoption_progress": progress,
                "agent_baseline_adoption_event": (
                    f"Completed by agent run {str(run_id or '')[:140]} at "
                    f"{finished_at}; {expected:,} exact rows; zero Master writes"
                ),
            },
            update_modified=False,
        )
        return {
            "queued": False,
            "state": "Succeeded",
            "baseline_adoption": True,
            "confirmed_existing": progress,
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
        }
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
    chain_nonce: str = "",
) -> dict[str, Any]:
    """Restartable chained job for work deliberately skipped by bulk SQL insert."""
    # Each batch is one committed, restartable transaction.  Suppress the
    # snapshot doc-event marker for intermediate batches so the long queue can
    # process the complete source before rebuilding its derived dashboard once.
    frappe.flags.ccd_dashboard_snapshot_defer_agent_postprocess = True
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
            chain_nonce=str(chain_nonce or ""),
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
        from db_connector.ccd_dashboard_snapshot import mark_dirty_after_commit

        frappe.flags.ccd_dashboard_snapshot_defer_agent_postprocess = False
        mark_dirty_after_commit(reason="agent-postprocessing-completed")
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
