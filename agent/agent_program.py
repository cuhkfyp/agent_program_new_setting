import requests
import time
import json
import os
import re
import sys
import platform
import socket
import subprocess
import threading
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from urllib.parse import quote as urlquote
import socketio
import urllib3

import truststore
truststore.inject_into_ssl()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

daemon_stop_event = threading.Event()
daemon_thread = None
_base_dir = os.path.dirname(os.path.abspath(__file__))
_log_dir = os.path.join(_base_dir, 'daemon_logs')

def file_reader_daemon(filepath, interval=5):
    """Daemon:1 — reads and prints file contents every `interval` seconds."""
    print(f"\n👻 Daemon started: reading '{filepath}' every {interval}s")
    while not daemon_stop_event.is_set():
        try:
            with open(filepath, 'r') as f:
                content = f.read()
            print(f"\n📄 [{time.strftime('%H:%M:%S')}] Contents of '{filepath}':")
            print(content)
        except FileNotFoundError:
            print(f"\n⚠️ [{time.strftime('%H:%M:%S')}] File '{filepath}' not found.")
        except Exception as e:
            print(f"\n❌ [{time.strftime('%H:%M:%S')}] Daemon read error: {e}")
        daemon_stop_event.wait(interval)
    print("👻 Daemon stopped.")

def start_detached_daemon(filepath):
    """Spawn a simple detached daemon (legacy Daemon:2). Survives parent exit. Cross-platform."""
    script = f"""
import time, os
filepath = r"{filepath}"
while True:
    try:
        with open(filepath, 'a') as f:
            f.write(f"[{{time.strftime('%Y-%m-%d %H:%M:%S')}}] hello from daemon that can't stop by ctl + c !\\n")
    except Exception:
        pass
    time.sleep(6)
"""
    proc = _spawn_detached_process(script)
    print(f"\n👻 Testing mode 2 daemon started (PID: {proc.pid})")
    print(f"   Description: daemon that won't stop when the program stop")
    print(f"   Appending to '{filepath}' every 6s")
    _print_kill_hint(proc.pid)
    return proc.pid

#-------------------------------------------------------------------------------------------------------#
#---- Config Parser (INI-style macro) ------------------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------#
# Parses [SYSTEM] and [JOBxx] sections from the Configure field.
# Lines starting with # are comments. Blank lines are ignored.
# Variable replacement: [:var_name] is replaced by sysVars values.
# @Password("xxx") is replaced with the literal password string.
#-------------------------------------------------------------------------------------------------------#

def parse_agent_config(raw_config):
    """Parse INI-style config string into sections dict. Comments (#) are skipped."""
    sections = {}
    current_section = None

    for line in (raw_config or '').splitlines():
        line = line.strip()
        # Skip empty lines and comments
        if not line or line.startswith('#'):
            continue
        # Section header: [SYSTEM], [JOB01], [JOB02], ...
        section_match = re.match(r'^\[(\w+)\]$', line)
        if section_match:
            current_section = section_match.group(1).upper()
            sections[current_section] = {}
            continue
        # Key=Value pair
        if current_section and '=' in line:
            key, _, value = line.partition('=')
            key = key.strip().upper()
            value = value.strip()
            # Strip surrounding quotes from value
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            sections[current_section][key] = value

    return sections

def resolve_password_macro(value):
    """Handle @Password("xxx") macro — extract the literal password."""
    match = re.match(r'^@Password\(["\'](.+?)["\']\)$', value)
    if match:
        return match.group(1)
    return value

def redact_sensitive_text(value):
    """Remove common inline secret forms before console/log output."""
    text = str(value or "")
    text = re.sub(
        r'@Password\(["\'].*?["\']\)',
        '@Password("<redacted>")',
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r'(?i)\b(password|passwd|pass|pwd|secret|token|api[_-]?key)\b\s*[:=]\s*[^\s,;]+',
        r'\1=<redacted>',
        text,
    )
    return text

def variable_replacement(text, sysVars):
    """Replace [:var_name] placeholders with values from sysVars dict."""
    def _replacer(m):
        var_name = m.group(1)
        return str(sysVars.get(var_name, m.group(0)))
    return re.sub(r'\[:(\w+)\]', _replacer, text)

def calc_interval_seconds(job_config):
    """Calculate daemon interval in seconds from DAY/HOUR/MINS keys."""
    total = 0
    if 'DAY' in job_config:
        total += int(job_config['DAY']) * 86400
    if 'HOUR' in job_config:
        total += int(job_config['HOUR']) * 3600
    if 'MINS' in job_config:
        total += int(job_config['MINS']) * 60
    return total if total > 0 else 60  # default 60s if nothing specified

def get_job_sections(sections):
    """Return sorted list of (section_name, config_dict) for all [JOBxx] sections."""
    jobs = []
    for name, config in sections.items():
        if re.match(r'^JOB\d+$', name):
            jobs.append((name, config))
    jobs.sort(key=lambda x: x[0])
    return jobs

#-------------------------------------------------------------------------------------------------------#
#---- Job Daemon Builder (Mode 2 — detached, survives parent exit) -------------------------------------#
#-------------------------------------------------------------------------------------------------------#
# Each [JOBxx] spawns a detached process that:
#   1. Executes the ACTION at the configured frequency (DAY/HOUR/MINS)
#   2. Logs every run to daemon_logs/<JOB_NAME>.log with next-run time
#   3. On unexpected stop: writes final log entry + sends email notification
#-------------------------------------------------------------------------------------------------------#

def build_job_daemon_script(job_name, task_name, actions, interval_seconds, log_file,
                            notify_email='', smtp_server='', smtp_port=587,
                            smtp_user='', smtp_pass='',
                            db_type='', db_server='', db_port=0,
                            db_database='', db_username='', db_password='',
                            erpnext_url='', erpnext_user='', erpnext_pass='',
                            ccd_table='', keyring_service='',
                            registration_id='', source_id='', physical_hostname='',
                            ccd_reg_doctype=''):
    """Build the Python source code string for a detached job daemon process."""
    import json as _json, base64 as _b64
    actions_b64 = _b64.b64encode(_json.dumps(actions).encode()).decode()
    task_name_escaped = task_name.replace("'", "\\'")
    log_file_escaped = log_file.replace('\\', '\\\\')
    notify_email_escaped = notify_email.replace("'", "\\'")
    smtp_server_escaped = smtp_server.replace("'", "\\'")
    smtp_user_escaped = smtp_user.replace("'", "\\'")
    db_server_escaped = db_server.replace("'", "\\'")
    db_database_escaped = db_database.replace("'", "\\'")
    db_username_escaped = db_username.replace("'", "\\'")
    erpnext_url_escaped = erpnext_url.replace("'", "\\'")
    erpnext_user_escaped = erpnext_user.replace("'", "\\'")
    ccd_table_escaped = ccd_table.replace("'", "\\'")
    registration_id_escaped = registration_id.replace("'", "\\'")
    source_id_escaped = source_id.replace("'", "\\'")
    physical_hostname_escaped = physical_hostname.replace("'", "\\'")
    ccd_reg_doctype_escaped = ccd_reg_doctype.replace("'", "\\'")

    return f'''
import time, os, sys, subprocess, platform, traceback, smtplib, json, csv, base64, re, hashlib as _hashlib
import keyring as _kr
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from urllib.parse import quote as _urlquote
import truststore
truststore.inject_into_ssl()

def _get_secret(service, username, default=""):
    try:
        return _kr.get_password(service, username) or default
    except Exception:
        return default

job_name     = "{job_name}"
task_name    = "{task_name_escaped}"
actions      = json.loads(base64.b64decode("{actions_b64}").decode())
interval     = {interval_seconds}
log_file     = r"{log_file_escaped}"
notify_email = "{notify_email_escaped}"
smtp_server  = "{smtp_server_escaped}"
smtp_port    = {smtp_port}
smtp_user    = "{smtp_user_escaped}"
smtp_pass    = _get_secret("{keyring_service}", "smtp_pass")

# Database connection parameters
db_type      = "{db_type}"
db_server    = "{db_server_escaped}"
db_port      = {db_port}
db_database  = "{db_database_escaped}"
db_username  = "{db_username_escaped}"
db_password  = _get_secret("{keyring_service}", "db_password")

# ERPNext connection parameters
erpnext_url  = "{erpnext_url_escaped}"
erpnext_user = "{erpnext_user_escaped}"
erpnext_pass = _get_secret("{keyring_service}", "erpnext_pass")
ccd_table    = "{ccd_table_escaped}"

import socket as _sock
physical_hostname = "{physical_hostname_escaped}" or _sock.gethostname()
registration_id = "{registration_id_escaped}" or physical_hostname
source_id = "{source_id_escaped}" or registration_id
ccd_reg_doctype = "{ccd_reg_doctype_escaped}" or f"CCD-REG-{{registration_id}}"
_hostname = physical_hostname

def write_log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{{ts}}] [{{level}}] [{{physical_hostname}}] [{{source_id}}] [{{job_name}}] {{msg}}"
    try:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(entry + "\\n")
    except Exception:
        pass

def acquire_instance_lock():
    """Hold one OS-level lock for this exact registration/job until exit."""
    lock_id = _hashlib.sha256(f"{{registration_id}}::{{job_name}}".encode("utf-8")).hexdigest()
    lock_dir = os.path.join(os.path.dirname(log_file), ".locks")
    lock_path = os.path.join(lock_dir, lock_id + ".lock")
    pid_path = os.path.join(lock_dir, lock_id + ".pid")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    lock_handle = open(lock_path, "a+b")
    lock_handle.seek(0, os.SEEK_END)
    if lock_handle.tell() == 0:
        lock_handle.write(b"0")
        lock_handle.flush()
    try:
        lock_handle.seek(0)
        if platform.system() == "Windows":
            import msvcrt
            msvcrt.locking(lock_handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        lock_handle.close()
        try:
            with open(pid_path, "r", encoding="ascii") as pid_file:
                existing_pid = pid_file.read().strip() or "unknown"
        except Exception:
            existing_pid = "unknown"
        return None, pid_path, existing_pid

    with open(pid_path, "w", encoding="ascii") as pid_file:
        pid_file.write(str(os.getpid()))
    return lock_handle, pid_path, None

def release_instance_lock(lock_handle, pid_path):
    try:
        with open(pid_path, "r", encoding="ascii") as pid_file:
            owns_pid_file = pid_file.read().strip() == str(os.getpid())
        if owns_pid_file:
            os.remove(pid_path)
    except Exception:
        pass
    try:
        lock_handle.close()
    except Exception:
        pass

def acquire_shared_sync_lock(lock_name, log_prefix):
    """Wait for an OS-level lock shared by all local registration daemons."""
    lock_id = _hashlib.sha256(lock_name.encode("utf-8")).hexdigest()
    lock_dir = os.path.join(os.path.dirname(log_file), ".locks")
    lock_path = os.path.join(lock_dir, "shared-" + lock_id + ".lock")
    os.makedirs(lock_dir, exist_ok=True)
    lock_handle = open(lock_path, "a+b")
    lock_handle.seek(0, os.SEEK_END)
    if lock_handle.tell() == 0:
        lock_handle.write(b"0")
        lock_handle.flush()
    wait_logged = False
    while True:
        try:
            lock_handle.seek(0)
            if platform.system() == "Windows":
                import msvcrt
                msvcrt.locking(lock_handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            if wait_logged:
                write_log(f"{{log_prefix}}: shared {{lock_name}} lock acquired")
            return lock_handle
        except (OSError, IOError):
            if not wait_logged:
                write_log(f"{{log_prefix}}: waiting for another registration to finish {{lock_name}}", "WARN")
                wait_logged = True
            time.sleep(2)

def release_shared_sync_lock(lock_handle):
    try:
        lock_handle.close()
    except Exception:
        pass

def send_notification(subject, body):
    if not notify_email or not smtp_server:
        return
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = smtp_user or "agent-daemon@localhost"
        msg["To"] = notify_email
        with smtplib.SMTP(smtp_server, smtp_port, timeout=10) as srv:
            if smtp_user and smtp_pass:
                srv.starttls()
                srv.login(smtp_user, smtp_pass)
            srv.sendmail(msg["From"], [notify_email], msg.as_string())
        write_log(f"Notification sent to {{notify_email}}")
    except Exception as e:
        write_log(f"Failed to send notification: {{e}}", "ERROR")

def get_db_connection(mssql_unicode_fallback=False):
    """Create a database connection based on db_type."""
    if not db_type:
        return None, None
    try:
        if db_type == "MYSQL":
            import mysql.connector
            conn = mysql.connector.connect(
                host=db_server,
                port=int(db_port) or 3306,
                user=db_username,
                password=db_password,
                database=db_database
            )
            return conn, conn.cursor()
        elif db_type == "MSSQL":
            import pyodbc
            conn_str = (
                f"DRIVER={{{{ODBC Driver 17 for SQL Server}}}};"
                f"SERVER={{db_server}},{{int(db_port) or 1433}};"
                f"DATABASE={{db_database}};"
                f"UID={{db_username}};"
                f"PWD={{db_password}}"
            )
            conn = pyodbc.connect(conn_str)
            if mssql_unicode_fallback:
                def decode_wide_text(value):
                    return value.decode("utf-16-le", errors="replace")

                for sql_type_name in ("SQL_WCHAR", "SQL_WVARCHAR", "SQL_WLONGVARCHAR"):
                    sql_type = getattr(pyodbc, sql_type_name, None)
                    if sql_type is not None:
                        conn.add_output_converter(sql_type, decode_wide_text)
            return conn, conn.cursor()
        elif db_type == "ORACLE":
            import oracledb
            dsn = f"{{db_server}}:{{int(db_port) or 1521}}/{{db_database}}"
            conn = oracledb.connect(user=db_username, password=db_password, dsn=dsn)
            return conn, conn.cursor()
        else:
            write_log(f"Unsupported db_type: {{db_type}}", "ERROR")
            return None, None
    except Exception as e:
        write_log(f"DB connection failed: {{e}}", "ERROR")
        return None, None

def fetch_db_rows(sql, log_prefix):
    """Execute a query, retrying malformed MSSQL wide text with replacement decoding."""
    attempts = (False, True) if db_type == "MSSQL" else (False,)
    for use_unicode_fallback in attempts:
        conn, cursor = get_db_connection(mssql_unicode_fallback=use_unicode_fallback)
        if not conn:
            return None
        try:
            cursor.execute(sql)
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = [list(row) for row in cursor.fetchall()]
            return columns, rows
        except UnicodeDecodeError as error:
            if db_type == "MSSQL" and not use_unicode_fallback and error.encoding.lower().replace("_", "-") == "utf-16-le":
                write_log(
                    f"{{log_prefix}}: MSSQL returned malformed UTF-16 text; retrying with replacement decoding",
                    "WARN"
                )
                continue
            raise
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass
    return None

def request_with_retry(session, method, url, log_prefix, max_attempts=3, safe_to_retry=True, **kwargs):
    """Retry transient ERPNext/proxy failures with bounded backoff."""
    retry_statuses = {{408, 429, 500, 502, 503, 504}}
    last_response = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.request(method, url, **kwargs)
            last_response = response
            if not safe_to_retry or response.status_code not in retry_statuses or attempt == max_attempts:
                return response
            write_log(
                f"{{log_prefix}}: transient HTTP {{response.status_code}}; retry {{attempt}}/{{max_attempts - 1}}",
                "WARN"
            )
        except Exception as error:
            if not safe_to_retry or attempt == max_attempts:
                raise
            write_log(
                f"{{log_prefix}}: request failed ({{error}}); retry {{attempt}}/{{max_attempts - 1}}",
                "WARN"
            )
        time.sleep(5 * attempt)
    return last_response

AGENT_SYNC_API = "db_connector.api_agent_sync"

def _api_message(response):
    try:
        return response.json().get("message", {{}})
    except Exception:
        return {{}}

def get_master_sync_config(session, log_prefix):
    """Fetch optional central settings; None means keep the legacy behavior."""
    response = request_with_retry(
        session,
        "GET",
        f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.get_sync_config",
        log_prefix,
        params={{
            "registration": registration_id,
            "source_id": source_id,
            "physical_hostname": physical_hostname,
            "database_name": db_database,
        }},
    )
    if response.status_code != 200:
        write_log(
            f"{{log_prefix}}: central sync API unavailable (HTTP {{response.status_code}}); using legacy compatibility mode",
            "WARN",
        )
        return None
    config = _api_message(response)
    return config if isinstance(config, dict) else None

def acquire_central_sync_lease(session, config, run_id, log_prefix):
    if not config or not config.get("coordination_enabled"):
        return None
    token_seed = f"{{source_id}}:{{registration_id}}:{{run_id}}:{{os.getpid()}}"
    lease_token = _hashlib.sha256(token_seed.encode("utf-8")).hexdigest()
    waiting_logged = False
    while True:
        response = request_with_retry(
            session,
            "POST",
            f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.acquire_sync_lease",
            log_prefix,
            json={{
                "registration": registration_id,
                "source_id": source_id,
                "run_id": run_id,
                "physical_hostname": physical_hostname,
                "database_name": db_database,
                "lease_token": lease_token,
            }},
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"central lease request failed (HTTP {{response.status_code}}): {{response.text[:300]}}"
            )
        result = _api_message(response)
        if result.get("acquired"):
            if waiting_logged:
                write_log(f"{{log_prefix}}: central CCD Master capacity acquired")
            return {{
                "token": result.get("lease_token") or lease_token,
                "run_id": run_id,
            }}
        if result.get("reason") == "coordination_disabled":
            return None
        if not waiting_logged:
            reason = result.get("reason") or "busy"
            write_log(
                f"{{log_prefix}}: waiting for central CCD Master capacity ({{reason}})",
                "WARN",
            )
            waiting_logged = True
        time.sleep(max(1, min(30, int(result.get("retry_after") or 5))))

def heartbeat_central_sync_lease(session, lease, log_prefix):
    if not lease:
        return
    response = request_with_retry(
        session,
        "POST",
        f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.heartbeat_sync_lease",
        log_prefix,
        json={{
            "registration": registration_id,
            "source_id": source_id,
            "lease_token": lease["token"],
            "run_id": lease["run_id"],
        }},
    )
    if response.status_code != 200 or not _api_message(response).get("renewed"):
        raise RuntimeError("central CCD Master sync lease expired")

def release_central_sync_lease(session, lease, log_prefix, error=""):
    if not lease:
        return
    try:
        response = request_with_retry(
            session,
            "POST",
            f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.release_sync_lease",
            log_prefix,
            json={{
                "registration": registration_id,
                "source_id": source_id,
                "lease_token": lease["token"],
                "run_id": lease["run_id"],
                "error": str(error or "")[:1000],
            }},
        )
        if response.status_code != 200:
            write_log(
                f"{{log_prefix}}: central lease release failed (HTTP {{response.status_code}})",
                "WARN",
            )
    except Exception as release_error:
        write_log(f"{{log_prefix}}: central lease release failed: {{release_error}}", "WARN")

def report_zero_delta(session, config, log_prefix):
    if not config or not config.get("enabled"):
        return
    try:
        request_with_retry(
            session,
            "POST",
            f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.report_no_changes",
            log_prefix,
            json={{
                "registration": registration_id,
                "source_id": source_id,
                "physical_hostname": physical_hostname,
                "database_name": db_database,
            }},
        )
    except Exception as report_error:
        write_log(f"{{log_prefix}}: could not report zero-delta state: {{report_error}}", "WARN")

def load_delta_cache(cache_path):
    """Load delta cache. Returns {{ccd_source_key: hash}} dict or None if missing/invalid."""
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("version") == 1 and isinstance(data.get("records"), dict):
            return data["records"]
    except Exception:
        pass
    return None

def save_delta_cache(cache_path, records_dict):
    """Save delta cache {{ccd_source_key: hash}} to file."""
    temp_path = cache_path + ".tmp"
    try:
        os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
        data = {{"version": 1, "last_sync": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "records": records_dict}}
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(temp_path, cache_path)
    except Exception as e:
        write_log(f"save_delta_cache: failed to write {{cache_path}}: {{e}}", "WARN")
        for stale_path in (temp_path, cache_path):
            try:
                if os.path.exists(stale_path):
                    os.remove(stale_path)
            except Exception:
                pass

def invalidate_delta_cache(cache_path, log_prefix):
    try:
        if os.path.exists(cache_path):
            os.remove(cache_path)
        write_log(f"{{log_prefix}}: cache invalidated; next run will perform a full resync", "WARN")
    except Exception as error:
        write_log(f"{{log_prefix}}: could not invalidate cache: {{error}}", "ERROR")

def compute_row_hash(cleaned_row):
    """MD5 hash of cleaned row dict, excluding ccd_source_key and ccd_reg_source."""
    to_hash = {{k: v for k, v in cleaned_row.items() if k not in ("ccd_source_key", "ccd_reg_source")}}
    return _hashlib.md5(json.dumps(sorted(to_hash.items())).encode()).hexdigest()

def extract_assignment_fields(expr):
    """Return source field names referenced by assignment helper calls."""
    if not expr:
        return []
    names = []
    for helper in ("field", "text", "num", "date", "unix_date"):
        pattern = helper + '[(]["\\']([^"\\']+)["\\']'
        for name in re.findall(pattern, expr):
            if name not in names:
                names.append(name)
    return names

def eval_assignment(expr, raw_row, log_prefix="ASSIGNMENT"):
    """Evaluate a restricted Python expression against one source row."""
    if expr is None:
        return ""
    expr = str(expr).strip()
    if not expr:
        return ""

    def field(name, default=""):
        val = raw_row.get(name, default)
        return default if val is None else val

    def text(name, default=""):
        val = field(name, default)
        return "" if val is None else str(val)

    def num(name, default=0):
        val = field(name, default)
        if val in (None, ""):
            return default
        try:
            return float(val)
        except Exception:
            return default

    def date(name, default=""):
        return text(name, default)

    def unix_date(name, default="", offset_hours=0, zero_is_empty=True):
        value = field(name, default)
        if value in (None, ""):
            return default
        try:
            timestamp = float(value)
            if zero_is_empty and timestamp == 0:
                return default
            converted = datetime(1970, 1, 1) + timedelta(
                seconds=timestamp, hours=float(offset_hours)
            )
            return converted.strftime("%Y-%m-%d")
        except (TypeError, ValueError, OverflowError):
            return default

    safe_globals = {{"__builtins__": {{}}}}
    safe_locals = {{
        "field": field,
        "text": text,
        "num": num,
        "date": date,
        "unix_date": unix_date,
        "int": int,
        "float": float,
        "str": str,
        "len": len,
        "round": round,
        "min": min,
        "max": max,
    }}
    try:
        return eval(expr, safe_globals, safe_locals)
    except Exception as e:
        write_log(f"{{log_prefix}} error for expression [{{expr}}]: {{e}}", "ERROR")
        return ""

def execute_step(step, prev_result):
    """Execute a single pipeline step. Returns result for next step."""
    step_stripped = step.strip()
    step_upper = step_stripped.upper()

    # ---- SAVE_TO_FILE macro ----
    if step_upper.startswith("SAVE_TO_FILE "):
        filename = step_stripped[len("SAVE_TO_FILE "):].strip()
        if prev_result is None:
            write_log("SAVE_TO_FILE: no data from previous step", "WARN")
            return prev_result
        filepath = os.path.join(os.path.dirname(log_file), filename)
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        try:
            if filename.lower().endswith(".json"):
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(prev_result, f, indent=2, default=str)
            elif filename.lower().endswith(".csv"):
                with open(filepath, "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    if isinstance(prev_result, dict) and "columns" in prev_result:
                        writer.writerow(prev_result["columns"])
                        writer.writerows(prev_result.get("rows", []))
                    elif isinstance(prev_result, list):
                        for row in prev_result:
                            writer.writerow(row if isinstance(row, (list, tuple)) else [row])
                    else:
                        writer.writerow([str(prev_result)])
            else:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(str(prev_result))
            write_log(f"SAVE_TO_FILE: wrote {{filepath}}")
            return prev_result
        except Exception as e:
            write_log(f"SAVE_TO_FILE error: {{e}}", "ERROR")
            return prev_result

    # ---- SEND_TO_ERPNEXT macro ----
    if step_upper.startswith("SEND_TO_ERPNEXT"):
        if not erpnext_url:
            write_log("SEND_TO_ERPNEXT: no erpnext_url configured", "ERROR")
            return prev_result
        try:
            import requests as _req
            sess = _req.Session()
            sess.verify = True
            login_r = request_with_retry(sess, "POST", f"{{erpnext_url}}/api/method/login",
                                         "SEND_TO_ERPNEXT", data={{"usr": erpnext_user, "pwd": erpnext_pass}})
            if login_r.status_code != 200:
                write_log(f"SEND_TO_ERPNEXT: login failed ({{login_r.status_code}})", "ERROR")
                return prev_result
            payload = prev_result
            if isinstance(payload, dict) and "columns" in payload and "rows" in payload:
                cols = payload["columns"]
                payload = [dict(zip(cols, row)) for row in payload["rows"]]
            post_r = request_with_retry(
                sess, "POST", f"{{erpnext_url}}/api/method/agent_receive_data", "SEND_TO_ERPNEXT",
                json={{"data": json.dumps(payload, default=str)}}
            )
            if post_r.status_code == 200:
                write_log(f"SEND_TO_ERPNEXT: OK")
            else:
                write_log(f"SEND_TO_ERPNEXT: failed ({{post_r.status_code}}): {{post_r.text[:500]}}", "ERROR")
        except Exception as e:
            write_log(f"SEND_TO_ERPNEXT error: {{e}}", "ERROR")
        return prev_result

    # ---- SYNC_TO_CCD_REG_BULK macro ----
    if step_upper.startswith("SYNC_TO_CCD_REG_BULK"):
        if not erpnext_url:
            write_log("SYNC_TO_CCD_REG_BULK: no erpnext_url configured", "ERROR")
            return prev_result
        try:
            import requests as _req, re as _re
            sess = _req.Session()
            sess.verify = True
            login_r = request_with_retry(sess, "POST", f"{{erpnext_url}}/api/method/login",
                                         "SYNC_TO_CCD_REG_BULK", data={{"usr": erpnext_user, "pwd": erpnext_pass}})
            if login_r.status_code != 200:
                write_log(f"SYNC_TO_CCD_REG_BULK: login failed ({{login_r.status_code}})", "ERROR")
                return prev_result
            ccd_doctype = ccd_reg_doctype

            # Fetch field_mapping for fieldtype-aware value cleaning
            fieldtype_map = {{}}
            assignment_map = {{}}
            reg_r = request_with_retry(
                sess, "GET", f"{{erpnext_url}}/api/resource/CCD Registration/{{_urlquote(registration_id, safe='')}}",
                "SYNC_TO_CCD_REG_BULK"
            )
            if reg_r.status_code == 200:
                _reg_data = reg_r.json().get("data", {{}})
                _fm = _reg_data.get("fieldmatch", [])
                fieldtype_map = {{
                    r.get("ccd_fieldname", ""): r.get("fieldtype", "Data")
                    for r in _fm if r.get("ccd_fieldname")
                }}
                assignment_map = {{
                    r.get("ccd_fieldname", ""): (r.get("assignment") or "").strip()
                    for r in _fm if r.get("ccd_fieldname")
                }}
            # Read primary key field(s) from Connection Information tab
            _pk_raw = reg_r.json().get("data", {{}}).get("ccd_primaykey_field", "") if reg_r.status_code == 200 else ""
            pk_fields_reg = [f.strip() for f in _pk_raw.split("+") if f.strip()]

            def normalize_phone_bulk_reg(val):
                s = str(val).strip() if val else ""
                if not s:
                    return ""
                if _re.match('^[+][0-9]', s):
                    return s
                m = _re.match('^[(][+]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                m = _re.match('^[(]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                m = _re.match('^00([0-9]{{1,4}})[ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                return s

            def clean_value_bulk_reg(field_name, val):
                ft = fieldtype_map.get(field_name, "Data")
                if val is None:
                    return ""
                if ft == "Phone":
                    return normalize_phone_bulk_reg(val)
                return str(val)

            if isinstance(prev_result, dict) and "columns" in prev_result and "rows" in prev_result:
                cols = prev_result["columns"]
                data_rows = [dict(zip(cols, row)) for row in prev_result["rows"]]
                write_log(f"SYNC_TO_CCD_REG_BULK: using piped data — {{len(data_rows)}} row(s)")
            else:
                if not ccd_table:
                    write_log("SYNC_TO_CCD_REG_BULK: ccd_table not configured", "ERROR")
                    return prev_result
                if reg_r.status_code != 200:
                    write_log(f"SYNC_TO_CCD_REG_BULK: cannot fetch CCD Registration ({{reg_r.status_code}})", "ERROR")
                    return prev_result
                ccd_fields = list(fieldtype_map.keys())
                if not ccd_fields:
                    write_log("SYNC_TO_CCD_REG_BULK: no fields in field_matching table", "ERROR")
                    return prev_result
                assignment_fields = []
                for assignment in assignment_map.values():
                    for assignment_field in extract_assignment_fields(assignment):
                        if assignment_field not in ccd_fields and assignment_field not in assignment_fields:
                            assignment_fields.append(assignment_field)
                # Include pk fields in SELECT even if not in Field Matching
                selected_fields = ccd_fields + assignment_fields
                extra_pk = [f for f in pk_fields_reg if f not in selected_fields]
                sql = "SELECT " + ", ".join(selected_fields + extra_pk) + " FROM " + ccd_table
                write_log(f"SYNC_TO_CCD_REG_BULK: SQL = {{sql}}")
                try:
                    query_result = fetch_db_rows(sql, "SYNC_TO_CCD_REG_BULK")
                    if query_result is None:
                        return prev_result
                    columns, rows = query_result
                    data_rows = [dict(zip(columns, row)) for row in rows]
                    write_log(f"SYNC_TO_CCD_REG_BULK: fetched {{len(data_rows)}} row(s) from client DB")
                except Exception as e:
                    write_log(f"SYNC_TO_CCD_REG_BULK: SQL error: {{e}}", "ERROR")
                    return prev_result

            # --- Phase 2: build client_map {{ccd_source_key: (hash, cleaned_row)}} ---
            client_map = {{}}
            all_cleaned = []
            for _row in data_rows:
                _cleaned = {{}}
                for field_name in fieldtype_map:
                    assignment = assignment_map.get(field_name, "")
                    if assignment:
                        value = eval_assignment(assignment, _row, f"SYNC_TO_CCD_REG_BULK {{field_name}}")
                    elif field_name in _row:
                        value = _row[field_name]
                    else:
                        continue
                    _cleaned[field_name.lower()] = clean_value_bulk_reg(field_name, value)
                if pk_fields_reg:
                    _cleaned["ccd_source_key"] = "+".join(str(_row.get(f, "")) for f in pk_fields_reg)
                _key = _cleaned.get("ccd_source_key", "")
                _hash = compute_row_hash(_cleaned)
                client_map[_key] = (_hash, _cleaned)
                all_cleaned.append(_cleaned)

            _cache_path = os.path.join(os.path.dirname(log_file), f"{{source_id}}_CCD-REG_delta_cache.json")
            _cache = load_delta_cache(_cache_path)
            _progress_cache = dict(_cache or {{}})
            _progress_ready = _cache is not None
            batch_size = 100
            created, deleted, updated, errors = 0, 0, 0, 0

            def checkpoint_reg_rows(rows_to_checkpoint):
                for checkpoint_row in rows_to_checkpoint:
                    checkpoint_key = checkpoint_row.get("ccd_source_key", "")
                    if checkpoint_key in client_map:
                        _progress_cache[checkpoint_key] = client_map[checkpoint_key][0]

            if _cache is None:
                write_log(f"SYNC_TO_CCD_REG_BULK: no cache — running full sync")
                clear_r = request_with_retry(
                    sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG_BULK",
                    json={{"action": "clear", "doctype": ccd_doctype}}
                )
                if clear_r.status_code != 200:
                    write_log(f"SYNC_TO_CCD_REG_BULK: clear failed ({{clear_r.status_code}}): {{clear_r.text[:300]}}", "ERROR")
                    return prev_result
                _progress_cache.clear()
                _progress_ready = True
                save_delta_cache(_cache_path, _progress_cache)
                write_log(f"SYNC_TO_CCD_REG_BULK: cleared all existing records (bulk)")
                total_rows = len(all_cleaned)
                for i in range(0, total_rows, batch_size):
                    chunk = all_cleaned[i:i + batch_size]
                    ins_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG_BULK",
                        safe_to_retry=False,
                        json={{"action": "insert_batch", "doctype": ccd_doctype, "rows": json.dumps(chunk)}}
                    )
                    if ins_r.status_code == 200:
                        msg = ins_r.json().get("message", {{}})
                        created += msg.get("inserted", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            checkpoint_reg_rows(chunk)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_REG_BULK: insert error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_REG_BULK: batch failed ({{ins_r.status_code}}): {{ins_r.text[:300]}}", "ERROR")
                    _bn = i // batch_size + 1
                    _bt = (total_rows + batch_size - 1) // batch_size
                    if _bn % 10 == 0 or _bn == _bt or ins_r.status_code != 200 or _berrs:
                        save_delta_cache(_cache_path, _progress_cache)
                    write_log(f"SYNC_TO_CCD_REG_BULK: insert batch {{_bn}}/{{_bt}} done")
            else:
                to_insert = [row for ck, (ch, row) in client_map.items() if ck not in _cache]
                to_delete = [ck for ck in _cache if ck not in client_map]
                to_update = [row for ck, (ch, row) in client_map.items() if ck in _cache and ch != _cache[ck]]
                write_log(f"SYNC_TO_CCD_REG_BULK: delta — {{len(to_insert)}} insert, {{len(to_delete)}} delete, {{len(to_update)}} update")

                for i in range(0, len(to_insert), batch_size):
                    chunk = to_insert[i:i + batch_size]
                    ins_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG_BULK",
                        safe_to_retry=False,
                        json={{"action": "insert_batch", "doctype": ccd_doctype, "rows": json.dumps(chunk)}}
                    )
                    if ins_r.status_code == 200:
                        msg = ins_r.json().get("message", {{}})
                        created += msg.get("inserted", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            checkpoint_reg_rows(chunk)
                            save_delta_cache(_cache_path, _progress_cache)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_REG_BULK: insert error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_REG_BULK: insert failed ({{ins_r.status_code}}): {{ins_r.text[:300]}}", "ERROR")

                for i in range(0, len(to_delete), batch_size):
                    chunk = to_delete[i:i + batch_size]
                    del_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG_BULK",
                        json={{"action": "delete_by_source_keys", "doctype": ccd_doctype, "keys": json.dumps(chunk)}}
                    )
                    if del_r.status_code == 200:
                        deleted += del_r.json().get("message", {{}}).get("deleted", len(chunk))
                        for deleted_key in chunk:
                            _progress_cache.pop(deleted_key, None)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_REG_BULK: delete failed ({{del_r.status_code}}): {{del_r.text[:300]}}", "ERROR")

                for i in range(0, len(to_update), batch_size):
                    chunk = to_update[i:i + batch_size]
                    upd_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG_BULK",
                        json={{"action": "update_batch", "doctype": ccd_doctype, "rows": json.dumps(chunk)}}
                    )
                    if upd_r.status_code == 200:
                        msg = upd_r.json().get("message", {{}})
                        updated += msg.get("updated", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            checkpoint_reg_rows(chunk)
                            save_delta_cache(_cache_path, _progress_cache)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_REG_BULK: update error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_REG_BULK: update failed ({{upd_r.status_code}}): {{upd_r.text[:300]}}", "ERROR")

            save_delta_cache(_cache_path, _progress_cache)
            if errors:
                pending = sum(1 for key, (row_hash, row) in client_map.items() if _progress_cache.get(key) != row_hash)
                pending += sum(1 for key in _progress_cache if key not in client_map)
                write_log(
                    f"SYNC_TO_CCD_REG_BULK: progress checkpoint saved; {{pending}} unconfirmed operation(s) will retry next run",
                    "WARN"
                )
            write_log(f"SYNC_TO_CCD_REG_BULK: done — {{created}} inserted, {{deleted}} deleted, {{updated}} updated, {{errors}} error(s)")
        except Exception as e:
            write_log(f"SYNC_TO_CCD_REG_BULK error: {{e}}", "ERROR")
            if "_progress_ready" in locals() and _progress_ready:
                save_delta_cache(_cache_path, _progress_cache)
                write_log("SYNC_TO_CCD_REG_BULK: progress checkpoint preserved after interruption", "WARN")
        return prev_result

    # ---- SYNC_TO_CCD_REG macro ----
    if step_upper.startswith("SYNC_TO_CCD_REG"):
        if not erpnext_url:
            write_log("SYNC_TO_CCD_REG: no erpnext_url configured", "ERROR")
            return prev_result
        try:
            import requests as _req, re as _re
            sess = _req.Session()
            sess.verify = True
            login_r = request_with_retry(sess, "POST", f"{{erpnext_url}}/api/method/login",
                                         "SYNC_TO_CCD_REG", data={{"usr": erpnext_user, "pwd": erpnext_pass}})
            if login_r.status_code != 200:
                write_log(f"SYNC_TO_CCD_REG: login failed ({{login_r.status_code}})", "ERROR")
                return prev_result
            ccd_doctype = ccd_reg_doctype

            # Always fetch field_mapping for fieldtype-aware value cleaning (both modes)
            fieldtype_map = {{}}
            assignment_map = {{}}
            reg_r = request_with_retry(
                sess, "GET", f"{{erpnext_url}}/api/resource/CCD Registration/{{_urlquote(registration_id, safe='')}}",
                "SYNC_TO_CCD_REG"
            )
            if reg_r.status_code == 200:
                _fm = reg_r.json().get("data", {{}}).get("fieldmatch", [])
                fieldtype_map = {{
                    r.get("ccd_fieldname", ""): r.get("fieldtype", "Data")
                    for r in _fm if r.get("ccd_fieldname")
                }}
                assignment_map = {{
                    r.get("ccd_fieldname", ""): (r.get("assignment") or "").strip()
                    for r in _fm if r.get("ccd_fieldname")
                }}
            # Read primary key field(s) from Connection Information tab
            _pk_raw = reg_r.json().get("data", {{}}).get("ccd_primaykey_field", "") if reg_r.status_code == 200 else ""
            pk_fields_reg = [f.strip() for f in _pk_raw.split("+") if f.strip()]

            def normalize_phone(val):
                """Normalize phone numbers of any common format to +CC NNNN NNNN."""
                s = str(val).strip() if val else ""
                if not s:
                    return ""
                # Already +XX... — pass through as-is
                if _re.match('^[+][0-9]', s):
                    return s
                # (+XX) NNN... → +XX NNN...
                m = _re.match('^[(][+]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                # (XX) NNN... → +XX NNN...
                m = _re.match('^[(]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                # 00XX NNN... → +XX NNN...
                m = _re.match('^00([0-9]{{1,4}})[ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                # No detectable country code — pass through as-is
                return s

            def clean_value(field_name, val):
                """Convert a DB value to a Frappe-safe string, with type-aware handling."""
                ft = fieldtype_map.get(field_name, "Data")
                if val is None:
                    return ""
                if ft == "Phone":
                    return normalize_phone(val)
                return str(val)

            if isinstance(prev_result, dict) and "columns" in prev_result and "rows" in prev_result:
                # Piped mode: data comes from a previous SELECT step
                cols = prev_result["columns"]
                data_rows = [dict(zip(cols, row)) for row in prev_result["rows"]]
                write_log(f"SYNC_TO_CCD_REG: using piped data — {{len(data_rows)}} row(s)")
            else:
                # Self-contained: build SELECT from fieldtype_map keys (= ccd_fieldname list)
                if not ccd_table:
                    write_log("SYNC_TO_CCD_REG: ccd_table not configured", "ERROR")
                    return prev_result
                if reg_r.status_code != 200:
                    write_log(f"SYNC_TO_CCD_REG: cannot fetch CCD Registration ({{reg_r.status_code}})", "ERROR")
                    return prev_result
                ccd_fields = list(fieldtype_map.keys())
                if not ccd_fields:
                    write_log("SYNC_TO_CCD_REG: no fields in field_matching table", "ERROR")
                    return prev_result
                assignment_fields = []
                for assignment in assignment_map.values():
                    for assignment_field in extract_assignment_fields(assignment):
                        if assignment_field not in ccd_fields and assignment_field not in assignment_fields:
                            assignment_fields.append(assignment_field)
                # Include pk fields in SELECT even if not in Field Matching
                selected_fields = ccd_fields + assignment_fields
                extra_pk = [f for f in pk_fields_reg if f not in selected_fields]
                sql = "SELECT " + ", ".join(selected_fields + extra_pk) + " FROM " + ccd_table
                write_log(f"SYNC_TO_CCD_REG: SQL = {{sql}}")
                try:
                    query_result = fetch_db_rows(sql, "SYNC_TO_CCD_REG")
                    if query_result is None:
                        return prev_result
                    columns, rows = query_result
                    data_rows = [dict(zip(columns, row)) for row in rows]
                    write_log(f"SYNC_TO_CCD_REG: fetched {{len(data_rows)}} row(s) from client DB")
                except Exception as e:
                    write_log(f"SYNC_TO_CCD_REG: SQL error: {{e}}", "ERROR")
                    return prev_result

            # --- Phase 2: build client_map {{ccd_source_key: (hash, cleaned_row)}} ---
            client_map = {{}}
            for row_data in data_rows:
                _cleaned = {{}}
                for field_name in fieldtype_map:
                    assignment = assignment_map.get(field_name, "")
                    if assignment:
                        value = eval_assignment(assignment, row_data, f"SYNC_TO_CCD_REG {{field_name}}")
                    elif field_name in row_data:
                        value = row_data[field_name]
                    else:
                        continue
                    _cleaned[field_name.lower()] = clean_value(field_name, value)
                if pk_fields_reg:
                    _cleaned["ccd_source_key"] = "+".join(str(row_data.get(f, "")) for f in pk_fields_reg)
                _key = _cleaned.get("ccd_source_key", "")
                _hash = compute_row_hash(_cleaned)
                client_map[_key] = (_hash, _cleaned)

            _cache_path = os.path.join(os.path.dirname(log_file), f"{{source_id}}_CCD-REG_delta_cache.json")
            _cache = load_delta_cache(_cache_path)
            _progress_cache = dict(_cache or {{}})
            _progress_ready = _cache is not None
            batch_size = 100
            created, deleted, updated, errors = 0, 0, 0, 0

            def checkpoint_reg_row(checkpoint_row):
                checkpoint_key = checkpoint_row.get("ccd_source_key", "")
                if checkpoint_key in client_map:
                    _progress_cache[checkpoint_key] = client_map[checkpoint_key][0]

            if _cache is None:
                write_log(f"SYNC_TO_CCD_REG: no cache — running full sync")
                all_existing = []
                _limit = 500
                _start = 0
                _ccd_doctype_url = _urlquote(ccd_doctype, safe='')
                while True:
                    page_r = request_with_retry(
                        sess, "GET", f"{{erpnext_url}}/api/resource/{{_ccd_doctype_url}}", "SYNC_TO_CCD_REG",
                        params={{"limit_page_length": _limit, "limit_start": _start, "fields": '["name"]'}}
                    )
                    if page_r.status_code != 200:
                        errors += 1
                        write_log(f"SYNC_TO_CCD_REG: list failed ({{page_r.status_code}}): {{page_r.text[:300]}}", "ERROR")
                        return prev_result
                    page_data = page_r.json().get("data", [])
                    if not page_data:
                        break
                    all_existing.extend(page_data)
                    if len(page_data) < _limit:
                        break
                    _start += _limit
                for item in all_existing:
                    delete_r = request_with_retry(
                        sess, "DELETE", f"{{erpnext_url}}/api/resource/{{_ccd_doctype_url}}/{{_urlquote(item['name'], safe='')}}",
                        "SYNC_TO_CCD_REG"
                    )
                    if delete_r.status_code not in (200, 202):
                        errors += 1
                        write_log(f"SYNC_TO_CCD_REG: delete failed ({{delete_r.status_code}}): {{delete_r.text[:300]}}", "ERROR")
                if errors:
                    invalidate_delta_cache(_cache_path, "SYNC_TO_CCD_REG")
                    return prev_result
                _progress_cache.clear()
                _progress_ready = True
                save_delta_cache(_cache_path, _progress_cache)
                write_log(f"SYNC_TO_CCD_REG: cleared {{len(all_existing)}} existing record(s)")
                for ck, (ch, crow) in client_map.items():
                    r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/resource/{{_ccd_doctype_url}}", "SYNC_TO_CCD_REG",
                        safe_to_retry=False, json=crow
                    )
                    if r.status_code in (200, 201):
                        created += 1
                        checkpoint_reg_row(crow)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += 1
                        write_log(f"SYNC_TO_CCD_REG: insert error ({{r.status_code}}): {{r.text[:300]}}", "ERROR")
            else:
                to_insert = [row for ck, (ch, row) in client_map.items() if ck not in _cache]
                to_delete = [ck for ck in _cache if ck not in client_map]
                to_update = [row for ck, (ch, row) in client_map.items() if ck in _cache and ch != _cache[ck]]
                write_log(f"SYNC_TO_CCD_REG: delta — {{len(to_insert)}} insert, {{len(to_delete)}} delete, {{len(to_update)}} update")

                for _row in to_insert:
                    _ccd_doctype_url = _urlquote(ccd_doctype, safe='')
                    r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/resource/{{_ccd_doctype_url}}", "SYNC_TO_CCD_REG",
                        safe_to_retry=False, json=_row
                    )
                    if r.status_code in (200, 201):
                        created += 1
                        checkpoint_reg_row(_row)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += 1
                        write_log(f"SYNC_TO_CCD_REG: insert error ({{r.status_code}}): {{r.text[:300]}}", "ERROR")

                for i in range(0, len(to_delete), batch_size):
                    chunk = to_delete[i:i + batch_size]
                    del_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG",
                        json={{"action": "delete_by_source_keys", "doctype": ccd_doctype, "keys": json.dumps(chunk)}}
                    )
                    if del_r.status_code == 200:
                        deleted += del_r.json().get("message", {{}}).get("deleted", len(chunk))
                        for deleted_key in chunk:
                            _progress_cache.pop(deleted_key, None)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_REG: delete failed ({{del_r.status_code}}): {{del_r.text[:300]}}", "ERROR")

                for i in range(0, len(to_update), batch_size):
                    chunk = to_update[i:i + batch_size]
                    upd_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_REG",
                        json={{"action": "update_batch", "doctype": ccd_doctype, "rows": json.dumps(chunk)}}
                    )
                    if upd_r.status_code == 200:
                        msg = upd_r.json().get("message", {{}})
                        updated += msg.get("updated", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            for checkpoint_row in chunk:
                                checkpoint_reg_row(checkpoint_row)
                            save_delta_cache(_cache_path, _progress_cache)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_REG: update error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_REG: update failed ({{upd_r.status_code}}): {{upd_r.text[:300]}}", "ERROR")

            save_delta_cache(_cache_path, _progress_cache)
            if errors:
                pending = sum(1 for key, (row_hash, row) in client_map.items() if _progress_cache.get(key) != row_hash)
                pending += sum(1 for key in _progress_cache if key not in client_map)
                write_log(
                    f"SYNC_TO_CCD_REG: progress checkpoint saved; {{pending}} unconfirmed operation(s) will retry next run",
                    "WARN"
                )
            write_log(f"SYNC_TO_CCD_REG: done — {{created}} inserted, {{deleted}} deleted, {{updated}} updated, {{errors}} error(s)")
        except Exception as e:
            write_log(f"SYNC_TO_CCD_REG error: {{e}}", "ERROR")
            if "_progress_ready" in locals() and _progress_ready:
                save_delta_cache(_cache_path, _progress_cache)
                write_log("SYNC_TO_CCD_REG: progress checkpoint preserved after interruption", "WARN")
        return prev_result

    # ---- SYNC_TO_CCD_MASTER_BULK macro ----
    if step_upper.startswith("SYNC_TO_CCD_MASTER_BULK"):
        if not erpnext_url:
            write_log("SYNC_TO_CCD_MASTER_BULK: no erpnext_url configured", "ERROR")
            return prev_result
        master_sync_lock = None
        central_master_lease = None
        _master_sync_config = None
        _master_sync_error = ""
        _master_run_id = _hashlib.sha256(
            f"{{source_id}}:{{registration_id}}:{{time.time_ns()}}:{{os.getpid()}}".encode("utf-8")
        ).hexdigest()
        try:
            import requests as _req, re as _re
            sess = _req.Session()
            sess.verify = True
            login_r = request_with_retry(sess, "POST", f"{{erpnext_url}}/api/method/login",
                                         "SYNC_TO_CCD_MASTER_BULK", data={{"usr": erpnext_user, "pwd": erpnext_pass}})
            if login_r.status_code != 200:
                write_log(f"SYNC_TO_CCD_MASTER_BULK: login failed ({{login_r.status_code}})", "ERROR")
                return prev_result

            _master_sync_config = get_master_sync_config(sess, "SYNC_TO_CCD_MASTER_BULK")

            def acquire_master_mutation_slot():
                nonlocal master_sync_lock, central_master_lease
                if central_master_lease or master_sync_lock:
                    return
                if _master_sync_config and _master_sync_config.get("coordination_enabled"):
                    central_master_lease = acquire_central_sync_lease(
                        sess,
                        _master_sync_config,
                        _master_run_id,
                        "SYNC_TO_CCD_MASTER_BULK",
                    )
                if not central_master_lease:
                    # Compatibility for a server where the new central API is
                    # not installed or is intentionally disabled.
                    master_sync_lock = acquire_shared_sync_lock(
                        "CCD Master sync", "SYNC_TO_CCD_MASTER_BULK"
                    )

            _use_fast_master_insert = bool(
                _master_sync_config
                and _master_sync_config.get("fast_insert_enabled")
                and _master_sync_config.get("coordination_enabled")
            )

            def insert_master_chunk(chunk):
                if _use_fast_master_insert:
                    response = request_with_retry(
                        sess,
                        "POST",
                        f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.fast_insert_master_batch",
                        "SYNC_TO_CCD_MASTER_BULK",
                        # The central endpoint treats existing source keys as
                        # confirmed retries, so an ambiguous HTTP response is
                        # safe to repeat with the same lease/run.
                        safe_to_retry=True,
                        json={{
                            "registration": registration_id,
                            "source_id": source_id,
                            "lease_token": central_master_lease["token"],
                            "run_id": central_master_lease["run_id"],
                            "rows": chunk,
                        }},
                    )
                else:
                    response = request_with_retry(
                        sess,
                        "POST",
                        f"{{erpnext_url}}/api/method/agent_bulk_sync",
                        "SYNC_TO_CCD_MASTER_BULK",
                        safe_to_retry=False,
                        json={{
                            "action": "insert_batch",
                            "doctype": "CCD Master",
                            "rows": json.dumps(chunk),
                        }},
                    )
                if central_master_lease:
                    heartbeat_central_sync_lease(
                        sess, central_master_lease, "SYNC_TO_CCD_MASTER_BULK"
                    )
                return response

            reg_r = request_with_retry(
                sess, "GET", f"{{erpnext_url}}/api/resource/CCD Registration/{{_urlquote(registration_id, safe='')}}",
                "SYNC_TO_CCD_MASTER_BULK"
            )
            if reg_r.status_code != 200:
                write_log(f"SYNC_TO_CCD_MASTER_BULK: cannot fetch CCD Registration ({{reg_r.status_code}})", "ERROR")
                return prev_result
            _fm = reg_r.json().get("data", {{}}).get("fieldmatch", [])

            def parse_sys_name_bulk(raw):
                return raw.split(":")[0].strip() if raw else ""

            master_rules_bulk = []
            for row in _fm:
                ccd_f = (row.get("ccd_fieldname") or "").strip()
                sys_f = parse_sys_name_bulk(row.get("sys_fieldname", ""))
                ftype = row.get("fieldtype", "Data")
                assignment = (row.get("assignment") or "").strip()
                if sys_f and (ccd_f or assignment):
                    master_rules_bulk.append({{
                        "ccd_field": ccd_f,
                        "sys_field": sys_f,
                        "fieldtype": ftype,
                        "assignment": assignment,
                    }})

            if not master_rules_bulk:
                write_log("SYNC_TO_CCD_MASTER_BULK: no field mappings found in fieldmatch", "ERROR")
                return prev_result

            # Read primary key field(s) from Connection Information tab
            _pk_raw_master = reg_r.json().get("data", {{}}).get("ccd_primaykey_field", "")
            pk_fields_master = [f.strip() for f in _pk_raw_master.split("+") if f.strip()]

            def normalize_phone_bulk_master(val):
                s = str(val).strip() if val else ""
                if not s:
                    return ""
                if _re.match('^[+][0-9]', s):
                    return s
                m = _re.match('^[(][+]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                m = _re.match('^[(]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                m = _re.match('^00([0-9]{{1,4}})[ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                return s

            def clean_value_bulk_master(rule, val):
                ft = rule.get("fieldtype", "Data")
                if val is None:
                    return ""
                if ft == "Phone":
                    return normalize_phone_bulk_master(val)
                return str(val)

            def _append_unique(target, values):
                for value in values:
                    if value and value not in target:
                        target.append(value)

            if isinstance(prev_result, dict) and "columns" in prev_result and "rows" in prev_result:
                cols = prev_result["columns"]
                raw_rows = [dict(zip(cols, row)) for row in prev_result["rows"]]
                write_log(f"SYNC_TO_CCD_MASTER_BULK: using piped data — {{len(raw_rows)}} row(s)")
            else:
                if not ccd_table:
                    write_log("SYNC_TO_CCD_MASTER_BULK: ccd_table not configured", "ERROR")
                    return prev_result
                ccd_fields = []
                for rule in master_rules_bulk:
                    _append_unique(ccd_fields, [rule.get("ccd_field", "")])
                    _append_unique(ccd_fields, extract_assignment_fields(rule.get("assignment", "")))
                # Include pk fields in SELECT even if not in Field Matching
                extra_pk = [f for f in pk_fields_master if f not in ccd_fields]
                sql = "SELECT " + ", ".join(ccd_fields + extra_pk) + " FROM " + ccd_table
                write_log(f"SYNC_TO_CCD_MASTER_BULK: SQL = {{sql}}")
                try:
                    query_result = fetch_db_rows(sql, "SYNC_TO_CCD_MASTER_BULK")
                    if query_result is None:
                        return prev_result
                    columns, rows = query_result
                    raw_rows = [dict(zip(columns, row)) for row in rows]
                    write_log(f"SYNC_TO_CCD_MASTER_BULK: fetched {{len(raw_rows)}} row(s) from client DB")
                except Exception as e:
                    write_log(f"SYNC_TO_CCD_MASTER_BULK: SQL error: {{e}}", "ERROR")
                    return prev_result

            # Remap to sys_fieldname keys
            master_rows = []
            for raw in raw_rows:
                mapped = {{"ccd_reg_source": source_id}}
                for rule in master_rules_bulk:
                    assignment = rule.get("assignment", "")
                    ccd_f = rule.get("ccd_field", "")
                    if assignment:
                        val = eval_assignment(assignment, raw, f"SYNC_TO_CCD_MASTER_BULK {{rule.get('sys_field')}}")
                        mapped[rule["sys_field"]] = clean_value_bulk_master(rule, val)
                    elif ccd_f in raw:
                        mapped[rule["sys_field"]] = clean_value_bulk_master(rule, raw[ccd_f])
                if pk_fields_master:
                    mapped["ccd_source_key"] = "+".join(str(raw.get(f, "")) for f in pk_fields_master)
                master_rows.append(mapped)

            # --- Phase 2: build client_map {{ccd_source_key: (hash, row)}} from remapped master_rows ---
            client_map_master = {{}}
            for _mrow in master_rows:
                _key = _mrow.get("ccd_source_key", "")
                _hash = compute_row_hash(_mrow)
                client_map_master[_key] = (_hash, _mrow)

            _cache_path = os.path.join(os.path.dirname(log_file), f"{{source_id}}_CCD-Master_delta_cache.json")
            _cache = load_delta_cache(_cache_path)
            _progress_cache = dict(_cache or {{}})
            _progress_ready = _cache is not None
            batch_size = (
                max(1, int(_master_sync_config.get("batch_size") or 500))
                if _use_fast_master_insert
                else 40
            )
            created, deleted, updated, errors = 0, 0, 0, 0
            _full_sync_pending_path = _cache_path + ".full_sync_pending"

            def checkpoint_master_rows(rows_to_checkpoint):
                for checkpoint_row in rows_to_checkpoint:
                    checkpoint_key = checkpoint_row.get("ccd_source_key", "")
                    if checkpoint_key in client_map_master:
                        _progress_cache[checkpoint_key] = client_map_master[checkpoint_key][0]

            def reconcile_master_rows(rows_to_reconcile):
                confirmed = 0
                master_url = _urlquote("CCD Master", safe='')
                for reconcile_row in rows_to_reconcile:
                    reconcile_key = reconcile_row.get("ccd_source_key", "")
                    filters = json.dumps([
                        ["ccd_reg_source", "=", source_id],
                        ["ccd_source_key", "=", reconcile_key]
                    ])
                    check_r = request_with_retry(
                        sess, "GET", f"{{erpnext_url}}/api/resource/{{master_url}}", "SYNC_TO_CCD_MASTER_BULK",
                        params={{"filters": filters, "fields": '["name"]', "limit_page_length": 1}}
                    )
                    if check_r.status_code == 200 and check_r.json().get("data"):
                        checkpoint_master_rows([reconcile_row])
                        confirmed += 1
                if confirmed:
                    save_delta_cache(_cache_path, _progress_cache)
                    write_log(f"SYNC_TO_CCD_MASTER_BULK: reconciled {{confirmed}} committed row(s) after an ambiguous insert")
                return confirmed

            if _cache is None:
                write_log(f"SYNC_TO_CCD_MASTER_BULK: no cache — running full sync")
                try:
                    with open(_full_sync_pending_path, "w", encoding="utf-8") as marker:
                        marker.write(_master_run_id)
                except Exception as marker_error:
                    write_log(
                        f"SYNC_TO_CCD_MASTER_BULK: could not create full-sync marker: {{marker_error}}",
                        "WARN",
                    )
                acquire_master_mutation_slot()
                clear_r = request_with_retry(
                    sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_MASTER_BULK",
                    json={{"action": "clear", "doctype": "CCD Master", "source_id": source_id, "hostname": source_id}}
                )
                if clear_r.status_code != 200:
                    write_log(f"SYNC_TO_CCD_MASTER_BULK: clear failed ({{clear_r.status_code}}): {{clear_r.text[:300]}}", "ERROR")
                    return prev_result
                _progress_cache.clear()
                _progress_ready = True
                save_delta_cache(_cache_path, _progress_cache)
                write_log(f"SYNC_TO_CCD_MASTER_BULK: cleared all existing records for {{source_id}} (bulk)")
                all_rows = [row for ck, (ch, row) in client_map_master.items()]
                total_rows = len(all_rows)
                for i in range(0, total_rows, batch_size):
                    chunk = all_rows[i:i + batch_size]
                    ins_r = insert_master_chunk(chunk)
                    _berrs = []
                    if ins_r.status_code == 200:
                        msg = ins_r.json().get("message", {{}})
                        created += msg.get("inserted", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            checkpoint_master_rows(chunk)
                        else:
                            reconcile_master_rows(chunk)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_MASTER_BULK: insert error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_MASTER_BULK: batch failed ({{ins_r.status_code}}): {{ins_r.text[:300]}}", "ERROR")
                        write_log("SYNC_TO_CCD_MASTER_BULK: pausing 15 seconds after ambiguous insert failure", "WARN")
                        time.sleep(15)
                        reconcile_master_rows(chunk)
                    _bn = i // batch_size + 1
                    _bt = (total_rows + batch_size - 1) // batch_size
                    if _bn % 10 == 0 or _bn == _bt or ins_r.status_code != 200 or _berrs:
                        save_delta_cache(_cache_path, _progress_cache)
                    write_log(f"SYNC_TO_CCD_MASTER_BULK: insert batch {{_bn}}/{{_bt}} done")
            else:
                pending_inserts = [row for ck, (ch, row) in client_map_master.items() if ck not in _progress_cache]
                if pending_inserts:
                    reconcile_master_rows(pending_inserts)
                to_insert = [row for ck, (ch, row) in client_map_master.items() if ck not in _progress_cache]
                to_delete = [ck for ck in _progress_cache if ck not in client_map_master]
                to_update = [row for ck, (ch, row) in client_map_master.items() if ck in _progress_cache and ch != _progress_cache[ck]]
                write_log(f"SYNC_TO_CCD_MASTER_BULK: delta — {{len(to_insert)}} insert, {{len(to_delete)}} delete, {{len(to_update)}} update")

                if to_insert or to_delete or to_update:
                    acquire_master_mutation_slot()
                else:
                    report_zero_delta(
                        sess, _master_sync_config, "SYNC_TO_CCD_MASTER_BULK"
                    )
                    write_log(
                        "SYNC_TO_CCD_MASTER_BULK: zero delta — no synchronization slot acquired"
                    )

                for i in range(0, len(to_insert), batch_size):
                    chunk = to_insert[i:i + batch_size]
                    ins_r = insert_master_chunk(chunk)
                    if ins_r.status_code == 200:
                        msg = ins_r.json().get("message", {{}})
                        created += msg.get("inserted", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            checkpoint_master_rows(chunk)
                            save_delta_cache(_cache_path, _progress_cache)
                        else:
                            reconcile_master_rows(chunk)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_MASTER_BULK: insert error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_MASTER_BULK: insert failed ({{ins_r.status_code}}): {{ins_r.text[:300]}}", "ERROR")
                        write_log("SYNC_TO_CCD_MASTER_BULK: pausing 15 seconds after ambiguous insert failure", "WARN")
                        time.sleep(15)
                        reconcile_master_rows(chunk)

                for i in range(0, len(to_delete), batch_size):
                    chunk = to_delete[i:i + batch_size]
                    del_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_MASTER_BULK",
                        json={{"action": "delete_by_source_keys", "doctype": "CCD Master", "source_id": source_id, "hostname": source_id, "keys": json.dumps(chunk)}}
                    )
                    if central_master_lease:
                        heartbeat_central_sync_lease(
                            sess, central_master_lease, "SYNC_TO_CCD_MASTER_BULK"
                        )
                    if del_r.status_code == 200:
                        deleted += del_r.json().get("message", {{}}).get("deleted", len(chunk))
                        for deleted_key in chunk:
                            _progress_cache.pop(deleted_key, None)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_MASTER_BULK: delete failed ({{del_r.status_code}}): {{del_r.text[:300]}}", "ERROR")

                for i in range(0, len(to_update), batch_size):
                    chunk = to_update[i:i + batch_size]
                    upd_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_MASTER_BULK",
                        json={{"action": "update_batch", "doctype": "CCD Master", "source_id": source_id, "hostname": source_id, "rows": json.dumps(chunk)}}
                    )
                    if central_master_lease:
                        heartbeat_central_sync_lease(
                            sess, central_master_lease, "SYNC_TO_CCD_MASTER_BULK"
                        )
                    if upd_r.status_code == 200:
                        msg = upd_r.json().get("message", {{}})
                        updated += msg.get("updated", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            checkpoint_master_rows(chunk)
                            save_delta_cache(_cache_path, _progress_cache)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_MASTER_BULK: update error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_MASTER_BULK: update failed ({{upd_r.status_code}}): {{upd_r.text[:300]}}", "ERROR")

            save_delta_cache(_cache_path, _progress_cache)

            if central_master_lease:
                if _use_fast_master_insert:
                    _full_sync_complete = bool(
                        errors == 0 and os.path.exists(_full_sync_pending_path)
                    )
                    _changed_keys = []
                    _deleted_keys = []
                    if errors == 0 and not _full_sync_complete:
                        _changed_keys = sorted({{
                            str(row.get("ccd_source_key") or "")
                            for row in (to_insert + to_update)
                            if str(row.get("ccd_source_key") or "")
                        }}) if "to_insert" in locals() else []
                        _deleted_keys = sorted(set(to_delete)) if "to_delete" in locals() else []
                        if len(_changed_keys) > 10000 or len(_deleted_keys) > 10000:
                            _full_sync_complete = True
                            _changed_keys = []
                            _deleted_keys = []
                    finish_r = request_with_retry(
                        sess,
                        "POST",
                        f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.finish_sync",
                        "SYNC_TO_CCD_MASTER_BULK",
                        json={{
                            "registration": registration_id,
                            "source_id": source_id,
                            "lease_token": central_master_lease["token"],
                            "run_id": central_master_lease["run_id"],
                            "full_sync": 1 if _full_sync_complete else 0,
                            "changed_source_keys": _changed_keys,
                            "deleted_source_keys": _deleted_keys,
                        }},
                    )
                    if finish_r.status_code != 200:
                        raise RuntimeError(
                            f"could not queue downstream processing (HTTP {{finish_r.status_code}}): {{finish_r.text[:300]}}"
                        )
                    write_log(
                        "SYNC_TO_CCD_MASTER_BULK: ingestion complete; downstream processing queued"
                    )
                    central_master_lease = None
                    if _full_sync_complete:
                        try:
                            os.remove(_full_sync_pending_path)
                        except FileNotFoundError:
                            pass
                elif errors:
                    release_central_sync_lease(
                        sess,
                        central_master_lease,
                        "SYNC_TO_CCD_MASTER_BULK",
                        error=f"{{errors}} synchronization operation(s) failed",
                    )
                    central_master_lease = None
                else:
                    complete_r = request_with_retry(
                        sess,
                        "POST",
                        f"{{erpnext_url}}/api/method/{{AGENT_SYNC_API}}.complete_legacy_sync",
                        "SYNC_TO_CCD_MASTER_BULK",
                        safe_to_retry=False,
                        json={{
                            "registration": registration_id,
                            "source_id": source_id,
                            "lease_token": central_master_lease["token"],
                            "run_id": central_master_lease["run_id"],
                            "result": (
                                f"Legacy sync completed: {{created}} inserted, "
                                f"{{deleted}} deleted, {{updated}} updated"
                            ),
                        }},
                    )
                    if complete_r.status_code != 200:
                        raise RuntimeError(
                            f"could not complete central legacy lease (HTTP {{complete_r.status_code}})"
                        )
                    central_master_lease = None
                    if os.path.exists(_full_sync_pending_path):
                        try:
                            os.remove(_full_sync_pending_path)
                        except FileNotFoundError:
                            pass

            if errors:
                remaining = sum(1 for key, (row_hash, row) in client_map_master.items() if _progress_cache.get(key) != row_hash)
                remaining += sum(1 for key in _progress_cache if key not in client_map_master)
                write_log(
                    f"SYNC_TO_CCD_MASTER_BULK: progress checkpoint saved; {{remaining}} unconfirmed operation(s) will retry next run",
                    "WARN"
                )
            write_log(f"SYNC_TO_CCD_MASTER_BULK: done — {{created}} inserted, {{deleted}} deleted, {{updated}} updated, {{errors}} error(s)")
        except Exception as e:
            _master_sync_error = str(e)
            write_log(f"SYNC_TO_CCD_MASTER_BULK error: {{e}}", "ERROR")
            if "_progress_ready" in locals() and _progress_ready:
                save_delta_cache(_cache_path, _progress_cache)
                write_log("SYNC_TO_CCD_MASTER_BULK: progress checkpoint preserved after interruption", "WARN")
        finally:
            if central_master_lease:
                release_central_sync_lease(
                    sess,
                    central_master_lease,
                    "SYNC_TO_CCD_MASTER_BULK",
                    error=_master_sync_error or "CCD Master sync ended before completion",
                )
            release_shared_sync_lock(master_sync_lock)
        return prev_result

    # ---- SYNC_TO_CCD_MASTER macro ----
    if step_upper.startswith("SYNC_TO_CCD_MASTER"):
        if not erpnext_url:
            write_log("SYNC_TO_CCD_MASTER: no erpnext_url configured", "ERROR")
            return prev_result
        master_sync_lock = acquire_shared_sync_lock("CCD Master sync", "SYNC_TO_CCD_MASTER")
        try:
            import requests as _req, re as _re
            sess = _req.Session()
            sess.verify = True
            login_r = request_with_retry(sess, "POST", f"{{erpnext_url}}/api/method/login",
                                         "SYNC_TO_CCD_MASTER", data={{"usr": erpnext_user, "pwd": erpnext_pass}})
            if login_r.status_code != 200:
                write_log(f"SYNC_TO_CCD_MASTER: login failed ({{login_r.status_code}})", "ERROR")
                return prev_result

            # Fetch fieldmatch from CCD Registration
            reg_r = request_with_retry(
                sess, "GET", f"{{erpnext_url}}/api/resource/CCD Registration/{{_urlquote(registration_id, safe='')}}",
                "SYNC_TO_CCD_MASTER"
            )
            if reg_r.status_code != 200:
                write_log(f"SYNC_TO_CCD_MASTER: cannot fetch CCD Registration ({{reg_r.status_code}})", "ERROR")
                return prev_result
            _fm = reg_r.json().get("data", {{}}).get("fieldmatch", [])

            # Parse sys_fieldname: "eng_surname: English Surname" → "eng_surname"
            def parse_sys_name(raw):
                return raw.split(":")[0].strip() if raw else ""

            # Build mapping rules: ccd_fieldname copy or assignment expression → sys_field_name
            master_rules = []
            for row in _fm:
                ccd_f = (row.get("ccd_fieldname") or "").strip()
                sys_f = parse_sys_name(row.get("sys_fieldname", ""))
                ftype = row.get("fieldtype", "Data")
                assignment = (row.get("assignment") or "").strip()
                if sys_f and (ccd_f or assignment):
                    master_rules.append({{
                        "ccd_field": ccd_f,
                        "sys_field": sys_f,
                        "fieldtype": ftype,
                        "assignment": assignment,
                    }})

            if not master_rules:
                write_log("SYNC_TO_CCD_MASTER: no field mappings found in fieldmatch", "ERROR")
                return prev_result

            # Read primary key field(s) from Connection Information tab
            _pk_raw_master = reg_r.json().get("data", {{}}).get("ccd_primaykey_field", "")
            pk_fields_master = [f.strip() for f in _pk_raw_master.split("+") if f.strip()]

            def normalize_phone(val):
                s = str(val).strip() if val else ""
                if not s:
                    return ""
                if _re.match('^[+][0-9]', s):
                    return s
                m = _re.match('^[(][+]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                m = _re.match('^[(]([0-9]+)[)][ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                m = _re.match('^00([0-9]{{1,4}})[ ]*(.*)', s)
                if m:
                    return '+' + m.group(1) + ' ' + m.group(2).strip()
                return s

            def clean_value_master(rule, val):
                ft = rule.get("fieldtype", "Data")
                if val is None:
                    return ""
                if ft == "Phone":
                    return normalize_phone(val)
                return str(val)

            def _append_unique(target, values):
                for value in values:
                    if value and value not in target:
                        target.append(value)

            if isinstance(prev_result, dict) and "columns" in prev_result and "rows" in prev_result:
                # Piped mode: data from previous SELECT step, columns are ccd_fieldnames
                cols = prev_result["columns"]
                raw_rows = [dict(zip(cols, row)) for row in prev_result["rows"]]
                write_log(f"SYNC_TO_CCD_MASTER: using piped data — {{len(raw_rows)}} row(s)")
            else:
                # Self-contained: SELECT ccd_fieldname columns FROM ccd_table
                if not ccd_table:
                    write_log("SYNC_TO_CCD_MASTER: ccd_table not configured", "ERROR")
                    return prev_result
                ccd_fields = []
                for rule in master_rules:
                    _append_unique(ccd_fields, [rule.get("ccd_field", "")])
                    _append_unique(ccd_fields, extract_assignment_fields(rule.get("assignment", "")))
                # Include pk fields in SELECT even if not in Field Matching
                extra_pk = [f for f in pk_fields_master if f not in ccd_fields]
                sql = "SELECT " + ", ".join(ccd_fields + extra_pk) + " FROM " + ccd_table
                write_log(f"SYNC_TO_CCD_MASTER: SQL = {{sql}}")
                try:
                    query_result = fetch_db_rows(sql, "SYNC_TO_CCD_MASTER")
                    if query_result is None:
                        return prev_result
                    columns, rows = query_result
                    raw_rows = [dict(zip(columns, row)) for row in rows]
                    write_log(f"SYNC_TO_CCD_MASTER: fetched {{len(raw_rows)}} row(s) from client DB")
                except Exception as e:
                    write_log(f"SYNC_TO_CCD_MASTER: SQL error: {{e}}", "ERROR")
                    return prev_result

            # Remap ccd_fieldname keys → sys_fieldname keys and apply value cleaning
            master_rows = []
            for raw in raw_rows:
                mapped = {{"ccd_reg_source": source_id}}
                for rule in master_rules:
                    assignment = rule.get("assignment", "")
                    ccd_f = rule.get("ccd_field", "")
                    if assignment:
                        val = eval_assignment(assignment, raw, f"SYNC_TO_CCD_MASTER {{rule.get('sys_field')}}")
                        mapped[rule["sys_field"]] = clean_value_master(rule, val)
                    elif ccd_f in raw:
                        mapped[rule["sys_field"]] = clean_value_master(rule, raw[ccd_f])
                if pk_fields_master:
                    mapped["ccd_source_key"] = "+".join(str(raw.get(f, "")) for f in pk_fields_master)
                master_rows.append(mapped)

            # --- Phase 2: build client_map {{ccd_source_key: (hash, row)}} from remapped master_rows ---
            client_map_master = {{}}
            for _mrow in master_rows:
                _key = _mrow.get("ccd_source_key", "")
                _hash = compute_row_hash(_mrow)
                client_map_master[_key] = (_hash, _mrow)

            _cache_path = os.path.join(os.path.dirname(log_file), f"{{source_id}}_CCD-Master_delta_cache.json")
            _cache = load_delta_cache(_cache_path)
            _progress_cache = dict(_cache or {{}})
            _progress_ready = _cache is not None
            batch_size = 100
            created, deleted, updated, errors = 0, 0, 0, 0

            def checkpoint_master_row(checkpoint_row):
                checkpoint_key = checkpoint_row.get("ccd_source_key", "")
                if checkpoint_key in client_map_master:
                    _progress_cache[checkpoint_key] = client_map_master[checkpoint_key][0]

            if _cache is None:
                write_log(f"SYNC_TO_CCD_MASTER: no cache — running full sync")
                filters_json = json.dumps([["ccd_reg_source", "=", source_id]])
                all_existing_master = []
                _limit = 500
                _start = 0
                _master_doctype_url = _urlquote("CCD Master", safe='')
                while True:
                    page_r = request_with_retry(
                        sess, "GET", f"{{erpnext_url}}/api/resource/{{_master_doctype_url}}", "SYNC_TO_CCD_MASTER",
                        params={{"filters": filters_json,
                                 "fields": '["name"]',
                                 "limit_page_length": _limit,
                                 "limit_start": _start}}
                    )
                    if page_r.status_code != 200:
                        errors += 1
                        write_log(f"SYNC_TO_CCD_MASTER: list failed ({{page_r.status_code}}): {{page_r.text[:300]}}", "ERROR")
                        return prev_result
                    page_data = page_r.json().get("data", [])
                    if not page_data:
                        break
                    all_existing_master.extend(page_data)
                    if len(page_data) < _limit:
                        break
                    _start += _limit
                for item in all_existing_master:
                    delete_r = request_with_retry(
                        sess, "DELETE", f"{{erpnext_url}}/api/resource/{{_master_doctype_url}}/{{_urlquote(item['name'], safe='')}}",
                        "SYNC_TO_CCD_MASTER"
                    )
                    if delete_r.status_code not in (200, 202):
                        errors += 1
                        write_log(f"SYNC_TO_CCD_MASTER: delete failed ({{delete_r.status_code}}): {{delete_r.text[:300]}}", "ERROR")
                if errors:
                    invalidate_delta_cache(_cache_path, "SYNC_TO_CCD_MASTER")
                    return prev_result
                _progress_cache.clear()
                _progress_ready = True
                save_delta_cache(_cache_path, _progress_cache)
                write_log(f"SYNC_TO_CCD_MASTER: cleared {{len(all_existing_master)}} existing record(s) for {{source_id}}")
                for ck, (ch, crow) in client_map_master.items():
                    r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/resource/{{_master_doctype_url}}", "SYNC_TO_CCD_MASTER",
                        safe_to_retry=False, json=crow
                    )
                    if r.status_code in (200, 201):
                        created += 1
                        checkpoint_master_row(crow)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += 1
                        write_log(f"SYNC_TO_CCD_MASTER: insert error ({{r.status_code}}): {{r.text[:300]}}", "ERROR")
            else:
                to_insert = [row for ck, (ch, row) in client_map_master.items() if ck not in _cache]
                to_delete = [ck for ck in _cache if ck not in client_map_master]
                to_update = [row for ck, (ch, row) in client_map_master.items() if ck in _cache and ch != _cache[ck]]
                write_log(f"SYNC_TO_CCD_MASTER: delta — {{len(to_insert)}} insert, {{len(to_delete)}} delete, {{len(to_update)}} update")

                for _row in to_insert:
                    _master_doctype_url = _urlquote("CCD Master", safe='')
                    r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/resource/{{_master_doctype_url}}", "SYNC_TO_CCD_MASTER",
                        safe_to_retry=False, json=_row
                    )
                    if r.status_code in (200, 201):
                        created += 1
                        checkpoint_master_row(_row)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += 1
                        write_log(f"SYNC_TO_CCD_MASTER: insert error ({{r.status_code}}): {{r.text[:300]}}", "ERROR")

                for i in range(0, len(to_delete), batch_size):
                    chunk = to_delete[i:i + batch_size]
                    del_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_MASTER",
                        json={{"action": "delete_by_source_keys", "doctype": "CCD Master", "source_id": source_id, "hostname": source_id, "keys": json.dumps(chunk)}}
                    )
                    if del_r.status_code == 200:
                        deleted += del_r.json().get("message", {{}}).get("deleted", len(chunk))
                        for deleted_key in chunk:
                            _progress_cache.pop(deleted_key, None)
                        save_delta_cache(_cache_path, _progress_cache)
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_MASTER: delete failed ({{del_r.status_code}}): {{del_r.text[:300]}}", "ERROR")

                for i in range(0, len(to_update), batch_size):
                    chunk = to_update[i:i + batch_size]
                    upd_r = request_with_retry(
                        sess, "POST", f"{{erpnext_url}}/api/method/agent_bulk_sync", "SYNC_TO_CCD_MASTER",
                        json={{"action": "update_batch", "doctype": "CCD Master", "source_id": source_id, "hostname": source_id, "rows": json.dumps(chunk)}}
                    )
                    if upd_r.status_code == 200:
                        msg = upd_r.json().get("message", {{}})
                        updated += msg.get("updated", len(chunk))
                        _berrs = msg.get("errors", [])
                        errors += len(_berrs)
                        if not _berrs:
                            for checkpoint_row in chunk:
                                checkpoint_master_row(checkpoint_row)
                            save_delta_cache(_cache_path, _progress_cache)
                        for be in _berrs[:3]:
                            write_log(f"SYNC_TO_CCD_MASTER: update error: {{be}}", "ERROR")
                    else:
                        errors += len(chunk)
                        write_log(f"SYNC_TO_CCD_MASTER: update failed ({{upd_r.status_code}}): {{upd_r.text[:300]}}", "ERROR")

            save_delta_cache(_cache_path, _progress_cache)
            if errors:
                pending = sum(1 for key, (row_hash, row) in client_map_master.items() if _progress_cache.get(key) != row_hash)
                pending += sum(1 for key in _progress_cache if key not in client_map_master)
                write_log(
                    f"SYNC_TO_CCD_MASTER: progress checkpoint saved; {{pending}} unconfirmed operation(s) will retry next run",
                    "WARN"
                )
            write_log(f"SYNC_TO_CCD_MASTER: done — {{created}} inserted, {{deleted}} deleted, {{updated}} updated, {{errors}} error(s)")
        except Exception as e:
            write_log(f"SYNC_TO_CCD_MASTER error: {{e}}", "ERROR")
            if "_progress_ready" in locals() and _progress_ready:
                save_delta_cache(_cache_path, _progress_cache)
                write_log("SYNC_TO_CCD_MASTER: progress checkpoint preserved after interruption", "WARN")
        finally:
            release_shared_sync_lock(master_sync_lock)
        return prev_result

    # ---- SQL statements ----
    if step_upper.startswith(("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "EXEC ", "CALL ")):
        write_log(f"Executing SQL: {{step_stripped}}")
        if not db_type:
            write_log("No db_type configured — cannot execute SQL", "ERROR")
            return "ERROR: no db_type"
        if step_upper.startswith("SELECT "):
            try:
                query_result = fetch_db_rows(step_stripped, "SELECT")
                if query_result is None:
                    return "ERROR: DB connection failed"
                columns, rows = query_result
                write_log(f"SELECT returned {{len(rows)}} row(s), {{len(columns)}} column(s)")
                return {{"columns": columns, "rows": rows}}
            except Exception as e:
                write_log(f"SQL error: {{e}}", "ERROR")
                return f"SQL ERROR: {{e}}"
        conn, cursor = get_db_connection()
        if not conn:
            return "ERROR: DB connection failed"
        try:
            cursor.execute(step_stripped)
            conn.commit()
            affected = cursor.rowcount
            write_log(f"SQL OK — {{affected}} row(s) affected")
            return {{"affected": affected}}
        except Exception as e:
            write_log(f"SQL error: {{e}}", "ERROR")
            return f"SQL ERROR: {{e}}"
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass

    # ---- OS command (default) ----
    write_log(f"Executing OS command: {{step_stripped}}")
    try:
        result = subprocess.run(step_stripped, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            write_log(f"Command OK: {{result.stdout[:2000]}}")
        else:
            write_log(f"Command FAILED (rc={{result.returncode}}): {{result.stderr[:2000]}}", "ERROR")
        return result.stdout[:2000]
    except subprocess.TimeoutExpired:
        write_log("Command TIMEOUT (300s)", "ERROR")
        return "TIMEOUT"
    except Exception as e:
        write_log(f"Command error: {{e}}", "ERROR")
        return str(e)

def execute_pipeline():
    """Run all actions sequentially; each step receives the previous result."""
    result = None
    total = len(actions)
    for idx, step in enumerate(actions, 1):
        write_log(f"Step {{idx}}/{{total}}: {{step}}")
        result = execute_step(step, result)
        write_log(f"Step {{idx}}/{{total}} completed")
    return result

# ---- Main daemon loop ----
_instance_lock, _instance_pid_path, _existing_pid = acquire_instance_lock()
if _instance_lock is None:
    write_log(f"Daemon NOT STARTED: another instance already owns this source/job (PID {{_existing_pid}})", "WARN")
    sys.exit(0)

write_log(f"Daemon STARTED | Task: {{task_name}} | Interval: {{interval}}s | PID: {{os.getpid()}} | Steps: {{len(actions)}}")
for _i, _a in enumerate(actions, 1):
    write_log(f"  ACTION_{{_i}}: {{_a}}")

try:
    while True:
        try:
            result = execute_pipeline()
            write_log(f"Pipeline completed: {{str(result)[:2000]}}")
        except Exception as e:
            write_log(f"Pipeline error: {{e}}", "ERROR")
        next_run = datetime.now() + timedelta(seconds=interval)
        write_log(f"Next execution at: {{next_run.strftime('%Y-%m-%d %H:%M:%S')}}")
        time.sleep(interval)
except KeyboardInterrupt:
    write_log("Daemon received KeyboardInterrupt — stopping", "WARN")
except Exception as e:
    tb = traceback.format_exc()
    write_log(f"Daemon CRASHED: {{e}}\\n{{tb}}", "FATAL")
    subject = f"[AGENT] Daemon {{job_name}} STOPPED unexpectedly"
    body = (
        f"Daemon: {{job_name}}\\n"
        f"Task: {{task_name}}\\n"
        f"PID: {{os.getpid()}}\\n"
        f"Time: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}\\n"
        f"Error: {{e}}\\n\\n"
        f"Traceback:\\n{{tb}}"
    )
    send_notification(subject, body)
finally:
    write_log("Daemon STOPPED", "WARN")
    subject = f"[AGENT] Daemon {{job_name}} has STOPPED"
    body = (
        f"Daemon: {{job_name}}\\n"
        f"Task: {{task_name}}\\n"
        f"PID: {{os.getpid()}}\\n"
        f"Stopped at: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}\\n"
    )
    send_notification(subject, body)
    release_instance_lock(_instance_lock, _instance_pid_path)
'''

def _spawn_detached_process(script):
    """Cross-platform detached process spawner."""
    import tempfile
    # Write script to a temp file to avoid Windows command-line length limit (WinError 206).
    # Large daemon scripts exceed the ~32767 char limit when passed via python -c.
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8')
    tmp.write(script)
    tmp.close()
    script_path = tmp.name

    kwargs = {
        'stdout': subprocess.DEVNULL,
        'stderr': subprocess.DEVNULL,
        'stdin': subprocess.DEVNULL,
    }
    if platform.system() == 'Windows':
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        #DETACHED_PROCESS = 0x00000008
        #kwargs['creationflags'] = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS
        CREATE_NO_WINDOW = 0x08000000
        kwargs['creationflags'] = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
        kwargs['close_fds'] = True
    else:
        kwargs['start_new_session'] = True

    return subprocess.Popen([sys.executable, script_path], **kwargs)

def _print_kill_hint(pid):
    """Print OS-appropriate instructions for killing a PID."""
    if platform.system() == 'Windows':
        print(f"   ⚠️  To stop it: Task Manager → Details → find PID {pid} → End Task")
    else:
        print(f"   ⚠️  To stop it: kill {pid}")

def start_job_daemon(job_name, job_config, sysVars, mail_config=None):
    """Parse a [JOBxx] section and spawn a detached daemon for it."""
    mail_config = mail_config or {}
    task_name = job_config.get('TASKNAME', job_name)
    interval_seconds = calc_interval_seconds(job_config)

    # Collect actions: single ACTION or numbered ACTION_1, ACTION_2, ...
    if 'ACTION' in job_config:
        actions = [job_config['ACTION']]
    else:
        action_keys = sorted(
            [k for k in job_config if re.match(r'^ACTION_\d+$', k)],
            key=lambda k: int(k.split('_')[1])
        )
        actions = [job_config[k] for k in action_keys]

    # Variable replacement and password resolution for each action
    actions = [variable_replacement(a, sysVars) for a in actions]
    actions = [re.sub(r'@Password\(["\'](.+?)["\']\)', r'\1', a) for a in actions]

    # Log file per job — include source id so multiple registrations on one host don't mix
    os.makedirs(_log_dir, exist_ok=True)
    _physical_hostname = sysVars.get('physical_hostname', sysVars.get('hostname', platform.node()))
    _registration_id = sysVars.get('registration_id', _physical_hostname)
    _source_id = sysVars.get('source_id', _registration_id)
    _ccd_reg_doctype = sysVars.get('ccd_reg_doctype', f'CCD-REG-{_registration_id}')
    log_file = os.path.join(_log_dir, f'{_source_id}_{job_name}.log')

    # Email notification config from [MAIL] section
    notify_email = mail_config.get('NOTIFY_EMAIL', mail_config.get('TO', ''))
    smtp_server = mail_config.get('SMTP_SERVER', mail_config.get('SERVER', ''))
    smtp_port = int(mail_config.get('SMTP_PORT', mail_config.get('PORT', 587)))
    smtp_user = mail_config.get('SMTP_USER', mail_config.get('USER', ''))
    smtp_pass_raw = mail_config.get('SMTP_PASS', mail_config.get('PASSWORD', ''))
    smtp_pass = resolve_password_macro(smtp_pass_raw)

    # DB connection info from sysVars
    _db_type     = sysVars.get('db_type', '')
    _db_server   = sysVars.get('db_server', '')
    _db_port     = int(sysVars.get('db_port', 0) or 0)
    _db_database = sysVars.get('db_database', '')
    _db_username = sysVars.get('db_username', '')
    _db_password = sysVars.get('db_password', '')

    # ERPNext connection info from sysVars
    _erpnext_url  = sysVars.get('erpnext_url', '')
    _erpnext_user = sysVars.get('erpnext_user', '')
    _erpnext_pass = sysVars.get('erpnext_pass', '')
    _ccd_table    = sysVars.get('ccd_table', '')

    # Store sensitive credentials in Windows Credential Manager so the daemon
    # temp file never contains plain-text passwords.
    _keyring_service = f'ccd_agent_daemon_{_source_id}_{job_name}'
    _safe_set_keyring_password(_keyring_service, 'erpnext_pass', _erpnext_pass)
    _safe_set_keyring_password(_keyring_service, 'db_password', _db_password)
    _safe_set_keyring_password(_keyring_service, 'smtp_pass', smtp_pass)

    script = build_job_daemon_script(
        job_name=job_name,
        task_name=task_name,
        actions=actions,
        interval_seconds=interval_seconds,
        log_file=log_file,
        notify_email=notify_email,
        smtp_server=smtp_server,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        db_type=_db_type,
        db_server=_db_server,
        db_port=_db_port,
        db_database=_db_database,
        db_username=_db_username,
        db_password=_db_password,
        erpnext_url=_erpnext_url,
        erpnext_user=_erpnext_user,
        erpnext_pass=_erpnext_pass,
        ccd_table=_ccd_table,
        keyring_service=_keyring_service,
        registration_id=_registration_id,
        source_id=_source_id,
        physical_hostname=_physical_hostname,
        ccd_reg_doctype=_ccd_reg_doctype
    )

    proc = _spawn_detached_process(script)

    # Duplicate daemons acquire no instance lock and exit immediately. Give the
    # child enough time to report that outcome before assigning it a watchdog.
    time.sleep(1)
    if proc.poll() is not None:
        print(f"\n⚠️  [{job_name}] Daemon not started for {_source_id}; another instance may already be running. See {log_file}")
        return None

    # Display info
    interval_desc = []
    if 'DAY' in job_config:  interval_desc.append(f"{job_config['DAY']} day(s)")
    if 'HOUR' in job_config: interval_desc.append(f"{job_config['HOUR']} hour(s)")
    if 'MINS' in job_config: interval_desc.append(f"{job_config['MINS']} min(s)")
    freq_str = ', '.join(interval_desc) if interval_desc else f"{interval_seconds}s"

    print(f"\n👻 [{job_name}] Daemon started (PID: {proc.pid})")
    print(f"   Task:      {task_name}")
    for i, act in enumerate(actions, 1):
        label = f"ACTION_{i}" if len(actions) > 1 else "ACTION"
        print(f"   {label}:{'  ' if len(actions) == 1 else ' '}{redact_sensitive_text(act)}")
    print(f"   Frequency: {freq_str}")
    print(f"   Log file:  {log_file}")
    _print_kill_hint(proc.pid)

    return proc.pid

#-------------------------------------------------------------------------------------------------------#
#---- Watchdog Daemon — monitors [JOBxx] PIDs, sends email when any die --------------------------------#
#-------------------------------------------------------------------------------------------------------#
# Spawned as a detached process after all [JOBxx] daemons are launched.
# Checks every 10s if each PID is still alive. When a PID disappears
# (Task Manager kill on Windows, kill on Linux, crash, etc.),
# it logs the event and sends an email notification via [MAIL] config.
#-------------------------------------------------------------------------------------------------------#

def build_watchdog_script(job_pids, log_file,
                          notify_email='', smtp_server='', smtp_port=587,
                          smtp_user='', smtp_pass='', keyring_service='', source_id=''):
    """Build Python source for the watchdog daemon that monitors job PIDs."""
    notify_email_escaped = notify_email.replace("'", "\\'")
    smtp_server_escaped = smtp_server.replace("'", "\\'")
    smtp_user_escaped = smtp_user.replace("'", "\\'")
    log_file_escaped = log_file.replace('\\', '\\\\')
    source_id_escaped = source_id.replace("'", "\\'")

    # job_pids is a dict: {"JOB01": 12345, "JOB02": 12346, ...}
    pids_repr = repr(job_pids)

    return f'''
import time, os, sys, smtplib, platform, traceback, subprocess
import keyring as _kr
from datetime import datetime
from email.mime.text import MIMEText
import socket as _sock
import truststore
truststore.inject_into_ssl()

def _get_secret(service, username, default=""):
    try:
        return _kr.get_password(service, username) or default
    except Exception:
        return default

_hostname    = _sock.gethostname()
source_id    = "{source_id_escaped}" or _hostname
job_pids     = {pids_repr}
log_file     = r"{log_file_escaped}"
notify_email = "{notify_email_escaped}"
smtp_server  = "{smtp_server_escaped}"
smtp_port    = {smtp_port}
smtp_user    = "{smtp_user_escaped}"
smtp_pass    = _get_secret("{keyring_service}", "smtp_pass")
check_interval = 10  # seconds

def write_log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{{ts}}] [{{level}}] [{{_hostname}}] [{{source_id}}] [WATCHDOG] {{msg}}"
    try:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(entry + "\\n")
    except Exception:
        pass

def is_pid_alive(pid):
    """Check if a process with given PID is still running. Cross-platform."""
    try:
        if platform.system() == "Windows":
            # Use tasklist command — reliable on 64-bit Windows, no ctypes handle bugs
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {{pid}}", "/NH", "/FO", "CSV"],
                capture_output=True, text=True, timeout=5,
                creationflags=0x08000000  # CREATE_NO_WINDOW
            )
            return f'"{{pid}}"' in result.stdout or f",{{pid}}," in result.stdout
        else:
            # Unix: first check if PID exists at all
            try:
                os.kill(pid, 0)
            except (OSError, ProcessLookupError):
                return False
            # PID exists — but check if it's a zombie (killed but not reaped by parent)
            try:
                with open(f"/proc/{{pid}}/status") as f:
                    for line in f:
                        if line.startswith("State:"):
                            if "Z" in line or "zombie" in line.lower():
                                return False  # zombie = effectively dead
                            return True
                return True
            except FileNotFoundError:
                return False  # /proc entry gone
            except Exception:
                return True  # can't read /proc, trust os.kill result
    except (OSError, ProcessLookupError):
        return False
    except Exception:
        return False

def send_notification(subject, body):
    if not notify_email or not smtp_server:
        return
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = smtp_user or "agent-watchdog@localhost"
        msg["To"] = notify_email
        with smtplib.SMTP(smtp_server, smtp_port, timeout=10) as srv:
            if smtp_user and smtp_pass:
                srv.starttls()
                srv.login(smtp_user, smtp_pass)
            srv.sendmail(msg["From"], [notify_email], msg.as_string())
        write_log(f"Notification sent to {{notify_email}}")
    except Exception as e:
        write_log(f"Failed to send notification: {{e}}", "ERROR")

# ---- Watchdog main loop ----
write_log(f"Watchdog STARTED | Monitoring {{len(job_pids)}} daemon(s) | PID: {{os.getpid()}}")
for jn, jp in job_pids.items():
    write_log(f"  Watching: {{jn}} (PID {{jp}})")

alive_pids = dict(job_pids)  # copy — remove entries as they die

try:
    while alive_pids:
        time.sleep(check_interval)
        dead = []
        for jname, jpid in alive_pids.items():
            if not is_pid_alive(jpid):
                dead.append((jname, jpid))
        for jname, jpid in dead:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            write_log(f"DETECTED: {{jname}} (PID {{jpid}}) has STOPPED", "WARN")
            del alive_pids[jname]
            # Send email
            subject = f"[AGENT] [{{_hostname}}] [{{source_id}}] Daemon {{jname}} (PID {{jpid}}) KILLED/STOPPED"
            body = (
                f"Hostname: {{_hostname}}\\n"
                f"Source ID: {{source_id}}\\n"
                f"Daemon: {{jname}}\\n"
                f"PID: {{jpid}}\\n"
                f"Detected at: {{ts}}\\n"
                f"Cause: Process no longer running (killed via Task Manager / kill command / crash)\\n"
                f"\\n"
                f"Remaining monitored daemons: {{len(alive_pids)}}\\n"
            )
            if alive_pids:
                body += "Still alive:\\n"
                for an, ap in alive_pids.items():
                    body += f"  {{an}} (PID {{ap}})\\n"
            send_notification(subject, body)

    write_log("All monitored daemons have stopped. Watchdog exiting.")
except KeyboardInterrupt:
    write_log("Watchdog received KeyboardInterrupt — stopping", "WARN")
except Exception as e:
    tb = traceback.format_exc()
    write_log(f"Watchdog CRASHED: {{e}}\\n{{tb}}", "FATAL")
finally:
    write_log("Watchdog STOPPED", "WARN")
'''

def start_watchdog_daemon(job_pids, sysVars, mail_config=None):
    """Launch a detached watchdog daemon to monitor all [JOBxx] PIDs."""
    mail_config = mail_config or {}
    _hostname = sysVars.get('physical_hostname', sysVars.get('hostname', platform.node()))
    _source_id = sysVars.get('source_id', _hostname)

    os.makedirs(_log_dir, exist_ok=True)
    log_file = os.path.join(_log_dir, f'{_source_id}_WATCHDOG.log')

    notify_email = mail_config.get('NOTIFY_EMAIL', mail_config.get('TO', ''))
    smtp_server = mail_config.get('SMTP_SERVER', mail_config.get('SERVER', ''))
    smtp_port = int(mail_config.get('SMTP_PORT', mail_config.get('PORT', 587)))
    smtp_user = mail_config.get('SMTP_USER', mail_config.get('USER', ''))
    smtp_pass_raw = mail_config.get('SMTP_PASS', mail_config.get('PASSWORD', ''))
    smtp_pass = resolve_password_macro(smtp_pass_raw)

    # Store smtp_pass in Windows Credential Manager — keep it out of the temp file
    _watchdog_keyring_service = f'ccd_agent_daemon_{_source_id}_WATCHDOG'
    _safe_set_keyring_password(_watchdog_keyring_service, 'smtp_pass', smtp_pass)

    script = build_watchdog_script(
        job_pids=job_pids,
        log_file=log_file,
        notify_email=notify_email,
        smtp_server=smtp_server,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        keyring_service=_watchdog_keyring_service,
        source_id=_source_id
    )

    proc = _spawn_detached_process(script)

    print(f"\n🐕 Watchdog daemon started (PID: {proc.pid})")
    print(f"   Monitoring: {len(job_pids)} daemon(s)")
    for jname, jpid in job_pids.items():
        print(f"     {jname} → PID {jpid}")
    print(f"   Check interval: 10s")
    print(f"   Log file:  {log_file}")
    if notify_email:
        print(f"   Notify:    {notify_email}")
    else:
        print(f"   Notify:    (no [MAIL] section — logging only)")
    _print_kill_hint(proc.pid)

    return proc.pid

ERPNEXT_URL = os.environ.get("CCD_ERPNEXT_URL", "").strip().rstrip("/")

import keyring as _keyring
def _safe_get_keyring_password(service, username, default=""):
    try:
        return _keyring.get_password(service, username) or default
    except Exception as e:
        print(f"WARNING: Could not read keyring secret {service}/{username}: {e}")
        return default

def _safe_set_keyring_password(service, username, password):
    try:
        _keyring.set_password(service, username, password or "")
        return True
    except Exception as e:
        print(f"WARNING: Could not store keyring secret {service}/{username}: {e}")
        return False

ERPNEXT_USER = _safe_get_keyring_password(
    "ccd_agent", "erpnext_user", os.environ.get("CCD_ERPNEXT_USER", "")
)
ERPNEXT_PASS = _safe_get_keyring_password("ccd_agent", "erpnext_pass", "")
if not ERPNEXT_URL:
    print("WARNING: CCD_ERPNEXT_URL is not configured.")
if not ERPNEXT_USER:
    print("WARNING: ERPNext username not found in the keyring or CCD_ERPNEXT_USER.")
if not ERPNEXT_PASS:
    print("WARNING: ERPNext password not found in Windows Credential Manager.")
    print("         Run the setup bat file again to store credentials.")

# Deployment names are supplied at runtime rather than embedded in source.
CCD_SITE_NAME = os.environ.get("CCD_SITE_NAME", "frontend").strip() or "frontend"
NAMESPACE = os.environ.get("CCD_SOCKET_NAMESPACE", f"/{CCD_SITE_NAME}").strip()

http_session = requests.Session()
#http_session.verify = r'/etc/ssl/certs/hksr.org.hk.pem'
http_session.verify = True

def login_to_erpnext():
    print("Authenticating...")
    response = http_session.post(f"{ERPNEXT_URL}/api/method/login", data={"usr": ERPNEXT_USER, "pwd": ERPNEXT_PASS})
    if response.status_code == 200:
        print("✅ Login successful! Cookies secured.")
        return True
    return False

def fetch_ccd_registration_doc(docname):
    """Fetch one CCD Registration document by registration id/docname."""
    response = http_session.get(f"{ERPNEXT_URL}/api/resource/CCD Registration/{urlquote(docname, safe='')}")
    if response.status_code == 200:
        return response.json().get("data", {})
    print(f"❌ Failed to fetch CCD Registration {docname}: {response.status_code} {response.text}")
    return None

def discover_ccd_registration_docs(physical_hostname):
    """Return CCD Registration docs assigned to this physical hostname.

    New mode: query records where physical_hostname equals socket.gethostname().
    Legacy fallback: fetch the document whose name equals the hostname.
    Optional env var CCD_AGENT_REGISTRATIONS restricts discovered docnames.
    """
    allowed_raw = os.environ.get("CCD_AGENT_REGISTRATIONS", "").strip()
    if allowed_raw:
        docs = []
        for docname in [name.strip() for name in allowed_raw.split(",") if name.strip()]:
            doc = fetch_ccd_registration_doc(docname)
            if doc:
                docs.append(doc)
        return docs

    discovered_names = []
    params = {
        "filters": json.dumps([
            ["physical_hostname", "=", physical_hostname],
            ["docstatus", "=", 1]
            ]),
        "fields": json.dumps(["name"]),
        "limit_page_length": 500,
    }
    response = http_session.get(f"{ERPNEXT_URL}/api/resource/CCD Registration", params=params)
    if response.status_code == 200:
        for row in response.json().get("data", []):
            docname = row.get("name")
            if docname:
                discovered_names.append(docname)
    else:
        print(f"⚠️ Failed to query CCD Registration by physical_hostname: {response.status_code} {response.text}")

    if not discovered_names:
        legacy_doc = fetch_ccd_registration_doc(physical_hostname)
        return [legacy_doc] if legacy_doc else []

    docs = []
    for docname in discovered_names:
        doc = fetch_ccd_registration_doc(docname)
        if doc:
            docs.append(doc)
    return docs


def process_ccd_registration_doc(doc_data, physical_hostname):
    """Launch jobs/watchdog for one CCD Registration document."""
    global daemon_thread

    registration_id = doc_data.get('name') or physical_hostname
    # A physical host may run several databases. Prefer the governed stable
    # source key, and never derive identity by blindly trimming -1/-2 suffixes.
    source_id = (
        doc_data.get('agent_sync_source_id')
        or doc_data.get('ccd_stable_source_key')
        or registration_id
    ).strip()
    ccd_reg_doctype = doc_data.get('ccd_reg_doctype') or f'CCD-REG-{registration_id}'
    agent_status = (doc_data.get('agent_status') or '').strip().lower()

    if agent_status in ('inactive', 'disabled', 'stopped', 'cancelled', 'canceled'):
        print(f"\n⏭️  Skipping CCD Registration {registration_id}: agent_status={doc_data.get('agent_status')}")
        return

    print("\n" + "=" * 70)
    print(f"📌 CCD Registration: {registration_id}")
    print(f"📌 Physical Hostname: {physical_hostname}")
    print(f"📌 CCD REG DocType: {ccd_reg_doctype}")
    print("=" * 70)

    print("\n📋 Agent Information: ")
    print("-" * 42)
    print(f"  Agent ID:       {doc_data.get('agent_id', '')}")
    print(f"  Security Type:  {doc_data.get('agent_sec_type', '')}")
    print(f"  Security Key:   {'(configured)' if doc_data.get('agent_sec_key') else '(not configured)'}")
    print(f"  Configure:      {'(configured)' if doc_data.get('agent_config') else '(not configured)'}")
    print(f"  Status:         {doc_data.get('agent_status', '')}")
    print(f"  Expiry Date:    {doc_data.get('expiry_date', '')}")
    print("-" * 42)

    print("\n📋 Connection Information:")
    print("-" * 42)
    print(f"  DB Type:        {doc_data.get('db_type', '')}")
    print(f"  Server/URL:     {doc_data.get('db_server', '')}")
    print(f"  Port:           {doc_data.get('db_port', '')}")
    print(f"  Database:       {doc_data.get('db_database', '')}")
    print(f"  Username:       {doc_data.get('db_username', '')}")

    db_password = ""
    pw_response = http_session.post(
        f"{ERPNEXT_URL}/api/method/get_agent_password",
        json={"docname": registration_id}
    )
    if pw_response.status_code == 200:
        db_password = pw_response.json().get("message", {}).get("password", "")
        print("  Password:       (retrieved securely)")
    else:
        print(f"  Password:       (failed to decrypt: HTTP {pw_response.status_code})")

    print(f"  Customer Table: {doc_data.get('ccd_table', '')}")
    print("-" * 42)

    db_type = (doc_data.get('db_type', '') or '').upper()
    db_server = doc_data.get('db_server', '') or 'localhost'
    db_port = doc_data.get('db_port', 0) or 0
    db_database = doc_data.get('db_database', '')
    db_username = doc_data.get('db_username', '')



    print(f"\n🔌 Connecting to {db_type} database '{db_database}'...")
    try:
        if db_type == 'MYSQL':
            import mysql.connector
            db_conn = mysql.connector.connect(
                host=db_server,
                port=int(db_port) or 3306,
                user=db_username,
                password=db_password,
                database=db_database
            )
            db_cursor = db_conn.cursor()
            db_cursor.execute("SHOW TABLES")
            tables = db_cursor.fetchall()
            print(f"\n📋 Tables in '{db_database}':")
            print("-" * 42)
            for (table_name,) in tables:
                print(f"  {table_name}")
            print("-" * 42)
            print(f"  Total: {len(tables)} tables")
            db_cursor.close()
            db_conn.close()

        elif db_type == 'MSSQL':
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_server},{int(db_port) or 1433};"
                f"DATABASE={db_database};"
                f"UID={db_username};"
                f"PWD={db_password}"
            )
            db_conn = pyodbc.connect(conn_str)
            db_cursor = db_conn.cursor()
            db_cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
            tables = db_cursor.fetchall()
            print(f"\n📋 Tables in '{db_database}':")
            print("-" * 42)
            for (table_name,) in tables:
                print(f"  {table_name}")
            print("-" * 42)
            print(f"  Total: {len(tables)} tables")
            db_cursor.close()
            db_conn.close()

        elif db_type == 'ORACLE':
            import oracledb
            dsn = f"{db_server}:{int(db_port) or 1521}/{db_database}"
            db_conn = oracledb.connect(user=db_username, password=db_password, dsn=dsn)
            db_cursor = db_conn.cursor()
            db_cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            tables = db_cursor.fetchall()
            print(f"\n📋 Tables in '{db_database}':")
            print("-" * 42)
            for (table_name,) in tables:
                print(f"  {table_name}")
            print("-" * 42)
            print(f"  Total: {len(tables)} tables")
            db_cursor.close()
            db_conn.close()

        elif db_type == 'API':
            print(f"  API mode — no database tables to list. Server/URL: {db_server}")

        else:
            print(f"  ⚠️ Unsupported DB type: '{db_type}'")

    except Exception as db_err:
        print(f"❌ Database connection error: {db_err}")

    agent_config = doc_data.get('agent_config', '') or ''

    if 'Daemon:1' in agent_config:
        print("\n🔧 Daemon:1 detected in config")
        daemon_thread = threading.Thread(
            target=file_reader_daemon,
            args=('for_daemon_testing.txt', 5),
            daemon=True
        )
        daemon_thread.start()

    if 'Daemon:2' in agent_config:
        print("\n🔧 Daemon:2 detected in config")
        daemon2_filepath = os.path.join(_base_dir, 'daemon_mode_two.txt')
        start_detached_daemon(daemon2_filepath)

    sections = parse_agent_config(agent_config)

    if sections:
        print("\n📋 Parsed Config Sections:")
        print("-" * 42)
        for sec_name in sections:
            print(f"  [{sec_name}]")
        print("-" * 42)

    job_sysVars = {}
    job_sysVars['ccd_table'] = doc_data.get('ccd_table', '')
    job_sysVars['db_type']   = doc_data.get('db_type', '')
    job_sysVars['db_server'] = doc_data.get('db_server', '')
    job_sysVars['db_port']   = str(doc_data.get('db_port', ''))
    job_sysVars['db_database'] = doc_data.get('db_database', '')
    job_sysVars['db_username'] = doc_data.get('db_username', '')
    job_sysVars['db_password'] = db_password
    job_sysVars['hostname']  = physical_hostname
    job_sysVars['physical_hostname'] = physical_hostname
    job_sysVars['registration_id'] = registration_id
    job_sysVars['source_id'] = source_id
    job_sysVars['ccd_reg_doctype'] = ccd_reg_doctype
    job_sysVars['erpnext_url']  = ERPNEXT_URL
    job_sysVars['erpnext_user'] = ERPNEXT_USER
    job_sysVars['erpnext_pass'] = ERPNEXT_PASS

    system_config = sections.get('SYSTEM', {})
    for k, v in system_config.items():
        resolved = resolve_password_macro(v)
        job_sysVars[k] = resolved
        job_sysVars[k.lower()] = resolved

    mail_config = sections.get('MAIL', {})
    if mail_config:
        print("\n📧 Mail Notification Settings:")
        print("-" * 42)
        print(f"  SMTP Server:  {mail_config.get('SMTP_SERVER', mail_config.get('SERVER', ''))}")
        print(f"  SMTP Port:    {mail_config.get('SMTP_PORT', mail_config.get('PORT', '587'))}")
        print(f"  SMTP User:    {mail_config.get('SMTP_USER', mail_config.get('USER', ''))}")
        print(f"  Notify Email: {mail_config.get('NOTIFY_EMAIL', mail_config.get('TO', ''))}")
        print("-" * 42)

    job_sections = get_job_sections(sections)
    job_pids = {}
    if job_sections:
        print(f"\n🚀 Launching {len(job_sections)} job daemon(s) for {registration_id}...")
        print("=" * 50)
        for job_name, job_config in job_sections:
            pid = start_job_daemon(job_name, job_config, job_sysVars, mail_config)
            if pid is not None:
                job_pids[f"{registration_id}:{job_name}"] = pid
        print("=" * 50)

    if job_pids:
        start_watchdog_daemon(job_pids, job_sysVars, mail_config)

# Loggers are turned ON so we can watch the upgrade happen.
sio = socketio.Client(ssl_verify=True, logger=True, engineio_logger=True, reconnection=True, reconnection_attempts=5, reconnection_delay=2)

# 1. Add namespace to the connect event
@sio.event(namespace=NAMESPACE)
def connect():
    print(f"\n✅ SUCCESS! Connected to Node.js on namespace: {NAMESPACE}")

    # 2. Add namespace to all emits
    sio.emit('setup', CCD_SITE_NAME, namespace=NAMESPACE)
    #time.sleep(0.4)
    sio.emit('user', ERPNEXT_USER, namespace=NAMESPACE)
    #time.sleep(0.4)
    sio.emit('task_subscribe', 'agent_room', namespace=NAMESPACE)
    #sio.emit('agent_room', namespace=NAMESPACE)
    print("✅ Handshakes sent. Frappe knows who we are.")

# 3. Add namespace to specific event listeners
@sio.on('custom_event', namespace=NAMESPACE)
def handle_direct_hit(data):
    print("\n🚨🚨 [DIRECT HIT!] SIGNAL RECEIVED 🚨🚨")
    print(f"Payload: {data}")
    print("---------------------------------------\n")

    try:
        test_db_host = os.environ.get("CCD_TEST_DB_HOST", "").strip()
        test_db_user = os.environ.get("CCD_TEST_DB_USER", "").strip()
        test_db_name = os.environ.get("CCD_TEST_DB_NAME", "").strip()
        if not (test_db_host and test_db_user and test_db_name):
            print("Test database event is disabled; CCD_TEST_DB_* is not configured.")
            return
        test_db_password = _safe_get_keyring_password(
            "ccd_agent_test", "db_password", ""
        )
        conn = mysql.connector.connect(
            host=test_db_host,
            user=test_db_user,
            password=test_db_password,
            database=test_db_name,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT name, price FROM productlist")
        rows = cursor.fetchall()
        print(f"{'name':<30} {'price':>10}")
        print("-" * 42)
        for name, price in rows:
            print(f"{name:<30} {price:>10}")
        cursor.close()
        conn.close()

        # Send results back to ERPNext
        result_data = [{"name": name, "price": float(price)} for name, price in rows]
        response = http_session.post(
            f"{ERPNEXT_URL}/api/method/agent_receive_data",
            json={"data": json.dumps(result_data)}
        )
        if response.status_code == 200:
            print("✅ Data sent to ERPNext successfully!")
            print(f"Server response: {response.json()}")
        else:
            print(f"❌ Failed to send data: {response.status_code} {response.text}")

    except mysql.connector.Error as e:
        print(f"❌ MySQL error: {e}")

# 4. Add namespace to the catch-all listener
#@sio.on('*', namespace=NAMESPACE)
#def catch_all(event, data):
#    print(f"📡 Radar caught background event [{event}]: {data}")

@sio.event(namespace=NAMESPACE)
def connect_error(data):
    print(f"\n❌ [NAMESPACE ERROR] {NAMESPACE} connection rejected by server: {data}\n")

@sio.on('ghost_event', namespace=NAMESPACE)
def handle_ghost(data):
    print(f"\n[GHOST] ghost_event: {data}\n")



if __name__ == "__main__":
    if login_to_erpnext():
        print("Connecting to Socket server...")

        cookie_string = "; ".join([f"{k}={v}" for k, v in http_session.cookies.items()])

        try:
            sio.connect(
                ERPNEXT_URL,
                namespaces=[NAMESPACE],
                transports=['websocket'],
                wait_timeout=10,            # default is 1s — too short for external clients (network + server session validation > 1s)
                headers={
                    'Cookie': cookie_string,
                    'X-Frappe-Site-Name': CCD_SITE_NAME,
                }
            )

            print("🎧 Agent is locked in. Waiting for broadcasts... new12345678901")

            hostname = socket.gethostname()
            print(f"\n📌 Hostname: {hostname}")
            print(f"📌 Discovering CCD Registration records for physical_hostname={hostname}")

            registration_docs = discover_ccd_registration_docs(hostname)
            if registration_docs:
                print(f"\n🚀 Found {len(registration_docs)} CCD Registration record(s) for {hostname}")
                for registration_doc in registration_docs:
                    process_ccd_registration_doc(registration_doc, hostname)
            else:
                print(f"❌ No CCD Registration found for physical_hostname or legacy hostname: {hostname}")

            #sio.wait()
            while sio.connected:
                time.sleep(0.4)

        except Exception as e:
            print(f"\n❌ ERROR: {e}")
        except KeyboardInterrupt:
            print("\nAgent terminated.")
        finally:
            daemon_stop_event.set()
            if daemon_thread and daemon_thread.is_alive():
                daemon_thread.join(timeout=2)
            if sio.connected:
                sio.disconnect()
