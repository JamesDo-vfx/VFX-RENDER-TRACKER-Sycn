# Copyright (c) 2026 Truong Do Trung Hieu | JamesDo . All rights reserved.
# Product: DoneYet / JDTool for Houdini
# Build: 2026.04.07
# Unauthorized copying, resale, redistribution, or modification is prohibited.

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from urllib.parse import urlparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
HOUDINI_SCRIPTS_PATH = os.path.join(os.path.dirname(BASE_PATH), "houdini_scripts")
SCRIPTS_PYTHON_DIR = os.path.join(os.path.dirname(os.path.dirname(BASE_PATH)), "python")
INDEX_PATH = os.path.join(BASE_PATH, "index.html")
RUNTIME_INFO_PATH = os.path.join(BASE_PATH, "server_runtime.json")
CLOUDFLARED_PATH = os.path.join(BASE_PATH, "bin", "cloudflared.exe")
WATCHDOG_STATE_DIR = HOUDINI_SCRIPTS_PATH

try:
    normalized_target = os.path.normcase(os.path.abspath(SCRIPTS_PYTHON_DIR))
    sys.path[:] = [
        entry for entry in sys.path
        if os.path.normcase(os.path.abspath(entry)) != normalized_target
    ]
except Exception:
    pass
sys.path.insert(0, SCRIPTS_PYTHON_DIR)

import doneyet_background_runtime
from doneyet_modes import job_model
from jd_metadata import (
    JD_BUILD_ID,
    JD_COPYRIGHT,
    JD_COPYRIGHT_SHORT,
    JD_OWNER,
    JD_PRODUCT_NAME,
    jd_runtime_label,
)

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000
PUBLIC_URL = ""
TUNNEL_PROCESS = None
DB_PATH = ""
TRACKER_MODE = "private"
_WARNED_KEYS = set()
SERVER_STARTED_AT = int(time.time())
_API_DATA_CACHE = {}
_WATCHDOG_RUNTIME_CACHE = {}
_FIREBASE_SYNC_CACHE = {}


def _perf_logging_enabled():
    return str(os.environ.get("JDTOOL_PERF_LOG", "")).strip().lower() in ("1", "true", "yes", "on")


def _perf_log(phase, started_at, **fields):
    if not _perf_logging_enabled():
        return
    elapsed_ms = (time.time() - float(started_at or time.time())) * 1000.0
    extras = " ".join(f"{key}={value}" for key, value in sorted(fields.items()) if value is not None)
    suffix = f" | {extras}" if extras else ""
    print(f"[WEBUI/PERF] {phase} elapsed_ms={elapsed_ms:.2f}{suffix}")


def crash_monitor_thread():
    """Keep runtime metadata consistent if the tunnel process exits unexpectedly."""
    global PUBLIC_URL
    tunnel_was_alive = False
    while True:
        try:
            if TUNNEL_PROCESS:
                if TUNNEL_PROCESS.poll() is None:
                    tunnel_was_alive = True
                elif tunnel_was_alive and PUBLIC_URL:
                    print("[WARN] Cloudflare tunnel stopped.")
                    PUBLIC_URL = ""
                    write_runtime_info()
                    tunnel_was_alive = False
        except Exception:
            pass
        time.sleep(1.0)


def _warn_once(key, message):
    if key in _WARNED_KEYS:
        return
    _WARNED_KEYS.add(key)
    print(message)


def _normalized_abspath(path_value):
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    try:
        return os.path.normcase(os.path.abspath(raw))
    except Exception:
        return os.path.normcase(raw)


def _pid_alive(pid_value):
    try:
        pid = int(pid_value)
    except Exception:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_json(path):
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _file_mtime(path):
    if not path or not os.path.exists(path):
        return 0.0
    try:
        return float(os.path.getmtime(path))
    except Exception:
        return 0.0


def _file_size(path):
    if not path or not os.path.exists(path):
        return 0
    try:
        return int(os.path.getsize(path))
    except Exception:
        return 0


def _db_content_signature(db_path):
    normalized = _normalized_abspath(db_path)
    if not normalized:
        return "0"
    candidates = [
        normalized,
        normalized + "-wal",
        normalized + "-shm",
    ]
    parts = []
    for path in candidates:
        parts.append(f"{_file_mtime(path):.6f}:{_file_size(path)}")
    return "|".join(parts)


def _watchdog_state_paths(db_path):
    return doneyet_background_runtime.watchdog_state_paths(WATCHDOG_STATE_DIR, db_path=db_path)


def _read_watchdog_state(paths, key):
    for path in list((paths or {}).get(key, []) or []):
        payload = _read_json(path)
        if payload:
            return payload, path
    candidates = list((paths or {}).get(key, []) or [])
    return {}, candidates[0] if candidates else ""


def _watchdog_cache_signature(db_path):
    paths = _watchdog_state_paths(db_path)
    status_candidates = list((paths or {}).get("status_candidates", []) or [])
    runtime_candidates = list((paths or {}).get("runtime_candidates", []) or [])
    pid_candidates = list((paths or {}).get("pid_candidates", []) or [])
    mtimes = [_file_mtime(db_path)]
    for path in status_candidates + runtime_candidates + pid_candidates:
        mtimes.append(_file_mtime(path))
    return "|".join(f"{value:.6f}" for value in mtimes)


def _crash_detection_enabled(config=None):
    cfg = config if isinstance(config, dict) else CONFIG_CACHE
    return _to_bool((cfg or {}).get("crash_detection_enabled"))


def _is_crashed_row(row_dict):
    status_raw = str((row_dict or {}).get("status") or "").strip().lower()
    alert_raw = str((row_dict or {}).get("alert_state") or "").strip().lower()
    return status_raw == "crashed" or alert_raw == "crash" or _to_bool((row_dict or {}).get("crash_detected"))


def _is_stalled_row(row_dict):
    status_raw = str((row_dict or {}).get("status") or "").strip().lower()
    alert_raw = str((row_dict or {}).get("alert_state") or "").strip().lower()
    return status_raw == "stalled" or alert_raw == "stall" or _to_bool((row_dict or {}).get("stall_detected"))


def _watchdog_job_state_counts(db_path):
    counts = {
        "tracked_jobs": 0,
        "running_jobs": 0,
        "slow_frame_jobs": 0,
        "suspect_stale_jobs": 0,
        "crashed_jobs": 0,
        "stalled_jobs": 0,
        "latest_heartbeat_ts": 0.0,
    }
    if not db_path or not os.path.exists(db_path):
        return counts

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT status, alert_state, worker_state, crash_detected, stall_detected, last_heartbeat_ts, updated_at "
            "FROM render_jobs"
        )
        rows = cur.fetchall()
        counts["tracked_jobs"] = len(rows)
        for row in rows:
            row_dict = dict(row)
            heartbeat_ts = float(row_dict.get("last_heartbeat_ts") or row_dict.get("updated_at") or 0.0)
            if heartbeat_ts > counts["latest_heartbeat_ts"]:
                counts["latest_heartbeat_ts"] = heartbeat_ts
            if _is_crashed_row(row_dict):
                counts["crashed_jobs"] += 1
            elif _is_stalled_row(row_dict):
                counts["stalled_jobs"] += 1
            else:
                worker_state = str(row_dict.get("worker_state") or "").strip().lower()
                if worker_state == "slow_frame":
                    counts["slow_frame_jobs"] += 1
                elif worker_state == "suspect_stale":
                    counts["suspect_stale_jobs"] += 1
                else:
                    counts["running_jobs"] += 1
    except Exception:
        pass
    finally:
        if conn:
            conn.close()
    return counts


def _build_watchdog_runtime_status(db_path):
    cache_key = (_normalized_abspath(db_path), f"{_file_mtime(db_path):.6f}")
    now_ts = time.time()
    cached = _WATCHDOG_RUNTIME_CACHE.get(cache_key)
    if isinstance(cached, dict):
        expires_at = float(cached.get("expires_at", 0.0) or 0.0)
        if now_ts < expires_at:
            return dict(cached.get("payload") or {})

    started_at = now_ts
    normalized_db = _normalized_abspath(db_path)
    service_status = {}
    service_error = ""
    if normalized_db and os.path.exists(normalized_db):
        try:
            service_status = job_model.sqlite_get_service_status(normalized_db, service_name="crash_detection")
        except Exception as exc:
            service_error = str(exc)

    counts = _watchdog_job_state_counts(db_path)
    status_state = str(service_status.get("state") or "").strip().lower() or "off"
    status_message = str(service_status.get("message") or "").strip()
    requested_state = str(service_status.get("requested_state") or "").strip().lower()
    updated_at = float(service_status.get("updated_at") or 0.0)
    pid_value = _to_int(service_status.get("pid"), 0)
    pid_alive = bool(pid_value > 0 and _pid_alive(pid_value))
    poll_seconds = float(service_status.get("poll_seconds") or 3.0)
    stale_seconds = float(service_status.get("stale_seconds") or 90.0)
    service_age_seconds = (time.time() - updated_at) if updated_at > 0 else None
    service_stale_after_seconds = max(10.0, poll_seconds * 3.0)
    service_is_stale = bool(service_age_seconds is not None and service_age_seconds > service_stale_after_seconds)
    latest_heartbeat_ts = float(counts.get("latest_heartbeat_ts") or 0.0)
    heartbeat_age_seconds = (time.time() - latest_heartbeat_ts) if latest_heartbeat_ts > 0 else None

    crash_detection_enabled = _crash_detection_enabled(CONFIG_CACHE)
    operational_state = "off"
    detail_state = status_state or "off"
    service_healthy = bool(
        status_state in ("active", "running")
        and pid_value > 0
        and pid_alive
        and (not service_is_stale)
    )
    active_stop_requested = bool(
        pid_alive
        and (
            status_state == "stopping"
            or (requested_state == "stop" and status_state not in ("stopped", "off"))
        )
    )
    service_starting = bool(
        status_state == "starting"
        or ((requested_state == "start") and status_state in ("", "off", "stopped") and (not service_healthy))
    )
    if not crash_detection_enabled:
        operational_state = "off"
        detail_state = "disabled"
    elif service_healthy:
        operational_state = "active"
        detail_state = "active"
    elif service_starting:
        operational_state = "starting"
        detail_state = "starting"
    elif status_state in ("failed", "error"):
        operational_state = "failed"
        detail_state = "failed"
    elif status_state in ("active", "running"):
        operational_state = "failed"
        detail_state = "failed"
    elif active_stop_requested or status_state in ("stopped", "off"):
        operational_state = "off"
        detail_state = "stopped"

    if operational_state == "active":
        display_state = "active"
        display_message = "Render tracking is online and watching live job data."
    elif operational_state == "starting":
        display_state = "starting"
        display_message = status_message or "Render tracking is starting."
    elif operational_state == "failed":
        display_state = "failed"
        display_message = status_message or service_error or "Render tracking failed to start or became unhealthy."
    elif active_stop_requested:
        display_state = "off"
        display_message = status_message or "Render tracking has stopped."
    else:
        display_state = "off"
        display_message = "Render tracking is turned off." if not crash_detection_enabled else "Render tracking is offline."

    payload = {
        "display_state": display_state,
        "operational_state": operational_state,
        "detail_state": detail_state,
        "message": display_message,
        "enabled": bool(crash_detection_enabled),
        "status_message": status_message or service_error,
        "pid": pid_value,
        "pid_alive": pid_alive,
        "db_matches": True,
        "db_path": normalized_db,
        "namespace": "sqlite.service_status",
        "python_executable": str(service_status.get("python_executable") or ""),
        "runtime_supported": True,
        "updated_at": updated_at,
        "service_age_seconds": service_age_seconds,
        "service_stale_after_seconds": service_stale_after_seconds,
        "service_is_stale": service_is_stale,
        "service_fresh": bool(operational_state == "active" and (not service_is_stale)),
        "poll_seconds": poll_seconds,
        "stale_seconds": stale_seconds,
        "latest_heartbeat_ts": latest_heartbeat_ts,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "changed_keys": list((service_status.get("changed_keys") or [])),
        "tracked_jobs": counts["tracked_jobs"],
        "running_jobs": counts["running_jobs"],
        "slow_frame_jobs": counts["slow_frame_jobs"],
        "suspect_stale_jobs": counts["suspect_stale_jobs"],
        "crashed_jobs": counts["crashed_jobs"],
        "stalled_jobs": counts["stalled_jobs"],
        "pid_source": "sqlite.service_status",
        "runtime_source": "sqlite.service_status",
        "status_source": "sqlite.service_status",
        "requested_state": str(service_status.get("requested_state") or "").strip().lower(),
        "stop_requested": active_stop_requested,
        "changed_jobs": _to_int(service_status.get("changed_jobs"), 0),
        "service_status_error": service_error,
    }
    ttl_seconds = 1.0 if counts["tracked_jobs"] > 0 else 3.0
    _WATCHDOG_RUNTIME_CACHE.clear()
    _WATCHDOG_RUNTIME_CACHE[cache_key] = {
        "expires_at": now_ts + ttl_seconds,
        "payload": dict(payload),
    }
    _perf_log("watchdog.runtime_status", started_at, tracked_jobs=counts["tracked_jobs"], display_state=display_state)
    return payload


def _watchdog_running(db_path):
    status = _build_watchdog_runtime_status(db_path)
    return status.get("operational_state") == "active"


def _ensure_scripts_python_path():
    try:
        normalized_target = os.path.normcase(os.path.abspath(SCRIPTS_PYTHON_DIR))
        sys.path[:] = [
            entry for entry in sys.path
            if os.path.normcase(os.path.abspath(entry)) != normalized_target
        ]
    except Exception:
        pass
    sys.path.insert(0, SCRIPTS_PYTHON_DIR)
    return SCRIPTS_PYTHON_DIR


def job_watchdog_thread():
    while True:
        try:
            if DB_PATH and os.path.exists(DB_PATH):
                _ensure_scripts_python_path()
                from doneyet_modes import collaborative_mode

                collaborative_mode.run_crash_watchdog(
                    DB_PATH,
                    CONFIG_CACHE,
                    stale_seconds=WATCHDOG_STALE_SECONDS,
                )
        except Exception as exc:
            print(f"[WATCHDOG] Job crash watchdog error: {exc}")
        time.sleep(WATCHDOG_INTERVAL_SECONDS)


def _load_config_file(path):
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


FIREBASE_URL = ""
WEBHOOK_URL = ""
GITHUB_URL = ""
CONFIG_PATH = ""
CONFIG_CACHE = {}
WATCHDOG_INTERVAL_SECONDS = 5
WATCHDOG_STALE_SECONDS = 90


def _to_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    return False


def _url_is_reachable(url, timeout=5):
    if not url or not url.startswith("http"):
        return False
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "DoneYet/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= int(resp.getcode()) < 500
    except Exception:
        return False


def _normalize_job_mode_value(row_dict):
    raw_mode = str(
        row_dict.get("job_mode", "")
    ).strip().lower()
    if raw_mode in ("cache", "render"):
        return raw_mode
    if row_dict.get("is_cache") in (1, True, "1", "true", "True"):
        return "cache"
    shot_name = str(row_dict.get("shotName", "") or "").lower()
    node_path = str(row_dict.get("node_path", "") or "").lower()
    output_path = str(row_dict.get("output_path", "") or "").lower()
    if ("cache" in shot_name) or ("cache" in node_path) or (".bgeo" in output_path):
        return "cache"
    return "render"


def _is_cache_row(row_dict):
    return _normalize_job_mode_value(row_dict) == "cache"


def _derive_shot_name(row_dict, is_cache):
    shot_name = str(row_dict.get("shotName", "") or "").strip()
    node_path = str(row_dict.get("node_path", "") or "").strip().replace("\\", "/")

    if (not shot_name or shot_name.upper() == "N/A") and node_path:
        parts = [p for p in node_path.split("/") if p]
        return parts[-1] if parts else "N/A"

    if is_cache and shot_name.lower() == "render" and node_path:
        parts = [p for p in node_path.split("/") if p]
        if parts:
            # Common File Cache internals end with ".../<filecache_node>/render"
            if parts[-1].lower() == "render" and len(parts) > 1:
                return parts[-2]
            return parts[-1]

    return shot_name or "N/A"


def _normalize_row_for_webui(row):
    row_dict = dict(row)
    if row_dict.get("frames_history_json"):
        try:
            row_dict["frames_history"] = json.loads(row_dict["frames_history_json"])
        except Exception:
            row_dict["frames_history"] = []
    else:
        row_dict["frames_history"] = []

    try:
        row_dict = job_model.augment_render_payload(
            row_dict,
            render_key=str(row_dict.get("render_key") or "").strip(),
            heartbeat_default=row_dict.get("last_heartbeat_ts", row_dict.get("updated_at")),
        )
    except Exception:
        pass

    job_mode = _normalize_job_mode_value(row_dict)
    is_cache = job_mode == "cache"
    row_dict["job_mode"] = job_mode
    row_dict["is_cache"] = is_cache
    row_dict["shotName"] = _derive_shot_name(row_dict, is_cache)
    row_dict["workerName"] = row_dict.get("workerName", "N/A")
    row_dict["hipFile"] = row_dict.get("hipFile", "N/A")
    row_dict["project_name"] = row_dict.get("project_name", row_dict["hipFile"])
    row_dict["currentFrame"] = _to_int(row_dict.get("currentFrame"), 0)
    row_dict["startFrame"] = _to_int(row_dict.get("startFrame"), 0)
    row_dict["endFrame"] = _to_int(row_dict.get("endFrame"), 0)
    row_dict["globalStart"] = _to_int(row_dict.get("globalStart"), row_dict["startFrame"])
    row_dict["globalEnd"] = _to_int(row_dict.get("globalEnd"), row_dict["endFrame"])
    row_dict["alertState"] = row_dict.get("alert_state", "")
    row_dict["workerState"] = row_dict.get("worker_state", "running")
    row_dict["workerStateReason"] = row_dict.get("worker_state_reason", "")
    row_dict["crashDetectionStatus"] = row_dict.get("crash_detection_status", "")
    row_dict["crashDetectionEnabled"] = _to_bool(row_dict.get("crash_detection_enabled"))
    row_dict["crashDetectionLastHeartbeatAt"] = row_dict.get("crash_detection_last_heartbeat_at", row_dict.get("last_heartbeat_ts", row_dict.get("updated_at")))
    row_dict["crashDetectionHeartbeatAgeSeconds"] = _to_float(row_dict.get("crash_detection_heartbeat_age_seconds"), 0.0)
    row_dict["crashDetectionWatchdogPid"] = _to_int(row_dict.get("crash_detection_watchdog_pid"), 0)
    row_dict["crashDetected"] = _to_bool(row_dict.get("crash_detected"))
    row_dict["crashReason"] = row_dict.get("crash_reason", "")
    row_dict["stallDetected"] = _to_bool(row_dict.get("stall_detected"))
    row_dict["stallReason"] = row_dict.get("stall_reason", "")
    row_dict["staleForSeconds"] = _to_float(row_dict.get("stale_for_seconds"), 0.0)
    row_dict["lastHeartbeatTs"] = row_dict.get("last_heartbeat_ts", row_dict.get("updated_at"))
    row_dict["lastFrameSeen"] = _to_int(row_dict.get("last_frame_seen"), row_dict["currentFrame"])
    row_dict["crashedAt"] = row_dict.get("crashed_at", "")
    row_dict["stalledAt"] = row_dict.get("stalled_at", "")
    row_dict["heartbeatScope"] = row_dict.get("heartbeat_scope", "local_only")
    row_dict["alertSentCrash"] = _to_bool(row_dict.get("alert_sent_crash"))
    row_dict["discordCrashAlertSent"] = _to_bool(row_dict.get("discord_crash_alert_sent"))
    row_dict["telegramCrashAlertSent"] = _to_bool(row_dict.get("telegram_crash_alert_sent"))
    row_dict["alertSentStall"] = _to_bool(row_dict.get("alert_sent_stall"))
    row_dict["discordStallAlertSent"] = _to_bool(row_dict.get("discord_stall_alert_sent"))
    row_dict["telegramStallAlertSent"] = _to_bool(row_dict.get("telegram_stall_alert_sent"))
    row_dict["houdiniPid"] = _to_int(row_dict.get("houdini_pid"), 0)
    row_dict["gpuTelemetryState"] = row_dict.get("gpu_telemetry_state", "unavailable")
    row_dict["gpuStatusMessage"] = row_dict.get("gpu_status_message", "")
    row_dict["gpuName"] = row_dict.get("gpu_name", row_dict.get("gpu", ""))
    row_dict["gpuIndex"] = row_dict.get("gpu_index", -1)
    row_dict["gpuUuid"] = row_dict.get("gpu_uuid", "")
    row_dict["gpuBusId"] = row_dict.get("gpu_bus_id", "")
    row_dict["gpuUtilPercent"] = row_dict.get("gpu_util_percent")
    row_dict["vramUsedMb"] = row_dict.get("vram_used_mb")
    row_dict["vramTotalMb"] = row_dict.get("vram_total_mb")
    row_dict["vramPercent"] = row_dict.get("vram_percent")
    row_dict["gpuTemperatureC"] = row_dict.get("temperature_c")
    row_dict["gpuPowerW"] = row_dict.get("power_w")
    row_dict["gpuSampledAt"] = row_dict.get("gpu_sampled_at", 0.0)
    row_dict["gpuCount"] = _to_int(row_dict.get("gpu_count"), 0)
    return row_dict


def _merge_gpu_metrics_row(row_dict, gpu_metrics_map):
    merged = dict(row_dict or {})
    metrics_by_worker = gpu_metrics_map if isinstance(gpu_metrics_map, dict) else {}
    worker_name = str(merged.get("workerName") or "").strip()
    metrics = dict(metrics_by_worker.get(worker_name) or {})
    if not metrics:
        return merged

    merged.update({
        "gpu_telemetry_state": metrics.get("gpu_telemetry_state", "unavailable"),
        "gpu_status_message": metrics.get("gpu_status_message", ""),
        "gpu_name": metrics.get("gpu_name", ""),
        "gpu_index": metrics.get("gpu_index", -1),
        "gpu_uuid": metrics.get("gpu_uuid", ""),
        "gpu_bus_id": metrics.get("gpu_bus_id", ""),
        "gpu_util_percent": metrics.get("gpu_util_percent"),
        "vram_used_mb": metrics.get("vram_used_mb"),
        "vram_total_mb": metrics.get("vram_total_mb"),
        "vram_percent": metrics.get("vram_percent"),
        "temperature_c": metrics.get("temperature_c"),
        "power_w": metrics.get("power_w"),
        "gpu_sampled_at": metrics.get("gpu_sampled_at", 0.0),
        "gpu_source": metrics.get("gpu_source", ""),
        "gpu_source_path": metrics.get("gpu_source_path", ""),
        "gpu_count": metrics.get("gpu_count", 0),
    })
    if metrics.get("gpu_name"):
        merged["gpu"] = metrics.get("gpu_name")
    return merged


def _build_firebase_renders_url(raw_url):
    raw = (raw_url or "").strip()
    if (not raw) or (not raw.startswith("http")):
        return ""
    try:
        parsed = urlparse(raw)
        host = (parsed.netloc or "").lower().split(":", 1)[0]
        if ("firebaseio.com" not in host) and ("firebasedatabase.app" not in host):
            return ""
    except Exception:
        return ""
    cleaned = raw.rstrip("/")
    cleaned = re.sub(r"/renders(?:\.json)?$", "", cleaned, flags=re.IGNORECASE)
    return cleaned + "/renders.json"


def _firebase_mirror_state(config=None):
    source = config if isinstance(config, dict) else CONFIG_CACHE
    mirror_url = _build_firebase_renders_url((source or {}).get("firebase_url", FIREBASE_URL) or FIREBASE_URL)
    return {
        "firebase_url": mirror_url,
        "firebase_mirror_enabled": bool(mirror_url),
        "local_only": not bool(mirror_url),
    }


def _firebase_sync_allowed(config=None):
    return _firebase_mirror_state(config).get("firebase_mirror_enabled", False)


def _sync_firebase(method, keys=None, clear_all=False):
    """Synchronize SQLite truth changes to the Firebase mirror."""
    mirror_state = _firebase_mirror_state()
    if not mirror_state.get("firebase_mirror_enabled"):
        _warn_once("mirror.delete.local_only", "[SYNC] Firebase mirror disabled; delete/clear stays local-only.")
        return
    url = mirror_state.get("firebase_url", "")
    if not url:
        return

    try:
        if clear_all:
            print("[SYNC] Mirror delete requested: clear all mirrored records from SQLite truth.")
            req = urllib.request.Request(url, method="DELETE")
        elif keys:
            unique_keys = list(set([k for k in keys if k]))
            if not unique_keys:
                return
            payload = {k: None for k in unique_keys}
            json_data = json.dumps(payload).encode("utf-8")
            print(f"[SYNC] Mirror delete requested for {len(unique_keys)} key(s).")
            req = urllib.request.Request(
                url, 
                data=json_data, 
                method="PATCH", 
                headers={"Content-Type": "application/json"}
            )
        else:
            return

        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"[SYNC] Firebase mirror delete OK: HTTP {getattr(resp, 'status', 200)}")
    except Exception as e:
        print(f"[SYNC] Firebase mirror delete failed ({method}): {e}")


def _build_api_data_snapshot():
    started_at = time.time()
    if not DB_PATH or not os.path.exists(DB_PATH):
        return None

    db_signature = _db_content_signature(DB_PATH)
    cached = _API_DATA_CACHE.get("data")
    if isinstance(cached, dict) and str(_API_DATA_CACHE.get("db_signature", "")) == db_signature:
        return cached

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        job_model.ensure_monitoring_schema(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM render_jobs")
        rows = cur.fetchall()
        gpu_metrics_map = job_model.sqlite_list_gpu_metrics_latest(conn)
        data = {}
        for row in rows:
            key = row["render_key"]
            row_dict = _merge_gpu_metrics_row(dict(row), gpu_metrics_map)
            row_dict = _normalize_row_for_webui(row_dict)
            data[key] = row_dict
        response_json = json.dumps(data, separators=(",", ":")).encode("utf-8")
    finally:
        conn.close()

    snapshot = {
        "db_signature": db_signature,
        "data": data,
        "response_json": response_json,
        "signature": f"{db_signature}:{len(data)}:{len(response_json)}",
    }
    _API_DATA_CACHE.clear()
    _API_DATA_CACHE["data"] = snapshot
    _perf_log("api.data.snapshot", started_at, rows=len(data))
    return snapshot


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_PATH, **kwargs)

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        if self.path == "/api/delete":
            self.handle_api_delete()
            return
        if self.path == "/api/clear_all":
            self.handle_api_clear_all()
            return
        if self.path == "/api/sync_to_firebase":
            self.handle_api_sync_to_firebase()
            return
        return super().do_POST()

    def handle_api_sync_to_firebase(self):
        try:
            refresh_runtime_config()
            result = sync_sqlite_to_firebase()
            status_code = 200 if result.get("ok") else 400
            response_json = json.dumps(result).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("Content-Length", len(response_json))
            self.end_headers()
            self.wfile.write(response_json)
        except Exception as e:
            self.send_error(500, f"Firebase Sync Error: {str(e)}")

    def handle_api_clear_all(self):
        if not DB_PATH or not os.path.exists(DB_PATH):
            self.send_error(404, "Database file not found or not configured")
            return

        try:
            print(f"[WEBUI] Clear action target=local_db db_path={DB_PATH}")
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("DELETE FROM render_jobs")
            conn.commit()
            conn.close()
            _API_DATA_CACHE.clear()
            _FIREBASE_SYNC_CACHE.clear()

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "target": "local_db",
                "message": "Local DB cleared. Firebase mirror unchanged.",
            }).encode("utf-8"))
        except Exception as e:
            self.send_error(500, f"Clear All Error: {str(e)}")

    def handle_api_delete(self):
        if not DB_PATH or not os.path.exists(DB_PATH):
            self.send_error(404, "Database file not found or not configured")
            return

        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            params = json.loads(post_data)
            
            job_key = params.get("job_key") # Single key
            explicit_keys = params.get("keys", []) # List of keys
            shot_name = params.get("shot_name")
            search_terms = params.get("search_terms", []) or []

            if not job_key and not explicit_keys and not shot_name and not search_terms:
                self.send_error(400, "Missing job_key, keys, shot_name or search_terms")
                return

            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            
            # --- Collect keys for optional Firebase mirror delete ---
            keys_to_sync = []
            if job_key: keys_to_sync.append(job_key)
            if explicit_keys and isinstance(explicit_keys, list):
                keys_to_sync.extend(explicit_keys)
            
            # Support search terms for grouped deletion
            all_search = list(search_terms)
            if shot_name and shot_name not in all_search:
                all_search.append(shot_name)
            
            for term in all_search:
                if not term: continue
                cur.execute("SELECT render_key FROM render_jobs WHERE shotName = ? OR node_path = ? OR node_path LIKE ?", (term, term, f"%/{term}"))
                for row in cur.fetchall():
                    keys_to_sync.append(row[0])

            # --- Delete from SQLite truth ---
            if job_key:
                cur.execute("DELETE FROM render_jobs WHERE render_key = ?", (job_key,))
            
            if explicit_keys and isinstance(explicit_keys, list):
                for k in explicit_keys:
                    cur.execute("DELETE FROM render_jobs WHERE render_key = ?", (k,))
            
            for term in all_search:
                if not term: continue
                cur.execute("DELETE FROM render_jobs WHERE shotName = ? OR node_path = ? OR node_path LIKE ?", (term, term, f"%/{term}"))
                
            conn.commit()
            conn.close()
            _API_DATA_CACHE.clear()
            _FIREBASE_SYNC_CACHE.clear()
            print(
                f"[WEBUI] Delete action target=local_db keys={len(list(set([k for k in keys_to_sync if k])))} "
                f"shot_name={shot_name or ''}"
            )

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "target": "local_db",
                "message": "Deleted from Local DB only. Firebase mirror unchanged.",
            }).encode("utf-8"))
        except Exception as e:
            self.send_error(500, f"Delete Error: {str(e)}")

    def do_GET(self):
        # API Endpoint for Local SQLite Data
        if self.path == "/api/data":
            self.handle_api_data()
            return
        if self.path == "/api/runtime_status" or self.path == "/api/service_status":
            self.handle_api_runtime_status()
            return
            
        if self.path in ["/", "/index.html"]:
            if os.path.exists(INDEX_PATH):
                self.send_response(200)
                self.send_header("Content-type", "text/html; charset=utf-8")
                self.end_headers()
                with open(INDEX_PATH, "rb") as f:
                    self.wfile.write(f.read())
                return
        return super().do_GET()

    def handle_api_data(self):
        if not DB_PATH or not os.path.exists(DB_PATH):
            self.send_error(404, "Database file not found or not configured")
            return

        try:
            started_at = time.time()
            _warn_once(f"api.data.sqlite.{DB_PATH}", f"[WEBUI] /api/data serving SQLite truth from {DB_PATH}")
            snapshot = _build_api_data_snapshot()
            if snapshot is None:
                self.send_error(404, "Database file not found or not configured")
                return
            response_json = snapshot["response_json"]
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*") # Enable CORS for debugging
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("X-JDTool-Data-Signature", snapshot.get("signature", ""))
            self.send_header("Content-Length", len(response_json))
            self.end_headers()
            
            if self.command == "HEAD":
                _perf_log("api.data.head", started_at, bytes=len(response_json))
                return

            self.wfile.write(response_json)
            _perf_log("api.data.get", started_at, bytes=len(response_json))
        except Exception as e:
            self.send_error(500, f"Database Error: {str(e)}")

    def handle_api_runtime_status(self):
        try:
            started_at = time.time()
            refresh_runtime_config()
            payload = _build_watchdog_runtime_status(DB_PATH)
            response_json = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("Content-Length", len(response_json))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response_json)
            _perf_log("api.runtime_status", started_at, bytes=len(response_json))
        except Exception as e:
            self.send_error(500, f"Runtime Status Error: {str(e)}")


def sync_sqlite_to_firebase():
    """Push SQLite truth to the optional Firebase mirror and return a status payload."""
    started_at = time.time()
    mirror_state = _firebase_mirror_state()
    if not mirror_state.get("firebase_mirror_enabled"):
        _warn_once("sync.local_only", "[SYNC] Local-only mode active; Firebase mirror sync skipped.")
        return {
            "ok": False,
            "reason": "sync_disabled",
            "message": "Firebase mirror is not configured. Tracker remains local-only on SQLite.",
            "pushed": 0,
        }
    print(f"[SYNC] Mirror sync requested: SQLite truth -> Firebase mirror ({mirror_state.get('firebase_url', '')})")

    if not DB_PATH or not os.path.exists(DB_PATH):
        print(f"[SYNC] SKIPPED: Local database not found at {DB_PATH}")
        return {
            "ok": False,
            "reason": "missing_db",
            "message": f"Local database not found at {DB_PATH}",
            "pushed": 0,
        }
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM render_jobs")
        rows = cur.fetchall()
        gpu_metrics_map = job_model.sqlite_list_gpu_metrics_latest(conn)

        data = {}
        excluded_map = {}
        for row in rows:
            key = row["render_key"]
            machine_data = job_model.sqlite_row_to_machine_data(dict(row))
            worker_name = str(machine_data.get("workerName") or "").strip()
            machine_data = job_model.merge_gpu_runtime_into_machine_data(
                machine_data,
                gpu_metrics_map.get(worker_name) if worker_name else {},
            )
            data[key] = job_model.firebase_mirror_machine_data(machine_data, render_key=key)
            excluded_map[key] = list(job_model.firebase_excluded_fields(machine_data, render_key=key))

        conn.close()

        if not data:
            print("[SYNC] SQLite truth is empty, nothing to mirror.")
            return {
                "ok": True,
                "reason": "empty",
                "message": "SQLite truth is empty, nothing to mirror.",
                "pushed": 0,
            }

        url = mirror_state.get("firebase_url", "")
        if not url:
            print("[SYNC] SKIPPED: Firebase mirror URL is invalid.")
            return {
                "ok": False,
                "reason": "invalid_url",
                "message": "Configured Firebase mirror URL is invalid.",
                "pushed": 0,
            }
            
        json_data = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        sync_signature = f"{_file_mtime(DB_PATH):.6f}:{len(data)}:{len(json_data)}"
        previous_signature = str(_FIREBASE_SYNC_CACHE.get("signature") or "")
        previous_url = str(_FIREBASE_SYNC_CACHE.get("url") or "")
        if previous_signature == sync_signature and previous_url == url:
            print("[SYNC] Firebase mirror already matches the latest SQLite snapshot.")
            return {
                "ok": True,
                "reason": "unchanged",
                "message": "Firebase mirror already matches the latest SQLite snapshot.",
                "pushed": 0,
            }
        req = urllib.request.Request(
            url,
            data=json_data,
            method="PATCH",
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            _FIREBASE_SYNC_CACHE["signature"] = sync_signature
            _FIREBASE_SYNC_CACHE["url"] = url
            _perf_log("sync_sqlite_to_firebase", started_at, pushed=len(data), status=getattr(resp, "status", 200))
            print(
                "[SYNC] Firebase mirror excluded local-only fields: "
                + ", ".join(f"{key}=>{','.join(value) if value else 'none'}" for key, value in sorted(excluded_map.items())[:8])
            )
            print(f"[SYNC] OK! Mirrored {len(data)} SQLite record(s) to Firebase.")
            return {
                "ok": True,
                "reason": "synced",
                "message": f"Successfully mirrored {len(data)} SQLite record(s) to Firebase.",
                "pushed": len(data),
                "status_code": getattr(resp, "status", 200),
            }
            
    except Exception as e:
        _perf_log("sync_sqlite_to_firebase", started_at, pushed=0, error="1")
        print(f"[SYNC] Firebase mirror sync failed: {e}")
        return {
            "ok": False,
            "reason": "exception",
            "message": str(e),
            "pushed": 0,
        }


def _normalize_mode(mode_value):
    raw = (mode_value or "").strip().lower()
    if raw in ("public", "collaborative", "shared", "cloud", "online"):
        return "collaborative"
    if raw in ("local", "private", "offline"):
        return "private"
    return "private"


def refresh_runtime_config():
    """Reload config from disk so manual actions use the latest saved settings."""
    global FIREBASE_URL, WEBHOOK_URL, GITHUB_URL, TRACKER_MODE, CONFIG_CACHE
    global WATCHDOG_INTERVAL_SECONDS, WATCHDOG_STALE_SECONDS

    if not CONFIG_PATH:
        return {}

    cfg = _load_config_file(CONFIG_PATH)
    if not isinstance(cfg, dict) or not cfg:
        return {}

    FIREBASE_URL = _build_firebase_renders_url(cfg.get("firebase_url", FIREBASE_URL))
    WEBHOOK_URL = cfg.get("webhook_url", WEBHOOK_URL)
    GITHUB_URL = cfg.get("github_url", GITHUB_URL)
    TRACKER_MODE = _normalize_mode(cfg.get("mode", TRACKER_MODE))
    CONFIG_CACHE = dict(cfg)
    WATCHDOG_INTERVAL_SECONDS = max(1, _to_int(cfg.get("watchdog_poll_interval_seconds", WATCHDOG_INTERVAL_SECONDS), WATCHDOG_INTERVAL_SECONDS))
    WATCHDOG_STALE_SECONDS = max(15, _to_int(cfg.get("watchdog_crash_timeout_seconds", WATCHDOG_STALE_SECONDS), WATCHDOG_STALE_SECONDS))
    write_runtime_info()
    return cfg


def write_runtime_info():
    watchdog_status = _build_watchdog_runtime_status(DB_PATH)
    mirror_state = _firebase_mirror_state(CONFIG_CACHE)
    runtime_payload = {
        "pid": os.getpid(),
        "started_at": SERVER_STARTED_AT,
        "python_executable": sys.executable,
        "host": SERVER_HOST,
        "port": SERVER_PORT,
        "local_url": f"http://127.0.0.1:{SERVER_PORT}",
        "public_url": PUBLIC_URL,
        "index_path": INDEX_PATH.replace("\\", "/"),
        "db_path": DB_PATH.replace("\\", "/"),
        "public_mode": "cloudflared" if PUBLIC_URL else "none",
        "tracker_mode": TRACKER_MODE,
        "firebase_url_default": FIREBASE_URL,
        "firebase_mirror_enabled": mirror_state.get("firebase_mirror_enabled", False),
        "firebase_sync_enabled": mirror_state.get("firebase_mirror_enabled", False),
        "local_only_mode": mirror_state.get("local_only", True),
        "data_authority": "sqlite",
        "dashboard_read_source": "sqlite_truth",
        "dashboard_read_label": "SQLite truth",
        "view_source_default": "local_db",
        "view_sources": ["local_db", "firebase_mirror"],
        "webhook_url_default": WEBHOOK_URL,
        "github_url_default": GITHUB_URL,
        "watchdog_status": watchdog_status,
        "crash_detection_status": watchdog_status,
        "product_name": JD_PRODUCT_NAME,
        "build_id": JD_BUILD_ID,
        "owner": JD_OWNER,
        "copyright": JD_COPYRIGHT,
        "copyright_short": JD_COPYRIGHT_SHORT,
    }
    try:
        existing_payload = _read_json(RUNTIME_INFO_PATH)
        if existing_payload == runtime_payload:
            return
        with open(RUNTIME_INFO_PATH, "w", encoding="utf-8") as f:
            json.dump(runtime_payload, f, indent=2)
    except Exception as e:
        print(f"[WARN] Failed to write runtime info: {e}")


def _consume_tunnel_logs(proc):
    global PUBLIC_URL
    url_pattern = re.compile(r"https://[a-zA-Z0-9\-]+\.trycloudflare\.com")
    start = time.time()
    while time.time() - start < 20:
        line = proc.stdout.readline()
        if not line:
            break
        line = line.strip()
        match = url_pattern.search(line)
        if match:
            url = match.group(0)
            if _url_is_reachable(url, timeout=4):
                PUBLIC_URL = url
                write_runtime_info()
                print(f"[PUBLIC] {PUBLIC_URL}")
                return


def maybe_open_public_tunnel(port):
    global TUNNEL_PROCESS
    if not os.path.exists(CLOUDFLARED_PATH):
        print(f"[WARN] Public tunnel unavailable: missing {CLOUDFLARED_PATH}")
        return
    try:
        cmd = [
            CLOUDFLARED_PATH,
            "tunnel",
            "--url",
            f"http://127.0.0.1:{port}",
            "--protocol",
            "http2",
            "--no-autoupdate",
        ]
        TUNNEL_PROCESS = subprocess.Popen(
            cmd,
            cwd=BASE_PATH,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        t = threading.Thread(target=_consume_tunnel_logs, args=(TUNNEL_PROCESS,), daemon=True)
        t.start()
    except Exception as e:
        print(f"[WARN] Failed to start cloudflared tunnel: {e}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run Render Tracker web server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--public", action="store_true", help="Try public URL via bundled cloudflared")
    parser.add_argument("--db", default="", help="Path to SQLite database file")
    parser.add_argument("--config", default="", help="Path to doneyet_config.json")
    return parser.parse_args()


def main():
    global SERVER_HOST, SERVER_PORT, DB_PATH, CONFIG_PATH
    args = parse_args()
    SERVER_HOST = args.host
    SERVER_PORT = args.port
    DB_PATH = args.db
    CONFIG_PATH = args.config

    if args.config:
        cfg = refresh_runtime_config()
    else:
        cfg = {}

    try:
        _ensure_scripts_python_path()
        from doneyet_modes import collaborative_mode

        collaborative_mode.print_runtime_diagnostics(
            mode=TRACKER_MODE,
            db_path=DB_PATH,
            config=cfg,
            watchdog_running=_watchdog_running(DB_PATH),
            local_api_reachable=False,
            prefix="[WEBUI]",
        )
    except Exception as exc:
        _warn_once("diagnostics.failed", f"[WEBUI] diagnostics unavailable: {exc}")

    if args.public:
        maybe_open_public_tunnel(SERVER_PORT)

    write_runtime_info()

    mirror_state = _firebase_mirror_state(CONFIG_CACHE)
    if mirror_state.get("firebase_mirror_enabled"):
        print("[WEBUI] Firebase mirror enabled. SQLite remains the source of truth; dashboard view source is user-selected.")
    else:
        print("[WEBUI] Local-only mode active. SQLite is the only data source and Firebase mirror is not configured.")

    # Start Crash Monitor
    monitor_t = threading.Thread(target=crash_monitor_thread, daemon=True)
    monitor_t.start()
    if bool(cfg.get("webui_watchdog_backup_enabled", False)):
        _warn_once("watchdog.backup.disabled", "[WEBUI] Backup in-process watchdog is disabled. Crash Detection now runs only as the external service.")

    print("\n--- RENDER DASHBOARD IS RUNNING ---")
    print(jd_runtime_label(include_owner=True))
    print(f"Local:  http://127.0.0.1:{SERVER_PORT}")
    if DB_PATH:
        print(f"DB:     {DB_PATH}")
    if PUBLIC_URL:
        print(f"Public: {PUBLIC_URL}")
    else:
        print("Public: unavailable (bundle cloudflared.exe to enable)")
    print("-----------------------------------\n")

    httpd = ThreadingHTTPServer((SERVER_HOST, SERVER_PORT), DashboardHandler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
