# utils/status_helpers.py
import json
import datetime
import time
import os
from pathlib import Path
from threading import Lock
import tempfile

# Path to dashboard status file
STATUS_PATH = Path("dashboard/status.json")
_LOCK = Lock()

# Tuning for retries (Windows may lock files briefly)
_RETRY_ATTEMPTS = 12
_RETRY_DELAY = 0.2  # seconds

def _atomic_write(path: Path, data: str) -> bool:
    """
    Write data to path atomically. On Windows os.replace can fail with PermissionError
    if another process (e.g., antivirus or the dashboard reader) is holding the file.
    We retry with small delay and fall back to overwrite if needed.
    """
    dirpath = path.parent
    dirpath.mkdir(parents=True, exist_ok=True)
    for attempt in range(_RETRY_ATTEMPTS):
        tmp_fd = None
        tmpname = None
        try:
            tmp_fd, tmpname = tempfile.mkstemp(prefix=path.name + ".", dir=str(dirpath))
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    # fsync might fail on some envs; ignore
                    pass
            # Try atomic replace
            try:
                os.replace(tmpname, str(path))
                return True
            except PermissionError:
                # target may be locked. remove temp and retry after delay
                try:
                    if os.path.exists(tmpname):
                        os.remove(tmpname)
                except Exception:
                    pass
                time.sleep(_RETRY_DELAY)
                continue
            except Exception:
                # fallback: clean temp and retry
                try:
                    if os.path.exists(tmpname):
                        os.remove(tmpname)
                except Exception:
                    pass
                time.sleep(_RETRY_DELAY)
                continue
        except Exception:
            # ensure temp cleaned
            try:
                if tmpname and os.path.exists(tmpname):
                    os.remove(tmpname)
            except Exception:
                pass
            time.sleep(_RETRY_DELAY)
            continue

    # Last-resort: overwrite the file (best-effort)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(data)
        return True
    except Exception:
        return False

def _safe_write(obj: dict):
    """
    Adds last_updated timestamp and writes status.json safely.
    This function should not raise; it returns True/False.
    """
    try:
        obj['last_updated'] = datetime.datetime.now().astimezone().isoformat()
        text = json.dumps(obj, indent=2, ensure_ascii=False)
        ok = _atomic_write(STATUS_PATH, text)
        if not ok:
            # try simple write as final fallback
            try:
                STATUS_PATH.write_text(text, encoding="utf-8")
                ok = True
            except Exception:
                ok = False
        return ok
    except Exception:
        return False

def read_status() -> dict:
    try:
        if not STATUS_PATH.exists():
            return {}
        return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def write_status(new_obj: dict) -> bool:
    with _LOCK:
        return _safe_write(new_obj)

def set_field(key: str, value) -> bool:
    with _LOCK:
        s = read_status()
        s[key] = value
        return _safe_write(s)

def increment(key: str, delta: int = 1) -> bool:
    with _LOCK:
        s = read_status()
        s[key] = s.get(key, 0) + delta
        return _safe_write(s)

def push_recent(item: dict, max_items: int = 80) -> bool:
    with _LOCK:
        s = read_status()
        rec = s.get("recent", [])
        if 'ts' not in item:
            item['ts'] = datetime.datetime.now().astimezone().isoformat()
        rec.insert(0, item)
        s['recent'] = rec[:max_items]
        return _safe_write(s)

def snapshot_counts(total_rows=None, enqueued=None, processed=None, done=None,
                    failed=None, workers_active=None, queue_len=None, message=None, stage=None) -> bool:
    with _LOCK:
        s = read_status()
        if total_rows is not None: s['total_rows'] = total_rows
        if enqueued is not None: s['enqueued'] = enqueued
        if processed is not None: s['processed'] = processed
        if done is not None: s['done'] = done
        if failed is not None: s['failed'] = failed
        if workers_active is not None: s['workers_active'] = workers_active
        if queue_len is not None: s['redis_queue_length'] = queue_len
        if message is not None: s['message'] = message
        if stage is not None: s['stage'] = stage
        return _safe_write(s)
