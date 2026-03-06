import os
import time
import json
import socket
import fcntl
import html
import base64
import mimetypes
import uuid
import re
import hashlib
import hmac
import threading
from urllib.parse import urlparse
from urllib.parse import quote
from urllib.parse import parse_qs
from urllib.parse import unquote_plus
from urllib.parse import urlsplit
from pathlib import Path
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


# -------------------------
# Helpers / logging
# -------------------------
def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")


LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "WARNING": 30, "ERROR": 40}


def _log_level_num() -> int:
    lvl = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    return LEVELS.get(lvl, 20)


def log(level, msg):
    lvl = LEVELS.get(level.upper(), 20)
    if lvl < _log_level_num():
        return
    print(f"[{now_iso()}] [{level.upper()}] {msg}", flush=True)


def die(msg: str):
    raise SystemExit(f"[FATAL] {msg}")


def env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip() == "1"


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: str) -> int:
    return int(env_str(name, default) or default)


def env_float(name: str, default: str) -> float:
    return float(env_str(name, default) or default)


def normalize_key(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


# -------------------------
# Load .env relative to app.py
# IMPORTANT: override=False so CLI env wins (DRY_RUN=1 ...)
# -------------------------
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


# -------------------------
# Force IPv4 (optional)
# -------------------------
_real_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _real_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


if env_bool("FORCE_IPV4", "1"):
    socket.getaddrinfo = _ipv4_only_getaddrinfo


# -------------------------
# HTTP session with retries
# -------------------------
def build_session() -> requests.Session:
    s = requests.Session()
    retry_total = max(env_int("HTTP_RETRY_TOTAL", "1"), 0)
    retry_connect = max(env_int("HTTP_RETRY_CONNECT", str(retry_total)), 0)
    retry_read = max(env_int("HTTP_RETRY_READ", str(retry_total)), 0)
    retry_backoff = max(env_float("HTTP_RETRY_BACKOFF", "0.4"), 0.0)
    retries = Retry(
        total=retry_total,
        connect=retry_connect,
        read=retry_read,
        backoff_factor=retry_backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET", "PUT"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def build_no_retry_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=0,
        connect=0,
        read=0,
        backoff_factor=0.0,
        status_forcelist=[],
        allowed_methods=False,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SESSION = build_session()
SESSION_NO_RETRY = build_no_retry_session()
LOCK_HANDLE = None
CLIENT_PHONE_CACHE: dict[str, str] = {}
WEB_ACCESS_TOKEN_CACHE = ""
WEBHOOK_WAKE_EVENT = threading.Event()
WEBHOOK_STATE_LOCK = threading.Lock()
WEBHOOK_PENDING_TICKET_IDS: set[str] = set()
WEBHOOK_RECENT_EVENT_IDS: dict[str, float] = {}
WEBHOOK_SERVER = None
PROJECT_TASK_IDS_CACHE: dict[str, dict] = {}
PROJECT_TASKS_CACHE_LOCK = threading.Lock()


class GoodDayTaskNotFoundError(RuntimeError):
    """Raised when a mapped GoodDay task no longer exists."""


# -------------------------
# Webhook helpers
# -------------------------
def _normalize_ticket_identifier(raw) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    digits = "".join(ch for ch in txt if ch.isdigit())
    if not digits:
        return ""
    return digits


def _text_ticket_ids(text: str) -> set[str]:
    out = set()
    src = str(text or "")
    if not src:
        return out
    # Examples:
    # - "WHMCS Ticket #709602"
    # - "ticketid=2362"
    for m in re.finditer(r"(?:whmcs\s*ticket\s*#?|ticket(?:id)?\s*[#:=>]?\s*)(\d{3,})", src, flags=re.IGNORECASE):
        ident = _normalize_ticket_identifier(m.group(1))
        if ident:
            out.add(ident)
    return out


def extract_ticket_identifiers_from_payload(payload) -> set[str]:
    out = set()
    queue = [payload]

    while queue:
        item = queue.pop(0)
        if isinstance(item, dict):
            for key, value in item.items():
                k = str(key or "").strip().lower()
                if k in ("ticketid", "ticket_id", "tid", "whmcs_ticket_id", "whmcs_ticketid"):
                    ident = _normalize_ticket_identifier(value)
                    if ident:
                        out.add(ident)
                elif k == "ticket":
                    if isinstance(value, (str, int, float)):
                        ident = _normalize_ticket_identifier(value)
                        if ident:
                            out.add(ident)
                    elif isinstance(value, dict):
                        for tk in ("id", "ticketid", "ticket_id", "tid", "number"):
                            ident = _normalize_ticket_identifier(value.get(tk))
                            if ident:
                                out.add(ident)
                elif isinstance(value, str):
                    out.update(_text_ticket_ids(value))

                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(item, list):
            for sub in item:
                if isinstance(sub, (dict, list)):
                    queue.append(sub)
                elif isinstance(sub, str):
                    out.update(_text_ticket_ids(sub))
        elif isinstance(item, str):
            out.update(_text_ticket_ids(item))

    return out


def _webhook_cleanup_recent_event_ids(max_entries: int = 4000):
    ttl = max(env_int("SYNC_WEBHOOK_EVENT_TTL_SECONDS", "900"), 60)
    cutoff = time.time() - ttl
    for key in list(WEBHOOK_RECENT_EVENT_IDS.keys()):
        if WEBHOOK_RECENT_EVENT_IDS.get(key, 0.0) < cutoff:
            WEBHOOK_RECENT_EVENT_IDS.pop(key, None)
    if len(WEBHOOK_RECENT_EVENT_IDS) > max_entries:
        for key in sorted(WEBHOOK_RECENT_EVENT_IDS, key=WEBHOOK_RECENT_EVENT_IDS.get)[: len(WEBHOOK_RECENT_EVENT_IDS) - max_entries]:
            WEBHOOK_RECENT_EVENT_IDS.pop(key, None)


def enqueue_webhook_ticket_ids(ticket_ids: set[str], event_id: str = "") -> int:
    with WEBHOOK_STATE_LOCK:
        if event_id:
            now_ts = time.time()
            if event_id in WEBHOOK_RECENT_EVENT_IDS:
                return 0
            WEBHOOK_RECENT_EVENT_IDS[event_id] = now_ts
            _webhook_cleanup_recent_event_ids()

        queued = 0
        for raw in sorted(ticket_ids):
            ident = _normalize_ticket_identifier(raw)
            if not ident:
                continue
            if ident not in WEBHOOK_PENDING_TICKET_IDS:
                WEBHOOK_PENDING_TICKET_IDS.add(ident)
                queued += 1

    WEBHOOK_WAKE_EVENT.set()
    return queued


def consume_webhook_ticket_ids() -> list[str]:
    with WEBHOOK_STATE_LOCK:
        if not WEBHOOK_PENDING_TICKET_IDS:
            return []
        out = sorted(WEBHOOK_PENDING_TICKET_IDS)
        WEBHOOK_PENDING_TICKET_IDS.clear()
        return out


def resolve_webhook_ticket_ids(raw_ids: list[str], tickets_state: dict) -> list[str]:
    if not raw_ids:
        return []
    resolved = []
    seen = set()

    for raw in raw_ids:
        ident = _normalize_ticket_identifier(raw)
        if not ident:
            continue

        internal_id = ""
        if ident in tickets_state:
            internal_id = ident
        else:
            short_ident = ident.lstrip("0") or "0"
            for state_tid, entry in list(tickets_state.items()):
                tid_txt = str(state_tid or "").strip()
                if not tid_txt:
                    continue
                if tid_txt == ident:
                    internal_id = tid_txt
                    break
                if not isinstance(entry, dict):
                    continue
                whmcs_tid = str(entry.get("whmcs_tid") or "").strip()
                if whmcs_tid and (whmcs_tid.lstrip("0") or "0") == short_ident:
                    internal_id = tid_txt
                    break
            if not internal_id:
                # Fallback: assume webhook already gave internal ticket id.
                internal_id = ident

        if internal_id not in seen:
            seen.add(internal_id)
            resolved.append(internal_id)

    return resolved


def fetch_tickets_by_internal_ids(ticket_ids: list[str]) -> list[dict]:
    out = []
    seen = set()
    for raw in ticket_ids:
        tid = _normalize_ticket_identifier(raw)
        if not tid or tid in seen:
            continue
        seen.add(tid)
        try:
            data = whmcs_api("GetTicket", ticketid=int(tid), repliessort="ASC")
        except Exception as e:
            log("WARNING", f"Webhook targeted fetch skipped ticket={tid}: {repr(e)}")
            continue
        if not isinstance(data, dict):
            continue
        if "ticketid" not in data:
            data["ticketid"] = int(tid)
        if "id" not in data:
            data["id"] = int(tid)
        out.append(data)
    return out


def _webhook_extract_event_id(payload, headers) -> str:
    header_keys = (
        env_str("SYNC_WEBHOOK_EVENT_ID_HEADER", "X-Event-ID"),
        "X-Request-ID",
        "X-Webhook-ID",
    )
    for hk in header_keys:
        val = str(headers.get(hk) or headers.get(hk.lower()) or "").strip()
        if val:
            return val

    queue = [payload]
    while queue:
        item = queue.pop(0)
        if isinstance(item, dict):
            for key, value in item.items():
                k = str(key or "").strip().lower()
                if k in ("eventid", "event_id", "webhookid", "webhook_id", "idempotencykey", "idempotency_key"):
                    val = str(value or "").strip()
                    if val:
                        return val
                if isinstance(value, (dict, list)):
                    queue.append(value)
    return ""


def _webhook_authorized(handler, body: bytes) -> bool:
    secret = env_str("SYNC_WEBHOOK_SECRET")
    if not secret:
        return True

    hdr_name = env_str("SYNC_WEBHOOK_SECRET_HEADER", "X-Webhook-Secret")
    token_hdr = str(handler.headers.get(hdr_name) or "").strip()
    if token_hdr and hmac.compare_digest(token_hdr, secret):
        return True

    sig_hdr_name = env_str("SYNC_WEBHOOK_SIGNATURE_HEADER", "X-Webhook-Signature")
    sig = str(handler.headers.get(sig_hdr_name) or "").strip().lower()
    if sig:
        digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest().lower()
        if hmac.compare_digest(sig, digest) or hmac.compare_digest(sig, f"sha256={digest}"):
            return True

    try:
        parsed = urlsplit(handler.path)
        query = parse_qs(parsed.query)
        token_q = str((query.get("secret") or [""])[0] or "").strip()
        if token_q and hmac.compare_digest(token_q, secret):
            return True
    except Exception:
        pass

    return False


def _webhook_response(handler, code: int, payload: dict):
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _flatten_form_pairs(pairs: dict) -> dict:
    out = {}
    for k, vals in (pairs or {}).items():
        key = str(k or "").strip()
        if not key:
            continue
        if isinstance(vals, list):
            if len(vals) == 1:
                out[key] = vals[0]
            else:
                out[key] = vals
        else:
            out[key] = vals
    return out


def _parse_webhook_payload(body: bytes, headers, path_query: str) -> tuple[dict, str]:
    payload = {}
    source = "empty"

    if body:
        content_type = str(headers.get("Content-Type") or "").lower()
        body_txt = body.decode("utf-8", "replace")

        # Prefer JSON, fallback to form-urlencoded (common webhook formats).
        if "application/json" in content_type:
            try:
                payload = json.loads(body_txt)
                source = "json"
            except Exception:
                payload = {"raw": body_txt}
                source = "raw"
        elif "application/x-www-form-urlencoded" in content_type:
            form = _flatten_form_pairs(parse_qs(body_txt, keep_blank_values=True))
            payload = form if form else {"raw": body_txt}
            source = "form"
        else:
            try:
                payload = json.loads(body_txt)
                source = "json"
            except Exception:
                form = _flatten_form_pairs(parse_qs(body_txt, keep_blank_values=True))
                payload = form if form else {"raw": body_txt}
                source = "form" if form else "raw"

    # Merge URL query params as fallback ticket identifiers.
    if path_query:
        q_payload = _flatten_form_pairs(parse_qs(path_query, keep_blank_values=True))
        if q_payload:
            if isinstance(payload, dict):
                for k, v in q_payload.items():
                    payload.setdefault(k, v)
            else:
                payload = {"payload": payload, **q_payload}
            if source == "empty":
                source = "query"
            else:
                source = f"{source}+query"

    if not isinstance(payload, dict):
        payload = {"payload": payload}
    return payload, source


class SyncWebhookHandler(BaseHTTPRequestHandler):
    server_version = "WHMCSGoodDayWebhook/1.0"

    def do_POST(self):
        path_only = urlsplit(self.path).path or ""
        expected_path = env_str("SYNC_WEBHOOK_PATH", "/webhook").strip() or "/webhook"
        if not expected_path.startswith("/"):
            expected_path = f"/{expected_path}"
        if path_only != expected_path:
            log(
                "WARNING",
                f"Webhook rejected: not_found path={path_only!r} expected={expected_path!r} "
                f"remote={getattr(self, 'client_address', ('-', '-'))[0]}",
            )
            _webhook_response(self, 404, {"ok": False, "error": "not_found"})
            return

        max_body = max(env_int("SYNC_WEBHOOK_MAX_BODY_BYTES", "262144"), 1024)
        try:
            content_len = int(str(self.headers.get("Content-Length") or "0").strip() or "0")
        except Exception:
            content_len = 0
        if content_len > max_body:
            _webhook_response(self, 413, {"ok": False, "error": "payload_too_large"})
            return

        body = self.rfile.read(content_len) if content_len > 0 else b""

        if not _webhook_authorized(self, body):
            log(
                "WARNING",
                f"Webhook rejected: unauthorized path={path_only!r} "
                f"remote={getattr(self, 'client_address', ('-', '-'))[0]}",
            )
            _webhook_response(self, 401, {"ok": False, "error": "unauthorized"})
            return

        payload, payload_source = _parse_webhook_payload(body, self.headers, urlsplit(self.path).query or "")

        event_id = _webhook_extract_event_id(payload, self.headers)
        ticket_ids = extract_ticket_identifiers_from_payload(payload)
        queued = enqueue_webhook_ticket_ids(ticket_ids, event_id=event_id)
        wake_unknown = env_bool("SYNC_WEBHOOK_WAKE_WITHOUT_TICKET", "1")
        if not ticket_ids and wake_unknown:
            WEBHOOK_WAKE_EVENT.set()

        log(
            "INFO",
            f"Webhook accepted: path={path_only} event_id={event_id or '-'} "
            f"ticket_ids={sorted(ticket_ids) if ticket_ids else []} queued={queued} payload={payload_source}",
        )
        _webhook_response(self, 200, {"ok": True, "queued": queued, "ticket_ids": sorted(ticket_ids)})

    def do_GET(self):
        path_only = urlsplit(self.path).path or ""
        health_path = env_str("SYNC_WEBHOOK_HEALTH_PATH", "/healthz").strip() or "/healthz"
        if not health_path.startswith("/"):
            health_path = f"/{health_path}"
        if path_only != health_path:
            _webhook_response(self, 404, {"ok": False, "error": "not_found"})
            return
        _webhook_response(self, 200, {"ok": True, "service": "whmcs-goodday-sync"})

    def log_message(self, format, *args):
        # Suppress default HTTP server stderr logging; use structured app logs above.
        return


def start_webhook_server():
    global WEBHOOK_SERVER
    host = env_str("SYNC_WEBHOOK_HOST", "0.0.0.0")
    port = env_int("SYNC_WEBHOOK_PORT", "8787")
    if WEBHOOK_SERVER is not None:
        return WEBHOOK_SERVER

    WEBHOOK_SERVER = ThreadingHTTPServer((host, port), SyncWebhookHandler)
    thread = threading.Thread(target=WEBHOOK_SERVER.serve_forever, name="sync-webhook-server", daemon=True)
    thread.start()
    log("INFO", f"Webhook server listening on {host}:{port}{env_str('SYNC_WEBHOOK_PATH', '/webhook')}")
    return WEBHOOK_SERVER


# -------------------------
# WHMCS API
# -------------------------
def whmcs_api(action: str, **params):
    whmcs_api_url = env_str("WHMCS_API_URL")
    whmcs_id = env_str("WHMCS_API_IDENTIFIER")
    whmcs_secret = env_str("WHMCS_API_SECRET")
    action_name = str(action or "").strip().lower()
    http_timeout = env_float("HTTP_TIMEOUT", "60")
    connect_timeout = env_float("HTTP_CONNECT_TIMEOUT", "10")
    if action_name == "getticket":
        # Keep GetTicket responsive; outer retry loop handles retries explicitly.
        http_timeout = env_float("WHMCS_GET_TICKET_HTTP_TIMEOUT", str(http_timeout))
        connect_timeout = env_float("WHMCS_GET_TICKET_CONNECT_TIMEOUT", str(connect_timeout))
    elif action_name == "gettickets":
        # Keep polling loops responsive under transient WHMCS slowness.
        http_timeout = env_float("WHMCS_GET_TICKETS_HTTP_TIMEOUT", str(http_timeout))
        connect_timeout = env_float("WHMCS_GET_TICKETS_CONNECT_TIMEOUT", str(connect_timeout))
    elif action_name == "deleteticketreply":
        # Deletes should fail fast and retry quickly in-process.
        http_timeout = env_float("WHMCS_DELETE_REPLY_HTTP_TIMEOUT", str(http_timeout))
        connect_timeout = env_float("WHMCS_DELETE_REPLY_CONNECT_TIMEOUT", str(connect_timeout))
    elif action_name == "deleteticket":
        # Full ticket deletes should also fail fast and retry quickly.
        http_timeout = env_float("WHMCS_DELETE_TICKET_HTTP_TIMEOUT", str(http_timeout))
        connect_timeout = env_float("WHMCS_DELETE_TICKET_CONNECT_TIMEOUT", str(connect_timeout))

    payload = {
        "action": action,
        "identifier": whmcs_id,
        "secret": whmcs_secret,
        "responsetype": "json",
        **params,
    }

    # WHMCS API uses POST for both reads and writes. For write actions we avoid
    # automatic read-retries to prevent duplicate side effects on transient timeouts.
    use_retry_session = action_name.startswith("get")
    if action_name == "getticket" and not env_bool("WHMCS_GET_TICKET_SESSION_RETRY", "0"):
        use_retry_session = False
    sess = SESSION if use_retry_session else SESSION_NO_RETRY
    r = sess.post(
        whmcs_api_url,
        data=payload,
        timeout=(max(float(connect_timeout), 1.0), max(float(http_timeout), 1.0)),
    )

    if r.status_code in (401, 403):
        log("ERROR", f"[WHMCS] {r.status_code} {((r.text or '')[:300])}")
        raise RuntimeError(f"WHMCS HTTP {r.status_code}")

    r.raise_for_status()

    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"WHMCS non-JSON response: {(r.text or '')[:500]}")

    if data.get("result") == "error":
        raise RuntimeError(f"WHMCS error: {data.get('message') or data}")

    return data


def whmcs_add_ticket_reply(ticket_id: str, message: str, attachments: list[dict] | None = None):
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would add WHMCS reply ticket={ticket_id} ({len(message)} chars, attachments={len(attachments or [])})")
        return {"result": "success", "dry_run": True}

    params = {"ticketid": int(ticket_id), "message": message}
    if attachments:
        # WHMCS expects base64(json([{name, data(base64)}...])).
        payload_files = []
        for f in attachments:
            name = str(f.get("name") or "attachment.bin").strip() or "attachment.bin"
            raw = f.get("data") or b""
            if not isinstance(raw, (bytes, bytearray)):
                continue
            payload_files.append({
                "name": name,
                "data": base64.b64encode(bytes(raw)).decode("ascii"),
            })
        if payload_files:
            params["attachments"] = base64.b64encode(json.dumps(payload_files, ensure_ascii=False).encode("utf-8")).decode("ascii")
    adminusername = env_str("WHMCS_ADMIN_USERNAME")
    if adminusername:
        params["adminusername"] = adminusername
    return whmcs_api("AddTicketReply", **params)


def whmcs_update_ticket_reply(reply_id: int, message: str):
    rid = int(reply_id or 0)
    if rid <= 0:
        raise RuntimeError(f"Invalid update ticket reply args reply_id={reply_id!r}")
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would update WHMCS reply replyid={rid} ({len(message)} chars)")
        return {"result": "success", "dry_run": True}

    params = {"replyid": rid, "message": message}
    adminusername = env_str("WHMCS_ADMIN_USERNAME")
    if adminusername:
        params["adminusername"] = adminusername
    return whmcs_api("UpdateTicketReply", **params)


def whmcs_extract_reply_id(data) -> int:
    if not isinstance(data, dict):
        return 0
    for key in ("replyid", "ticketreplyid", "id"):
        raw = data.get(key)
        try:
            val = int(str(raw).strip())
            if val > 0:
                return val
        except Exception:
            continue
    return 0


def whmcs_delete_ticket_reply(ticket_id: str, reply_id: int):
    rid = int(reply_id or 0)
    tid = int(ticket_id or 0)
    if rid <= 0 or tid <= 0:
        raise RuntimeError(f"Invalid delete ticket reply args ticket_id={ticket_id!r} reply_id={reply_id!r}")
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would delete WHMCS reply ticket={tid} replyid={rid}")
        return {"result": "success", "dry_run": True}
    return whmcs_api("DeleteTicketReply", ticketid=tid, replyid=rid)


def whmcs_delete_ticket_reply_retry(ticket_id: str, reply_id: int):
    tid = int(ticket_id or 0)
    rid = int(reply_id or 0)
    attempts = max(env_int("WHMCS_DELETE_REPLY_RETRIES", "3"), 1)
    wait_base = max(env_float("WHMCS_DELETE_REPLY_RETRY_WAIT", "0.6"), 0.0)
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            return whmcs_delete_ticket_reply(tid, rid)
        except Exception as e:
            err_low = repr(e).lower()
            # Idempotent end-state: already removed.
            if "reply id not found" in err_low or "ticket id not found" in err_low:
                raise
            is_retryable = isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            if not is_retryable or attempt >= attempts:
                raise
            last_err = e
            wait_s = min(wait_base * attempt, 3.0)
            log("WARNING", f"DeleteTicketReply retry ticket={tid} reply={rid} attempt={attempt}/{attempts} wait={wait_s}s")
            if wait_s > 0:
                time.sleep(wait_s)

    if last_err:
        raise last_err
    return whmcs_delete_ticket_reply(tid, rid)


def whmcs_delete_ticket(ticket_id: str | int):
    tid = int(ticket_id or 0)
    if tid <= 0:
        raise RuntimeError(f"Invalid delete ticket args ticket_id={ticket_id!r}")
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would delete WHMCS ticket ticket={tid}")
        return {"result": "success", "dry_run": True}
    return whmcs_api("DeleteTicket", ticketid=tid)


def whmcs_delete_ticket_retry(ticket_id: str | int):
    tid = int(ticket_id or 0)
    attempts = max(env_int("WHMCS_DELETE_TICKET_RETRIES", "3"), 1)
    wait_base = max(env_float("WHMCS_DELETE_TICKET_RETRY_WAIT", "0.6"), 0.0)
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            return whmcs_delete_ticket(tid)
        except Exception as e:
            err_low = repr(e).lower()
            # Idempotent end-state: already removed.
            if "ticket id not found" in err_low or "ticket not found" in err_low:
                return {"result": "not-found"}
            is_retryable = isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            if not is_retryable or attempt >= attempts:
                raise
            last_err = e
            wait_s = min(wait_base * attempt, 3.0)
            log("WARNING", f"DeleteTicket retry ticket={tid} attempt={attempt}/{attempts} wait={wait_s}s")
            if wait_s > 0:
                time.sleep(wait_s)

    if last_err:
        raise last_err
    return whmcs_delete_ticket(tid)


def whmcs_update_ticket_status(ticket_id: str, status: str):
    status = str(status or "").strip()
    if not status:
        return {"result": "success", "skipped": True}
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would update WHMCS ticket status ticket={ticket_id} -> {status!r}")
        return {"result": "success", "dry_run": True}
    return whmcs_api("UpdateTicket", ticketid=int(ticket_id), status=status)


def whmcs_get_client_phone(client_id: str) -> str:
    cid = str(client_id or "").strip()
    if not cid:
        return ""
    if cid in CLIENT_PHONE_CACHE:
        return CLIENT_PHONE_CACHE[cid]
    try:
        data = whmcs_api("GetClientsDetails", clientid=int(cid), stats=False)
        phone = str(data.get("phonenumber") or "").strip()
        phone = normalize_phone_with_country_prefix(phone)
        CLIENT_PHONE_CACHE[cid] = phone
        return phone
    except Exception as e:
        log("ERROR", f"Failed to fetch client phone for client_id={cid}: {repr(e)}")
        CLIENT_PHONE_CACHE[cid] = ""
        return ""


def normalize_phone_with_country_prefix(phone: str) -> str:
    raw = str(phone or "").strip()
    if not raw:
        return ""

    # keep leading plus only, remove spaces/separators
    cleaned = raw.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")

    # already international format
    if cleaned.startswith("+"):
        return cleaned
    if cleaned.startswith("00"):
        return f"+{cleaned[2:]}"

    default_prefix = env_str("GOODDAY_PHONE_COUNTRY_PREFIX", "+357").strip()
    if not default_prefix:
        return cleaned
    if not default_prefix.startswith("+"):
        default_prefix = f"+{default_prefix}"

    # local number -> prepend default country code
    return f"{default_prefix}{cleaned}"


# -------------------------
# GoodDay API v2
# -------------------------
def gd_headers():
    return {
        "gd-api-token": env_str("GOODDAY_API_TOKEN"),
        "Content-Type": "application/json",
    }


def gd_web_headers_json():
    cid = env_str("GOODDAY_COMPANY_ID")
    return {
        "Gd-Access-Token": gd_get_web_access_token(),
        "gd-cid": cid,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": env_str("GOODDAY_WEB_ORIGIN", "https://www.goodday.work"),
        "Referer": env_str("GOODDAY_WEB_ORIGIN", "https://www.goodday.work") + "/",
    }


def _jwt_exp_utc(token: str) -> datetime | None:
    t = (token or "").strip()
    if not t:
        return None
    parts = t.split(".")
    if len(parts) < 2:
        return None
    payload_b64 = parts[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
    except Exception:
        return None
    exp = payload.get("exp")
    try:
        return datetime.fromtimestamp(int(exp), timezone.utc)
    except Exception:
        return None


def _web_token_valid(token: str, min_ttl_seconds: int = 60) -> bool:
    t = (token or "").strip()
    if not t:
        return False
    exp_utc = _jwt_exp_utc(t)
    if exp_utc is None:
        # Non-JWT token (or unknown format): assume valid and let request decide.
        return True
    return exp_utc > (datetime.now(timezone.utc) + timedelta(seconds=min_ttl_seconds))


def _gd_login_get_new_access_token() -> str:
    email = env_str("GOODDAY_LOGIN_EMAIL")
    password = env_str("GOODDAY_LOGIN_PASSWORD")
    if not email or not password:
        return ""

    origin = env_str("GOODDAY_WEB_ORIGIN", "https://www.goodday.work").rstrip("/")
    retries = max(env_int("GOODDAY_WEB_LOGIN_RETRIES", "3"), 1)
    timeout_s = max(env_float("GOODDAY_WEB_LOGIN_TIMEOUT", env_str("HTTP_TIMEOUT", "60")), 10.0)
    last_err = None

    for attempt in range(1, retries + 1):
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        try:
            login_page = s.get(f"{origin}/login", timeout=(10, timeout_s))
            login_page.raise_for_status()
            html_txt = login_page.text or ""
            m = re.search(r"name=['\"]csrf_token['\"][^>]*value=['\"]([^'\"]+)['\"]", html_txt, flags=re.IGNORECASE)
            if not m:
                m = re.search(r"value=['\"]([^'\"]+)['\"][^>]*name=['\"]csrf_token['\"]", html_txt, flags=re.IGNORECASE)
            csrf = (m.group(1) if m else "").strip()
            if not csrf:
                raise RuntimeError("GoodDay login failed: csrf_token not found")

            headers = {
                "X-CSRF-Token": csrf,
                "Origin": origin,
                "Referer": f"{origin}/login",
            }
            payload = {
                "email": email,
                "password": password,
                "rememberMe": "true",
            }
            r = s.post(f"{origin}/api/auth/login", data=payload, headers=headers, timeout=(10, timeout_s))
            if r.status_code >= 400:
                raise RuntimeError(f"GoodDay login failed {r.status_code}: {(r.text or '')[:300]}")

            data = r.json() if r.text else {}
            token = str(data.get("accessToken") or "").strip()
            if token:
                return token
            raise RuntimeError(f"GoodDay login returned no accessToken: {data}")
        except Exception as e:
            last_err = e
            if attempt < retries:
                wait_s = min(float(attempt), 3.0)
                log("WARNING", f"GoodDay login attempt {attempt}/{retries} failed: {repr(e)}")
                time.sleep(wait_s)
                continue
            raise

    if last_err:
        raise last_err
    return ""


def gd_get_web_access_token() -> str:
    global WEB_ACCESS_TOKEN_CACHE

    if _web_token_valid(WEB_ACCESS_TOKEN_CACHE):
        return WEB_ACCESS_TOKEN_CACHE

    env_token = env_str("GOODDAY_ACCESS_TOKEN")
    if _web_token_valid(env_token):
        WEB_ACCESS_TOKEN_CACHE = env_token
        return WEB_ACCESS_TOKEN_CACHE

    try:
        fresh = _gd_login_get_new_access_token()
        if fresh:
            WEB_ACCESS_TOKEN_CACHE = fresh
            exp = _jwt_exp_utc(fresh)
            log("INFO", f"Refreshed GoodDay web access token via login (exp={exp.isoformat() if exp else 'unknown'})")
            return WEB_ACCESS_TOKEN_CACHE
    except Exception as e:
        log("WARNING", f"Could not refresh GoodDay web access token via login: {repr(e)}")

    return ""


def gd_has_valid_web_access_token() -> bool:
    return bool(gd_get_web_access_token())


def gd_generate_upload_url(filename: str, mime: str) -> tuple[str, str]:
    endpoint = env_str("GOODDAY_UPLOAD_URL_ENDPOINT", "https://7vrzn6fzld.execute-api.eu-central-1.amazonaws.com/generate-upload-url")
    company_id = env_str("GOODDAY_COMPANY_ID")
    if not company_id:
        raise RuntimeError("GOODDAY_COMPANY_ID is required for web upload flow")

    file_id = str(uuid.uuid4())
    payload = {
        "fileId": file_id,
        "companyId": company_id,
        "storageProvider": 12,
        "fileName": filename,
        "contentType": mime or "application/octet-stream",
    }
    r = SESSION.post(endpoint, data=json.dumps(payload), headers={"Content-Type": "text/plain;charset=UTF-8"}, timeout=(10, env_float("HTTP_TIMEOUT", "60")))
    r.raise_for_status()
    data = r.json()
    upload_url = data.get("uploadUrl")
    if not upload_url:
        raise RuntimeError(f"generate-upload-url failed: {data}")
    return file_id, upload_url


def gd_upload_blob_to_presigned_url(upload_url: str, data: bytes, mime: str):
    parsed = urlparse(upload_url)
    q = parse_qs(parsed.query)
    headers = {"Content-Type": mime or "application/octet-stream"}

    # GoodDay signs these S3 headers in the presigned URL.
    x_acl = (q.get("x-amz-acl") or [""])[0]
    x_company = (q.get("x-amz-meta-companyid") or [""])[0]
    x_filename = (q.get("x-amz-meta-filename") or [""])[0]
    if x_acl:
        headers["x-amz-acl"] = unquote_plus(x_acl)
    if x_company:
        headers["x-amz-meta-companyid"] = unquote_plus(x_company)
    if x_filename:
        headers["x-amz-meta-filename"] = unquote_plus(x_filename)

    r = SESSION.put(upload_url, data=data, headers=headers, timeout=(10, env_float("HTTP_TIMEOUT", "60")))
    r.raise_for_status()


def gd_web_reply(task_id: str, message: str, file_ref: dict | None = None):
    base = env_str("GOODDAY_WEB_ORIGIN", "https://www.goodday.work").rstrip("/")
    company_id = env_str("GOODDAY_COMPANY_ID")
    if not company_id:
        raise RuntimeError("GOODDAY_COMPANY_ID is required for web reply flow")

    payload = {
        "cid": company_id,
        "companyId": company_id,
        "message": message,
        "actionRequireUser": None,
        "emailNotifications": [],
        "zz": env_str("GOODDAY_TZ_OFFSET", "+0200"),
        "timeReport": {
            "isBillable": True,
            "date": now_iso()[:10],
            "momentCreated": None,
            "task": None,
        },
        "messageRTF": {
            "v": "lexical-1",
            "content": {
                "root": {
                    "type": "root",
                    "version": 1,
                    "format": "",
                    "indent": 0,
                    "direction": "ltr",
                    "children": [
                        {
                            "type": "custom-paragraph",
                            "version": 1,
                            "format": "",
                            "indent": 0,
                            "direction": "ltr",
                            "children": [
                                {
                                    "type": "text",
                                    "version": 1,
                                    "text": message,
                                    "format": 0,
                                    "style": "",
                                    "detail": 0,
                                    "mode": "normal",
                                }
                            ],
                        }
                    ],
                }
            },
        },
    }
    sid = env_str("GOODDAY_SID")
    if sid:
        payload["sid"] = sid
    if file_ref:
        att = {
            "fileId": file_ref["fileId"],
            "name": file_ref.get("name") or "",
            "storageProvider": int(file_ref.get("storageProvider", 12) or 12),
            "fileType": int(file_ref.get("fileType", 0) or 0),
            "isFlagged": bool(file_ref.get("isFlagged", False)),
            "preview": int(file_ref.get("preview", 2) or 2),
            "size": int(file_ref.get("size", 0) or 0),
        }
        if file_ref.get("mime"):
            att["mime"] = file_ref["mime"]
        payload["attachments"] = [att]
        payload["files"] = [dict(att)]

    url = f"{base}/api/app/task/{task_id}/reply"
    r = SESSION.post(url, headers=gd_web_headers_json(), json=payload, timeout=(10, env_float("HTTP_TIMEOUT", "60")))
    if r.status_code in (401, 403):
        raise RuntimeError("GoodDay web reply auth failed (check GOODDAY_ACCESS_TOKEN)")
    if r.status_code >= 400:
        raise RuntimeError(f"GoodDay web reply failed {r.status_code}: {(r.text or '')[:500]}")
    try:
        data = r.json() if (r.text or "").strip() else {}
    except Exception:
        data = {}
    return _gd_extract_message_id_from_web_response(data)


def _gd_extract_message_id_from_web_response(data) -> str:
    if isinstance(data, dict):
        direct = str(data.get("taskMessageId") or data.get("messageId") or "").strip()
        if direct:
            return direct

    queue = [data]
    while queue:
        item = queue.pop(0)
        if isinstance(item, dict):
            tm = item.get("taskMessage")
            if isinstance(tm, dict):
                mid = str(tm.get("id") or tm.get("taskMessageId") or "").strip()
                if mid:
                    return mid

            direct = str(item.get("taskMessageId") or item.get("messageId") or "").strip()
            if direct:
                return direct

            obj_type = str(item.get("object") or "").strip().lower()
            obj_id = str(item.get("id") or "").strip()
            if obj_id and obj_type in ("task-message", "taskmessage", "task_message"):
                return obj_id

            if obj_id and "message" in item and ("taskId" in item or "fromUserId" in item):
                return obj_id

            for val in item.values():
                if isinstance(val, (dict, list)):
                    queue.append(val)
        elif isinstance(item, list):
            for sub in item:
                if isinstance(sub, (dict, list)):
                    queue.append(sub)
    return ""


def _gd_lexical_rtf_from_text(message: str) -> dict:
    return {
        "v": "lexical-1",
        "content": {
            "root": {
                "type": "root",
                "version": 1,
                "format": "",
                "indent": 0,
                "direction": "ltr",
                "children": [
                    {
                        "type": "custom-paragraph",
                        "version": 1,
                        "format": "",
                        "indent": 0,
                        "direction": "ltr",
                        "children": [
                            {
                                "type": "text",
                                "version": 1,
                                "text": str(message or ""),
                                "format": 0,
                                "style": "",
                                "detail": 0,
                                "mode": "normal",
                            }
                        ],
                    }
                ],
            }
        },
    }


def gd_update_task_message(task_id: str, message_id: str, message: str, message_rtf: dict | None = None):
    if not task_id or not message_id:
        raise RuntimeError("gd_update_task_message requires task_id and message_id")
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would update GoodDay message task={task_id} message_id={message_id} ({len(message)} chars)")
        return {"result": "success", "dry_run": True}

    base = env_str("GOODDAY_WEB_ORIGIN", "https://www.goodday.work").rstrip("/")
    company_id = env_str("GOODDAY_COMPANY_ID")
    if not company_id:
        raise RuntimeError("GOODDAY_COMPANY_ID is required for web edit flow")

    payload = {
        "cid": company_id,
        "companyId": company_id,
        "message": message,
        "messageRTF": message_rtf if isinstance(message_rtf, dict) else _gd_lexical_rtf_from_text(message),
        "zz": env_str("GOODDAY_TZ_OFFSET", "+0200"),
    }
    sid = env_str("GOODDAY_SID")
    if sid:
        payload["sid"] = sid

    retries = max(env_int("GOODDAY_WEB_EDIT_RETRIES", "2"), 1)
    timeout_s = max(env_float("GOODDAY_WEB_EDIT_TIMEOUT", env_str("HTTP_TIMEOUT", "60")), 10.0)
    url = f"{base}/api/app/task/{task_id}/message/{message_id}"

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = SESSION.put(url, headers=gd_web_headers_json(), json=payload, timeout=(10, timeout_s))
            if r.status_code in (401, 403):
                raise RuntimeError("GoodDay web edit auth failed (check GOODDAY_ACCESS_TOKEN)")
            if r.status_code >= 400:
                raise RuntimeError(f"GoodDay web edit failed {r.status_code}: {(r.text or '')[:500]}")
            try:
                data = r.json() if (r.text or "").strip() else {}
            except Exception:
                data = {}
            return data
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < retries:
                wait_s = min(float(attempt), 3.0)
                log("WARNING", f"GoodDay web edit timeout task={task_id} message_id={message_id} attempt={attempt}/{retries} wait={wait_s}s")
                time.sleep(wait_s)
                continue
            raise

    if last_err:
        raise last_err
    return {}


def gd_delete_task_message(task_id: str, message_id: str):
    if not task_id or not message_id:
        raise RuntimeError("gd_delete_task_message requires task_id and message_id")
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would delete GoodDay message task={task_id} message_id={message_id}")
        return {"result": "success", "dry_run": True}

    base = env_str("GOODDAY_WEB_ORIGIN", "https://www.goodday.work").rstrip("/")
    company_id = env_str("GOODDAY_COMPANY_ID")
    if not company_id:
        raise RuntimeError("GOODDAY_COMPANY_ID is required for web delete flow")
    retries = max(env_int("GOODDAY_DELETE_RETRIES", "2"), 1)
    timeout_s = max(env_float("GOODDAY_DELETE_TIMEOUT", env_str("HTTP_TIMEOUT", "60")), 10.0)
    url = f"{base}/api/app/task/{task_id}/message/{message_id}"
    payload = {
        "cid": company_id,
        "companyId": company_id,
        "zz": env_str("GOODDAY_TZ_OFFSET", "+0200"),
    }
    sid = env_str("GOODDAY_SID")
    if sid:
        payload["sid"] = sid

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = SESSION.delete(url, headers=gd_web_headers_json(), json=payload, timeout=(10, timeout_s))
            if r.status_code in (401, 403):
                raise RuntimeError("GoodDay web delete auth failed (check GOODDAY_ACCESS_TOKEN)")
            if r.status_code == 404:
                return {"result": "not-found"}
            if r.status_code >= 400:
                raise RuntimeError(f"GoodDay web delete failed {r.status_code}: {(r.text or '')[:500]}")
            try:
                return r.json() if (r.text or "").strip() else {"result": "success"}
            except Exception:
                return {"result": "success"}
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < retries:
                wait_s = min(float(attempt), 3.0)
                log("WARNING", f"GoodDay delete timeout task={task_id} message_id={message_id} attempt={attempt}/{retries} wait={wait_s}s")
                time.sleep(wait_s)
                continue
            raise

    if last_err:
        raise last_err
    return {"result": "unknown"}


def gd_create_task(title: str, message: str, project_id: str) -> str:
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would create GoodDay task title={title!r}")
        return "DRY_RUN_TASK_ID"

    base = env_str("GOODDAY_API_BASE", "https://api.goodday.work/2.0").rstrip("/")
    http_timeout = env_float("HTTP_TIMEOUT", "60")

    payload = {
        "title": title,
        "message": message,
        "projectId": project_id,
        "taskTypeId": "hB9A7F",
        "fromUserId": env_str("GOODDAY_FROM_USER_ID"),
    }
    to_user = env_str("GOODDAY_TO_USER_ID")
    if to_user:
        payload["toUserId"] = to_user

    url = f"{base}/tasks"
    r = SESSION.post(url, headers=gd_headers(), json=payload, timeout=(10, http_timeout))
    r.raise_for_status()
    data = r.json()
    tid = data.get("id")

    if not tid:
        raise RuntimeError(f"GoodDay create task failed: {data}")
    return tid


def gd_update_task_custom_fields(ticket: dict, task_id: str):
    if not task_id:
        raise RuntimeError("GoodDay custom fields update: task_id is empty")

    tid = ticket.get("tid") or ticket.get("ticketid") or ticket.get("id")
    subject = ticket.get("subject", "")
    dept = ticket.get("deptname", "")
    requestor = ticket.get("requestor_name") or ""
    requestor_email = ticket.get("requestor_email") or ticket.get("email") or ""
    created = ticket.get("date", "")
    status = ticket.get("status", "")
    user_id = ticket.get("userid")
    requestor_phone = whmcs_get_client_phone(user_id) if user_id is not None else ""

    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would update GoodDay custom fields task_id={task_id} tid={tid}")
        return

    base = env_str("GOODDAY_API_BASE", "https://api.goodday.work/2.0").rstrip("/")
    http_timeout = env_float("HTTP_TIMEOUT", "60")

    payload = {
        "customFields": [
            {"id": "uojOkJ", "value": str(tid) if tid is not None else ""},
            {"id": "SQiqbD", "value": str(created) if created is not None else ""},
            {"id": "ZvSBHp", "value": str(subject) if subject is not None else ""},
            {"id": "mkKgtU", "value": str(dept) if dept is not None else ""},
            {"id": "VpSUEj", "value": str(requestor) if requestor is not None else ""},
            {"id": "TCJX4l", "value": str(requestor_email) if requestor_email is not None else ""},
        ]
    }
    status_cf_id = env_str("GOODDAY_CF_WHMCS_STATUS_ID")
    if status_cf_id:
        payload["customFields"].append({"id": status_cf_id, "value": str(status) if status is not None else ""})
    phone_cf_id = env_str("GOODDAY_CF_REQUESTOR_PHONE_ID")
    if phone_cf_id:
        payload["customFields"].append({"id": phone_cf_id, "value": str(requestor_phone) if requestor_phone is not None else ""})

    url = f"{base}/task/{task_id}/custom-fields"
    r = SESSION.put(url, headers=gd_headers(), json=payload, timeout=(10, http_timeout))
    r.raise_for_status()

    # GoodDay μπορεί να απαντήσει είτε "OK" string είτε JSON
    try:
        data = r.json()
    except Exception:
        data = (r.text or "").strip()

    ok = False
    if isinstance(data, str) and data.upper() == "OK":
        ok = True
    elif isinstance(data, dict) and str(data.get("result", "")).upper() == "OK":
        ok = True

    if not ok:
        raise RuntimeError(f"GoodDay custom fields update failed for task {task_id}: {data}")


def sync_ticket_status_to_goodday(ticket: dict, entry: dict):
    """
    Sync WHMCS ticket status to GoodDay custom field (if configured).
    Avoids repeated writes when status has not changed.
    """
    if not env_bool("SYNC_STATUS_TO_GOODDAY", "1"):
        return
    if not env_str("GOODDAY_CF_WHMCS_STATUS_ID"):
        return

    task_id = str(entry.get("task_id") or "").strip()
    if not task_id or task_id == "DRY_RUN_TASK_ID":
        return

    status_now = str(ticket.get("status") or "").strip()
    status_last = str(entry.get("last_status_synced") or "").strip()
    if status_now == status_last:
        return

    gd_update_task_custom_fields(ticket, task_id)
    entry["last_status_synced"] = status_now
    tid = str(ticket.get("ticketid") or ticket.get("id") or "").strip()
    log("INFO", f"Synced ticket status to GoodDay for ticket {tid}: {status_now!r}")


def gd_add_comment(task_id: str, message: str, attachment_files: list[dict] | None = None):
    if env_bool("DRY_RUN", "0"):
        log("INFO", f"[DRY_RUN] Would add comment to task_id={task_id} ({len(message)} chars, files={len(attachment_files or [])})")
        return "DRY_RUN_MESSAGE_ID"

    base = env_str("GOODDAY_API_BASE", "https://api.goodday.work/2.0").rstrip("/")
    http_timeout = env_float("HTTP_TIMEOUT", "60")

    # Prefer web flow for true file attachments when web token is valid.
    if attachment_files and gd_has_valid_web_access_token():
        try:
            log("DEBUG", f"GoodDay web attachment flow: task={task_id} files={len(attachment_files)}")
            uploaded_refs = []
            for f in attachment_files:
                fname = f.get("filename") or "attachment.bin"
                data = f.get("data") or b""
                mime = f.get("mime") or "application/octet-stream"
                fid, upload_url = gd_generate_upload_url(fname, mime)
                gd_upload_blob_to_presigned_url(upload_url, data, mime)
                uploaded_refs.append({
                    "fileId": fid,
                    "name": fname,
                    "storageProvider": 12,
                    "fileType": 0,
                    "isFlagged": False,
                    "preview": 2,
                    "size": len(data),
                    "mime": mime,
                })

            first_mid = ""
            for idx, ref in enumerate(uploaded_refs):
                msg = message if idx == 0 else f"(attachment {idx + 1}/{len(uploaded_refs)})"
                mid = gd_web_reply(task_id, msg, file_ref=ref)
                if idx == 0 and mid:
                    first_mid = str(mid).strip()
            log("DEBUG", f"GoodDay web attachment flow done: task={task_id} files={len(uploaded_refs)}")
            return first_mid
        except Exception as e:
            log("WARNING", f"Web attachment flow failed, fallback to message-only comment: {repr(e)}")
    elif attachment_files:
        log("WARNING", f"Skipping GoodDay file upload for task={task_id}: missing/expired GOODDAY_ACCESS_TOKEN. Posting message with attachment links only.")

    url = f"{base}/task/{task_id}/comment"
    payload = {"userId": env_str("GOODDAY_FROM_USER_ID"), "message": message}
    r = SESSION.post(url, headers=gd_headers(), json=payload, timeout=(10, http_timeout))
    r.raise_for_status()
    try:
        data = r.json() if (r.text or "").strip() else {}
    except Exception:
        data = {}
    return _gd_extract_message_id_from_web_response(data)


def gd_get_task_messages(task_id: str) -> list[dict]:
    base = env_str("GOODDAY_API_BASE", "https://api.goodday.work/2.0").rstrip("/")
    http_timeout = env_float("HTTP_TIMEOUT", "60")
    read_timeout = max(env_float("GOODDAY_MESSAGES_HTTP_TIMEOUT", str(http_timeout)), 5.0)
    connect_timeout = max(env_float("GOODDAY_MESSAGES_CONNECT_TIMEOUT", "15"), 5.0)
    retries = max(env_int("GOODDAY_MESSAGES_RETRIES", "3"), 1)
    use_session_retry = env_bool("GOODDAY_MESSAGES_SESSION_RETRY", "0")
    sess = SESSION if use_session_retry else SESSION_NO_RETRY
    url = f"{base}/task/{task_id}/messages"
    last_timeout = None

    for attempt in range(1, retries + 1):
        try:
            r = sess.get(url, headers=gd_headers(), timeout=(connect_timeout, read_timeout))
            if r.status_code in (404, 410):
                raise GoodDayTaskNotFoundError(f"GoodDay task not found task={task_id} http={r.status_code}")
            if r.status_code >= 400:
                body_low = (r.text or "").lower()
                if "task" in body_low and "not found" in body_low:
                    raise GoodDayTaskNotFoundError(f"GoodDay task not found task={task_id} body={(r.text or '')[:200]}")
            r.raise_for_status()
            data = r.json()

            raw_msgs = None
            if isinstance(data, dict):
                raw_msgs = data.get("messages")
            elif isinstance(data, list):
                raw_msgs = data

            if raw_msgs is None:
                return []
            if isinstance(raw_msgs, dict):
                raw_msgs = [raw_msgs]
            if not isinstance(raw_msgs, list):
                return []

            out = []
            for item in raw_msgs:
                if isinstance(item, dict):
                    out.append(item)
                elif isinstance(item, list):
                    for sub in item:
                        if isinstance(sub, dict):
                            out.append(sub)
            return out
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_timeout = e
            if attempt < retries:
                wait_s = min(float(attempt), 3.0)
                log("WARNING", f"GoodDay messages timeout task={task_id} attempt={attempt}/{retries} wait={wait_s}s")
                time.sleep(wait_s)
                continue
            raise

    if last_timeout:
        raise last_timeout
    return []


def _gd_task_is_deleted_marker(task_obj) -> bool:
    if task_obj is None:
        return False
    if not isinstance(task_obj, dict):
        return False
    for key in ("isDeleted", "deleted", "isTrashed", "trashed"):
        val = task_obj.get(key)
        if isinstance(val, bool) and val:
            return True
        if str(val).strip().lower() in ("1", "true", "yes", "y", "deleted", "trashed", "removed"):
            return True
    for key in ("deletedAt", "removedAt", "trashedAt"):
        if str(task_obj.get(key) or "").strip():
            return True
    status_raw = str(task_obj.get("status") or "").strip().lower()
    if status_raw in ("deleted", "trashed", "removed"):
        return True
    status_obj = task_obj.get("status")
    if isinstance(status_obj, dict):
        status_name = str(status_obj.get("name") or status_obj.get("title") or "").strip().lower()
        if status_name in ("deleted", "trashed", "removed", "archived"):
            return True
    return False


def gd_get_task_meta(task_id: str) -> dict:
    base = env_str("GOODDAY_API_BASE", "https://api.goodday.work/2.0").rstrip("/")
    http_timeout = max(env_float("GOODDAY_TASK_HTTP_TIMEOUT", env_str("HTTP_TIMEOUT", "60")), 5.0)
    connect_timeout = max(env_float("GOODDAY_TASK_CONNECT_TIMEOUT", "8"), 3.0)
    retries = max(env_int("GOODDAY_TASK_RETRIES", "2"), 1)
    url = f"{base}/task/{task_id}"
    last_timeout = None

    for attempt in range(1, retries + 1):
        try:
            r = SESSION_NO_RETRY.get(url, headers=gd_headers(), timeout=(connect_timeout, http_timeout))
            if r.status_code in (404, 410):
                return {"state": "missing", "reason": f"http:{r.status_code}"}
            if r.status_code in (401, 403):
                body_low = (r.text or "").lower()
                if env_bool("GOODDAY_TASK_FORBIDDEN_AS_MISSING", "1") and ("permission" in body_low or "not found" in body_low):
                    return {"state": "missing", "reason": f"http:{r.status_code}"}
            r.raise_for_status()
            raw = (r.text or "").strip()
            if not raw or raw.lower() == "null":
                return {"state": "unknown", "reason": "null"}
            try:
                data = json.loads(raw)
            except Exception:
                return {"state": "unknown", "reason": "non-json"}
            if _gd_task_is_deleted_marker(data):
                return {"state": "missing", "reason": "deleted-marker"}
            return {"state": "present", "data": data}
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_timeout = e
            if attempt < retries:
                wait_s = min(float(attempt), 3.0)
                log("WARNING", f"GoodDay task meta timeout task={task_id} attempt={attempt}/{retries} wait={wait_s}s")
                time.sleep(wait_s)
                continue
            raise

    if last_timeout:
        raise last_timeout
    return {"state": "unknown", "reason": "empty"}


def parse_department_project_map() -> dict[str, str]:
    raw = env_str("GOODDAY_PROJECT_BY_DEPARTMENT_JSON")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as e:
        log("ERROR", f"Invalid GOODDAY_PROJECT_BY_DEPARTMENT_JSON: {repr(e)}")
        return {}
    if not isinstance(data, dict):
        return {}
    out = {}
    for dept, proj in data.items():
        d = normalize_key(str(dept or ""))
        p = str(proj or "").strip()
        if d and p:
            out[d] = p
    return out


def resolve_project_id_for_ticket(ticket: dict, dept_project_map: dict[str, str]) -> str:
    dept = normalize_key(ticket.get("deptname") or "")
    if dept and dept in dept_project_map:
        return dept_project_map[dept]
    return env_str("GOODDAY_PROJECT_ID")


def gd_get_project_tasks(project_id: str) -> list[dict]:
    base = env_str("GOODDAY_API_BASE", "https://api.goodday.work/2.0").rstrip("/")
    http_timeout = max(
        env_float(
            "GOODDAY_PROJECT_TASKS_HTTP_TIMEOUT",
            env_str("GOODDAY_TASK_HTTP_TIMEOUT", env_str("HTTP_TIMEOUT", "60")),
        ),
        5.0,
    )
    connect_timeout = max(
        env_float("GOODDAY_PROJECT_TASKS_CONNECT_TIMEOUT", env_str("GOODDAY_TASK_CONNECT_TIMEOUT", "8")),
        3.0,
    )
    retries = max(env_int("GOODDAY_PROJECT_TASKS_RETRIES", "2"), 1)
    use_session_retry = env_bool("GOODDAY_PROJECT_TASKS_SESSION_RETRY", "0")
    sess = SESSION if use_session_retry else SESSION_NO_RETRY
    if not project_id:
        return []
    url = f"{base}/project/{project_id}/tasks"
    last_timeout = None

    for attempt in range(1, retries + 1):
        try:
            r = sess.get(url, headers=gd_headers(), timeout=(connect_timeout, http_timeout))
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
            if isinstance(data, dict):
                tasks = data.get("tasks") or data.get("items") or []
                if isinstance(tasks, list):
                    return [x for x in tasks if isinstance(x, dict)]
            return []
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_timeout = e
            if attempt < retries:
                wait_s = min(float(attempt), 3.0)
                log("WARNING", f"GoodDay project tasks timeout project={project_id} attempt={attempt}/{retries} wait={wait_s}s")
                time.sleep(wait_s)
                continue
            raise

    if last_timeout:
        raise last_timeout
    return []


def gd_project_task_present(project_id: str, task_id: str):
    pid = str(project_id or "").strip()
    tid = str(task_id or "").strip()
    if not pid or not tid:
        return None

    cache_ttl = max(env_int("GOODDAY_PROJECT_TASKS_CACHE_SECONDS", "20"), 0)
    now_ts = time.time()

    with PROJECT_TASKS_CACHE_LOCK:
        rec = PROJECT_TASK_IDS_CACHE.get(pid)
        if cache_ttl > 0 and isinstance(rec, dict):
            try:
                ts = float(rec.get("ts") or 0.0)
            except Exception:
                ts = 0.0
            ids = rec.get("ids")
            if ts > 0 and (now_ts - ts) < cache_ttl and isinstance(ids, set):
                return tid in ids

    try:
        tasks = gd_get_project_tasks(pid)
    except Exception as e:
        log("WARNING", f"Could not load GoodDay project tasks for delete fallback project={pid}: {repr(e)}")
        return None

    ids = set()
    for t in tasks or []:
        if not isinstance(t, dict):
            continue
        t_id = str(t.get("id") or "").strip()
        if t_id:
            ids.add(t_id)

    with PROJECT_TASKS_CACHE_LOCK:
        PROJECT_TASK_IDS_CACHE[pid] = {"ts": time.time(), "ids": ids}
        # Keep cache bounded.
        if len(PROJECT_TASK_IDS_CACHE) > 64:
            for k in sorted(PROJECT_TASK_IDS_CACHE.keys())[:-64]:
                PROJECT_TASK_IDS_CACHE.pop(k, None)

    return tid in ids


def collect_status_sync_project_ids(dept_project_map: dict[str, str]) -> list[str]:
    ids = []
    default_id = env_str("GOODDAY_PROJECT_ID")
    if default_id:
        ids.append(default_id)
    for v in dept_project_map.values():
        vv = str(v or "").strip()
        if vv:
            ids.append(vv)
    seen = set()
    out = []
    for pid in ids:
        if pid in seen:
            continue
        seen.add(pid)
        out.append(pid)
    return out


def parse_goodday_whmcs_status_map() -> dict[str, str]:
    raw = env_str("GOODDAY_TO_WHMCS_STATUS_MAP_JSON")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as e:
        log("ERROR", f"Invalid GOODDAY_TO_WHMCS_STATUS_MAP_JSON: {repr(e)}")
        return {}
    if not isinstance(data, dict):
        return {}
    out = {}
    for k, v in data.items():
        ks = str(k or "").strip().lower()
        vs = str(v or "").strip()
        if ks and vs:
            out[ks] = vs
    return out


def _gd_task_status_candidates(task: dict) -> tuple[str, str]:
    status_name = ""
    status_obj = task.get("status")
    if isinstance(status_obj, dict):
        status_name = str(status_obj.get("name") or status_obj.get("title") or "").strip()
    if not status_name:
        status_name = str(task.get("statusName") or task.get("statusTitle") or "").strip()
    system_status = str(task.get("systemStatus") or "").strip()
    return status_name, system_status


def sync_goodday_status_to_whmcs(ticket_id: str, entry: dict, task: dict, status_map: dict[str, str]):
    if not env_bool("SYNC_STATUS_FROM_GOODDAY", "1"):
        return
    if not isinstance(task, dict) or not status_map:
        return

    gd_status_name, gd_system_status = _gd_task_status_candidates(task)
    gd_status_key = gd_status_name.lower().strip()

    # Strict mode by default: sync only explicitly mapped status names.
    # Optional fallback to systemStatus (numeric) can be enabled by env.
    target = ""
    if gd_status_key and gd_status_key in status_map:
        target = status_map[gd_status_key]
    elif env_bool("GOODDAY_STATUS_USE_SYSTEM_FALLBACK", "0") and gd_system_status and gd_system_status.lower() in status_map:
        target = status_map[gd_system_status.lower()]
    if not target:
        return

    source_key = f"{gd_status_key}|{gd_system_status}".strip("|")
    last_source = str(entry.get("last_gd_status_synced") or "").strip()
    last_target = str(entry.get("last_whmcs_status_set_from_gd") or "").strip()
    if source_key == last_source and target == last_target:
        return

    whmcs_update_ticket_status(ticket_id, target)
    entry["last_gd_status_synced"] = source_key
    entry["last_whmcs_status_set_from_gd"] = target
    log("INFO", f"Synced GoodDay status -> WHMCS ticket {ticket_id}: gd=({gd_status_name or '-'}|{gd_system_status or '-'}) => {target!r}")


# -------------------------
# State
# -------------------------
def _normalize_numeric_ticket_id(raw) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    digits = "".join(ch for ch in txt if ch.isdigit())
    if not digits:
        return ""
    return digits.lstrip("0") or "0"


def _delete_guard_seconds() -> int:
    return max(env_int("WHMCS_DELETE_RECREATE_GUARD_SECONDS", "900"), 0)


def _state_delete_tombstones(state: dict) -> dict:
    tomb = state.get("deleted_ticket_tombstones")
    if not isinstance(tomb, dict):
        tomb = {}
        state["deleted_ticket_tombstones"] = tomb
    return tomb


def _cleanup_delete_tombstones(state: dict) -> int:
    ttl = _delete_guard_seconds()
    tomb = _state_delete_tombstones(state)
    if not tomb:
        return 0
    if ttl <= 0:
        removed = len(tomb)
        tomb.clear()
        return removed

    now_ts = time.time()
    removed = 0
    for key, rec in list(tomb.items()):
        if not isinstance(rec, dict):
            tomb.pop(key, None)
            removed += 1
            continue
        try:
            ts = float(rec.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        if ts <= 0.0 or (now_ts - ts) >= ttl:
            tomb.pop(key, None)
            removed += 1
    return removed


def _mark_deleted_ticket_tombstone(state: dict, ticket_id: str, whmcs_tid: str, task_id: str, source: str):
    ttl = _delete_guard_seconds()
    if ttl <= 0:
        return
    tomb = _state_delete_tombstones(state)
    now_ts = time.time()
    ticket_norm = _normalize_numeric_ticket_id(ticket_id)
    whmcs_tid_norm = _normalize_numeric_ticket_id(whmcs_tid)
    rec = {
        "ts": now_ts,
        "source": str(source or "").strip() or "unknown",
        "ticket_id": str(ticket_id or "").strip(),
        "whmcs_tid": str(whmcs_tid or "").strip(),
        "task_id": str(task_id or "").strip(),
    }
    if ticket_norm:
        tomb[f"id:{ticket_norm}"] = rec
    if whmcs_tid_norm:
        tomb[f"tid:{whmcs_tid_norm}"] = rec


def _delete_guard_lookup(state: dict, ticket_id: str, whmcs_tid: str) -> tuple[int, str]:
    ttl = _delete_guard_seconds()
    if ttl <= 0:
        return 0, ""
    tomb = _state_delete_tombstones(state)
    if not tomb:
        return 0, ""

    keys = []
    ticket_norm = _normalize_numeric_ticket_id(ticket_id)
    whmcs_tid_norm = _normalize_numeric_ticket_id(whmcs_tid)
    if ticket_norm:
        keys.append(f"id:{ticket_norm}")
    if whmcs_tid_norm:
        keys.append(f"tid:{whmcs_tid_norm}")

    now_ts = time.time()
    for key in keys:
        rec = tomb.get(key)
        if not isinstance(rec, dict):
            continue
        try:
            ts = float(rec.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        if ts <= 0.0:
            continue
        age = now_ts - ts
        if age < 0.0:
            age = 0.0
        if age >= ttl:
            continue
        left = int(max(ttl - age, 0))
        source = str(rec.get("source") or "").strip() or "unknown"
        return left, source
    return 0, ""


def load_state():
    state_file = env_str("STATE_FILE", str(BASE_DIR / "state.json"))
    p = Path(state_file)
    if not p.exists():
        return {"tickets": {}, "deleted_ticket_tombstones": {}, "_backfill_done": False}
    try:
        data = json.loads(p.read_text("utf-8"))
        if not isinstance(data, dict):
            return {"tickets": {}, "deleted_ticket_tombstones": {}, "_backfill_done": False}
        if "tickets" not in data or not isinstance(data["tickets"], dict):
            data["tickets"] = {}
        for _tid, entry in list(data["tickets"].items()):
            if not isinstance(entry, dict):
                data["tickets"][_tid] = {
                    "created": False,
                    "task_id": "",
                    "whmcs_tid": "",
                    "deleted_in_whmcs": False,
                    "last_reply_id": 0,
                    "baseline_done": False,
                    "gd_seen_message_ids": [],
                    "gd_task_missing_hits": 0,
                    "reply_signatures": {},
                    "whmcs_reply_to_gd_message_ids": {},
                    "gd_message_to_whmcs_reply_ids": {},
                    "gd_public_to_whmcs_reply_ids": {},
                    "gd_pending_public_reply_signatures": {},
                    "gd_hidden_edit_notes": {},
                    "gd_public_message_signatures": {},
                    "gd_mirror_message_signatures": {},
                    "gd_to_whmcs_backoff_until": 0.0,
                    "last_status_synced": "",
                    "last_gd_status_synced": "",
                    "last_whmcs_status_set_from_gd": "",
                }
                continue
            if "whmcs_tid" not in entry:
                entry["whmcs_tid"] = ""
            if "deleted_in_whmcs" not in entry:
                entry["deleted_in_whmcs"] = False
            if "baseline_done" not in entry:
                entry["baseline_done"] = False
            if "gd_seen_message_ids" not in entry or not isinstance(entry.get("gd_seen_message_ids"), list):
                entry["gd_seen_message_ids"] = []
            if "gd_task_missing_hits" not in entry:
                entry["gd_task_missing_hits"] = 0
            if "reply_signatures" not in entry or not isinstance(entry.get("reply_signatures"), dict):
                entry["reply_signatures"] = {}
            if "whmcs_reply_to_gd_message_ids" not in entry or not isinstance(entry.get("whmcs_reply_to_gd_message_ids"), dict):
                entry["whmcs_reply_to_gd_message_ids"] = {}
            if "gd_message_to_whmcs_reply_ids" not in entry or not isinstance(entry.get("gd_message_to_whmcs_reply_ids"), dict):
                entry["gd_message_to_whmcs_reply_ids"] = {}
            if "gd_public_to_whmcs_reply_ids" not in entry or not isinstance(entry.get("gd_public_to_whmcs_reply_ids"), dict):
                entry["gd_public_to_whmcs_reply_ids"] = {}
            if "gd_pending_public_reply_signatures" not in entry or not isinstance(entry.get("gd_pending_public_reply_signatures"), dict):
                entry["gd_pending_public_reply_signatures"] = {}
            if "gd_hidden_edit_notes" not in entry or not isinstance(entry.get("gd_hidden_edit_notes"), dict):
                entry["gd_hidden_edit_notes"] = {}
            if "gd_public_message_signatures" not in entry or not isinstance(entry.get("gd_public_message_signatures"), dict):
                entry["gd_public_message_signatures"] = {}
            if "gd_mirror_message_signatures" not in entry or not isinstance(entry.get("gd_mirror_message_signatures"), dict):
                entry["gd_mirror_message_signatures"] = {}
            if "gd_to_whmcs_backoff_until" not in entry:
                entry["gd_to_whmcs_backoff_until"] = 0.0
            if "last_status_synced" not in entry:
                entry["last_status_synced"] = ""
            if "last_gd_status_synced" not in entry:
                entry["last_gd_status_synced"] = ""
            if "last_whmcs_status_set_from_gd" not in entry:
                entry["last_whmcs_status_set_from_gd"] = ""
        if "deleted_ticket_tombstones" not in data or not isinstance(data.get("deleted_ticket_tombstones"), dict):
            data["deleted_ticket_tombstones"] = {}
        _cleanup_delete_tombstones(data)
        if "_backfill_done" not in data:
            data["_backfill_done"] = False
        return data
    except Exception:
        return {"tickets": {}, "deleted_ticket_tombstones": {}, "_backfill_done": False}


def save_state(state):
    state_file = env_str("STATE_FILE", str(BASE_DIR / "state.json"))
    Path(state_file).write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")


# -------------------------
# Ticket fetching
# -------------------------
def parse_statuses(raw: str) -> list[str]:
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


def fetch_tickets_for_status(status: str | None, page_size: int) -> list[dict]:
    all_tickets = []
    start = 0
    while True:
        params = {"limitstart": start, "limitnum": page_size}
        if status:
            params["status"] = status

        data = whmcs_api("GetTickets", **params)

        total = int(data.get("totalresults", 0) or 0)
        returned = int(data.get("numreturned", 0) or 0)

        tickets = (data.get("tickets") or {}).get("ticket") or []
        if isinstance(tickets, dict):
            tickets = [tickets]

        all_tickets.extend(tickets)

        if start == 0:
            log("INFO", f"Polling tickets... status={status!r} totalresults={total}")
            log("DEBUG", "Polling first page for this status")

        if returned <= 0:
            break

        start += returned
        if start >= total:
            break

    return all_tickets


def fetch_tickets_multi_status_incremental() -> list[dict]:
    raw = env_str("WHMCS_STATUSES") or env_str("WHMCS_STATUS")
    page_size = env_int("WHMCS_PAGE_SIZE", "50")

    if not raw:
        return fetch_tickets_for_status(None, page_size)

    statuses = parse_statuses(raw)
    merged = {}
    for st in statuses:
        for t in fetch_tickets_for_status(st, page_size):
            key = str(t.get("ticketid") or t.get("id"))
            merged[key] = t
    return list(merged.values())


def get_ticket_with_replies(ticketid: str):
    data = whmcs_api("GetTicket", ticketid=int(ticketid), repliessort="ASC")
    replies = (data.get("replies") or {}).get("reply") or []
    if isinstance(replies, dict):
        replies = [replies]
    return data, replies


def get_ticket_with_replies_retry(ticketid: str):
    attempts = max(env_int("WHMCS_GET_TICKET_RETRIES", "3"), 1)
    wait_base = max(env_float("WHMCS_GET_TICKET_RETRY_WAIT", "1.0"), 0.0)
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            return get_ticket_with_replies(ticketid)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < attempts:
                wait_s = min(wait_base * attempt, 5.0)
                log("WARNING", f"GetTicket retry ticket={ticketid} attempt={attempt}/{attempts} wait={wait_s}s")
                if wait_s > 0:
                    time.sleep(wait_s)
                continue
            raise
        except Exception:
            raise

    if last_err:
        raise last_err
    return get_ticket_with_replies(ticketid)


def reply_id_int(reply: dict) -> int:
    # WHMCS uses replyid (often as string "0")
    rid = reply.get("replyid") or reply.get("id") or 0
    try:
        return int(str(rid).strip())
    except Exception:
        return 0


def extract_reply_attachment_entries(reply: dict) -> list[dict]:
    entries = []
    seen = set()

    raw = reply.get("attachments")
    if isinstance(raw, dict):
        raw = [raw]
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                idx = int(item.get("index"))
            except Exception:
                continue
            name = str(item.get("filename") or "").strip()
            key = (idx, name)
            if key in seen:
                continue
            seen.add(key)
            entries.append({"index": idx, "filename": name})

    # Fallback when WHMCS returns only "attachment" string.
    if not entries:
        raw_names = _split_attachment_parts(str(reply.get("attachment") or ""))
        for idx, name in enumerate(raw_names):
            key = (idx, name)
            if key in seen:
                continue
            seen.add(key)
            entries.append({"index": idx, "filename": name})

    return entries


def whmcs_get_reply_attachment_blob(reply: dict, index: int, ticket_id: str = "") -> dict | None:
    rid_txt = str(reply.get("replyid") or reply.get("id") or "").strip()
    rid = reply_id_int(reply)

    # Reply attachments are fetched by reply id, while initial-ticket attachments
    # (replyid=0) are exposed under ticket attachment type.
    att_type = "reply"
    related_id = rid
    if rid <= 0 and rid_txt == "0" and str(ticket_id).strip():
        att_type = "ticket"
        related_id = int(str(ticket_id).strip())

    if related_id <= 0:
        return None

    # Some WHMCS setups expose attachment indexes as 1-based, others as 0-based.
    idx_candidates = []
    for c in (index, index + 1, index - 1):
        if c < 0:
            continue
        if c not in idx_candidates:
            idx_candidates.append(c)

    last_err = ""
    for idx in idx_candidates:
        data = whmcs_api("GetTicketAttachment", type=att_type, relatedid=related_id, index=idx)
        if str(data.get("result", "")).lower() != "success":
            last_err = str(data.get("message") or data)
            continue

        filename = str(data.get("filename") or f"attachment-{related_id}-{idx}.bin")
        b64 = data.get("data") or ""
        if not b64:
            last_err = "missing data"
            continue

        try:
            blob = base64.b64decode(b64)
        except Exception as e:
            last_err = f"base64 decode failed: {e!r}"
            continue

        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        return {"filename": filename, "data": blob, "mime": mime}

    log("WARNING", f"Attachment blob fetch failed: type={att_type} relatedid={related_id} index={index} tried={idx_candidates} err={last_err}")
    return None


# -------------------------
# Formatting
# -------------------------
def fmt_ticket_intro(ticket: dict) -> str:
    tid = ticket.get("tid") or ticket.get("ticketid") or ticket.get("id")
    subject = ticket.get("subject", "")
    dept = ticket.get("deptname", "")
    status = ticket.get("status", "")
    priority = ticket.get("priority", "")
    requestor = ticket.get("requestor_name") or ticket.get("name") or ""
    requestor_email = ticket.get("requestor_email") or ticket.get("email") or ""
    created = ticket.get("date", "")
    lastreply = ticket.get("lastreply", "")

    return "\n".join([
        "WHMCS → GoodDay (auto)",
        "",
        f"Ticket: #{tid}",
        f"Subject: {subject}",
        f"Department: {dept}",
        f"Status: {status} | Priority: {priority}",
        f"Requestor: {requestor} <{requestor_email}>",
        f"Created: {created}",
        f"Last reply: {lastreply}",
    ])


def extract_initial_message(replies: list[dict]) -> str:
    """
    Put the ORIGINAL customer message (usually replyid=0) inside the task description,
    so you always see what the customer wrote, even when ONLY_NEW_REPLIES=1.
    """
    r0 = None
    for r in replies:
        if str(r.get("replyid", "")).strip() == "0":
            r0 = r
            break

    if r0 is None and replies:
        r0 = replies[0]

    if not r0:
        return ""

    author = (r0.get("name") or r0.get("admin") or r0.get("requestor_name") or "Unknown").strip()
    dt = (r0.get("date") or "").strip()
    msg = (r0.get("message") or "").strip()

    if not msg:
        return ""

    lines = ["", "—", "Αρχικό μήνυμα πελάτη (WHMCS):"]
    if author or dt:
        lines.append(f"Από: {author}" + (f" | {dt}" if dt else ""))
    lines.append("")
    lines.append(msg)
    return "\n".join(lines)


def fmt_reply_comment(ticket: dict, reply: dict) -> str:
    tid = ticket.get("tid") or ticket.get("ticketid") or ticket.get("id")
    subj = (ticket.get("subject") or "").strip()

    rid = reply.get("replyid") or reply.get("id") or ""
    dt = reply.get("date") or ""
    author = reply.get("admin") or reply.get("name") or reply.get("contactname") or "Unknown"
    msg = (reply.get("message") or "").strip()

    lines = [f"WHMCS Ticket #{tid}"]
    if subj:
        lines.append(f"Subject: {subj}")
    lines += ["", f"Reply ID: {rid}"]
    if dt:
        lines.append(f"Date: {dt}")
    lines += [f"From: {author}", "", msg if msg else "(empty)"]
    return "\n".join(lines)


def fmt_reply_edit_comment(ticket: dict, reply: dict) -> str:
    edit_marker = env_str("WHMCS_EDIT_NOTE_PREFIX", "[Edited in WHMCS]").strip()
    base = fmt_reply_comment(ticket, reply)
    if not edit_marker:
        return base
    return f"{edit_marker}\n\n{base}"


def _whmcs_reply_signature(reply: dict) -> str:
    msg = (reply.get("message") or "").strip()
    attachment_entries = []
    for item in extract_reply_attachment_entries(reply):
        idx = item.get("index")
        try:
            idx = int(idx)
        except Exception:
            idx = -1
        attachment_entries.append({
            "index": idx,
            "filename": str(item.get("filename") or "").strip(),
        })
    attachment_entries.sort(key=lambda x: (int(x.get("index", -1)), str(x.get("filename") or "")))

    payload = {
        "message": msg,
        "attachments": attachment_entries,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _split_attachment_parts(raw: str) -> list[str]:
    if not raw:
        return []
    txt = str(raw).strip()
    if not txt:
        return []
    if "|" in txt:
        return [p.strip() for p in txt.split("|") if p.strip()]
    if "\n" in txt:
        return [p.strip() for p in txt.splitlines() if p.strip()]
    if "," in txt and "http" not in txt.lower():
        return [p.strip() for p in txt.split(",") if p.strip()]
    return [txt]


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def extract_reply_attachments(ticket_id: str, reply: dict) -> list[dict]:
    reply_id = str(reply.get("replyid") or reply.get("id") or "").strip()
    template = env_str("WHMCS_ATTACHMENT_URL_TEMPLATE")
    out = []
    seen = set()

    def add_item(name: str = "", url: str = "", index: int | None = None):
        n = (name or "").strip()
        u = html.unescape((url or "").strip())
        if not n and u:
            parsed = urlparse(u)
            n = Path(parsed.path).name or u
        if not u and n:
            if template:
                try:
                    u = template.format(ticketid=ticket_id, replyid=reply_id, filename=n, index=index if index is not None else "")
                except Exception:
                    u = ""
            if not u:
                # Fallback: common WHMCS attachment endpoint pattern.
                parsed = urlparse(env_str("WHMCS_API_URL"))
                if parsed.scheme and parsed.netloc and reply_id:
                    if index is not None:
                        u = f"{parsed.scheme}://{parsed.netloc}/dl.php?type={'t' if reply_id == '0' else 'r'}&id={ticket_id if reply_id == '0' else reply_id}&i={index}"
                    else:
                        encoded_name = quote(n)
                        u = f"{parsed.scheme}://{parsed.netloc}/dl.php?type={'t' if reply_id == '0' else 'r'}&id={ticket_id if reply_id == '0' else reply_id}&i={encoded_name}"
        key = (n, u)
        if key in seen or (not n and not u):
            return
        seen.add(key)
        out.append({"name": n, "url": u})

    # Prefer structured attachment entries (one item per real attachment index).
    entries = extract_reply_attachment_entries(reply)
    if entries:
        for e in entries:
            idx = e.get("index")
            try:
                idx = int(idx)
            except Exception:
                idx = None
            add_item(name=e.get("filename") or "", index=idx)
        return out

    # Fallback for WHMCS installs/modules that expose only loose text fields.
    attach_index = 0
    for key in ("attachment", "attachments", "attachmenturl", "attachmentsurl", "file", "files"):
        value = reply.get(key)
        for item in _as_list(value):
            if isinstance(item, dict):
                add_item(
                    item.get("name") or item.get("filename") or item.get("file"),
                    item.get("url") or item.get("link") or item.get("downloadurl"),
                    attach_index,
                )
                attach_index += 1
                continue
            for part in _split_attachment_parts(str(item)):
                if part.startswith("http://") or part.startswith("https://"):
                    add_item(url=part, index=attach_index)
                else:
                    add_item(name=part, index=attach_index)
                attach_index += 1

    return out


def _gd_message_sort_key(msg: dict):
    raw = (
        msg.get("dateCreated")
        or msg.get("momentCreated")
        or msg.get("createdAt")
        or ""
    )
    txt = str(raw).strip()
    if txt:
        norm = txt.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(norm)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts = dt.timestamp()
        except Exception:
            ts = 0.0
    else:
        ts = 0.0
    return (ts, str(msg.get("id") or ""))


def _strip_public_prefix(message: str, prefix: str) -> str:
    if not prefix:
        return message.strip()
    t = (message or "").strip()
    p = prefix.strip()
    if t.lower().startswith(p.lower()):
        t = t[len(p):]
    return t.strip()


def _extract_public_body(message: str, prefix: str) -> str | None:
    txt = (message or "").strip()
    pfx = (prefix or "").strip()
    if not txt or not pfx:
        return None

    low = txt.lower()
    pfx_low = pfx.lower()

    # 1) Direct prefix at the start.
    if low.startswith(pfx_low):
        return txt[len(pfx):].strip()

    # 2) Prefix at start of any non-empty line (common with RTF/markdown formatting).
    for line in txt.splitlines():
        ln = line.strip().lstrip("-*>\u2022 ").strip()
        if ln.lower().startswith(pfx_low):
            return ln[len(pfx):].strip()

    # 3) Prefix appears later in the text; take content after first occurrence.
    idx = low.find(pfx_low)
    if idx >= 0:
        return txt[idx + len(pfx):].strip()

    return None


def _gd_message_text(msg: dict) -> str:
    text = str(msg.get("message") or "").strip()
    if text:
        return text

    # Fallback for cases where GoodDay keeps plain text only in lexical RTF payload.
    rtf = msg.get("messageRTF") or msg.get("messageRtf")
    if not isinstance(rtf, dict):
        return ""

    parts = []

    def collect(node):
        if isinstance(node, dict):
            t = node.get("text")
            if isinstance(t, str) and t:
                parts.append(t)
            for v in node.values():
                if isinstance(v, (dict, list)):
                    collect(v)
            return
        if isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    collect(item)

    collect(rtf)
    return "\n".join(parts).strip()


def _gd_message_attachment_entries(msg: dict) -> list[dict]:
    raw = msg.get("attachments") or msg.get("files") or []
    if isinstance(raw, dict):
        raw = [raw]
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        out.append(item)
    return out


def _gd_download_attachment_blob(att: dict) -> dict | None:
    name = str(att.get("name") or att.get("filename") or "").strip() or "attachment.bin"
    url = str(att.get("downloadUrl") or "").strip()
    if not url:
        return None

    # Safety cap to avoid pulling huge files in memory.
    max_mb = max(env_int("GOODDAY_TO_WHMCS_MAX_ATTACHMENT_MB", "25"), 1)
    max_bytes = max_mb * 1024 * 1024

    r = SESSION.get(url, timeout=(10, env_float("HTTP_TIMEOUT", "60")), stream=True)
    r.raise_for_status()

    chunks = []
    size = 0
    for chunk in r.iter_content(chunk_size=65536):
        if not chunk:
            continue
        size += len(chunk)
        if size > max_bytes:
            raise RuntimeError(f"Attachment too large: {name} size>{max_mb}MB")
        chunks.append(chunk)

    return {"name": name, "data": b"".join(chunks)}


def _gd_public_message_signature(public_body: str, gd_attachments: list[dict]) -> str:
    attachment_items = []
    for item in gd_attachments:
        if not isinstance(item, dict):
            continue
        attachment_items.append({
            "fileId": str(item.get("fileId") or "").strip(),
            "name": str(item.get("name") or item.get("filename") or "").strip(),
            "size": int(item.get("size") or 0) if str(item.get("size") or "").strip() else 0,
            "downloadUrl": str(item.get("downloadUrl") or "").strip(),
        })
    attachment_items.sort(key=lambda x: (x["fileId"], x["name"], x["downloadUrl"], x["size"]))

    payload = {
        "body": (public_body or "").strip(),
        "attachments": attachment_items,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _gd_extract_whmcs_mirror_body(text: str) -> str:
    lines = (text or "").splitlines()
    if not lines:
        return ""

    from_idx = -1
    for idx, line in enumerate(lines):
        if str(line).strip().lower().startswith("from:"):
            from_idx = idx
            break

    start = from_idx + 1 if from_idx >= 0 else 0
    while start < len(lines) and not str(lines[start]).strip():
        start += 1

    return "\n".join(lines[start:]).strip()


def _gd_extract_whmcs_mirror_reply_id(text: str) -> int:
    m = re.search(r"Reply ID:\s*(\d+)", str(text or ""), flags=re.IGNORECASE)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


def _normalize_text_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _whmcs_message_to_text(text: str) -> str:
    raw = html.unescape(str(text or ""))
    raw = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    raw = re.sub(r"(?i)</(p|div|li|tr|table|ul|ol)>", "\n", raw)
    raw = re.sub(r"(?i)<li[^>]*>", "- ", raw)
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


def _whmcs_canonical_message_for_match(text: str) -> str:
    return _normalize_text_for_compare(_whmcs_message_to_text(text))


def _strip_repeated_marker_prefix(text: str, marker: str) -> str:
    body = str(text or "").strip()
    mk = str(marker or "").strip()
    if not body or not mk:
        return body

    lines = body.splitlines()
    mk_low = mk.lower()
    idx = 0
    saw_marker = False

    while idx < len(lines):
        ln = str(lines[idx]).strip()
        if not ln:
            idx += 1
            continue
        if ln.lower() == mk_low:
            saw_marker = True
            idx += 1
            while idx < len(lines) and not str(lines[idx]).strip():
                idx += 1
            continue
        break

    if not saw_marker:
        return body
    return "\n".join(lines[idx:]).strip()


def _apply_single_marker_prefix(text: str, marker: str) -> str:
    body = _strip_repeated_marker_prefix(text, marker)
    mk = str(marker or "").strip()
    if not mk:
        return body
    if not body:
        return mk
    return f"{mk}\n\n{body}".strip()


def _gd_message_is_soft_deleted(msg: dict) -> bool:
    if not isinstance(msg, dict):
        return False
    mid = str(msg.get("id") or "").strip()
    if not mid:
        return False
    def _flag_true(v) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        s = str(v or "").strip().lower()
        return s in ("1", "true", "yes", "y", "deleted", "trashed", "removed")

    # Prefer explicit delete flags from GoodDay payload.
    for key in ("isDeleted", "deleted", "isTrashed", "trashed"):
        if _flag_true(msg.get(key)):
            return True
    for key in ("deletedAt", "removedAt", "trashedAt"):
        if str(msg.get(key) or "").strip():
            return True
    status_raw = str(msg.get("status") or msg.get("state") or "").strip().lower()
    if status_raw in ("deleted", "trashed", "removed"):
        return True

    if _gd_message_text(msg):
        return False
    if _gd_message_attachment_entries(msg):
        return False
    # Fallback for payloads where delete has no explicit flag and message is now empty.
    return env_bool("GOODDAY_SOFT_DELETE_EMPTY_HEURISTIC", "1")


def _gd_collect_whmcs_reply_message_map(messages: list[dict]) -> dict[int, str]:
    out = {}
    fallback = {}
    edit_marker = env_str("WHMCS_EDIT_NOTE_PREFIX", "[Edited in WHMCS]").strip().lower()

    for msg in sorted(messages or [], key=_gd_message_sort_key):
        if not isinstance(msg, dict):
            continue
        if _gd_message_is_soft_deleted(msg):
            continue
        mid = str(msg.get("id") or "").strip()
        if not mid:
            continue
        text = _gd_message_text(msg)
        if not text:
            continue
        rid = _gd_extract_whmcs_mirror_reply_id(text)
        if rid <= 0:
            continue

        low = text.lower()
        starts_with_edit = bool(edit_marker and low.startswith(edit_marker))

        if not starts_with_edit and rid not in out:
            out[rid] = mid
            continue
        if rid not in out and rid not in fallback:
            fallback[rid] = mid

    for rid, mid in fallback.items():
        out.setdefault(rid, mid)
    return out


def _gd_find_whmcs_reply_message_id(task_id: str, ticket_id: str, reply_id: int, messages: list[dict] | None = None, reply_map: dict[int, str] | None = None) -> str:
    rid = int(reply_id or 0)
    if rid <= 0:
        return ""

    if isinstance(reply_map, dict):
        mid_cached = str(reply_map.get(rid) or "").strip()
        if mid_cached:
            return mid_cached

    marker = f"reply id: {rid}"
    edit_marker = env_str("WHMCS_EDIT_NOTE_PREFIX", "[Edited in WHMCS]").strip().lower()

    local_messages = messages
    if local_messages is None:
        try:
            local_messages = sorted(gd_get_task_messages(task_id), key=_gd_message_sort_key)
        except Exception as e:
            log("WARNING", f"Could not fetch GoodDay messages to resolve reply mapping task={task_id} rid={rid}: {repr(e)}")
            return ""

    mapped = _gd_collect_whmcs_reply_message_map(local_messages)
    mid_mapped = str(mapped.get(rid) or "").strip()
    if mid_mapped:
        if isinstance(reply_map, dict):
            reply_map[rid] = mid_mapped
        return mid_mapped

    fallback_mid = ""
    for msg in local_messages:
        mid = str(msg.get("id") or "").strip()
        if not mid:
            continue
        text = _gd_message_text(msg)
        low = text.lower()
        if marker not in low:
            continue
        if not fallback_mid:
            fallback_mid = mid

        # Prefer the original mirrored comment (without the old edit-note prefix),
        # so future edits keep updating the same conversation item.
        starts_with_edit = bool(edit_marker and low.startswith(edit_marker))
        if not starts_with_edit:
            return mid

    return fallback_mid


def sync_goodday_public_to_whmcs(ticket_id: str, entry: dict) -> int:
    task_id = str(entry.get("task_id") or "").strip()
    if not task_id:
        return 0

    try:
        task_meta_state = ""
        task_meta_reason = ""
        task_meta_error = None
        if env_bool("SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS", "0") and env_bool("GOODDAY_TASK_DELETE_CHECK_META", "1"):
            try:
                task_meta = gd_get_task_meta(task_id)
            except Exception as meta_err:
                task_meta_error = meta_err
                # Do not block delete/edit sync when task-meta endpoint is slow.
                # Continue with /messages lookup, which can still confirm missing task.
                if env_bool("GOODDAY_TASK_META_TIMEOUT_CONTINUE", "1"):
                    log(
                        "WARNING",
                        f"Ticket {ticket_id}: task meta check failed task={task_id}, "
                        f"continuing with messages: {repr(meta_err)}",
                    )
                else:
                    raise
            else:
                if isinstance(task_meta, dict):
                    task_meta_state = str(task_meta.get("state") or "").strip().lower()
                    task_meta_reason = str(task_meta.get("reason") or "").strip()
                if isinstance(task_meta, dict) and str(task_meta.get("state") or "").strip().lower() == "missing":
                    reason = str(task_meta.get("reason") or "").strip()
                    raise GoodDayTaskNotFoundError(
                        f"GoodDay task metadata indicates missing/deleted task={task_id}"
                        f"{f' reason={reason}' if reason else ''}"
                    )
        if env_bool("SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS", "0") and env_bool("GOODDAY_TASK_DELETE_CHECK_PROJECT_LIST", "0"):
            project_id = str(entry.get("project_id") or "").strip()
            if project_id:
                allow_fallback_on_meta_error = env_bool("GOODDAY_TASK_DELETE_PROJECT_LIST_ON_META_ERROR", "0")
                should_fallback_check = (
                    ((task_meta_state == "unknown" or not task_meta_state) and task_meta_error is None)
                    or (allow_fallback_on_meta_error and task_meta_error is not None)
                )
                if should_fallback_check:
                    present = gd_project_task_present(project_id, task_id)
                    if present is False:
                        why = task_meta_reason or ("meta-error" if task_meta_error is not None else "meta-unknown")
                        raise GoodDayTaskNotFoundError(
                            f"GoodDay project list missing task={task_id} project={project_id}"
                            f"{f' reason={why}' if why else ''}"
                        )
        messages = gd_get_task_messages(task_id)
    except GoodDayTaskNotFoundError:
        entry["gd_task_missing_hits"] = int(entry.get("gd_task_missing_hits", 0) or 0) + 1
        missing_hits = int(entry.get("gd_task_missing_hits", 0) or 0)
        required_hits = max(env_int("GOODDAY_TASK_DELETE_CONFIRMATION_HITS", "3"), 1)

        if missing_hits < required_hits:
            log("WARNING", f"Ticket {ticket_id}: mapped GoodDay task missing (task={task_id}) hit={missing_hits}/{required_hits}")
            return 0

        if not env_bool("SYNC_DELETE_TICKETS_GOODDAY_TO_WHMCS", "0"):
            log("WARNING", f"Ticket {ticket_id}: GoodDay task missing task={task_id} but full-ticket delete sync is disabled")
            return 0

        del_res = whmcs_delete_ticket_retry(ticket_id)
        if str((del_res or {}).get("result") or "").strip().lower() == "not-found":
            log("WARNING", f"WHMCS ticket {ticket_id} already missing while deleting after GoodDay task removal task={task_id}")
        else:
            log("INFO", f"Deleted WHMCS ticket {ticket_id} because mapped GoodDay task was removed task={task_id}")

        # Mark for removal from local sync state so we do not reprocess it.
        entry["deleted_in_whmcs"] = True
        entry["_drop_from_state"] = True
        entry["_deleted_tombstone"] = {
            "ts": time.time(),
            "source": "goodday_task_missing",
            "task_id": task_id,
            "whmcs_tid": str(entry.get("whmcs_tid") or "").strip(),
        }
        entry["created"] = False
        entry["task_id"] = ""
        entry["gd_task_missing_hits"] = 0
        entry["gd_to_whmcs_backoff_until"] = 0.0
        return 0

    entry["gd_task_missing_hits"] = 0
    if not messages:
        return 0

    prefix = env_str("GOODDAY_PUBLIC_PREFIX", "!public").strip()
    sync_user_id = env_str("GOODDAY_FROM_USER_ID").strip()
    sync_edits = env_bool("SYNC_EDITS_GOODDAY_TO_WHMCS", "1")
    edit_marker = env_str("GOODDAY_EDIT_NOTE_PREFIX", "[Edited in GoodDay]").strip()
    show_public_edit_marker = env_bool("GOODDAY_EDIT_NOTE_VISIBLE", "0")
    sync_mirror_edits = env_bool("SYNC_EDITS_GD_MIRRORED_TO_WHMCS", "1")
    mirror_edit_marker = env_str("GOODDAY_MIRROR_EDIT_NOTE_PREFIX", "[Edited in GoodDay (mirrored WHMCS message)]").strip()
    show_mirror_edit_marker = env_bool("GOODDAY_MIRROR_EDIT_NOTE_VISIBLE", "0")
    fallback_public_edit_new_reply = env_bool("GOODDAY_EDIT_FALLBACK_NEW_REPLY", "0")
    fallback_mirror_edit_new_reply = env_bool("GOODDAY_MIRROR_EDIT_FALLBACK_NEW_REPLY", "0")
    sync_deletes = env_bool("SYNC_DELETES_GOODDAY_TO_WHMCS", "1")

    gd_public_signatures = entry.setdefault("gd_public_message_signatures", {})
    if not isinstance(gd_public_signatures, dict):
        gd_public_signatures = {}
    gd_mirror_signatures = entry.setdefault("gd_mirror_message_signatures", {})
    if not isinstance(gd_mirror_signatures, dict):
        gd_mirror_signatures = {}
    gd_public_to_whmcs_reply_ids = entry.setdefault("gd_public_to_whmcs_reply_ids", {})
    if not isinstance(gd_public_to_whmcs_reply_ids, dict):
        gd_public_to_whmcs_reply_ids = {}
    gd_pending_public_reply_signatures = entry.setdefault("gd_pending_public_reply_signatures", {})
    if not isinstance(gd_pending_public_reply_signatures, dict):
        gd_pending_public_reply_signatures = {}
    gd_hidden_edit_notes = entry.setdefault("gd_hidden_edit_notes", {})
    if not isinstance(gd_hidden_edit_notes, dict):
        gd_hidden_edit_notes = {}
    gd_to_whmcs_reply_ids = entry.setdefault("gd_message_to_whmcs_reply_ids", {})
    if not isinstance(gd_to_whmcs_reply_ids, dict):
        gd_to_whmcs_reply_ids = {}
    # One-time compatibility backfill from legacy mixed map.
    for mid_key, raw_rid in list(gd_to_whmcs_reply_ids.items()):
        mid_txt = str(mid_key or "").strip()
        if not mid_txt or mid_txt not in gd_public_signatures:
            continue
        try:
            rid_val = int(str(raw_rid).strip())
        except Exception:
            continue
        if rid_val > 0 and mid_txt not in gd_public_to_whmcs_reply_ids:
            gd_public_to_whmcs_reply_ids[mid_txt] = rid_val
    seen_ids = entry.setdefault("gd_seen_message_ids", [])
    if not isinstance(seen_ids, list):
        seen_ids = []
    seen_set = {str(x) for x in seen_ids if str(x).strip()}

    def mark_seen(mid: str):
        if not mid:
            return
        if mid in seen_set:
            return
        seen_ids.append(mid)
        seen_set.add(mid)

    def unmark_seen(mid: str):
        if not mid:
            return
        if mid in seen_set:
            seen_set.discard(mid)
        if seen_ids:
            seen_ids[:] = [x for x in seen_ids if str(x).strip() != mid]

    def drop_message_state(mid: str):
        if not mid:
            return
        gd_public_signatures.pop(mid, None)
        gd_mirror_signatures.pop(mid, None)
        gd_public_to_whmcs_reply_ids.pop(mid, None)
        gd_pending_public_reply_signatures.pop(mid, None)
        gd_hidden_edit_notes.pop(mid, None)
        gd_to_whmcs_reply_ids.pop(mid, None)
        unmark_seen(mid)

    whmcs_reply_texts_by_id = None

    def get_whmcs_reply_texts_by_id() -> dict[int, str]:
        nonlocal whmcs_reply_texts_by_id
        if whmcs_reply_texts_by_id is not None:
            return whmcs_reply_texts_by_id
        try:
            _ticket, replies = get_ticket_with_replies(ticket_id)
        except Exception as e:
            log("WARNING", f"Could not fetch WHMCS replies for mirror-compare ticket={ticket_id}: {repr(e)}")
            whmcs_reply_texts_by_id = {}
            return whmcs_reply_texts_by_id
        out = {}
        for rpl in replies or []:
            rid = reply_id_int(rpl)
            if rid <= 0:
                continue
            out[rid] = str(rpl.get("message") or "").strip()
        whmcs_reply_texts_by_id = out
        return whmcs_reply_texts_by_id

    current_message_ids = set()
    for m in messages:
        if not isinstance(m, dict):
            continue
        mid_txt = str(m.get("id") or "").strip()
        if not mid_txt:
            continue
        if _gd_message_is_soft_deleted(m):
            continue
        current_message_ids.add(mid_txt)
    if sync_deletes and gd_public_to_whmcs_reply_ids:
        missing_public_message_ids = [mid for mid in list(gd_public_to_whmcs_reply_ids.keys()) if str(mid).strip() and str(mid).strip() not in current_message_ids]
        for mid in missing_public_message_ids:
            mid_txt = str(mid).strip()
            raw_reply_id = gd_public_to_whmcs_reply_ids.get(mid_txt)
            try:
                rid = int(str(raw_reply_id).strip())
            except Exception:
                rid = 0

            if rid <= 0:
                drop_message_state(mid_txt)
                continue

            try:
                whmcs_delete_ticket_reply_retry(ticket_id, rid)
                log("INFO", f"Deleted WHMCS reply {rid} for removed GoodDay public message {mid_txt} ticket={ticket_id}")
                drop_message_state(mid_txt)
            except Exception as e:
                err = repr(e).lower()
                if "reply id not found" in err or "ticket id not found" in err:
                    log("WARNING", f"Delete WHMCS reply skipped (already missing) rid={rid} ticket={ticket_id} from removed GoodDay public message {mid_txt}")
                    drop_message_state(mid_txt)
                    continue
                log("ERROR", f"Failed deleting WHMCS reply rid={rid} ticket={ticket_id} from removed GoodDay public message {mid_txt}: {repr(e)}")

    if sync_deletes and gd_to_whmcs_reply_ids:
        missing_message_ids = [
            mid for mid in list(gd_to_whmcs_reply_ids.keys())
            if str(mid).strip()
            and str(mid).strip() not in current_message_ids
            and str(mid).strip() not in gd_public_to_whmcs_reply_ids
        ]
        for mid in missing_message_ids:
            mid_txt = str(mid).strip()
            raw_reply_id = gd_to_whmcs_reply_ids.get(mid_txt)
            try:
                rid = int(str(raw_reply_id).strip())
            except Exception:
                rid = 0

            if rid <= 0:
                drop_message_state(mid_txt)
                continue

            try:
                whmcs_delete_ticket_reply_retry(ticket_id, rid)
                log("INFO", f"Deleted WHMCS reply {rid} for removed GoodDay message {mid_txt} ticket={ticket_id}")
                drop_message_state(mid_txt)
            except Exception as e:
                err = repr(e).lower()
                if "reply id not found" in err or "ticket id not found" in err:
                    log("WARNING", f"Delete WHMCS reply skipped (already missing) rid={rid} ticket={ticket_id} from GoodDay message {mid_txt}")
                    drop_message_state(mid_txt)
                    continue
                log("ERROR", f"Failed deleting WHMCS reply rid={rid} ticket={ticket_id} from removed GoodDay message {mid_txt}: {repr(e)}")

    sent = 0
    for msg in sorted(messages, key=_gd_message_sort_key):
        if not isinstance(msg, dict):
            continue
        mid = str(msg.get("id") or "").strip()
        if not mid:
            continue
        is_soft_deleted = _gd_message_is_soft_deleted(msg)
        if is_soft_deleted:
            # Process deletes by mapping even when GoodDay returns empty/flagless
            # tombstone payloads (no text, no mirror header).
            if sync_deletes:
                deleted_reply_id = 0
                try:
                    deleted_reply_id = int(str(gd_public_to_whmcs_reply_ids.get(mid) or "").strip())
                except Exception:
                    deleted_reply_id = 0
                if deleted_reply_id <= 0:
                    try:
                        deleted_reply_id = int(str(gd_to_whmcs_reply_ids.get(mid) or "").strip())
                    except Exception:
                        deleted_reply_id = 0

                if deleted_reply_id > 0:
                    try:
                        whmcs_delete_ticket_reply_retry(ticket_id, deleted_reply_id)
                        log("INFO", f"Deleted WHMCS reply {deleted_reply_id} for soft-deleted GoodDay message {mid} ticket={ticket_id}")
                    except Exception as e:
                        err = repr(e).lower()
                        if "reply id not found" in err or "ticket id not found" in err:
                            log("WARNING", f"Delete WHMCS reply skipped (already missing) rid={deleted_reply_id} ticket={ticket_id} from soft-deleted GoodDay message {mid}")
                        else:
                            log("ERROR", f"Failed deleting WHMCS reply rid={deleted_reply_id} ticket={ticket_id} from soft-deleted GoodDay message {mid}: {repr(e)}")
                            # Keep state for retry on next cycle.
                            mark_seen(mid)
                            continue
                    drop_message_state(mid)
                    continue
            mark_seen(mid)
            continue

        from_uid = str(msg.get("fromUserId") or "").strip()
        text = _gd_message_text(msg)
        gd_attachments = _gd_message_attachment_entries(msg)

        # Keep internal by default: sync only explicit public messages.
        public_body = _extract_public_body(text, prefix)
        is_public_command = public_body is not None
        is_bot_echo = text.strip().startswith("WHMCS Ticket #")

        if not text:
            mark_seen(mid)
            continue

        # Allow edits on mirrored WHMCS comments in GoodDay to go back to WHMCS
        # by updating the same WHMCS reply id.
        if is_bot_echo:
            mirror_body = _gd_extract_whmcs_mirror_body(text)
            mirror_sig = _gd_public_message_signature(mirror_body or text, gd_attachments)
            prev_mirror_sig = str(gd_mirror_signatures.get(mid) or "").strip()
            is_mirror_edited = bool(prev_mirror_sig and prev_mirror_sig != mirror_sig)
            mirror_reply_id = _gd_extract_whmcs_mirror_reply_id(text)
            if mirror_reply_id <= 0:
                try:
                    mirror_reply_id = int(str(gd_to_whmcs_reply_ids.get(mid) or "").strip())
                except Exception:
                    mirror_reply_id = 0
            if mirror_reply_id > 0:
                gd_to_whmcs_reply_ids[mid] = mirror_reply_id

            # On first-seen mirrored comments, compare with the matching WHMCS reply.
            # If content already differs there, sync immediately (edited in GoodDay).
            if not prev_mirror_sig:
                whmcs_has_difference = False
                if sync_mirror_edits and mirror_reply_id > 0:
                    whmcs_replies = get_whmcs_reply_texts_by_id()
                    whmcs_reply_text = str(whmcs_replies.get(mirror_reply_id) or "").strip()
                    if whmcs_reply_text:
                        mirror_body_cmp = _strip_repeated_marker_prefix(mirror_body, mirror_edit_marker)
                        whmcs_reply_cmp = _strip_repeated_marker_prefix(whmcs_reply_text, mirror_edit_marker)
                        whmcs_has_difference = (_normalize_text_for_compare(mirror_body_cmp) != _normalize_text_for_compare(whmcs_reply_cmp))
                if not whmcs_has_difference:
                    gd_mirror_signatures[mid] = mirror_sig
                    mark_seen(mid)
                    continue
                is_mirror_edited = True

            if prev_mirror_sig == mirror_sig:
                mark_seen(mid)
                continue

            if is_mirror_edited and not sync_mirror_edits:
                gd_mirror_signatures[mid] = mirror_sig
                mark_seen(mid)
                continue

            mirror_core_body = _strip_repeated_marker_prefix(mirror_body, mirror_edit_marker)
            body = (mirror_core_body or "").strip()
            upload_files = []
            for att in gd_attachments:
                try:
                    blob = _gd_download_attachment_blob(att)
                    if blob:
                        upload_files.append(blob)
                except Exception as e:
                    log("ERROR", f"Failed downloading GoodDay attachment for mirrored msg={mid}: {repr(e)}")

            if not body and upload_files:
                body = env_str("GOODDAY_PUBLIC_EMPTY_BODY", "Shared files from GoodDay.").strip()
            if not body and not upload_files:
                gd_mirror_signatures[mid] = mirror_sig
                mark_seen(mid)
                continue

            if is_mirror_edited and mirror_edit_marker and show_mirror_edit_marker:
                body = _apply_single_marker_prefix(body, mirror_edit_marker)

            if upload_files and env_bool("GOODDAY_TO_WHMCS_INCLUDE_ATTACHMENT_NAMES", "1"):
                names = [str(f.get("name") or "").strip() for f in upload_files if str(f.get("name") or "").strip()]
                if names:
                    body = body.rstrip() + "\n\nAttachments:\n" + "\n".join(f"- {n}" for n in names)

            if is_mirror_edited and mirror_reply_id > 0:
                whmcs_replies = get_whmcs_reply_texts_by_id()
                whmcs_reply_text = str(whmcs_replies.get(mirror_reply_id) or "").strip()
                if whmcs_reply_text:
                    target_cmp = _normalize_text_for_compare(body)
                    if mirror_edit_marker and show_mirror_edit_marker:
                        current_body = _apply_single_marker_prefix(whmcs_reply_text, mirror_edit_marker)
                    else:
                        current_body = _strip_repeated_marker_prefix(whmcs_reply_text, mirror_edit_marker)
                    current_canon = _normalize_text_for_compare(current_body)
                    if target_cmp == current_canon:
                        gd_mirror_signatures[mid] = mirror_sig
                        mark_seen(mid)
                        continue

            if is_mirror_edited and mirror_reply_id > 0:
                whmcs_update_ticket_reply(mirror_reply_id, body)
                status_after_reply = env_str("WHMCS_STATUS_ON_GD_PUBLIC", "Answered")
                if status_after_reply:
                    whmcs_update_ticket_status(ticket_id, status_after_reply)
                sent += 1
                gd_mirror_signatures[mid] = mirror_sig
                mark_seen(mid)
                log("INFO", f"Updated WHMCS reply {mirror_reply_id} from GoodDay mirrored msg {mid} ticket {ticket_id} (status={status_after_reply!r})")
                continue

            if is_mirror_edited and mirror_reply_id <= 0 and not fallback_mirror_edit_new_reply:
                log("WARNING", f"Edited GoodDay mirrored msg {mid} has no mapped WHMCS reply id; skipping to avoid duplicate reply")
                continue

            reply_res = whmcs_add_ticket_reply(ticket_id, body, attachments=upload_files)
            new_reply_id = whmcs_extract_reply_id(reply_res)
            if new_reply_id > 0:
                gd_to_whmcs_reply_ids[mid] = new_reply_id
                try:
                    entry["last_reply_id"] = max(int(entry.get("last_reply_id", 0) or 0), int(new_reply_id))
                except Exception:
                    pass
            else:
                if isinstance(reply_res, dict):
                    keys = ",".join(list(reply_res.keys())[:8])
                else:
                    keys = type(reply_res).__name__
                log("WARNING", f"WHMCS AddTicketReply returned no reply id for GoodDay mirrored msg {mid} ticket {ticket_id}; response={keys}")
            status_after_reply = env_str("WHMCS_STATUS_ON_GD_PUBLIC", "Answered")
            if status_after_reply:
                whmcs_update_ticket_status(ticket_id, status_after_reply)
            sent += 1
            gd_mirror_signatures[mid] = mirror_sig
            mark_seen(mid)
            log("INFO", f"Synced GoodDay mirrored msg {mid} (edited) -> WHMCS ticket {ticket_id} (status={status_after_reply!r}, attachments={len(upload_files)})")
            continue

        if sync_user_id and from_uid == sync_user_id and not is_public_command:
            mark_seen(mid)
            continue

        if not is_public_command:
            mark_seen(mid)
            continue

        body = (public_body or "").strip()
        msg_sig = _gd_public_message_signature(body, gd_attachments)
        prev_sig = str(gd_public_signatures.get(mid) or "").strip()
        mapped_reply_id = 0
        try:
            mapped_reply_id = int(str(gd_public_to_whmcs_reply_ids.get(mid) or "").strip())
        except Exception:
            mapped_reply_id = 0
        if mapped_reply_id <= 0:
            try:
                mapped_reply_id = int(str(gd_to_whmcs_reply_ids.get(mid) or "").strip())
            except Exception:
                mapped_reply_id = 0
        is_edited = bool(prev_sig and prev_sig != msg_sig)

        if prev_sig and prev_sig == msg_sig:
            mark_seen(mid)
            continue

        # When state signatures are missing (restart/manual cleanup) but the GoodDay
        # message is already mapped to a WHMCS reply, avoid posting a duplicate.
        # If content differs, treat it as an edit and update the mapped reply in-place.
        if not prev_sig and mapped_reply_id > 0:
            whmcs_replies = get_whmcs_reply_texts_by_id()
            whmcs_reply_text = str(whmcs_replies.get(mapped_reply_id) or "").strip()
            if whmcs_reply_text:
                gd_body_cmp = _normalize_text_for_compare(_strip_repeated_marker_prefix(body, edit_marker))
                whmcs_body_cmp = _normalize_text_for_compare(
                    _strip_repeated_marker_prefix(_whmcs_message_to_text(whmcs_reply_text), edit_marker)
                )
                if gd_body_cmp and gd_body_cmp == whmcs_body_cmp:
                    gd_public_signatures[mid] = msg_sig
                    mark_seen(mid)
                    continue
                is_edited = True

        if is_edited and not sync_edits:
            gd_public_signatures[mid] = msg_sig
            mark_seen(mid)
            continue

        upload_files = []
        for att in gd_attachments:
            try:
                blob = _gd_download_attachment_blob(att)
                if blob:
                    upload_files.append(blob)
            except Exception as e:
                log("ERROR", f"Failed downloading GoodDay attachment for msg={mid}: {repr(e)}")

        if not body and upload_files:
            body = env_str("GOODDAY_PUBLIC_EMPTY_BODY", "Shared files from GoodDay.").strip()

        if not body and not upload_files:
            mark_seen(mid)
            gd_public_signatures[mid] = msg_sig
            continue

        body = _strip_repeated_marker_prefix(body, edit_marker)
        if is_edited and edit_marker and show_public_edit_marker:
            body = _apply_single_marker_prefix(body, edit_marker)

        # Keep a text fallback with filenames only (no URLs), because some WHMCS
        # installations may acknowledge attachment payloads but not persist files.
        if upload_files and env_bool("GOODDAY_TO_WHMCS_INCLUDE_ATTACHMENT_NAMES", "1"):
            names = [str(f.get("name") or "").strip() for f in upload_files if str(f.get("name") or "").strip()]
            if names:
                body = body.rstrip() + "\n\nAttachments:\n" + "\n".join(f"- {n}" for n in names)

        if is_edited and mapped_reply_id <= 0:
            candidate_ids = []
            for raw in list(gd_public_to_whmcs_reply_ids.values()) + list(gd_to_whmcs_reply_ids.values()):
                try:
                    rid_val = int(str(raw).strip())
                except Exception:
                    continue
                if rid_val > 0 and rid_val not in candidate_ids:
                    candidate_ids.append(rid_val)
            if candidate_ids:
                mapped_reply_id = max(candidate_ids)
                gd_public_to_whmcs_reply_ids[mid] = mapped_reply_id
                log("WARNING", f"Recovered missing public mapping mid={mid} -> reply={mapped_reply_id} ticket={ticket_id}")

        if is_edited and mapped_reply_id > 0:
            whmcs_update_ticket_reply(mapped_reply_id, body)
            status_after_reply = env_str("WHMCS_STATUS_ON_GD_PUBLIC", "Answered")
            if status_after_reply:
                whmcs_update_ticket_status(ticket_id, status_after_reply)
            sent += 1
            try:
                entry["last_reply_id"] = max(int(entry.get("last_reply_id", 0) or 0), int(mapped_reply_id))
            except Exception:
                pass
            gd_public_to_whmcs_reply_ids[mid] = mapped_reply_id
            gd_public_signatures[mid] = msg_sig
            gd_hidden_edit_notes[mid] = now_iso()
            mark_seen(mid)
            log("INFO", f"Updated WHMCS reply {mapped_reply_id} from edited GoodDay public msg {mid} ticket {ticket_id} (status={status_after_reply!r})")
            continue

        if is_edited and mapped_reply_id <= 0 and not fallback_public_edit_new_reply:
            log("WARNING", f"Edited GoodDay public msg {mid} has no mapped WHMCS reply id; skipping to avoid duplicate reply")
            continue

        pending_sig = _whmcs_canonical_message_for_match(body)
        if pending_sig:
            gd_pending_public_reply_signatures[mid] = pending_sig

        reply_res = whmcs_add_ticket_reply(ticket_id, body, attachments=upload_files)
        new_reply_id = whmcs_extract_reply_id(reply_res)
        if new_reply_id > 0:
            gd_public_to_whmcs_reply_ids[mid] = new_reply_id
            gd_pending_public_reply_signatures.pop(mid, None)
            try:
                entry["last_reply_id"] = max(int(entry.get("last_reply_id", 0) or 0), int(new_reply_id))
            except Exception:
                pass
        else:
            if isinstance(reply_res, dict):
                keys = ",".join(list(reply_res.keys())[:8])
            else:
                keys = type(reply_res).__name__
            log("WARNING", f"WHMCS AddTicketReply returned no reply id for GoodDay msg {mid} ticket {ticket_id}; response={keys}")
        status_after_reply = env_str("WHMCS_STATUS_ON_GD_PUBLIC", "Answered")
        if status_after_reply:
            whmcs_update_ticket_status(ticket_id, status_after_reply)
        sent += 1
        gd_public_signatures[mid] = msg_sig
        if is_edited:
            gd_hidden_edit_notes[mid] = now_iso()
        mark_seen(mid)
        log("INFO", f"Synced GoodDay public msg {mid} ({'edited' if is_edited else 'new'}) -> WHMCS ticket {ticket_id} (status={status_after_reply!r}, attachments={len(upload_files)})")

    if len(seen_ids) > 1000:
        seen_ids[:] = seen_ids[-1000:]
    if len(gd_public_signatures) > 2000:
        keep_ids = set(str(m.get("id") or "").strip() for m in messages if isinstance(m, dict))
        for key in list(gd_public_signatures.keys()):
            if str(key) not in keep_ids:
                gd_public_signatures.pop(key, None)
            if len(gd_public_signatures) <= 1500:
                break
    if len(gd_mirror_signatures) > 2000:
        keep_ids = set(str(m.get("id") or "").strip() for m in messages if isinstance(m, dict))
        for key in list(gd_mirror_signatures.keys()):
            if str(key) not in keep_ids:
                gd_mirror_signatures.pop(key, None)
            if len(gd_mirror_signatures) <= 1500:
                break
    if len(gd_to_whmcs_reply_ids) > 2000:
        keep_ids = set(str(m.get("id") or "").strip() for m in messages if isinstance(m, dict))
        for key in list(gd_to_whmcs_reply_ids.keys()):
            if str(key) not in keep_ids:
                gd_to_whmcs_reply_ids.pop(key, None)
            if len(gd_to_whmcs_reply_ids) <= 1500:
                break
    if len(gd_public_to_whmcs_reply_ids) > 2000:
        keep_ids = set(str(m.get("id") or "").strip() for m in messages if isinstance(m, dict))
        for key in list(gd_public_to_whmcs_reply_ids.keys()):
            if str(key) not in keep_ids:
                gd_public_to_whmcs_reply_ids.pop(key, None)
            if len(gd_public_to_whmcs_reply_ids) <= 1500:
                break
    if len(gd_pending_public_reply_signatures) > 2000:
        keep_ids = set(str(m.get("id") or "").strip() for m in messages if isinstance(m, dict))
        for key in list(gd_pending_public_reply_signatures.keys()):
            if str(key) not in keep_ids:
                gd_pending_public_reply_signatures.pop(key, None)
            if len(gd_pending_public_reply_signatures) <= 1500:
                break
    if len(gd_hidden_edit_notes) > 2000:
        keep_ids = set(str(m.get("id") or "").strip() for m in messages if isinstance(m, dict))
        for key in list(gd_hidden_edit_notes.keys()):
            if str(key) not in keep_ids:
                gd_hidden_edit_notes.pop(key, None)
            if len(gd_hidden_edit_notes) <= 1500:
                break
    entry["gd_seen_message_ids"] = seen_ids
    entry["gd_public_message_signatures"] = gd_public_signatures
    entry["gd_mirror_message_signatures"] = gd_mirror_signatures
    entry["gd_public_to_whmcs_reply_ids"] = gd_public_to_whmcs_reply_ids
    entry["gd_pending_public_reply_signatures"] = gd_pending_public_reply_signatures
    entry["gd_hidden_edit_notes"] = gd_hidden_edit_notes
    entry["gd_message_to_whmcs_reply_ids"] = gd_to_whmcs_reply_ids
    return sent


def gd_backoff_remaining_seconds(entry: dict) -> int:
    if not isinstance(entry, dict):
        return 0
    try:
        until = float(entry.get("gd_to_whmcs_backoff_until", 0.0) or 0.0)
    except Exception:
        until = 0.0
    if until <= 0:
        return 0
    remain = int(until - time.time())
    return remain if remain > 0 else 0


def gd_mark_backoff(entry: dict, err: Exception, ticket_id: str = ""):
    if not isinstance(entry, dict):
        return
    generic_s = max(env_int("GD_TO_WHMCS_FAILURE_BACKOFF_SECONDS", "20"), 0)
    timeout_s = max(env_int("GD_TO_WHMCS_TIMEOUT_BACKOFF_SECONDS", "90"), generic_s)
    err_low = repr(err).lower()
    is_timeout = ("timeout" in err_low) or ("timed out" in err_low) or ("readtimeout" in err_low)
    backoff_s = timeout_s if is_timeout else generic_s
    if backoff_s <= 0:
        return
    entry["gd_to_whmcs_backoff_until"] = time.time() + float(backoff_s)
    if ticket_id:
        log("WARNING", f"Ticket {ticket_id}: GoodDay->WHMCS backoff set to {backoff_s}s ({'timeout' if is_timeout else 'error'})")


def fmt_attachments_block(attachments: list[dict]) -> str:
    if not attachments:
        return ""
    lines = ["", "Attachments (WHMCS):"]
    for a in attachments:
        name = (a.get("name") or "").strip()
        if name:
            lines.append(f"- {name}")
    return "\n".join(lines)


# -------------------------
# Main
# -------------------------
def validate_env():
    if not env_str("WHMCS_API_URL"):
        die("WHMCS_API_URL is empty")
    if not env_str("WHMCS_API_IDENTIFIER") or not env_str("WHMCS_API_SECRET"):
        die("WHMCS_API_IDENTIFIER / WHMCS_API_SECRET are empty")
    if not env_str("GOODDAY_API_TOKEN"):
        die("GOODDAY_API_TOKEN is empty")
    if not env_str("GOODDAY_PROJECT_ID") and not env_str("GOODDAY_PROJECT_BY_DEPARTMENT_JSON"):
        die("Set GOODDAY_PROJECT_ID or GOODDAY_PROJECT_BY_DEPARTMENT_JSON")
    if not env_str("GOODDAY_FROM_USER_ID"):
        die("GOODDAY_FROM_USER_ID is empty")
    if env_bool("SYNC_GOODDAY_TO_WHMCS", "0") and not env_str("WHMCS_ADMIN_USERNAME"):
        die("WHMCS_ADMIN_USERNAME is required when SYNC_GOODDAY_TO_WHMCS=1")


def acquire_single_instance_lock():
    global LOCK_HANDLE
    # Keep lock separate from state file to avoid corrupting JSON state.
    state_file = env_str("STATE_FILE", str(BASE_DIR / "state.json"))
    lock_file = env_str("LOCK_FILE") or f"{state_file}.lock"
    LOCK_HANDLE = open(lock_file, "a+", encoding="utf-8")
    try:
        fcntl.flock(LOCK_HANDLE.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        die(f"Another instance is already running (lock={lock_file})")
    LOCK_HANDLE.seek(0)
    LOCK_HANDLE.truncate()
    LOCK_HANDLE.write(f"{os.getpid()}\n")
    LOCK_HANDLE.flush()


def main():
    log("INFO", "WHMCS → GoodDay sync started (GOODDAY API MODE)")

    raw_statuses = env_str("WHMCS_STATUSES") or env_str("WHMCS_STATUS")
    dept_project_map = parse_department_project_map()
    status_sync_project_ids = collect_status_sync_project_ids(dept_project_map)

    # ✅ FIX: this was a tuple, not a log call
    log(
        "INFO",
        "[ENV] "
        f"URL_set={bool(env_str('WHMCS_API_URL'))} | "
        f"ID_len={len(env_str('WHMCS_API_IDENTIFIER'))} | "
        f"SECRET_len={len(env_str('WHMCS_API_SECRET'))} | "
        f"STATUSES={raw_statuses!r} | "
        f"GD_BASE={env_str('GOODDAY_API_BASE','https://api.goodday.work/2.0')} | "
        f"GD_PROJECT={env_str('GOODDAY_PROJECT_ID')} | "
        f"GD_DEPT_MAP={len(dept_project_map)} | "
        f"GD_FROM={env_str('GOODDAY_FROM_USER_ID')} | "
        f"HTTP_TIMEOUT={env_str('HTTP_TIMEOUT','60')} | "
        f"HTTP_RETRY_TOTAL={env_str('HTTP_RETRY_TOTAL','1')} | "
        f"GD_MSG_TIMEOUT={env_str('GOODDAY_MESSAGES_HTTP_TIMEOUT', env_str('HTTP_TIMEOUT','60'))} | "
        f"GD_MSG_RETRIES={env_str('GOODDAY_MESSAGES_RETRIES','3')} | "
        f"FLAGS create={int(env_bool('SYNC_CREATE_TASK','1'))} replies={int(env_bool('SYNC_REPLIES','1'))} "
        f"only_new={int(env_bool('ONLY_NEW_REPLIES','1'))} "
        f"DRY_RUN={int(env_bool('DRY_RUN','0'))}"
    )

    validate_env()
    acquire_single_instance_lock()

    res = whmcs_api("GetConfigurationValue", setting="CompanyName")
    log("INFO", f"WHMCS OK: {res.get('value')}")

    state = load_state()
    tickets_state = state.setdefault("tickets", {})
    state.setdefault("deleted_ticket_tombstones", {})

    poll_seconds = env_int("WHMCS_POLL_SECONDS", "60")
    only_new = env_bool("ONLY_NEW_REPLIES", "1")
    sync_create = env_bool("SYNC_CREATE_TASK", "1")
    sync_replies = env_bool("SYNC_REPLIES", "1")
    sync_gd_to_whmcs = env_bool("SYNC_GOODDAY_TO_WHMCS", "0")
    sync_status_from_gd = env_bool("SYNC_STATUS_FROM_GOODDAY", "1")
    gd_status_map = parse_goodday_whmcs_status_map()
    webhook_enabled = env_bool("SYNC_WEBHOOK_ENABLED", "0")
    webhook_targeted = env_bool("SYNC_WEBHOOK_TARGETED", "1")
    webhook_full_fallback = env_bool("SYNC_WEBHOOK_FALLBACK_FULL_POLL", "1")

    if webhook_enabled:
        try:
            start_webhook_server()
        except Exception as e:
            die(f"Failed to start webhook server: {repr(e)}")

    while True:
        try:
            tombstones_dropped = _cleanup_delete_tombstones(state)
            if tombstones_dropped:
                log("INFO", f"Dropped {tombstones_dropped} expired delete-guard tombstone(s)")
            pending_ids = consume_webhook_ticket_ids() if webhook_enabled else []
            resolved_pending_ids = resolve_webhook_ticket_ids(pending_ids, tickets_state) if pending_ids else []
            webhook_targeted_cycle = bool(resolved_pending_ids and webhook_targeted)
            webhook_targeted_gd_only = env_bool("SYNC_WEBHOOK_TARGETED_GD_ONLY", "1")
            skip_whmcs_to_goodday = bool(webhook_targeted_cycle and webhook_targeted_gd_only)
            full_cycle_preemptible = bool(webhook_enabled and webhook_targeted and not resolved_pending_ids)
            cycle_interrupted_by_webhook = False
            cycle_reason = "poll"
            new_created = 0
            new_replies = 0
            new_public_back = 0
            gd_back_checked = set()
            gd_tasks_by_id = {}

            # Prioritize GoodDay -> WHMCS message sync (edit/delete) before expensive
            # WHMCS ticket polling, so transient WHMCS list timeouts do not delay deletes.
            if sync_gd_to_whmcs:
                pre_targeted_filter = set(resolved_pending_ids) if (resolved_pending_ids and webhook_targeted) else set()
                for state_tid, state_entry in tickets_state.items():
                    if full_cycle_preemptible and WEBHOOK_WAKE_EVENT.is_set():
                        cycle_interrupted_by_webhook = True
                        log("INFO", "Preempting early mapped GoodDay->WHMCS pass due to webhook event")
                        break
                    tid = str(state_tid).strip()
                    if not tid:
                        continue
                    if pre_targeted_filter and tid not in pre_targeted_filter:
                        continue
                    if not isinstance(state_entry, dict):
                        continue
                    if state_entry.get("deleted_in_whmcs"):
                        continue
                    if not state_entry.get("task_id") or state_entry.get("task_id") == "DRY_RUN_TASK_ID":
                        continue
                    backoff_left = 0 if webhook_targeted_cycle else gd_backoff_remaining_seconds(state_entry)
                    if backoff_left > 0:
                        log("DEBUG", f"Ticket {tid}: skipping GoodDay->WHMCS due backoff ({backoff_left}s left)")
                    else:
                        try:
                            new_public_back += sync_goodday_public_to_whmcs(tid, state_entry)
                            if float(state_entry.get("gd_to_whmcs_backoff_until", 0.0) or 0.0) > 0:
                                state_entry["gd_to_whmcs_backoff_until"] = 0.0
                        except Exception as e:
                            log("ERROR", f"GoodDay public -> WHMCS sync failed for ticket {tid}: {repr(e)}")
                            gd_mark_backoff(state_entry, e, tid)
                    gd_back_checked.add(tid)

            if full_cycle_preemptible and WEBHOOK_WAKE_EVENT.is_set():
                tickets = []
                cycle_interrupted_by_webhook = True
                cycle_reason = "poll->preempt-before-fetch"
            elif resolved_pending_ids and webhook_targeted:
                tickets = fetch_tickets_by_internal_ids(resolved_pending_ids)
                cycle_reason = f"webhook-targeted:{','.join(resolved_pending_ids[:8])}"
                if not tickets and webhook_full_fallback:
                    tickets = fetch_tickets_multi_status_incremental()
                    cycle_reason = f"{cycle_reason}->fallback-full"
            else:
                tickets = fetch_tickets_multi_status_incremental()
                if pending_ids and not webhook_targeted:
                    cycle_reason = "webhook-wakeup-full"

            if pending_ids:
                log(
                    "INFO",
                    f"Webhook wake-up pending={pending_ids} resolved={resolved_pending_ids} "
                    f"mode={'targeted' if webhook_targeted else 'full'}",
                )

            if full_cycle_preemptible and WEBHOOK_WAKE_EVENT.is_set():
                cycle_interrupted_by_webhook = True
                log("INFO", f"Preempting full cycle before status/reply scan ({cycle_reason}) to prioritize webhook event")
            if sync_status_from_gd and not (resolved_pending_ids and webhook_targeted):
                try:
                    for pid in status_sync_project_ids:
                        if full_cycle_preemptible and WEBHOOK_WAKE_EVENT.is_set():
                            cycle_interrupted_by_webhook = True
                            log("INFO", f"Preempting status prefetch for cycle {cycle_reason} due to webhook event")
                            break
                        for tsk in gd_get_project_tasks(pid):
                            tid_ = str(tsk.get("id") or "").strip()
                            if tid_:
                                gd_tasks_by_id[tid_] = tsk
                        if cycle_interrupted_by_webhook:
                            break
                except Exception as e:
                    log("ERROR", f"Failed to fetch GoodDay project tasks for status sync: {repr(e)}")
                    gd_tasks_by_id = {}

            for t in tickets:
                if full_cycle_preemptible and WEBHOOK_WAKE_EVENT.is_set():
                    cycle_interrupted_by_webhook = True
                    log("INFO", f"Preempting ticket loop for cycle {cycle_reason} due to webhook event")
                    break
                tid = str(t.get("ticketid") or t.get("id") or "").strip()
                if not tid:
                    continue
                whmcs_tid = str(t.get("tid") or "").strip()
                guard_left, guard_source = _delete_guard_lookup(state, tid, whmcs_tid)
                if guard_left > 0 and tid not in tickets_state:
                    log(
                        "WARNING",
                        f"Ticket {tid} (tid={whmcs_tid or '-'}) skipped from create for {guard_left}s "
                        f"due delete guard source={guard_source}",
                    )
                    continue

                entry = tickets_state.setdefault(tid, {
                    "created": False,
                    "task_id": "",
                    "whmcs_tid": "",
                    "deleted_in_whmcs": False,
                    "last_reply_id": 0,
                    "baseline_done": False,
                    "gd_seen_message_ids": [],
                    "gd_task_missing_hits": 0,
                    "reply_signatures": {},
                    "whmcs_reply_to_gd_message_ids": {},
                    "gd_message_to_whmcs_reply_ids": {},
                    "gd_public_to_whmcs_reply_ids": {},
                    "gd_pending_public_reply_signatures": {},
                    "gd_hidden_edit_notes": {},
                    "gd_public_message_signatures": {},
                    "gd_mirror_message_signatures": {},
                    "gd_to_whmcs_backoff_until": 0.0,
                    "last_status_synced": "",
                    "last_gd_status_synced": "",
                    "last_whmcs_status_set_from_gd": "",
                })
                if whmcs_tid:
                    entry["whmcs_tid"] = whmcs_tid

                if entry.get("deleted_in_whmcs"):
                    continue

                # CREATE TASK
                if (not skip_whmcs_to_goodday) and sync_create and not entry.get("created", False):
                    title = f"[WHMCS #{tid}] {(t.get('subject') or '').strip() or '(no subject)'}"
                    target_project_id = resolve_project_id_for_ticket(t, dept_project_map)
                    if not target_project_id:
                        log("ERROR", f"Ticket {tid}: no GoodDay project mapping for department={(t.get('deptname') or '').strip()!r}")
                        continue

                    create_without_details_on_failure = env_bool("WHMCS_CREATE_WITHOUT_DETAILS_ON_FAILURE", "1")
                    # ✅ fetch full ticket to include initial customer message (replyid=0)
                    try:
                        full_ticket, replies = get_ticket_with_replies_retry(tid)
                    except Exception as e:
                        if not create_without_details_on_failure:
                            log("ERROR", f"Ticket {tid} create sync failed while fetching ticket details: {repr(e)}")
                            continue
                        full_ticket = t
                        replies = []
                        log("WARNING", f"Ticket {tid}: GetTicket failed, creating GoodDay task with fallback body: {repr(e)}")

                    # Prefer initial customer message when available; fallback to ticket intro
                    # so new tickets still sync even if GetTicket temporarily fails.
                    body = extract_initial_message(replies) or fmt_ticket_intro(full_ticket) or "WHMCS → GoodDay (auto)"

                    try:
                        task_id = gd_create_task(title, body, target_project_id)
                    except Exception as e:
                        log("ERROR", f"Ticket {tid} create sync failed while creating GoodDay task: {repr(e)}")
                        continue
                    entry["task_id"] = task_id
                    entry["project_id"] = target_project_id
                    entry["created"] = True
                    # baseline last_reply_id after create (so we don't spam old replies as comments)
                    if replies:
                        entry["last_reply_id"] = max(reply_id_int(r) for r in replies)
                    entry["baseline_done"] = True
                    save_state(state)

                    if task_id and task_id != "DRY_RUN_TASK_ID":
                        try:
                            gd_update_task_custom_fields(t, task_id)
                            entry["last_status_synced"] = str(t.get("status") or "").strip()
                        except Exception as e:
                            log("ERROR", f"Custom fields update failed for ticket {tid} task {task_id}: {repr(e)}")

                        # If the ticket already has customer/admin replies, push them now so they are not lost.
                        existing_reply_ids = []
                        reply_signatures = entry.setdefault("reply_signatures", {})
                        if not isinstance(reply_signatures, dict):
                            reply_signatures = {}
                        reply_to_gd_message_ids = entry.setdefault("whmcs_reply_to_gd_message_ids", {})
                        if not isinstance(reply_to_gd_message_ids, dict):
                            reply_to_gd_message_ids = {}
                        gd_to_whmcs_reply_ids = entry.setdefault("gd_message_to_whmcs_reply_ids", {})
                        if not isinstance(gd_to_whmcs_reply_ids, dict):
                            gd_to_whmcs_reply_ids = {}
                        for rpl in replies:
                            rid_txt = str(rpl.get("replyid") or rpl.get("id") or "").strip()
                            rid = reply_id_int(rpl)
                            if rid <= 0 and rid_txt != "0":
                                continue
                            try:
                                attachment_entries = extract_reply_attachment_entries(rpl)
                                upload_files = []
                                for a in attachment_entries:
                                    blob = whmcs_get_reply_attachment_blob(rpl, int(a.get("index", -1)), ticket_id=tid)
                                    if blob:
                                        upload_files.append(blob)

                                attachments = extract_reply_attachments(tid, rpl)
                                comment = fmt_reply_comment(full_ticket, rpl) + fmt_attachments_block(attachments)
                                if attachment_entries:
                                    log("INFO", f"Ticket {tid} reply {rid}: detected {len(attachment_entries)} attachment(s)")

                                posted_mid = gd_add_comment(task_id, comment, attachment_files=upload_files)
                                existing_reply_ids.append(rid)
                                reply_signatures[str(rid)] = _whmcs_reply_signature(rpl)
                                if rid > 0:
                                    key = str(rid)
                                    posted_mid = str(posted_mid or "").strip()
                                    if posted_mid:
                                        reply_to_gd_message_ids[key] = posted_mid
                                        gd_to_whmcs_reply_ids[posted_mid] = rid
                                    elif key not in reply_to_gd_message_ids:
                                        found_mid = _gd_find_whmcs_reply_message_id(task_id, tid, rid)
                                        if found_mid:
                                            reply_to_gd_message_ids[key] = found_mid
                                            gd_to_whmcs_reply_ids[found_mid] = rid
                                new_replies += 1
                            except Exception as e:
                                log("ERROR", f"Ticket {tid} existing reply sync failed rid={rid}: {repr(e)}")
                                continue

                        entry["reply_signatures"] = reply_signatures
                        entry["whmcs_reply_to_gd_message_ids"] = reply_to_gd_message_ids
                        entry["gd_message_to_whmcs_reply_ids"] = gd_to_whmcs_reply_ids
                        if existing_reply_ids:
                            log("INFO", f"Ticket {tid}: synced existing replies during create ids={existing_reply_ids}")

                    new_created += 1
                    log("INFO", f"Created GoodDay task for ticket {tid}: task_id={task_id}")

                # GOODDAY -> WHMCS early in cycle, so WHMCS->GoodDay reply fetch failures
                # do not delay public edit/delete propagation from GoodDay.
                if (
                    sync_gd_to_whmcs
                    and tid not in gd_back_checked
                    and entry.get("task_id")
                    and entry.get("task_id") != "DRY_RUN_TASK_ID"
                ):
                    backoff_left = 0 if webhook_targeted_cycle else gd_backoff_remaining_seconds(entry)
                    if backoff_left > 0:
                        log("DEBUG", f"Ticket {tid}: skipping GoodDay->WHMCS due backoff ({backoff_left}s left)")
                    else:
                        try:
                            new_public_back += sync_goodday_public_to_whmcs(tid, entry)
                            if float(entry.get("gd_to_whmcs_backoff_until", 0.0) or 0.0) > 0:
                                entry["gd_to_whmcs_backoff_until"] = 0.0
                        except Exception as e:
                            log("ERROR", f"GoodDay public -> WHMCS sync failed for ticket {tid}: {repr(e)}")
                            gd_mark_backoff(entry, e, tid)
                    gd_back_checked.add(tid)
                    if sync_status_from_gd and gd_status_map:
                        try:
                            task_obj = gd_tasks_by_id.get(str(entry.get("task_id") or "").strip(), {})
                            sync_goodday_status_to_whmcs(tid, entry, task_obj, gd_status_map)
                        except Exception as e:
                            log("ERROR", f"GoodDay status -> WHMCS sync failed for ticket {tid}: {repr(e)}")

                # REPLIES AS COMMENTS
                if (not skip_whmcs_to_goodday) and sync_replies and entry.get("task_id") and entry.get("task_id") != "DRY_RUN_TASK_ID":
                    try:
                        full_ticket, replies = get_ticket_with_replies_retry(tid)
                    except Exception as e:
                        log("ERROR", f"Ticket {tid} reply sync skipped: could not fetch replies: {repr(e)}")
                        continue
                    if not replies:
                        continue

                    last_id = int(entry.get("last_reply_id", 0) or 0)
                    sync_whmcs_edits = env_bool("SYNC_EDITS_WHMCS_TO_GOODDAY", "1")
                    sync_whmcs_deletes = env_bool("SYNC_DELETES_WHMCS_TO_GOODDAY", "1")
                    clear_stale_missing_delete_map = env_bool("WHMCS_DELETE_CLEAR_STALE_MISSING_MAP", "1")
                    reply_signatures = entry.setdefault("reply_signatures", {})
                    if not isinstance(reply_signatures, dict):
                        reply_signatures = {}
                    reply_to_gd_message_ids = entry.setdefault("whmcs_reply_to_gd_message_ids", {})
                    if not isinstance(reply_to_gd_message_ids, dict):
                        reply_to_gd_message_ids = {}
                    gd_to_whmcs_reply_ids = entry.setdefault("gd_message_to_whmcs_reply_ids", {})
                    if not isinstance(gd_to_whmcs_reply_ids, dict):
                        gd_to_whmcs_reply_ids = {}
                    gd_public_to_whmcs_reply_ids = entry.setdefault("gd_public_to_whmcs_reply_ids", {})
                    if not isinstance(gd_public_to_whmcs_reply_ids, dict):
                        gd_public_to_whmcs_reply_ids = {}
                    gd_pending_public_reply_signatures = entry.setdefault("gd_pending_public_reply_signatures", {})
                    if not isinstance(gd_pending_public_reply_signatures, dict):
                        gd_pending_public_reply_signatures = {}
                    gd_public_signatures = entry.setdefault("gd_public_message_signatures", {})
                    if not isinstance(gd_public_signatures, dict):
                        gd_public_signatures = {}
                    public_prefix = env_str("GOODDAY_PUBLIC_PREFIX", "!public").strip()
                    # Backfill reply->message mapping from reverse map so edit/delete
                    # operations can target existing mirrored messages in-place.
                    for mid_key, raw_rid in list(gd_to_whmcs_reply_ids.items()):
                        try:
                            rid_val = int(str(raw_rid).strip())
                        except Exception:
                            continue
                        if rid_val > 0 and str(rid_val) not in reply_to_gd_message_ids:
                            mid_txt = str(mid_key or "").strip()
                            if mid_txt:
                                reply_to_gd_message_ids[str(rid_val)] = mid_txt
                    gd_public_reply_ids = set()
                    gd_public_mid_by_reply_id = {}
                    for raw in list(gd_public_to_whmcs_reply_ids.values()):
                        try:
                            rid_val = int(str(raw).strip())
                        except Exception:
                            continue
                        if rid_val > 0:
                            gd_public_reply_ids.add(rid_val)
                    for mid_key, raw_rid in list(gd_public_to_whmcs_reply_ids.items()):
                        try:
                            rid_val = int(str(raw_rid).strip())
                        except Exception:
                            continue
                        mid_txt = str(mid_key or "").strip()
                        if rid_val > 0 and mid_txt and rid_val not in gd_public_mid_by_reply_id:
                            gd_public_mid_by_reply_id[rid_val] = mid_txt

                    if only_new and last_id == 0 and not entry.get("baseline_done", False):
                        entry["last_reply_id"] = max(reply_id_int(r) for r in replies)
                        entry["baseline_done"] = True
                        for base_reply in replies:
                            rid_txt = str(base_reply.get("replyid") or base_reply.get("id") or "").strip()
                            rid = reply_id_int(base_reply)
                            if rid <= 0 and rid_txt != "0":
                                continue
                            if rid_txt == "0":
                                continue
                            reply_signatures[str(rid)] = _whmcs_reply_signature(base_reply)
                        entry["reply_signatures"] = reply_signatures
                        log("DEBUG", f"Baseline last_reply_id for {tid} set to {entry['last_reply_id']} (replies={len(replies)})")
                        continue

                    current_reply_ids = set()
                    for cur_reply in replies:
                        rid_txt = str(cur_reply.get("replyid") or cur_reply.get("id") or "").strip()
                        rid = reply_id_int(cur_reply)
                        if rid <= 0 and rid_txt != "0":
                            continue
                        if rid_txt == "0":
                            continue
                        current_reply_ids.add(rid)

                    known_reply_ids = set()
                    for k in list(reply_signatures.keys()) + list(reply_to_gd_message_ids.keys()):
                        try:
                            rid = int(str(k).strip())
                        except Exception:
                            continue
                        if rid > 0:
                            known_reply_ids.add(rid)
                    for raw in list(gd_to_whmcs_reply_ids.values()):
                        try:
                            rid = int(str(raw).strip())
                        except Exception:
                            continue
                        if rid > 0:
                            known_reply_ids.add(rid)

                    deleted_source_reply_ids = sorted(rid for rid in known_reply_ids if rid not in current_reply_ids)
                    if deleted_source_reply_ids:
                        log("INFO", f"Ticket {tid}: found {len(deleted_source_reply_ids)} deleted replies ids={deleted_source_reply_ids}")

                    gd_messages_for_mapping = None
                    gd_reply_mid_by_rid = {}
                    gd_reply_map_scan_ok = False
                    if deleted_source_reply_ids and entry.get("task_id"):
                        try:
                            gd_messages_for_mapping = gd_get_task_messages(entry["task_id"])
                            gd_reply_mid_by_rid = _gd_collect_whmcs_reply_message_map(gd_messages_for_mapping)
                            gd_reply_map_scan_ok = True

                            backfilled = 0
                            for mapped_rid, mapped_mid in gd_reply_mid_by_rid.items():
                                k = str(int(mapped_rid))
                                mid_txt = str(mapped_mid or "").strip()
                                if not mid_txt:
                                    continue
                                if k not in reply_to_gd_message_ids:
                                    reply_to_gd_message_ids[k] = mid_txt
                                    backfilled += 1
                                if mid_txt not in gd_to_whmcs_reply_ids:
                                    gd_to_whmcs_reply_ids[mid_txt] = int(mapped_rid)
                            if backfilled:
                                log("INFO", f"Ticket {tid}: backfilled {backfilled} reply mappings from GoodDay history")
                        except Exception as e:
                            gd_messages_for_mapping = None
                            gd_reply_mid_by_rid = {}
                            gd_reply_map_scan_ok = False
                            log("WARNING", f"Ticket {tid}: could not preload GoodDay reply mappings: {repr(e)}")

                    for rid in deleted_source_reply_ids:
                        key = str(rid)
                        mapped_mid = str(reply_to_gd_message_ids.get(key) or "").strip()
                        if not mapped_mid:
                            for mid_key, raw_val in list(gd_to_whmcs_reply_ids.items()):
                                try:
                                    mapped_rid = int(str(raw_val).strip())
                                except Exception:
                                    continue
                                if mapped_rid == rid:
                                    mapped_mid = str(mid_key or "").strip()
                                    if mapped_mid:
                                        reply_to_gd_message_ids[key] = mapped_mid
                                    break
                        if not mapped_mid:
                            mapped_mid = str(gd_reply_mid_by_rid.get(rid) or "").strip()
                            if mapped_mid:
                                reply_to_gd_message_ids[key] = mapped_mid
                                gd_to_whmcs_reply_ids[mapped_mid] = rid
                        if not mapped_mid:
                            mapped_mid = _gd_find_whmcs_reply_message_id(
                                entry["task_id"],
                                tid,
                                rid,
                                messages=gd_messages_for_mapping,
                                reply_map=gd_reply_mid_by_rid if gd_reply_mid_by_rid else None,
                            )
                            if mapped_mid:
                                reply_to_gd_message_ids[key] = mapped_mid
                                gd_to_whmcs_reply_ids[mapped_mid] = rid

                        if sync_whmcs_deletes:
                            if not mapped_mid:
                                if clear_stale_missing_delete_map and gd_reply_map_scan_ok:
                                    log("INFO", f"Ticket {tid} delete stale mapping cleared rid={rid}: no GoodDay message found in history")
                                    reply_signatures.pop(key, None)
                                    reply_to_gd_message_ids.pop(key, None)
                                    continue
                                log("WARNING", f"Ticket {tid} delete sync pending rid={rid}: mapped GoodDay message id not found yet")
                                continue
                            try:
                                del_res = gd_delete_task_message(entry["task_id"], mapped_mid)
                                if str((del_res or {}).get("result") or "").strip().lower() == "not-found":
                                    log("WARNING", f"GoodDay message {mapped_mid} already missing while deleting for removed WHMCS reply {rid} ticket {tid}")
                                else:
                                    log("INFO", f"Deleted GoodDay message {mapped_mid} for removed WHMCS reply {rid} ticket {tid}")
                            except Exception as e:
                                log("ERROR", f"Ticket {tid} delete sync failed rid={rid} message_id={mapped_mid}: {repr(e)}")
                                continue

                        reply_signatures.pop(key, None)
                        reply_to_gd_message_ids.pop(key, None)
                        if mapped_mid:
                            gd_to_whmcs_reply_ids.pop(mapped_mid, None)

                    new_reply_ids = []
                    edited_reply_ids = []
                    actions = []
                    for rpl in replies:
                        rid_txt = str(rpl.get("replyid") or rpl.get("id") or "").strip()
                        rid = reply_id_int(rpl)

                        # Skip invalid and initial-message pseudo-reply from this sync loop.
                        if rid <= 0 and rid_txt != "0":
                            continue
                        if rid_txt == "0":
                            continue

                        key = str(rid)
                        sig_now = _whmcs_reply_signature(rpl)
                        sig_prev = str(reply_signatures.get(key) or "").strip()

                        # Recover pending GoodDay-public -> WHMCS mappings when AddTicketReply
                        # succeeds but does not return replyid, to avoid mirror duplicates.
                        if rid not in gd_public_reply_ids and gd_pending_public_reply_signatures:
                            reply_body_canon = _whmcs_canonical_message_for_match(rpl.get("message"))
                            if reply_body_canon:
                                matched_mid = ""
                                for pending_mid, pending_sig in list(gd_pending_public_reply_signatures.items()):
                                    pending_canon = _normalize_text_for_compare(pending_sig)
                                    if pending_canon == reply_body_canon:
                                        matched_mid = str(pending_mid or "").strip()
                                        break
                                    if pending_canon and (pending_canon in reply_body_canon or reply_body_canon in pending_canon):
                                        matched_mid = str(pending_mid or "").strip()
                                        break
                                if not matched_mid and len(gd_pending_public_reply_signatures) == 1 and rid in new_reply_ids:
                                    # Fallback when WHMCS AddTicketReply did not return replyid and
                                    # the message got edited before first exact-signature recovery.
                                    matched_mid = str(next(iter(gd_pending_public_reply_signatures.keys())) or "").strip()
                                if matched_mid:
                                    gd_public_to_whmcs_reply_ids[matched_mid] = rid
                                    gd_public_mid_by_reply_id[rid] = matched_mid
                                    gd_pending_public_reply_signatures.pop(matched_mid, None)
                                    gd_public_reply_ids.add(rid)
                                    reply_signatures[key] = sig_now
                                    last_id = max(last_id, rid)
                                    log("INFO", f"Recovered pending public mapping mid={matched_mid} -> reply={rid} ticket {tid}")
                                    continue

                        # Replies created from GoodDay public flow should not be re-posted
                        # as new comments. For edited replies, update the same GoodDay
                        # public message in-place to keep both sides aligned.
                        if rid in gd_public_reply_ids:
                            is_new_public = rid > last_id
                            is_edited_public = bool(sig_prev and sig_prev != sig_now)
                            public_mid = str(gd_public_mid_by_reply_id.get(rid) or "").strip()

                            if is_edited_public and sync_whmcs_edits and public_mid:
                                try:
                                    whmcs_plain = _whmcs_message_to_text(rpl.get("message"))
                                    public_body = _strip_public_prefix(whmcs_plain, public_prefix).strip()
                                    if public_prefix:
                                        gd_text = f"{public_prefix} {public_body}".strip() if public_body else public_prefix
                                    else:
                                        gd_text = public_body
                                    if gd_text:
                                        gd_update_task_message(entry["task_id"], public_mid, gd_text)
                                        gd_public_signatures[public_mid] = _gd_public_message_signature(public_body, [])
                                        new_replies += 1
                                        log("INFO", f"Updated GoodDay public message {public_mid} for edited WHMCS reply {rid} ticket {tid}")
                                except Exception as e:
                                    log("ERROR", f"Ticket {tid} public edit-in-place failed rid={rid} message_id={public_mid}: {repr(e)}")

                            reply_signatures[key] = sig_now
                            last_id = max(last_id, rid)
                            continue

                        is_new = rid > last_id
                        is_edited = bool(sig_prev and sig_prev != sig_now)

                        if not is_new and not is_edited:
                            if not sig_prev:
                                reply_signatures[key] = sig_now
                            continue

                        if is_edited and not sync_whmcs_edits:
                            reply_signatures[key] = sig_now
                            continue

                        actions.append({
                            "reply": rpl,
                            "rid": rid,
                            "is_edited": is_edited,
                            "signature": sig_now,
                        })
                        if is_new:
                            new_reply_ids.append(rid)
                        if is_edited:
                            edited_reply_ids.append(rid)

                    if new_reply_ids:
                        log("INFO", f"Ticket {tid}: found {len(new_reply_ids)} new replies ids={new_reply_ids}")
                    if edited_reply_ids:
                        log("INFO", f"Ticket {tid}: found {len(edited_reply_ids)} edited replies ids={edited_reply_ids}")

                    for action in actions:
                        rpl = action["reply"]
                        rid = int(action["rid"])
                        is_edited = bool(action["is_edited"])
                        sig_now = str(action["signature"] or "")
                        try:
                            attachment_entries = extract_reply_attachment_entries(rpl)
                            upload_files = []
                            for a in attachment_entries:
                                blob = whmcs_get_reply_attachment_blob(rpl, int(a.get("index", -1)), ticket_id=tid)
                                if blob:
                                    upload_files.append(blob)

                            attachments = extract_reply_attachments(tid, rpl)
                            base_comment = fmt_reply_comment(full_ticket, rpl) + fmt_attachments_block(attachments)
                            edit_comment = fmt_reply_edit_comment(full_ticket, rpl) + fmt_attachments_block(attachments)
                            if attachment_entries:
                                log("INFO", f"Ticket {tid} reply {rid}: detected {len(attachment_entries)} attachment(s)")

                            key = str(rid)
                            if is_edited:
                                mapped_mid = str(reply_to_gd_message_ids.get(key) or "").strip()
                                if not mapped_mid:
                                    mapped_mid = _gd_find_whmcs_reply_message_id(
                                        entry["task_id"],
                                        tid,
                                        rid,
                                        messages=gd_messages_for_mapping,
                                        reply_map=gd_reply_mid_by_rid if gd_reply_mid_by_rid else None,
                                    )
                                    if mapped_mid:
                                        reply_to_gd_message_ids[key] = mapped_mid
                                        gd_to_whmcs_reply_ids[mapped_mid] = rid

                                if mapped_mid:
                                    gd_to_whmcs_reply_ids[mapped_mid] = rid
                                    try:
                                        gd_update_task_message(entry["task_id"], mapped_mid, base_comment)
                                        new_replies += 1
                                        last_id = max(last_id, rid)
                                        reply_signatures[key] = sig_now
                                        log("INFO", f"Updated GoodDay message {mapped_mid} for edited reply {rid} ticket {tid} -> task {entry['task_id']}")
                                        continue
                                    except Exception as e:
                                        log("ERROR", f"Ticket {tid} edit-in-place failed rid={rid} message_id={mapped_mid}: {repr(e)}")
                                        if not env_bool("WHMCS_EDIT_FALLBACK_NEW_COMMENT", "0"):
                                            continue
                                else:
                                    log("WARNING", f"Ticket {tid} edited reply {rid}: no mapped GoodDay message id found")
                                    if not env_bool("WHMCS_EDIT_FALLBACK_NEW_COMMENT", "0"):
                                        continue

                            comment_to_post = edit_comment if is_edited else base_comment
                            posted_mid = gd_add_comment(entry["task_id"], comment_to_post, attachment_files=upload_files)
                            new_replies += 1
                            last_id = max(last_id, rid)
                            reply_signatures[key] = sig_now
                            posted_mid = str(posted_mid or "").strip()
                            if posted_mid:
                                reply_to_gd_message_ids[key] = posted_mid
                                gd_to_whmcs_reply_ids[posted_mid] = rid
                            elif not is_edited:
                                found_mid = _gd_find_whmcs_reply_message_id(
                                    entry["task_id"],
                                    tid,
                                    rid,
                                    messages=gd_messages_for_mapping,
                                    reply_map=gd_reply_mid_by_rid if gd_reply_mid_by_rid else None,
                                )
                                if found_mid:
                                    reply_to_gd_message_ids[key] = found_mid
                                    gd_to_whmcs_reply_ids[found_mid] = rid
                            log("INFO", f"Synced {'edited ' if is_edited else ''}reply {rid} for ticket {tid} -> task {entry['task_id']}")
                        except Exception as e:
                            log("ERROR", f"Ticket {tid} reply sync failed rid={rid}: {repr(e)}")
                            continue

                    entry["last_reply_id"] = last_id
                    entry["reply_signatures"] = reply_signatures
                    entry["whmcs_reply_to_gd_message_ids"] = reply_to_gd_message_ids
                    entry["gd_message_to_whmcs_reply_ids"] = gd_to_whmcs_reply_ids
                    entry["gd_public_to_whmcs_reply_ids"] = gd_public_to_whmcs_reply_ids
                    entry["gd_pending_public_reply_signatures"] = gd_pending_public_reply_signatures
                    entry["gd_public_message_signatures"] = gd_public_signatures

                # STATUS -> GOODDAY
                if (not skip_whmcs_to_goodday) and entry.get("task_id") and entry.get("task_id") != "DRY_RUN_TASK_ID":
                    try:
                        sync_ticket_status_to_goodday(t, entry)
                    except Exception as e:
                        log("ERROR", f"Status sync failed for ticket {tid}: {repr(e)}")

                # GOODDAY -> WHMCS (explicit public messages only)
                if (
                    sync_gd_to_whmcs
                    and tid not in gd_back_checked
                    and entry.get("task_id")
                    and entry.get("task_id") != "DRY_RUN_TASK_ID"
                ):
                    try:
                        new_public_back += sync_goodday_public_to_whmcs(tid, entry)
                    except Exception as e:
                        log("ERROR", f"GoodDay public -> WHMCS sync failed for ticket {tid}: {repr(e)}")
                    gd_back_checked.add(tid)
                    if sync_status_from_gd and gd_status_map:
                        try:
                            task_obj = gd_tasks_by_id.get(str(entry.get("task_id") or "").strip(), {})
                            sync_goodday_status_to_whmcs(tid, entry, task_obj, gd_status_map)
                        except Exception as e:
                            log("ERROR", f"GoodDay status -> WHMCS sync failed for ticket {tid}: {repr(e)}")

            # Also check GoodDay->WHMCS for mapped tickets that may not appear
            # in current WHMCS status filters (e.g. moved to another status).
            if sync_gd_to_whmcs:
                targeted_filter = set(resolved_pending_ids) if (resolved_pending_ids and webhook_targeted) else set()
                for state_tid, state_entry in tickets_state.items():
                    if full_cycle_preemptible and WEBHOOK_WAKE_EVENT.is_set():
                        cycle_interrupted_by_webhook = True
                        log("INFO", f"Preempting mapped-ticket GoodDay->WHMCS pass for cycle {cycle_reason} due to webhook event")
                        break
                    tid = str(state_tid).strip()
                    if not tid or tid in gd_back_checked:
                        continue
                    if targeted_filter and tid not in targeted_filter:
                        continue
                    if not isinstance(state_entry, dict):
                        continue
                    if state_entry.get("deleted_in_whmcs"):
                        continue
                    if not state_entry.get("task_id") or state_entry.get("task_id") == "DRY_RUN_TASK_ID":
                        continue
                    backoff_left = 0 if webhook_targeted_cycle else gd_backoff_remaining_seconds(state_entry)
                    if backoff_left > 0:
                        log("DEBUG", f"Ticket {tid}: skipping GoodDay->WHMCS due backoff ({backoff_left}s left)")
                    else:
                        try:
                            new_public_back += sync_goodday_public_to_whmcs(tid, state_entry)
                            if float(state_entry.get("gd_to_whmcs_backoff_until", 0.0) or 0.0) > 0:
                                state_entry["gd_to_whmcs_backoff_until"] = 0.0
                        except Exception as e:
                            log("ERROR", f"GoodDay public -> WHMCS sync failed for ticket {tid}: {repr(e)}")
                            gd_mark_backoff(state_entry, e, tid)
                    if sync_status_from_gd and gd_status_map:
                        try:
                            task_obj = gd_tasks_by_id.get(str(state_entry.get("task_id") or "").strip(), {})
                            sync_goodday_status_to_whmcs(tid, state_entry, task_obj, gd_status_map)
                        except Exception as e:
                            log("ERROR", f"GoodDay status -> WHMCS sync failed for ticket {tid}: {repr(e)}")

            dropped_ticket_ids = []
            for drop_tid, drop_entry in list(tickets_state.items()):
                if not isinstance(drop_entry, dict):
                    continue
                if drop_entry.pop("_drop_from_state", False):
                    tomb = drop_entry.pop("_deleted_tombstone", None)
                    if isinstance(tomb, dict):
                        _mark_deleted_ticket_tombstone(
                            state,
                            ticket_id=str(drop_tid or "").strip(),
                            whmcs_tid=str(tomb.get("whmcs_tid") or drop_entry.get("whmcs_tid") or "").strip(),
                            task_id=str(tomb.get("task_id") or "").strip(),
                            source=str(tomb.get("source") or "goodday_task_missing").strip(),
                        )
                    tickets_state.pop(drop_tid, None)
                    dropped_ticket_ids.append(str(drop_tid))
            if dropped_ticket_ids:
                log("INFO", f"Dropped {len(dropped_ticket_ids)} ticket(s) from local state after full delete sync ids={dropped_ticket_ids}")

            log(
                "INFO",
                f"Cycle done. created={new_created} replies_sent={new_replies} "
                f"gd_public_to_whmcs={new_public_back} reason={cycle_reason} "
                f"interrupted={int(cycle_interrupted_by_webhook)} next_poll_in={poll_seconds}s",
            )

        except Exception as e:
            log("ERROR", f"{repr(e)}")
        finally:
            # Persist progress even on partial-cycle failures (timeouts/network errors),
            # so we do not lose reply/message mappings and re-process the same edits.
            try:
                save_state(state)
            except Exception as save_err:
                log("ERROR", f"Failed to persist state: {repr(save_err)}")

        if webhook_enabled:
            if WEBHOOK_WAKE_EVENT.is_set():
                WEBHOOK_WAKE_EVENT.clear()
                log("INFO", "Wake-up triggered by webhook event")
                continue
            woke = WEBHOOK_WAKE_EVENT.wait(timeout=max(poll_seconds, 1))
            if woke:
                WEBHOOK_WAKE_EVENT.clear()
                log("INFO", "Wake-up triggered by webhook event")
        else:
            time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
