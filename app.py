#!/usr/bin/env python3
"""
Ring Video Archiver — a tiny local web app to bulk-download and preserve
Ring camera footage before Ring's rolling retention window deletes it.

How it works:
  * Starts a small web server on your own computer (nothing is uploaded anywhere).
  * Opens your browser to a friendly page: sign in, pick a camera, click Download.
  * Downloads OLDEST videos FIRST (the ones about to expire), validates every
    file it writes, skips anything already saved-and-intact, re-fetches partials,
    and adaptively throttles itself so Ring doesn't rate-limit the account.
  * Writes a manifest.csv so you have a record of everything pulled.

Privacy/security:
  * Your Ring email + password are sent only to Ring's own servers, via the
    `ring_doorbell` library. They are never written to disk by this tool.
  * After the first login (with the 2FA text code) a *token* is cached in
    ring_token.json so you don't have to log in again. Treat that file like a
    password — delete it when you're done if you like.

Run it:
  pip install -r requirements.txt
  python3 app.py
  (or just double-click start-mac.command / start-windows.bat)

Preview the interface WITHOUT a Ring account:
  RING_ARCHIVER_DEMO=1 python3 app.py
"""

import asyncio
import csv
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# TLS fix: a packaged (PyInstaller) app doesn't ship OpenSSL's default CA
# bundle, so HTTPS verification of Ring's servers fails with
# "CERTIFICATE_VERIFY_FAILED / unable to get local issuer certificate".
# Point OpenSSL (used by aiohttp) at certifi's bundled roots. Must run before
# any TLS connection is created.
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
except Exception:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

APP_VERSION = "2.2"                       # bump on each release; compared to GitHub
GITHUB_REPO = "cfiorelli/ring-archiver"
RELEASES_PAGE = "https://github.com/%s/releases/latest" % GITHUB_REPO
LATEST_API = "https://api.github.com/repos/%s/releases/latest" % GITHUB_REPO
RELEASES_API = "https://api.github.com/repos/%s/releases?per_page=20" % GITHUB_REPO
HELP_URL = "https://cfiorelli.github.io/ring-archiver/"     # the step-by-step guide page
PORTFOLIO_URL = "https://github.com/cfiorelli"              # TODO: swap for real portfolio URL

HERE = Path(__file__).resolve().parent
# When packaged with PyInstaller, bundled assets (web/) live under sys._MEIPASS.
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", HERE))
WEB_DIR = BUNDLE_DIR / "web"
# The login token must persist in a writable place (NOT the temp bundle dir).
DATA_DIR = Path.home() / ".ring-archiver"
DATA_DIR.mkdir(parents=True, exist_ok=True)
try:
    os.chmod(DATA_DIR, 0o700)  # token dir: owner-only
except Exception:
    pass
TOKEN_FILE = DATA_DIR / "ring_token.json"
DEFAULT_DEST = Path.home() / "Ring Videos"
USER_AGENT = "RingVideoArchiver/1.0"
PORT = int(os.environ.get("RING_ARCHIVER_PORT", "8765"))
ACTIVE_PORT = PORT   # the port we actually bound (may differ if PORT was busy)
DEMO = os.environ.get("RING_ARCHIVER_DEMO") == "1"

# Be gentle with Ring's (unofficial) API. The throttle is ADAPTIVE: it starts at
# the floor and backs off automatically when Ring rate-limits, then eases down.
THROTTLE_SECONDS = float(os.environ.get("RING_ARCHIVER_THROTTLE", "1.0"))   # floor
THROTTLE_CEIL = float(os.environ.get("RING_ARCHIVER_THROTTLE_CEIL", "8"))   # max
MAX_RETRIES = int(os.environ.get("RING_ARCHIVER_RETRIES", "5"))
HISTORY_PAGE = 100  # events fetched per history request
AVG_CLIP_BYTES = 12 * 1024 * 1024  # rough per-clip estimate for free-space preflight

# ---------------------------------------------------------------------------
# Shared progress state (read by the browser via /api/progress)
# ---------------------------------------------------------------------------

STATE_LOCK = threading.Lock()
STATE = {
    "phase": "login",        # login | choose | scanning | downloading | paused
                             # | reauth | done | error
    "signed_in": False,
    "cameras": [],           # [{id, name}]
    "total": 0,
    "done": 0,
    "skipped": 0,
    "already": 0,            # already-saved-and-intact files found on resume
    "retried": 0,
    "failed": 0,
    "failed_recoverable": 0, # rate-limit/network — worth re-running for
    "failed_permanent": 0,   # Ring no longer has the recording
    "current": "",
    "eta_seconds": None,
    "dest": str(DEFAULT_DEST),
    "message": "",
    "warning": "",           # non-blocking heads-up (e.g. low disk space)
    "error": "",
    "needs_otp": False,
    "needs_reauth": False,   # session expired mid-run; waiting for re-login
    "verifying": False,
    "version": APP_VERSION,
}
PAUSE = threading.Event()
STOP = threading.Event()
REAUTH = threading.Event()   # set while a run is paused waiting for re-login
WORKER = None                # the active download thread (one run at a time)


def update(**kw):
    with STATE_LOCK:
        STATE.update(kw)


def snapshot():
    with STATE_LOCK:
        return dict(STATE)


def _human(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return "%.0f %s" % (n, unit)
        n /= 1024
    return "%.1f PB" % n


def diag(dest_path, line):
    """Append a line to a local (never-uploaded) diagnostics log."""
    try:
        with open(dest_path / "diagnostics.log", "a") as f:
            f.write("%s\t%s\n" % (datetime.now().isoformat(timespec="seconds"), line))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# MP4 structural validation (pure Python, no external tools required)
# ---------------------------------------------------------------------------

def mp4_status(path):
    """Walk top-level MP4 boxes. Returns ok / empty / truncated / corrupt /
    no-moov / missing. A complete file's boxes tile it exactly and include
    a 'moov' atom — a cut-off download fails one of those checks."""
    try:
        size = os.path.getsize(path)
    except OSError:
        return "missing"
    if size == 0:
        return "empty"
    saw_moov = False
    try:
        with open(path, "rb") as f:
            offset = 0
            while offset < size:
                f.seek(offset)
                header = f.read(8)
                if len(header) < 8:
                    return "truncated"
                box_size, box_type = struct.unpack(">I4s", header)
                if box_size == 1:               # 64-bit largesize
                    ext = f.read(8)
                    if len(ext) < 8:
                        return "truncated"
                    box_size = struct.unpack(">Q", ext)[0]
                elif box_size == 0:             # legal: final box runs to EOF
                    box_size = size - offset
                if box_size < 8:
                    return "corrupt"
                if box_type == b"moov":
                    saw_moov = True
                if offset + box_size > size:
                    return "truncated"
                offset += box_size
    except OSError:
        return "corrupt"
    return "ok" if saw_moov else "no-moov"


def _safe_unlink(path):
    try:
        path.unlink()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Adaptive rate control + error classification
# ---------------------------------------------------------------------------

class RateController:
    """AIMD throttle: additive-ish ease-down on success, multiplicative
    back-off when Ring rate-limits. Shared across one download run."""

    def __init__(self, floor, ceil):
        self.delay = floor
        self.floor = floor
        self.ceil = ceil
        self._lock = threading.Lock()

    def current(self):
        with self._lock:
            return self.delay

    def wait(self):
        time.sleep(self.current())

    def ok(self):
        with self._lock:
            self.delay = max(self.floor, self.delay * 0.6)

    def slow(self, retry_after=0.0):
        with self._lock:
            self.delay = min(self.ceil, max(self.delay * 2.0, retry_after, self.floor))
            return self.delay


def _retry_after(e):
    h = getattr(e, "headers", None)
    if h:
        try:
            ra = h.get("Retry-After")
            return float(ra) if ra else 0.0
        except Exception:
            return 0.0
    return 0.0


def classify_error(e):
    """Map an exception to (kind, status). kind in:
    nospace | auth | gone | rate | retry."""
    status = getattr(e, "status", None) or getattr(e, "code", None)
    msg = str(e) or e.__class__.__name__
    name = e.__class__.__name__.lower()
    low = msg.lower()
    if status is None:
        m = re.search(r"\b([45]\d\d)\b", msg)
        status = int(m.group(1)) if m else None
    if getattr(e, "errno", None) == 28 or "no space left" in low:
        return "nospace", status
    if (status in (401, 403) or "authent" in low or "token" in low or "2fa" in low
            or "unauthor" in low or "authent" in name or "requires2fa" in name):
        return "auth", status
    if (status == 404 or "not found" in low or "no recording" in low
            or "no such" in low or "404" in low):
        return "gone", status
    if (status in (429, 503) or "too many" in low or "rate limit" in low
            or "ratelimit" in low or "throttl" in low):
        return "rate", status
    return "retry", status


# ---------------------------------------------------------------------------
# Keep the computer awake while downloading (cross-platform, best-effort)
# ---------------------------------------------------------------------------

class KeepAwake:
    """Hold a system wake lock for the lifetime of a download so the machine
    doesn't sleep mid-run. macOS: caffeinate. Windows: SetThreadExecutionState
    (must be called on the running thread). Linux: systemd-inhibit if present."""

    def __init__(self):
        self._proc = None
        self._win = False

    def __enter__(self):
        try:
            if sys.platform == "darwin":
                self._proc = subprocess.Popen(
                    ["caffeinate", "-dimsu"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif os.name == "nt":
                import ctypes
                ES_CONTINUOUS = 0x80000000
                ES_SYSTEM_REQUIRED = 0x00000001
                ES_DISPLAY_REQUIRED = 0x00000002
                ctypes.windll.kernel32.SetThreadExecutionState(
                    ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED)
                self._win = True
            elif sys.platform.startswith("linux") and shutil.which("systemd-inhibit"):
                self._proc = subprocess.Popen(
                    ["systemd-inhibit", "--what=idle:sleep:handle-lid-switch",
                     "--why=Ring Video Archiver download", "sleep", "2147483647"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            self._proc = None
        return self

    def __exit__(self, *a):
        try:
            if self._proc:
                self._proc.terminate()
            if self._win:
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Update check (notify-only; opens the download page in the browser)
# ---------------------------------------------------------------------------

_UPDATE_CACHE = {}


def _version_gt(a, b):
    def parts(v):
        out = []
        for p in re.split(r"[._]", (v or "").strip()):
            try:
                out.append(int(p))
            except ValueError:
                out.append(0)
        return out
    pa, pb = parts(a), parts(b)
    n = max(len(pa), len(pb))
    pa += [0] * (n - len(pa))
    pb += [0] * (n - len(pb))
    return pa > pb


def check_update():
    if _UPDATE_CACHE:
        return _UPDATE_CACHE
    res = {"update_available": False, "latest_version": "",
           "release_url": RELEASES_PAGE, "version": APP_VERSION}
    try:
        import urllib.request
        req = urllib.request.Request(
            LATEST_API, headers={"User-Agent": USER_AGENT,
                                  "Accept": "application/vnd.github+json"})
        with urllib.request.urlopen(req, timeout=4) as r:
            data = json.loads(r.read().decode("utf-8"))
        tag = (data.get("tag_name") or "").lstrip("vV")
        res["latest_version"] = tag
        res["release_url"] = data.get("html_url") or RELEASES_PAGE
        res["update_available"] = bool(tag) and _version_gt(tag, APP_VERSION)
    except Exception:
        pass
    _UPDATE_CACHE.update(res)
    return res


_RELEASES_CACHE = []


def list_releases():
    """Version history from GitHub Releases: [{version, date, notes, url}]."""
    if _RELEASES_CACHE:
        return _RELEASES_CACHE
    out = []
    try:
        import urllib.request
        req = urllib.request.Request(
            RELEASES_API, headers={"User-Agent": USER_AGENT,
                                   "Accept": "application/vnd.github+json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read().decode("utf-8"))
        for rel in data:
            if rel.get("draft"):
                continue
            out.append({
                "version": (rel.get("tag_name") or "").lstrip("vV"),
                "date": (rel.get("published_at") or "")[:10],
                "notes": (rel.get("body") or "").strip(),
                "url": rel.get("html_url") or RELEASES_PAGE,
            })
    except Exception:
        pass
    _RELEASES_CACHE[:] = out
    return out


def notify_done(state):
    """Best-effort native notification (the browser UI also fires one)."""
    try:
        n = state.get("total", 0)
        fp = state.get("failed_permanent", 0)
        msg = "Saved %d videos." % n
        if fp:
            msg += " %d are no longer on Ring." % fp
        if sys.platform == "darwin":
            subprocess.Popen(
                ["osascript", "-e",
                 'display notification "%s" with title "Ring Video Archiver"'
                 % msg.replace('"', "'")],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Destination / drive handling
# ---------------------------------------------------------------------------

def _volume_label(path):
    s = str(path)
    if sys.platform == "darwin":
        m = re.match(r"^/Volumes/([^/]+)", s)
        return m.group(1) if m else "This Mac"
    if os.name == "nt":
        m = re.match(r"^([A-Za-z]):", s)
        return ("Drive %s:" % m.group(1).upper()) if m else "This PC"
    return "This computer"


def _expected_mount(path):
    """If `path` is meant to live on a removable/external volume, return that
    volume's mount root; else None (internal disk — no mount guard needed)."""
    s = str(path)
    if sys.platform == "darwin":
        m = re.match(r"^(/Volumes/[^/]+)", s)
        return m.group(1) if m else None
    if os.name == "nt":
        m = re.match(r"^([A-Za-z]:[\\/])", s)
        if m and m.group(1)[0].upper() != "C":
            return m.group(1)
        return None
    if sys.platform.startswith("linux"):
        m = re.match(r"^(/(?:media|run/media|mnt)/[^/]+(?:/[^/]+)?)", s)
        return m.group(1) if m else None
    return None


def _is_mounted(root):
    try:
        return os.path.ismount(root)
    except Exception:
        return os.path.exists(root)


def mount_problem(dest):
    """Return a user-facing warning if dest targets a drive that isn't currently
    connected — this prevents silently creating the folder (and writing gigabytes)
    on the internal disk when the external drive is unplugged."""
    root = _expected_mount(Path(dest).expanduser())
    if root and not _is_mounted(root):
        return ("That drive (%s) doesn't look connected. Plug it in, or choose "
                "another folder." % root)
    return None


def dest_info(dest):
    """Live readout for the chosen destination: which volume, free space, and
    whether that volume is actually mounted."""
    p = Path(dest).expanduser()
    problem = mount_problem(dest)
    info = {"dest": str(p), "volume": _volume_label(p),
            "mounted": problem is None, "free": 0, "free_h": "",
            "warning": problem or ""}
    # dest may not exist yet — probe the nearest existing ancestor.
    probe = p
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        du = shutil.disk_usage(str(probe))
        info["free"] = du.free
        info["free_h"] = _human(du.free)
    except Exception:
        pass
    return info


def list_volumes():
    """Save targets to offer in the UI: the home disk plus any mounted
    external/removable volumes, each with free space."""
    vols = []
    seen = set()

    def add(path, label):
        try:
            p = Path(path)
            if not p.is_dir():
                return
            rp = str(p.resolve())
            if rp in seen:
                return
            seen.add(rp)
            du = shutil.disk_usage(str(p))
            vols.append({"path": str(p), "label": label,
                         "free": du.free, "free_h": _human(du.free)})
        except Exception:
            pass

    add(str(Path.home()),
        "This Mac" if sys.platform == "darwin"
        else ("This PC" if os.name == "nt" else "Home folder"))
    try:
        if sys.platform == "darwin":
            home_dev = os.stat(str(Path.home())).st_dev
            vroot = Path("/Volumes")
            if vroot.exists():
                for v in sorted(vroot.iterdir()):
                    try:
                        if os.stat(str(v)).st_dev == home_dev:
                            continue  # same as internal disk, already listed
                    except OSError:
                        continue
                    add(str(v), v.name)
        elif os.name == "nt":
            import string
            sysdrive = os.environ.get("SystemDrive", "C:").rstrip("\\/").upper()
            for letter in string.ascii_uppercase:
                root = "%s:\\" % letter
                if os.path.exists(root) and ("%s:" % letter) != sysdrive:
                    add(root, "Drive %s:" % letter)
        elif sys.platform.startswith("linux"):
            for base in ("/media", "/run/media", "/mnt"):
                b = Path(base)
                if not b.exists():
                    continue
                for v in b.iterdir():
                    if not v.is_dir():
                        continue
                    subs = [s for s in v.iterdir() if s.is_dir()] if base != "/mnt" else []
                    if subs:
                        for s in subs:
                            add(str(s), s.name)
                    else:
                        add(str(v), v.name)
    except Exception:
        pass
    return vols


_CHOOSE_LOCK = threading.Lock()


def choose_folder():
    """Open the OS-native folder chooser and return the chosen absolute path,
    or None if the user canceled / no dialog is available. Blocks until the user
    responds — fine because the server is threaded (one thread per request).
    A non-blocking lock ensures only ONE dialog is ever open at a time, so rapid
    repeat clicks can't queue a backlog of Finder windows."""
    if not _CHOOSE_LOCK.acquire(blocking=False):
        return None
    try:
        if sys.platform == "darwin":
            script = ('POSIX path of (choose folder with prompt '
                      '"Choose where to save your Ring videos")')
            r = subprocess.run(["osascript", "-e", script],
                               capture_output=True, text=True)
            out = r.stdout.strip()
            return str(Path(out)) if r.returncode == 0 and out else None
        if os.name == "nt":
            ps = (
                "Add-Type -AssemblyName System.Windows.Forms;"
                "$f=New-Object System.Windows.Forms.FolderBrowserDialog;"
                "$f.Description='Choose where to save your Ring videos';"
                "$f.ShowNewFolderButton=$true;"
                "if($f.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK)"
                "{[Console]::Out.Write($f.SelectedPath)}"
            )
            r = subprocess.run(
                ["powershell", "-NoProfile", "-STA", "-Command", ps],
                capture_output=True, text=True)
            out = r.stdout.strip()
            return str(Path(out)) if out else None
        if sys.platform.startswith("linux") and shutil.which("zenity"):
            r = subprocess.run(
                ["zenity", "--file-selection", "--directory",
                 "--title=Choose where to save your Ring videos"],
                capture_output=True, text=True)
            out = r.stdout.strip()
            return str(Path(out)) if out else None
    except Exception:
        return None
    finally:
        _CHOOSE_LOCK.release()
    return None


# ---------------------------------------------------------------------------
# Async bridge: run a single asyncio loop in a background thread so the
# synchronous HTTP handlers can call the async `ring_doorbell` API.
# ---------------------------------------------------------------------------

class RingBridge:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        self.auth = None
        self.ring = None
        self._creds = {}  # held in memory only between password step and OTP step

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def call(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result()

    # -- auth -------------------------------------------------------------
    def _token_updated(self, token):
        try:
            TOKEN_FILE.write_text(json.dumps(token))
            os.chmod(TOKEN_FILE, 0o600)  # refresh token: owner read/write only
        except Exception:
            pass

    async def _make_ring(self, token=None):
        from ring_doorbell import Auth, Ring
        self.auth = Auth(USER_AGENT, token, self._token_updated)
        self.ring = Ring(self.auth)

    async def _try_cached_token(self):
        if not TOKEN_FILE.exists():
            return False
        try:
            token = json.loads(TOKEN_FILE.read_text())
            await self._make_ring(token)
            await self.ring.async_update_data()
            return True
        except Exception:
            return False

    async def _login(self, email, password, otp):
        from ring_doorbell import Auth, Ring
        from ring_doorbell.exceptions import Requires2FAError
        if self.auth is None:
            self.auth = Auth(USER_AGENT, None, self._token_updated)
        try:
            if otp:
                await self.auth.async_fetch_token(email, password, otp)
            else:
                await self.auth.async_fetch_token(email, password)
        except Requires2FAError:
            return {"ok": False, "needs_otp": True}
        self.ring = Ring(self.auth)
        await self.ring.async_update_data()
        return {"ok": True}

    async def _cameras(self):
        await self.ring.async_update_data()
        cams = []
        # Preferred: the library's canonical list of all video-capable devices.
        try:
            for d in self.ring.video_devices():
                cid = getattr(d, "id", None)
                name = getattr(d, "name", None) or ("Camera %s" % cid)
                if cid is not None:
                    cams.append({"id": cid, "name": name, "_obj": d})
        except Exception:
            pass
        # Fallback: scan device buckets in case a library version differs.
        if not cams:
            devices = self.ring.devices()
            for attr in ("video_devices", "stickup_cams", "doorbots",
                         "authorized_doorbots", "other"):
                bucket = getattr(devices, attr, None)
                if not bucket:
                    continue
                try:
                    items = list(bucket)
                except TypeError:
                    continue
                for d in items:
                    cid = getattr(d, "id", None) or getattr(d, "device_id", None)
                    name = getattr(d, "name", None) or "Camera %s" % cid
                    if cid is not None and not any(c["id"] == cid for c in cams):
                        cams.append({"id": cid, "name": name, "_obj": d})
        return cams

    def _camera_obj(self, cams, camera_id):
        for c in cams:
            if str(c["id"]) == str(camera_id):
                return c["_obj"]
        return None

    async def _all_events(self, cam, start_dt=None, end_dt=None):
        """Page through history, newest->oldest, keep events inside the
        [start_dt, end_dt] window, then return OLDEST first."""
        events = []
        older_than = None
        while not STOP.is_set():
            batch = await cam.async_history(
                limit=HISTORY_PAGE, older_than=older_than, enforce_limit=True
            )
            if not batch:
                break
            stop_paging = False
            for ev in batch:
                created = ev.get("created_at")
                if start_dt and created and created < start_dt:
                    stop_paging = True   # older than the window: stop after this page
                    continue
                if end_dt and created and created > end_dt:
                    continue             # newer than the window: skip, keep paging back
                events.append(ev)
            older_than = batch[-1].get("id")
            update(message="Scanning history… %d videos found" % len(events))
            if stop_paging or len(batch) < HISTORY_PAGE:
                break
        events.sort(key=lambda e: e.get("created_at")
                    or datetime.min.replace(tzinfo=timezone.utc))
        return events


BRIDGE = RingBridge()


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------

def _filename_for(ev):
    created = ev.get("created_at")
    # Sanitize: never let API-supplied text introduce path separators / traversal.
    kind = re.sub(r"[^A-Za-z0-9._-]", "-", str(ev.get("kind") or "event"))[:40]
    if isinstance(created, datetime):
        local = created.astimezone()
        month = local.strftime("%Y-%m")
        stamp = local.strftime("%Y-%m-%d_%H-%M-%S")
    else:
        month = "unknown-date"
        stamp = "event-%s" % ev.get("id")
    return month, "%s_%s.mp4" % (stamp, kind)


def _target_for(dest_path, cam_name, ev, multi):
    """Single camera keeps the original dest/<month>/ layout (so existing
    archives stay valid). Multi-camera runs nest under a per-camera folder to
    avoid same-timestamp filename collisions."""
    month, fname = _filename_for(ev)
    if multi:
        safe = re.sub(r"[^A-Za-z0-9._ -]", "-", str(cam_name or "camera"))[:60].strip()
        return dest_path / (safe or "camera") / month / fname
    return dest_path / month / fname


def _parse_window(start_iso, end_iso, since_days):
    """Build a tz-aware [start, end] UTC window from the UI inputs.
    Falls back to since_days for backward compatibility."""
    start_dt = end_dt = None
    if since_days:
        start_dt = datetime.now(timezone.utc) - timedelta(days=int(since_days))
    if start_iso:
        try:
            start_dt = datetime.strptime(start_iso, "%Y-%m-%d").replace(
                tzinfo=timezone.utc)
        except ValueError:
            pass
    if end_iso:
        try:
            end_dt = datetime.strptime(end_iso, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except ValueError:
            pass
    return start_dt, end_dt


def run_download(camera_ids, start_iso, end_iso, dest, since_days=None, verify=False):
    """Runs in its own thread; drives the async engine, updates STATE."""
    try:
        STOP.clear()
        PAUSE.clear()
        REAUTH.clear()
        # Defense-in-depth: never create the dest on the internal disk when an
        # external drive was intended but isn't connected.
        problem = mount_problem(dest)
        if problem:
            update(phase="error", error=problem)
            return
        dest_path = Path(dest).expanduser()
        dest_path.mkdir(parents=True, exist_ok=True)
        manifest = dest_path / "manifest.csv"
        new_manifest = not manifest.exists()

        update(phase="scanning", dest=str(dest_path),
               message=("Re-checking every video…" if verify else "Finding videos…"),
               verifying=verify, total=0, done=0, skipped=0, already=0, retried=0,
               failed=0, failed_recoverable=0, failed_permanent=0,
               warning="", current="", error="", needs_reauth=False)

        if DEMO:
            _demo_download(dest_path, manifest, new_manifest)
            return

        cams_meta = BRIDGE.call(BRIDGE._cameras())
        wanted = {str(x) for x in (camera_ids or [])}
        selected = [c for c in cams_meta if str(c["id"]) in wanted]
        if not selected:
            update(phase="error", error="No matching camera was found on the account.")
            return

        multi = len(selected) > 1
        start_dt, end_dt = _parse_window(start_iso, end_iso, since_days)

        with KeepAwake():
            # Enumerate events for every selected camera up front (so the total
            # and the free-space preflight reflect the whole job).
            jobs = []  # (cam_obj, cam_name, [events])
            total = 0
            for c in selected:
                try:
                    evs = BRIDGE.call(BRIDGE._all_events(c["_obj"], start_dt, end_dt))
                except Exception as e:
                    # One camera's history failing shouldn't throw away the whole
                    # multi-minute scan — log it and keep the others.
                    diag(dest_path, "SCAN-FAIL camera=%s %s" % (c.get("name"), str(e)[:120]))
                    continue
                jobs.append((c["_obj"], c["name"], evs))
                total += len(evs)
                if STOP.is_set():
                    update(message="Stopped."); return

            update(phase="downloading", total=total,
                   message=("Re-checking %d videos…" if verify
                            else "Saving %d videos, oldest first…") % total)

            # Free-space preflight (non-blocking: we'd rather save what fits than
            # refuse and save nothing).
            try:
                free = shutil.disk_usage(dest_path).free
                need = total * AVG_CLIP_BYTES
                if need > free:
                    update(warning=("This could need about %s but the disk has only "
                                    "about %s free. It'll save what fits and stop "
                                    "safely — nothing already saved is lost. Consider "
                                    "an external drive for the rest."
                                    % (_human(need), _human(free))))
                    diag(dest_path, "PREFLIGHT low-space need=%d free=%d" % (need, free))
            except Exception:
                pass

            rc = RateController(THROTTLE_SECONDS, THROTTLE_CEIL)
            with open(manifest, "a", newline="") as mf:
                writer = csv.writer(mf)
                if new_manifest:
                    writer.writerow(["created_at", "camera", "kind", "file", "status"])

                started = time.time()
                for cam, cam_name, events in jobs:
                    for ev in events:
                        if STOP.is_set():
                            update(message="Stopped."); break
                        while PAUSE.is_set() and not STOP.is_set():
                            update(phase="paused"); time.sleep(0.4)
                        if STOP.is_set():
                            break
                        update(phase="downloading")

                        target = _target_for(dest_path, cam_name, ev, multi)
                        target.parent.mkdir(parents=True, exist_ok=True)

                        # Resume / validate-on-skip: keep a file only if it's a
                        # structurally complete MP4; a partial is deleted and
                        # re-fetched so it can never be mistaken for done.
                        if target.exists():
                            if mp4_status(target) == "ok":
                                # Already saved — count it, but don't re-log a row
                                # every resume (keeps manifest.csv from bloating).
                                with STATE_LOCK:
                                    STATE["skipped"] += 1
                                    STATE["already"] += 1
                                    STATE["done"] += 1
                                continue
                            _safe_unlink(target)
                            diag(dest_path, "PARTIAL re-fetch %s" % target.name)

                        # Download (with re-auth handling) — retry the SAME event
                        # after the user signs back in.
                        while True:
                            status = _download_one(cam, ev, target, rc, dest_path)
                            if status == "auth" and not STOP.is_set():
                                update(phase="reauth", needs_reauth=True, signed_in=False,
                                       message="Your Ring session expired — sign in "
                                               "again to keep going.")
                                REAUTH.set()
                                while REAUTH.is_set() and not STOP.is_set():
                                    time.sleep(0.4)
                                if STOP.is_set():
                                    break
                                update(phase="downloading", needs_reauth=False, message="")
                                continue
                            break
                        if STOP.is_set():
                            break

                        if status == "nospace":
                            diag(dest_path, "NOSPACE halting run")
                            update(phase="error", error=(
                                "The disk is full — couldn't save more videos. "
                                "Everything downloaded so far is safe. Free up space "
                                "(or plug in an external drive and set Save to → Choose…), "
                                "then start again to resume where it left off."))
                            STOP.set()
                            break

                        with STATE_LOCK:
                            STATE["done"] += 1
                            if status == "ok":
                                STATE["current"] = target.name  # only show real saves
                            elif status == "gone":
                                STATE["failed"] += 1
                                STATE["failed_permanent"] += 1
                            else:
                                STATE["failed"] += 1
                                STATE["failed_recoverable"] += 1
                            elapsed = time.time() - started
                            done = STATE["done"]
                            if done:
                                per = elapsed / done
                                STATE["eta_seconds"] = int(per * (STATE["total"] - done))
                        writer.writerow([ev.get("created_at"), cam_name,
                                         ev.get("kind"), str(target),
                                         "saved" if status == "ok" else status])
                        mf.flush()
                        # Pace only real downloads. "gone" 404s are instant and
                        # shouldn't inherit a long cooldown; recoverable failures
                        # already slept inside their retry loop.
                        time.sleep(rc.current() if status == "ok" else 0.3)
                    if STOP.is_set():
                        break

        if not STOP.is_set():
            final = snapshot()
            msg = "Your videos are saved to %s" % dest_path
            if final.get("failed_permanent"):
                msg += ("  (%d had already expired from Ring — those are gone for "
                        "everyone, not a problem with the app.)" % final["failed_permanent"])
            if final.get("failed_recoverable"):
                msg += ("  (%d to retry — click Check & fix saved videos.)"
                        % final["failed_recoverable"])
            update(phase="done", message=msg, eta_seconds=0)
            notify_done(snapshot())
    except Exception as e:  # noqa
        update(phase="error", error="%s" % e)


def _download_one(cam, ev, target, rc, dest_path):
    """Download one event with adaptive backoff and validate-on-write.
    Returns: ok | gone | auth | failed."""
    delay = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            BRIDGE.call(cam.async_recording_download(
                int(ev.get("id")), filename=str(target), override=True))
        except Exception as e:
            kind, status = classify_error(e)
            diag(dest_path, "ERR id=%s status=%s kind=%s %s"
                 % (ev.get("id"), status, kind, str(e)[:140]))
            _safe_unlink(target)
            if kind == "nospace":
                return "nospace"
            if kind == "auth":
                return "auth"
            if kind == "gone":
                rc.ok()  # Ring answered (no recording) — not a rate signal; ease back
                return "gone"
            with STATE_LOCK:
                STATE["retried"] += 1
            if attempt < MAX_RETRIES - 1:
                wait = rc.slow(_retry_after(e)) if kind == "rate" else delay
                time.sleep(wait)
                delay = min(delay * 2, rc.ceil)
                continue
            return "failed"

        # Download returned without raising — verify the bytes are a real MP4.
        st = mp4_status(target)
        if st == "ok":
            rc.ok()
            return "ok"
        _safe_unlink(target)
        diag(dest_path, "BADFILE id=%s mp4=%s -> retry" % (ev.get("id"), st))
        with STATE_LOCK:
            STATE["retried"] += 1
        if attempt < MAX_RETRIES - 1:
            time.sleep(delay)
            delay = min(delay * 2, rc.ceil)
            continue
        return "failed"
    return "failed"


def _demo_download(dest_path, manifest, new_manifest):
    """Fake a run so the interface can be previewed with no Ring account."""
    total = 240
    update(phase="downloading", total=total,
           message="DEMO: pretending to download %d videos…" % total)
    with open(manifest, "a", newline="") as mf:
        writer = csv.writer(mf)
        if new_manifest:
            writer.writerow(["created_at", "camera", "kind", "file", "status"])
        started = time.time()
        base = datetime.now() - timedelta(days=160)
        for i in range(total):
            if STOP.is_set():
                break
            while PAUSE.is_set() and not STOP.is_set():
                update(phase="paused"); time.sleep(0.4)
            ts = base + timedelta(hours=i * 4)
            fname = ts.strftime("%Y-%m-%d_%H-%M-%S_motion.mp4")
            with STATE_LOCK:
                STATE["done"] = i + 1
                STATE["current"] = fname
                elapsed = time.time() - started
                per = elapsed / (i + 1)
                STATE["eta_seconds"] = int(per * (total - (i + 1)))
            writer.writerow([ts.isoformat(), "Backyard (sample)", "motion", fname, "saved"])
            mf.flush()
            time.sleep(0.05)
    if not STOP.is_set():
        update(phase="done", message="DEMO complete (no real files downloaded).",
               eta_seconds=0)


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json_body(self):
        n = int(self.headers.get("Content-Length", 0))
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except Exception:
            return {}

    def _local_only(self):
        """Loopback hardening: defeat DNS-rebinding (Host header must be
        loopback) and cross-site POSTs (any Origin must be ours). Same-origin
        requests from our own page always pass; plain curl passes too."""
        host = (self.headers.get("Host") or "").split(",")[0].strip().lower()
        ok_hosts = {"127.0.0.1:%d" % ACTIVE_PORT, "localhost:%d" % ACTIVE_PORT,
                    "127.0.0.1", "localhost"}
        if host and host not in ok_hosts:
            self._send(403, json.dumps({"error": "forbidden host"})); return False
        origin = self.headers.get("Origin")
        if origin and origin not in ("http://127.0.0.1:%d" % ACTIVE_PORT,
                                     "http://localhost:%d" % ACTIVE_PORT):
            self._send(403, json.dumps({"error": "forbidden origin"})); return False
        return True

    def do_GET(self):
        if not self._local_only():
            return
        route = urlparse(self.path)
        if route.path == "/api/volumes":
            return self._send(200, json.dumps({"volumes": list_volumes()}))
        if route.path == "/api/space":
            q = parse_qs(route.query)
            dest = (q.get("dest") or [str(DEFAULT_DEST)])[0]
            return self._send(200, json.dumps(dest_info(dest)))
        if route.path == "/api/releases":
            return self._send(200, json.dumps(
                {"releases": list_releases(), "current": APP_VERSION}))
        if self.path in ("/", "/index.html"):
            html = (WEB_DIR / "index.html").read_text()
            return self._send(200, html, "text/html; charset=utf-8")
        if self.path == "/api/progress":
            return self._send(200, json.dumps(snapshot()))
        if self.path == "/api/update-check":
            info = {"update_available": False, "version": APP_VERSION,
                    "latest_version": "", "release_url": RELEASES_PAGE}
            if not DEMO:
                try:
                    info = check_update()
                except Exception:
                    pass
            return self._send(200, json.dumps(info))
        if self.path == "/api/session":
            # On load: try a cached token so she may skip login entirely.
            signed = False
            if DEMO:
                signed = False
            elif TOKEN_FILE.exists():
                signed = BRIDGE.call(BRIDGE._try_cached_token())
            # Don't clobber the phase of an in-progress run: only the idle
            # login/choose screens are driven by the session check.
            if snapshot()["phase"] in ("login", "choose"):
                update(signed_in=signed, phase="choose" if signed else "login")
            else:
                update(signed_in=signed)
            return self._send(200, json.dumps(
                {"signed_in": signed, "demo": DEMO, "version": APP_VERSION,
                 "help_url": HELP_URL, "portfolio_url": PORTFOLIO_URL}))
        return self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if not self._local_only():
            return
        body = self._json_body()
        if self.path == "/api/login":
            return self._login(body)
        if self.path == "/api/cameras":
            return self._cameras()
        if self.path == "/api/start":
            return self._start(body)
        if self.path == "/api/verify":
            return self._start(body, verify=True)
        if self.path == "/api/pause":
            PAUSE.set(); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/resume":
            PAUSE.clear(); update(phase="downloading"); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/stop":
            STOP.set(); PAUSE.clear(); REAUTH.clear(); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/logout":
            try:
                TOKEN_FILE.unlink()
            except Exception:
                pass
            BRIDGE.ring = None
            BRIDGE.auth = None
            update(signed_in=False, phase="login", cameras=[])
            return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/open-folder":
            self._open_folder(); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/choose-folder":
            path = choose_folder()
            if not path:
                return self._send(200, json.dumps({"ok": False, "canceled": True}))
            info = dest_info(path)
            info["ok"] = True
            return self._send(200, json.dumps(info))
        return self._send(404, json.dumps({"error": "not found"}))

    # -- endpoints --------------------------------------------------------
    def _login(self, body):
        email = body.get("email", "")
        password = body.get("password", "")
        otp = body.get("otp", "")
        if DEMO:
            update(signed_in=True, phase="choose", needs_otp=False)
            return self._send(200, json.dumps({"ok": True}))
        try:
            res = BRIDGE.call(BRIDGE._login(email, password, otp))
        except Exception as e:
            return self._send(200, json.dumps({"ok": False, "error": str(e)}))
        if res.get("needs_otp"):
            update(needs_otp=True)
            return self._send(200, json.dumps({"ok": False, "needs_otp": True}))
        update(signed_in=True, needs_otp=False, needs_reauth=False)
        # If a running download was paused waiting for re-auth, let it resume.
        if REAUTH.is_set():
            REAUTH.clear()
        else:
            update(phase="choose")
        return self._send(200, json.dumps({"ok": True}))

    def _cameras(self):
        if DEMO:
            cams = [{"id": "demo1", "name": "Backyard (sample)"},
                    {"id": "demo2", "name": "Front Door (sample)"}]
            update(cameras=cams)
            return self._send(200, json.dumps({"cameras": cams}))
        try:
            cams = BRIDGE.call(BRIDGE._cameras())
            public = [{"id": c["id"], "name": c["name"]} for c in cams]
            update(cameras=public)
            return self._send(200, json.dumps({"cameras": public}))
        except Exception as e:
            return self._send(200, json.dumps({"cameras": [], "error": str(e)}))

    def _start(self, body, verify=False):
        # Accept camera_ids (list, possibly ["all"]) or legacy camera_id.
        ids = body.get("camera_ids")
        if not ids:
            single = body.get("camera_id")
            ids = [single] if single else []
        ids = [str(x) for x in ids]
        if "all" in ids:
            ids = [str(c["id"]) for c in snapshot().get("cameras", [])]
        if not ids:
            return self._send(200, json.dumps({"ok": False, "error": "Pick a camera."}))

        start_date = body.get("start_date") or ""
        end_date = body.get("end_date") or ""
        since_days = body.get("since_days") or None
        dest = body.get("dest") or str(DEFAULT_DEST)

        # One run at a time: a double-click/duplicate Start would spawn a second
        # worker sharing STATE + manifest + the rate limiter, corrupting counts
        # and doubling Ring's rate-limit pressure.
        global WORKER
        if WORKER is not None and WORKER.is_alive():
            return self._send(200, json.dumps(
                {"ok": False, "error": "A download is already running."}))

        # Mount guard: refuse to start if the chosen drive isn't connected,
        # rather than silently writing to the internal disk.
        problem = mount_problem(dest)
        if problem:
            return self._send(200, json.dumps({"ok": False, "error": problem}))

        update(phase="scanning", total=0, done=0, skipped=0, already=0, retried=0,
               failed=0, failed_recoverable=0, failed_permanent=0,
               current="", error="", warning="", eta_seconds=None)
        WORKER = threading.Thread(
            target=run_download,
            args=(ids, start_date, end_date, dest),
            kwargs={"since_days": since_days, "verify": verify}, daemon=True)
        WORKER.start()
        return self._send(200, json.dumps({"ok": True}))

    def _open_folder(self):
        dest = snapshot()["dest"]
        try:
            if sys.platform == "darwin":
                subprocess.Popen(["open", dest])
            elif os.name == "nt":
                os.startfile(dest)  # noqa
            else:
                subprocess.Popen(["xdg-open", dest])
        except Exception:
            pass


def _report_fatal(summary, detail=""):
    """A packaged (windowed) app has no console, so a startup crash just makes
    the icon bounce and vanish. Instead: write a log the user can find, show a
    native dialog that carries the key error line (so a single screenshot is
    enough to diagnose), and reveal the log file so it's one tap to send over."""
    log = DATA_DIR / "launch-error.log"
    body = summary + (("\n\n" + detail) if detail else "")
    try:
        log.write_text("%s\n%s\n" % (datetime.now().isoformat(timespec="seconds"), body))
    except Exception:
        pass
    print(body, file=sys.stderr)
    # Fold the last couple of traceback lines into the dialog itself.
    tail = ""
    if detail:
        lines = [ln for ln in detail.strip().splitlines() if ln.strip()]
        if lines:
            tail = "\n\nDetail: " + "  ".join(lines[-2:])
    msg = (summary + tail)[:1000]
    try:
        if sys.platform == "darwin":
            subprocess.run(
                ["osascript", "-e",
                 'display alert "Ring Video Archiver couldn\'t start" message "%s"'
                 % msg.replace('"', "'")])
            subprocess.run(["open", "-R", str(log)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif os.name == "nt":
            import ctypes
            ctypes.windll.user32.MessageBoxW(0, msg, "Ring Video Archiver", 0)
            subprocess.Popen(["explorer", "/select,", str(log)])
    except Exception:
        pass


def _bind_server():
    """Bind the first free port from PORT upward — so a leftover/old instance
    holding 8765 makes us pick 8766 instead of crashing."""
    global ACTIVE_PORT
    last = None
    for p in range(PORT, PORT + 10):
        try:
            srv = ThreadingHTTPServer(("127.0.0.1", p), Handler)
            ACTIVE_PORT = p
            return srv, p
        except OSError as e:
            last = e
    raise last


def main():
    # v1-style launch (browser tab — no webview, which tripped Local Network on
    # Sequoia), but bind the first FREE port so a leftover instance on 8765
    # doesn't silently kill the app with no window to show why.
    try:
        server, port = _bind_server()
    except OSError:
        _report_fatal(
            "Ring Video Archiver is already running (or ports %d–%d are in use). "
            "Check your Dock, or open http://127.0.0.1:%d in your browser."
            % (PORT, PORT + 9, PORT))
        return
    url = "http://127.0.0.1:%d" % port
    banner = " (DEMO MODE — no real account needed)" if DEMO else ""
    print("Ring Video Archiver v%s running at %s%s" % (APP_VERSION, url, banner))
    print("Close this window to stop.")
    if os.environ.get("RING_ARCHIVER_NO_BROWSER") != "1":
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException:
        import traceback
        _report_fatal(
            "Ring Video Archiver hit an unexpected error starting up and had to "
            "close. Details saved to ~/.ring-archiver/launch-error.log.",
            traceback.format_exc())
        raise
