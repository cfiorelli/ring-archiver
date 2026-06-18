#!/usr/bin/env python3
"""
Ring Video Archiver — a tiny local web app to bulk-download and preserve
Ring camera footage before Ring's rolling retention window deletes it.

How it works:
  * Starts a small web server on your own computer (nothing is uploaded anywhere).
  * Opens your browser to a friendly page: sign in, pick a camera, click Download.
  * Downloads OLDEST videos FIRST (the ones about to expire), skips anything
    already saved, retries failures, and throttles itself so Ring doesn't
    rate-limit the account.
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
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

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
DEMO = os.environ.get("RING_ARCHIVER_DEMO") == "1"

# Be gentle with Ring's (unofficial) API: pause between downloads + retry/backoff.
THROTTLE_SECONDS = float(os.environ.get("RING_ARCHIVER_THROTTLE", "1.0"))
MAX_RETRIES = 4
HISTORY_PAGE = 100  # events fetched per history request

# ---------------------------------------------------------------------------
# Shared progress state (read by the browser via /api/progress)
# ---------------------------------------------------------------------------

STATE_LOCK = threading.Lock()
STATE = {
    "phase": "login",        # login | choose | scanning | downloading | paused | done | error
    "signed_in": False,
    "cameras": [],           # [{id, name}]
    "total": 0,
    "done": 0,
    "skipped": 0,
    "retried": 0,
    "failed": 0,
    "current": "",
    "eta_seconds": None,
    "dest": str(DEFAULT_DEST),
    "message": "",
    "error": "",
    "needs_otp": False,
}
PAUSE = threading.Event()
STOP = threading.Event()


def update(**kw):
    with STATE_LOCK:
        STATE.update(kw)


def snapshot():
    with STATE_LOCK:
        return dict(STATE)


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

    async def _all_events(self, cam, since_days):
        """Page through history, newest->oldest, then return OLDEST first."""
        cutoff = None
        if since_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(since_days))
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
                if cutoff and created and created < cutoff:
                    stop_paging = True
                    continue
                events.append(ev)
            older_than = batch[-1].get("id")
            update(message="Scanning history… %d videos found" % len(events))
            if stop_paging or len(batch) < HISTORY_PAGE:
                break
        events.sort(key=lambda e: e.get("created_at") or datetime.min.replace(tzinfo=timezone.utc))
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


def run_download(camera_id, since_days, dest):
    """Runs in its own thread; drives the async engine, updates STATE."""
    try:
        STOP.clear()
        PAUSE.clear()
        dest_path = Path(dest).expanduser()
        dest_path.mkdir(parents=True, exist_ok=True)
        manifest = dest_path / "manifest.csv"
        new_manifest = not manifest.exists()

        update(phase="scanning", dest=str(dest_path), message="Finding videos…",
               total=0, done=0, skipped=0, retried=0, failed=0, current="")

        if DEMO:
            _demo_download(dest_path, manifest, new_manifest)
            return

        cams = BRIDGE.call(BRIDGE._cameras())
        cam = BRIDGE._camera_obj(cams, camera_id)
        if cam is None:
            update(phase="error", error="That camera was not found on the account.")
            return

        events = BRIDGE.call(BRIDGE._all_events(cam, since_days))
        update(phase="downloading", total=len(events),
               message="Saving %d videos, oldest first…" % len(events))

        with open(manifest, "a", newline="") as mf:
            writer = csv.writer(mf)
            if new_manifest:
                writer.writerow(["created_at", "camera", "kind", "file", "status"])

            started = time.time()
            for i, ev in enumerate(events):
                if STOP.is_set():
                    update(message="Stopped."); break
                while PAUSE.is_set() and not STOP.is_set():
                    update(phase="paused"); time.sleep(0.4)
                if STOP.is_set():
                    break
                update(phase="downloading")

                month, fname = _filename_for(ev)
                folder = dest_path / month
                folder.mkdir(parents=True, exist_ok=True)
                target = folder / fname

                if target.exists() and target.stat().st_size > 0:
                    with STATE_LOCK:
                        STATE["skipped"] += 1
                        STATE["done"] += 1
                    writer.writerow([ev.get("created_at"), getattr(cam, "name", ""),
                                     ev.get("kind"), str(target), "skipped"])
                    continue

                ok = _download_one(cam, ev, target)

                with STATE_LOCK:
                    STATE["done"] += 1
                    STATE["current"] = fname
                    if not ok:
                        STATE["failed"] += 1
                    elapsed = time.time() - started
                    done = STATE["done"]
                    if done:
                        per = elapsed / done
                        STATE["eta_seconds"] = int(per * (STATE["total"] - done))
                writer.writerow([ev.get("created_at"), getattr(cam, "name", ""),
                                 ev.get("kind"), str(target),
                                 "saved" if ok else "failed"])
                mf.flush()
                time.sleep(THROTTLE_SECONDS)

        if not STOP.is_set():
            update(phase="done", message="All done. Videos saved to %s" % dest_path,
                   eta_seconds=0)
    except Exception as e:  # noqa
        update(phase="error", error="%s" % e)


def _download_one(cam, ev, target):
    delay = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            BRIDGE.call(cam.async_recording_download(
                int(ev.get("id")), filename=str(target), override=True))
            return True
        except Exception:
            if attempt < MAX_RETRIES - 1:
                with STATE_LOCK:
                    STATE["retried"] += 1
                time.sleep(delay)
                delay *= 2
            else:
                return False
    return False


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
            writer.writerow([ts.isoformat(), "Mom's Room (DEMO)", "motion", fname, "saved"])
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
        ok_hosts = {"127.0.0.1:%d" % PORT, "localhost:%d" % PORT,
                    "127.0.0.1", "localhost"}
        if host and host not in ok_hosts:
            self._send(403, json.dumps({"error": "forbidden host"})); return False
        origin = self.headers.get("Origin")
        if origin and origin not in ("http://127.0.0.1:%d" % PORT,
                                     "http://localhost:%d" % PORT):
            self._send(403, json.dumps({"error": "forbidden origin"})); return False
        return True

    def do_GET(self):
        if not self._local_only():
            return
        if self.path in ("/", "/index.html"):
            html = (WEB_DIR / "index.html").read_text()
            return self._send(200, html, "text/html; charset=utf-8")
        if self.path == "/api/progress":
            return self._send(200, json.dumps(snapshot()))
        if self.path == "/api/session":
            # On load: try a cached token so she may skip login entirely.
            signed = False
            if DEMO:
                signed = False
            elif TOKEN_FILE.exists():
                signed = BRIDGE.call(BRIDGE._try_cached_token())
            update(signed_in=signed, phase="choose" if signed else "login")
            return self._send(200, json.dumps({"signed_in": signed, "demo": DEMO}))
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
        if self.path == "/api/pause":
            PAUSE.set(); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/resume":
            PAUSE.clear(); update(phase="downloading"); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/stop":
            STOP.set(); PAUSE.clear(); return self._send(200, json.dumps({"ok": True}))
        if self.path == "/api/open-folder":
            self._open_folder(); return self._send(200, json.dumps({"ok": True}))
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
        update(signed_in=True, phase="choose", needs_otp=False)
        return self._send(200, json.dumps({"ok": True}))

    def _cameras(self):
        if DEMO:
            cams = [{"id": "demo1", "name": "Mom's Room (DEMO)"},
                    {"id": "demo2", "name": "Front Door (DEMO)"}]
            update(cameras=cams)
            return self._send(200, json.dumps({"cameras": cams}))
        try:
            cams = BRIDGE.call(BRIDGE._cameras())
            public = [{"id": c["id"], "name": c["name"]} for c in cams]
            update(cameras=public)
            return self._send(200, json.dumps({"cameras": public}))
        except Exception as e:
            return self._send(200, json.dumps({"cameras": [], "error": str(e)}))

    def _start(self, body):
        camera_id = body.get("camera_id")
        since_days = body.get("since_days") or None
        dest = body.get("dest") or str(DEFAULT_DEST)
        update(phase="scanning", total=0, done=0, skipped=0, retried=0,
               failed=0, current="", error="", eta_seconds=None)
        t = threading.Thread(target=run_download,
                             args=(camera_id, since_days, dest), daemon=True)
        t.start()
        return self._send(200, json.dumps({"ok": True}))

    def _open_folder(self):
        import subprocess, sys
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


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    url = "http://127.0.0.1:%d" % PORT
    banner = " (DEMO MODE — no real account needed)" if DEMO else ""
    print("Ring Video Archiver running at %s%s" % (url, banner))
    print("Close this window to stop.")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
