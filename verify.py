#!/usr/bin/env python3
"""
Ring Video Archiver — verify & recover.

Independent companion to app.py for making sure NOTHING is silently missing or
half-downloaded. It reuses the same cached Ring login (~/.ring-archiver/
ring_token.json) and the same filename scheme, then:

  1. Re-enumerates every event from Ring (the authoritative "what should exist").
  2. Checks each expected file on disk and validates its MP4 structure.
  3. Reports a full breakdown: ok / missing / empty / truncated / no-moov / failed.
  4. With --fix, deletes any broken/partial file and re-downloads it directly
     from Ring (patient throttle, generous retries, re-validated after writing).

Why this exists separately from the app:
  * The app skips any file that merely *exists* and is non-empty, so a partial
    download left on disk would be silently skipped forever — and once Ring's
    retention window deletes the source, that clip is gone. This tool validates
    structure, not just existence, and re-pulls anything it can't vouch for.

Safe by default: with no flags it only READS and REPORTS. It never deletes or
downloads unless you pass --fix.

Usage:
    python3 verify.py                         # audit the default ~/Ring Videos
    python3 verify.py --dest "/path/to/Ring Videos"
    python3 verify.py --camera "front"        # match camera by name substring
    python3 verify.py --since-days 365        # limit how far back to check
    python3 verify.py --fix                   # delete partials + re-download
"""

import argparse
import asyncio
import json
import os
import re
import struct
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Same TLS fix as the app: point OpenSSL at certifi's CA bundle so HTTPS to
# Ring works in a packaged/bundled context. Must run before any TLS connection.
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
except Exception:
    pass

DATA_DIR = Path.home() / ".ring-archiver"
TOKEN_FILE = DATA_DIR / "ring_token.json"
DEFAULT_DEST = Path.home() / "Ring Videos"
USER_AGENT = "RingVideoArchiver/1.0"

# Be extra gentle here — this is the recovery pass for the clips that already
# failed once, so favor patience over speed.
THROTTLE_SECONDS = float(os.environ.get("RING_ARCHIVER_THROTTLE", "2.0"))
MAX_RETRIES = int(os.environ.get("RING_ARCHIVER_RETRIES", "8"))
HISTORY_PAGE = 100


# ---------------------------------------------------------------------------
# MP4 structural validation (pure Python, no external tools required)
# ---------------------------------------------------------------------------

def mp4_status(path):
    """Walk top-level MP4 boxes. Returns one of:
    ok / empty / truncated / corrupt / no-moov.
    A complete file's boxes tile the whole file exactly and include a 'moov'."""
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
                    return "truncated"          # dangling partial box header
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
                if offset + box_size > size:    # box claims more than file holds
                    return "truncated"
                offset += box_size
    except OSError:
        return "corrupt"
    return "ok" if saw_moov else "no-moov"


def ffprobe_ok(path):
    """Optional deeper check: full-decode with ffmpeg if it's installed.
    Returns True (clean), False (errors), or None (ffmpeg not available)."""
    import shutil
    import subprocess
    exe = shutil.which("ffmpeg")
    if not exe:
        return None
    try:
        r = subprocess.run(
            [exe, "-v", "error", "-i", str(path), "-f", "null", "-"],
            capture_output=True, text=True, timeout=120,
        )
        return r.returncode == 0 and not r.stderr.strip()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Ring access (mirrors app.py's auth + enumeration, kept minimal)
# ---------------------------------------------------------------------------

def _token_updated(token):
    try:
        TOKEN_FILE.write_text(json.dumps(token))
        os.chmod(TOKEN_FILE, 0o600)
    except Exception:
        pass


async def connect():
    from ring_doorbell import Auth, Ring
    if not TOKEN_FILE.exists():
        raise SystemExit(
            "No saved login found at %s.\n"
            "Open the Ring Archiver app and sign in once first, then re-run."
            % TOKEN_FILE
        )
    token = json.loads(TOKEN_FILE.read_text())
    auth = Auth(USER_AGENT, token, _token_updated)
    ring = Ring(auth)
    await ring.async_update_data()
    return ring


def list_cameras(ring):
    cams = []
    try:
        for d in ring.video_devices():
            cid = getattr(d, "id", None)
            name = getattr(d, "name", None) or ("Camera %s" % cid)
            if cid is not None:
                cams.append((cid, name, d))
    except Exception:
        pass
    if not cams:
        devices = ring.devices()
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
                if cid is not None and not any(c[0] == cid for c in cams):
                    cams.append((cid, name, d))
    return cams


async def all_events(cam, since_days):
    cutoff = None
    if since_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(since_days))
    events = []
    older_than = None
    while True:
        batch = await cam.async_history(
            limit=HISTORY_PAGE, older_than=older_than, enforce_limit=True
        )
        if not batch:
            break
        stop = False
        for ev in batch:
            created = ev.get("created_at")
            if cutoff and created and created < cutoff:
                stop = True
                continue
            events.append(ev)
        older_than = batch[-1].get("id")
        sys.stderr.write("\r  scanning history… %d events" % len(events))
        sys.stderr.flush()
        if stop or len(batch) < HISTORY_PAGE:
            break
    sys.stderr.write("\n")
    events.sort(key=lambda e: e.get("created_at")
                 or datetime.min.replace(tzinfo=timezone.utc))
    return events


def filename_for(ev):
    """Identical to app.py's _filename_for so paths line up exactly."""
    created = ev.get("created_at")
    kind = re.sub(r"[^A-Za-z0-9._-]", "-", str(ev.get("kind") or "event"))[:40]
    if isinstance(created, datetime):
        local = created.astimezone()
        month = local.strftime("%Y-%m")
        stamp = local.strftime("%Y-%m-%d_%H-%M-%S")
    else:
        month = "unknown-date"
        stamp = "event-%s" % ev.get("id")
    return month, "%s_%s.mp4" % (stamp, kind)


async def download_one(cam, ev, target):
    """Patient re-download with exponential backoff. Verifies structure after
    writing; a structurally-bad result counts as a failed attempt and retries."""
    delay = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            await cam.async_recording_download(
                int(ev.get("id")), filename=str(target), override=True)
            status = mp4_status(target)
            if status == "ok":
                return "ok"
            # wrote something unusable — drop it before retrying so a partial
            # never lingers on disk to be mistaken for a good file.
            try:
                target.unlink()
            except OSError:
                pass
        except Exception:
            pass
        if attempt < MAX_RETRIES - 1:
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    return "failed"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    ap = argparse.ArgumentParser(description="Verify & recover Ring downloads.")
    ap.add_argument("--dest", default=str(DEFAULT_DEST),
                    help="Ring Videos folder (default: %s)" % DEFAULT_DEST)
    ap.add_argument("--camera", default=None,
                    help="match a camera by name substring (default: all)")
    ap.add_argument("--since-days", type=int, default=None,
                    help="only check events newer than N days")
    ap.add_argument("--fix", action="store_true",
                    help="delete partials and re-download missing/broken clips")
    ap.add_argument("--ffprobe", action="store_true",
                    help="also deep-decode each OK file with ffmpeg (slow)")
    args = ap.parse_args()

    dest = Path(args.dest).expanduser()
    if not dest.exists():
        raise SystemExit("Folder not found: %s" % dest)

    print("Connecting to Ring with your saved login…")
    ring = await connect()
    cams = list_cameras(ring)
    if not cams:
        raise SystemExit("No cameras found on the account.")
    if args.camera:
        sub = args.camera.lower()
        cams = [c for c in cams if sub in c[1].lower()]
        if not cams:
            raise SystemExit("No camera matched %r." % args.camera)

    buckets = {"ok": [], "missing": [], "empty": [], "truncated": [],
               "corrupt": [], "no-moov": []}

    for cid, name, cam in cams:
        print("\nCamera: %s" % name)
        events = await all_events(cam, args.since_days)
        print("  %d events on Ring; checking files in %s" % (len(events), dest))
        for ev in events:
            month, fname = filename_for(ev)
            target = dest / month / fname
            status = mp4_status(target)
            if status == "ok" and args.ffprobe:
                deep = ffprobe_ok(target)
                if deep is False:
                    status = "corrupt"
            buckets.setdefault(status, []).append((ev, target, cam))

    total = sum(len(v) for v in buckets.values())
    bad = {k: v for k, v in buckets.items() if k != "ok"}
    print("\n" + "=" * 56)
    print("AUDIT SUMMARY  (%d events checked)" % total)
    print("  ok:        %d" % len(buckets["ok"]))
    for k in ("missing", "empty", "truncated", "corrupt", "no-moov"):
        if buckets.get(k):
            print("  %-10s %d" % (k + ":", len(buckets[k])))
    n_bad = sum(len(v) for v in bad.values())
    print("=" * 56)

    if n_bad == 0:
        print("Every expected clip is present and structurally complete. 🎉")
        return

    # Write the list of problem clips so there's always a record.
    report = dest / "verify-report.csv"
    import csv as _csv
    with open(report, "w", newline="") as rf:
        w = _csv.writer(rf)
        w.writerow(["status", "created_at", "file"])
        for status, items in bad.items():
            for ev, target, _ in items:
                w.writerow([status, ev.get("created_at"), str(target)])
    print("%d clip(s) need recovery. Full list written to:\n  %s" % (n_bad, report))

    if not args.fix:
        print("\nThis was a read-only audit — nothing was changed.")
        print("Re-run with --fix to delete the partial files and re-download")
        print("the missing/broken clips directly from Ring.")
        return

    print("\n--fix: re-downloading %d clip(s) from Ring "
          "(throttle %.1fs, up to %d retries each)…"
          % (n_bad, THROTTLE_SECONDS, MAX_RETRIES))
    recovered, lost = 0, []
    for status, items in bad.items():
        for ev, target, cam in items:
            # Remove any partial leftover so it can't be mistaken for good.
            if target.exists():
                try:
                    target.unlink()
                except OSError:
                    pass
            target.parent.mkdir(parents=True, exist_ok=True)
            result = await download_one(cam, ev, target)
            if result == "ok":
                recovered += 1
                mark = "✓"
            else:
                lost.append((ev, target))
                mark = "✗ STILL MISSING"
            print("  %s %s" % (mark, target.name))
            time.sleep(THROTTLE_SECONDS)

    print("\nRecovered %d of %d." % (recovered, n_bad))
    if lost:
        print("%d could not be recovered (Ring may no longer have the source):"
              % len(lost))
        for ev, target in lost:
            print("  - %s  (%s)" % (target.name, ev.get("created_at")))
        print("These are logged in %s with their final status." % report)


if __name__ == "__main__":
    asyncio.run(main())
