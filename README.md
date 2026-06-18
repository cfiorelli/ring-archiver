# Ring Video Archiver

A small app that runs on **your own computer** and downloads your Ring camera
videos so you can keep them before Ring's rolling retention window deletes them.
Your videos and password never touch anyone else's servers — only Ring's.

---

## For the person using it (simple version)

1. **Get the app:** open the download page and click the button for your
   computer (Mac or Windows). No installing anything.
2. **Open it.** The first time, your computer may show a safety prompt:
   - **Mac:** right-click the app → **Open** → **Open**.
   - **Windows:** **More info** → **Run anyway**.
3. Your **web browser opens** to a sign-in page. Enter your Ring email &
   password, then the 6-digit code Ring texts you (just once).
4. Pick your camera, choose how far back, click **Download videos**, walk away.

Videos save **oldest first** (those expire soonest) into a **Ring Videos**
folder, organized by month, with a `manifest.csv` list. Close and reopen
anytime — already-saved videos are skipped.

---

## Why it's a downloadable app and not just a website

Your friend's instinct — "a password-protected URL that just does it" — is the
right *goal*, but it isn't physically possible, for two reasons:

1. **The videos have to land on *her* disk.** A website can't write hundreds of
   GB to someone's computer by itself; something has to run locally to receive
   the files.
2. **Browsers can't talk to Ring directly.** Ring's API rejects calls from web
   pages (no CORS), which is why every working Ring tool runs as a local app or
   server, never as a plain web page.

Routing it through a server *we* run would mean storing her private family
footage on our infrastructure and putting her Ring password through us — a bad
trade on privacy, cost, and trust.

**So:** the engine is a tiny local app, and we use the web only for the
friendly parts — a **GitHub Pages** download page (`docs/`) linking to
prebuilt apps. That keeps it OS-agnostic *and* private.

---

## For the technical helper

### Try it right now (no Ring account)
```bash
cd ring-archiver
RING_ARCHIVER_DEMO=1 python3 app.py      # opens the real UI with a fake run
```

### Run from source
```bash
pip install -r requirements.txt
python3 app.py
```

### Ship OS-agnostic apps (no Python for the end user)
Push a tag and GitHub Actions builds standalone apps for **macOS (Apple Silicon
+ Intel), Windows, and Linux** and attaches them to a Release:
```bash
git tag v1.0 && git push origin v1.0
```
Then enable **GitHub Pages** (Settings → Pages → Deploy from `main`, folder
`/docs`) and set `const REPO = "OWNER/REPO"` in `docs/index.html`. Her URL
becomes `https://OWNER.github.io/REPO/`.

Build one locally instead:
```bash
pyinstaller --onefile --name RingArchiver --add-data "web:web" \
  --collect-all ring_doorbell --collect-all firebase_messaging \
  --collect-all http_ece --collect-submodules google.protobuf app.py
# -> dist/RingArchiver   (verified working on macOS arm64)
```

### Password-protecting the page
GitHub Pages has **no built-in password protection**. If you want it, put the
Pages site behind **Cloudflare Access** (Zero Trust) — email-OTP gate, no code.
Honestly the page only holds download links + instructions (nothing sensitive),
so protection is optional; the *app* keeps her credentials local regardless.

### How it behaves
- **Engine:** [`python-ring-doorbell`](https://github.com/python-ring-doorbell/python-ring-doorbell)
  0.9.14 (API verified against this code).
- Oldest-first ordering, resumable (skips existing files), retry-with-backoff,
  throttling (`RING_ARCHIVER_THROTTLE`, default 1s), `manifest.csv` audit log.
- Auth/2FA happen once; refresh token cached to `~/.ring-archiver/ring_token.json`
  (treat like a password; delete when done).
- Env vars: `RING_ARCHIVER_PORT` (8765), `RING_ARCHIVER_THROTTLE`, `RING_ARCHIVER_DEMO=1`.

### Known caveats
- **Not yet tested against a live Ring account** (we don't have one) — the demo
  proves the UI + plumbing and the library API is verified, but the first real
  login should be done attended to catch any account-specific quirks.
- Requires an active **Ring Protect** subscription (needed for video history).
- Check **disk space** before a big run — thousands of clips can be tens to
  hundreds of GB. Point "Save to" at an external drive if needed.
- Downloaded apps are **unsigned** → the one-time OS prompt above. To remove it,
  code-sign/notarize (Apple Developer / Windows cert) — optional polish.
- Fallback if Ring's API ever balks: Ring's website does manual bulk download
  (History → Manage → select → Download, ~50 at a time).
