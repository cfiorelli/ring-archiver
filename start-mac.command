#!/bin/bash
# Double-click this on a Mac to start the Ring Video Archiver.
cd "$(dirname "$0")" || exit 1

# Use a self-contained virtual environment so nothing else on the Mac is touched.
if [ ! -d ".venv" ]; then
  echo "First-time setup (one minute)…"
  python3 -m venv .venv || { echo "Could not create environment. Is Python 3 installed?"; read -r; exit 1; }
fi
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# To preview the interface with no Ring account, run:  RING_ARCHIVER_DEMO=1 ./start-mac.command
python3 app.py
echo
echo "Stopped. You can close this window."
read -r
