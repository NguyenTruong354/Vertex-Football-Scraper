# Vertex Football - VM (e2-micro) Deployment Guide

This guide walks you through deploying the new AI & RSS features (Live Insight, Match Story, Player Trend, and News Radar) to your Google Cloud `e2-micro` virtual machine.

## Prerequisites

1.  **SSH into your e2-micro instance.**
2.  Ensure you have your new `.env` variables prepared natively on the VM (specifically the new `GEMINI_API_KEY_2`).

## 1. Stop the Running Daemon

Before pulling the new code, safely stop the current `scheduler_master.py` process.

```bash
# If running as a systemd service:
sudo systemctl stop scheduler-master

# If running via screen or tmux:
# Attach to the session and press Ctrl+C
```

## 2. Pull the Latest Code

Navigate to your scraper directory and git pull the latest changes we just made.

```bash
cd /path/to/Vertex_Football_Scraper2
git pull origin main  # Or whichever branch you use
```

## 3. Update Dependencies (Important!)

We added `google-genai`, `groq`, and `feedparser` to `requirements.txt`. You must install these in the VM's Python virtual environment.

```bash
# Activate your virtual environment
source .venv/bin/activate

# Install the new packages
pip install -r requirements.txt
```

## 4. Update the `.env` File

Copy over the changes from `.env.example` to your actual `.env` file on the VM. It now requires the two Gemini keys and the Groq fallback key.

```bash
sudo nano .env
```
Ensure these are present:
```ini
GEMINI_API_KEY="your-main-gemini-key"
GEMINI_API_KEY_2="your-alt-gemini-key"
GROQ_API_KEY="your-groq-key"
```

## 5. (Optional but Recommended) Verify the Database Migrations

The daemon itself (`scheduler_master.py`) will **not** automatically run the SQL schema migrations. However, we already ran the `ALTER TABLE` statements against the live Aiven PostgreSQL server during our local development. 
You can verify the tables exist by running the python REPL on the VM:
```bash
python -c 'from db.config_db import get_connection; cur = get_connection().cursor(); cur.execute("SELECT * FROM player_insights LIMIT 1"); print("DB Good!");'
```

## 6. Restart the Daemon

Start the central master scheduler again.

```bash
# If using systemd (recommended):
sudo systemctl start scheduler-master
sudo systemctl status scheduler-master

# Or using your Windows script `scripts/windows/daemon.bat` equivalent / bash run script:
nohup python scheduler_master.py --league EPL LALIGA BUNDESLIGA SERIEA LIGUE1 &
```

## 7. Verifying the New Features in Logs

Once started, tail the logs to ensure the new AI & RSS features are booting up correctly:

```bash
tail -f logs/scheduler_master.log
```

**Look for:**
*   `fetching RSS feed from BBC Sport` (Runs every 30 mins)
*   `Daily Maintenance` at 4 AM -> Next log will show `🤖 Generating AI insights for X notable players...`
*   Mid-match: `Live Match Insight generated`
*   Post-match: `📝 Match story generated`
