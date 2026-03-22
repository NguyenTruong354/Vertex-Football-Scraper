import json
import logging
import logging.handlers
import os
import time

from core.config import ROOT

def setup_logging() -> logging.Logger:
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Configure ROOT logger so all modules share the same settings
    root_log = logging.getLogger()
    root_log.setLevel(logging.DEBUG)

    # Remove existing handlers to avoid duplicates on restart
    for handler in root_log.handlers[:]:
        root_log.removeHandler(handler)

    fmt = logging.Formatter(
        "%(asctime)s [MASTER] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root_log.addHandler(ch)

    fh = logging.handlers.RotatingFileHandler(
        log_dir / "scheduler_master.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    root_log.addHandler(fh)
    
    return logging.getLogger("master")


class Notifier:
    """Routes messages to specific Discord webhooks based on event type."""

    EMOJI = {
        "match_start": "🟢",
        "goal": "⚽",
        "match_end": "🏁",
        "match_event": "⚡",
        "post_match_done": "📊",
        "error": "🔴",
        "daily_done": "🔧",
        "info": "ℹ️",
        "recycle": "♻️",
        "ai_insight": "🤖",
    }

    # Embed color map (decimal)
    COLOR = {
        "goal": 0x00FF00,        # Green
        "match_start": 0x3498DB,  # Blue
        "match_end": 0x95A5A6,   # Gray
        "error": 0xFF0000,       # Red
        "ai_insight": 0xFFA500,  # Orange
        "info": 0x2ECC71,        # Emerald
        "daily_done": 0x9B59B6,  # Purple
    }

    # Map event types to specific webhook environment variables
    ROUTING = {
        "match_start": "DISCORD_WEBHOOK_LIVE",
        "goal": "DISCORD_WEBHOOK_LIVE",
        "match_event": "DISCORD_WEBHOOK_LIVE",
        "match_end": "DISCORD_WEBHOOK_LIVE",
        "ai_insight": "DISCORD_WEBHOOK_LIVE",
        "error": "DISCORD_WEBHOOK_ERROR",
        "info": "DISCORD_WEBHOOK_INFO",
        "daily_done": "DISCORD_WEBHOOK_INFO",
        "post_match_done": "DISCORD_WEBHOOK_INFO",
        "recycle": "DISCORD_WEBHOOK_INFO",
    }

    def __init__(self, log: logging.Logger):
        self.log = log
        # Default webhook if specific ones aren't set
        self.default_webhook = os.environ.get("DISCORD_WEBHOOK", "")

    @property
    def is_enabled(self) -> bool:
        return bool(
            self.default_webhook
            or any(os.environ.get(env) for env in set(self.ROUTING.values()))
        )

    def _get_webhook_url(self, event_type: str) -> str:
        env_var = self.ROUTING.get(event_type, "")
        webhook_url = os.environ.get(env_var) if env_var else ""
        return webhook_url or self.default_webhook

    def _post_webhook(self, webhook_url: str, payload: dict) -> bool:
        """Post JSON to Discord webhook with 429 retry_after handling."""
        if not webhook_url:
            return False
        import urllib.request
        import urllib.error

        data = json.dumps(payload).encode()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "VertexFootballScraper/2.0",
        }

        for attempt in range(2):  # max 1 retry
            try:
                req = urllib.request.Request(
                    webhook_url, data=data, headers=headers, method="POST"
                )
                resp = urllib.request.urlopen(req, timeout=10)
                return True
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt == 0:
                    try:
                        body = json.loads(e.read().decode())
                        retry_after = body.get("retry_after", 5)
                    except Exception:
                        retry_after = 5
                    self.log.warning(
                        "Discord rate limited, retrying after %.1fs", retry_after
                    )
                    time.sleep(retry_after)
                    continue
                self.log.warning("Discord HTTP %d: %s", e.code, e.reason)
                return False
            except Exception as exc:
                self.log.warning("Discord send failed: %s", exc)
                return False
        return False

    def send(self, event_type: str, message: str) -> None:
        """Legacy plain-text send (backward compatible)."""
        emoji = self.EMOJI.get(event_type, "📢")
        full_msg = f"{emoji} **[MASTER]** {message}"
        self.log.info("NOTIFY [%s]: %s", event_type, message)

        webhook_url = self._get_webhook_url(event_type)
        self._post_webhook(webhook_url, {"content": full_msg})

    def send_embed(
        self,
        event_type: str,
        title: str,
        *,
        text_en: str = "",
        text_vi: str = "",
        description: str = "",
        footer: str = "",
        fields: list[dict] | None = None,
    ) -> None:
        """Send a Rich Embed to Discord with optional dual-language fields."""
        color = self.COLOR.get(event_type, 0x95A5A6)
        emoji = self.EMOJI.get(event_type, "📢")

        embed: dict = {
            "title": f"{emoji} {title}",
            "color": color,
        }
        if description:
            embed["description"] = description

        embed_fields = []
        if text_en:
            embed_fields.append({"name": "🇺🇸 English", "value": text_en, "inline": False})
        if text_vi:
            embed_fields.append({"name": "🇻🇳 Tiếng Việt", "value": text_vi, "inline": False})
        if fields:
            embed_fields.extend(fields)
        if embed_fields:
            embed["fields"] = embed_fields

        if footer:
            embed["footer"] = {"text": footer}

        self.log.info("NOTIFY-EMBED [%s]: %s", event_type, title)
        webhook_url = self._get_webhook_url(event_type)
        self._post_webhook(webhook_url, {"embeds": [embed]})
