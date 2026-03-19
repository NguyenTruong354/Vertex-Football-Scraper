import asyncio
import logging
import random
import time

from curl_cffi.requests import AsyncSession

import sofascore.config_sofascore as cfg
from core.antiban import AntiBanStateMachine, LiveMetrics
from core.config import BROWSER_RECYCLE_EVERY
from core.utils import Notifier


class CurlCffiClient:
    """Lightweight HTTP client that bypasses Cloudflare using TLS impersonation."""

    # Retry backoff timings (seconds)
    RETRY_BACKOFF = [4, 10, 20]
    MAX_RETRIES = 3

    def __init__(
        self, log: logging.Logger, notifier: "Notifier", *, dry_run: bool = False
    ):
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._session: AsyncSession | None = None
        self._lock = asyncio.Lock()
        self._request_count = 0
        self._last_request: float = 0.0
        self._consecutive_403 = 0
        self._consecutive_429 = 0
        self._last_block_alert = 0.0
        self.antiban: AntiBanStateMachine | None = None
        self.metrics: LiveMetrics | None = None

    def _check_block_alert(self, url: str, code: int) -> None:
        """Trigger a Discord alert if Cloudflare/rate-limit blocking is persistent."""
        count = self._consecutive_403 if code == 403 else self._consecutive_429
        if count >= 5:
            now = time.time()
            if now - self._last_block_alert > 3600:  # Alert once per hour max
                if code == 403:
                    self.log.error(
                        "CRITICAL: Cloudflare persistently blocked our TLS footprint (403 Forbidden)."
                    )
                    self.notifier.send(
                        "error",
                        f"**🚨 CLOUDFLARE BLOCK ALERT**\n"
                        f"Received {count} consecutive `403 Forbidden` responses.\n"
                        f"The impersonate profile (`chrome120`) may have been detected and blocked by Cloudflare.\n"
                        f"Last blocked URL: `{url}`\n"
                        f"*Action required: Update impersonate version in CurlCffiClient.*",
                    )
                else:
                    self.log.error(
                        "CRITICAL: SofaScore rate limiting our IP (429 Too Many Requests)."
                    )
                    self.notifier.send(
                        "error",
                        f"**⚡ RATE LIMIT ALERT**\n"
                        f"Received {count} consecutive `429 Too Many Requests` responses.\n"
                        f"SofaScore is throttling our requests. IP may be temporarily blocked.\n"
                        f"*The system will auto-backoff, but monitor closely.*",
                    )
                self._last_block_alert = now

    async def start(self) -> None:
        if self.dry_run:
            self.log.info("[DRY-RUN] HTTP Client start skipped")
            return
        self.log.info("🌐 Starting Curl CFFI HTTP Client (impersonating Chrome 120)...")
        self._session = AsyncSession(
            impersonate="chrome120",
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Referer": "https://www.sofascore.com/",
                "Origin": "https://www.sofascore.com",
            },
        )
        self._request_count = 0

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def recycle(self) -> None:
        """curl_cffi doesn't leak memory like Chrome, but we recycle the session to clear cookies/state."""
        self.log.info(
            "♻️  Recycling HTTP session (after %d requests)...", self._request_count
        )
        await self.stop()
        await self.start()

    def needs_recycle(self) -> bool:
        return self._request_count >= BROWSER_RECYCLE_EVERY

    async def get_json(self, endpoint: str, *, tier: str = "A") -> dict | list | None:
        """Fetch JSON from SofaScore API with retry and jitter.
        Lock scope narrowed: only wraps the actual HTTP call.
        Jitter before lock + backoff after lock = anti-bot natural spacing."""
        if self.dry_run:
            return None

        url = f"{cfg.SS_API_BASE}{endpoint}"
        sc = 0  # track last status code for backoff decisions outside lock

        for attempt in range(self.MAX_RETRIES):
            # 1. JITTER OUTSIDE LOCK: natural spacing between requests
            #    prevents bot-like burst when multiple matches queue up
            mult = self.antiban.global_sleep_multiplier if self.antiban else 1.0
            jitter = random.uniform(1.5, 3.5) * mult
            await asyncio.sleep(jitter)

            # 2. NARROW LOCK: only wraps the actual HTTP request
            async with self._lock:
                try:
                    if not self._session:
                        await self.start()

                    self._last_request = time.monotonic()
                    self._request_count += 1

                    resp = await self._session.get(url, timeout=15)
                    sc = resp.status_code

                    # ── record into anti-ban state machine + metrics ──
                    if self.antiban:
                        self.antiban.record_response(sc, tier)
                        self.antiban.check_timeout()
                        if self.antiban.needs_recycle:
                            self.log.info("♻️  Anti-ban triggered session recycle")
                            await self.recycle()
                    if self.metrics:
                        self.metrics.record_request(tier, sc)

                    if sc == 200:
                        self._consecutive_403 = 0
                        self._consecutive_429 = 0
                        data = resp.json()
                        if isinstance(data, dict) and data.get("error"):
                            if data["error"].get("code") == 404:
                                return None
                        return data

                    elif sc == 403:
                        self._consecutive_403 += 1
                        self.log.warning(
                            "HTTP 403 Forbidden (attempt %d/%d): %s",
                            attempt + 1,
                            self.MAX_RETRIES,
                            url,
                        )
                        self._check_block_alert(url, 403)

                    elif sc == 429:
                        self._consecutive_429 += 1
                        self.log.warning(
                            "HTTP 429 Rate Limited (attempt %d/%d): %s",
                            attempt + 1,
                            self.MAX_RETRIES,
                            url,
                        )
                        self._check_block_alert(url, 429)

                    else:
                        self.log.warning("HTTP %d: %s", sc, url)

                except Exception as exc:
                    self.log.debug(
                        "Request failed (attempt %d): %s — %s",
                        attempt + 1,
                        endpoint,
                        exc,
                    )
                    sc = 0  # mark as failed for backoff logic below

            # 3. BACKOFF OUTSIDE LOCK: sleep without blocking other matches
            if attempt < self.MAX_RETRIES - 1:
                if sc == 429:
                    backoff = self.RETRY_BACKOFF[attempt] * 3
                    self.log.info("  ⏳ Rate limit backoff %ds...", backoff)
                    await asyncio.sleep(backoff)
                elif sc == 403:
                    backoff = self.RETRY_BACKOFF[attempt] * 2
                    self.log.info("  ⏳ Backoff %ds before retry...", backoff)
                    await asyncio.sleep(backoff)
                elif sc != 200:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt])

        return None

    async def get_schedule_json(self, date_str: str) -> dict:
        """Fetch scheduled-events for a date with retry."""
        if self.dry_run:
            return {}

        url = f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date_str}"

        sc = 0

        for attempt in range(self.MAX_RETRIES):
            # 1. JITTER OUTSIDE LOCK
            mult = self.antiban.global_sleep_multiplier if self.antiban else 1.0
            jitter = random.uniform(1.5, 3.5) * mult
            await asyncio.sleep(jitter)

            # 2. NARROW LOCK: only the HTTP call
            async with self._lock:
                try:
                    if not self._session:
                        await self.start()

                    self._last_request = time.monotonic()
                    self._request_count += 1

                    resp = await self._session.get(url, timeout=20)
                    sc = resp.status_code

                    # ── anti-ban + metrics ──
                    if self.antiban:
                        self.antiban.record_response(sc, "A")
                        self.antiban.check_timeout()
                        if self.antiban.needs_recycle:
                            self.log.info(
                                "♻️  Anti-ban triggered session recycle (schedule)"
                            )
                            await self.recycle()
                    if self.metrics:
                        self.metrics.record_request("A", sc)

                    if sc == 200:
                        self._consecutive_403 = 0
                        self._consecutive_429 = 0
                        return resp.json()
                    elif sc == 403:
                        self._consecutive_403 += 1
                        self.log.warning(
                            "HTTP 403 on schedule fetch (attempt %d): %s",
                            attempt + 1,
                            url,
                        )
                        self._check_block_alert(url, 403)
                    elif sc == 429:
                        self._consecutive_429 += 1
                        self.log.warning(
                            "HTTP 429 on schedule fetch (attempt %d): %s",
                            attempt + 1,
                            url,
                        )
                        self._check_block_alert(url, 429)

                except Exception as exc:
                    self.log.warning(
                        "Schedule fetch failed for %s (attempt %d): %s",
                        date_str,
                        attempt + 1,
                        exc,
                    )
                    sc = 0

            # 3. BACKOFF OUTSIDE LOCK
            if attempt < self.MAX_RETRIES - 1:
                if sc == 429:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt] * 3)
                elif sc == 403:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt] * 2)
                elif sc != 200:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt])

        return {}
