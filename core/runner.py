import asyncio
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path

from core.config import BACKOFF_SECONDS, MAX_ATTEMPTS


def run_with_retry(
    cmd: list[str],
    cwd: Path,
    label: str,
    log: logging.Logger,
    *,
    dry_run: bool = False,
    timeout: int = 7200,
    shutdown_event: asyncio.Event | None = None,
) -> bool:
    if dry_run:
        log.info("[DRY-RUN] %s", label)
        return True
    for attempt in range(MAX_ATTEMPTS):
        if shutdown_event and shutdown_event.is_set():
            log.info("⏹ %s — aborted (shutdown)", label)
            return False
        log.info("▶ %s (attempt %d/%d)", label, attempt + 1, MAX_ATTEMPTS)
        t0 = time.perf_counter()
        try:
            child_env = os.environ.copy()
            child_env.setdefault("PYTHONUTF8", "1")
            child_env.setdefault("PYTHONIOENCODING", "utf-8")

            with (
                tempfile.NamedTemporaryFile(
                    mode="w+", encoding="utf-8", errors="replace", delete=False
                ) as out_f,
                tempfile.NamedTemporaryFile(
                    mode="w+", encoding="utf-8", errors="replace", delete=False
                ) as err_f,
            ):
                out_path = out_f.name
                err_path = err_f.name

                proc = subprocess.Popen(
                    cmd,
                    cwd=str(cwd),
                    stdout=out_f,
                    stderr=err_f,
                    text=True,
                    env=child_env,
                )

                timed_out = False
                next_heartbeat = t0 + 30
                while True:
                    if shutdown_event and shutdown_event.is_set():
                        log.info("⏹ %s — terminating child process (shutdown)", label)
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                        return False

                    if (time.monotonic() - t0) >= timeout:
                        timed_out = True
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                        break

                    try:
                        proc.wait(timeout=2)
                        break
                    except subprocess.TimeoutExpired:
                        now = time.monotonic()
                        if now >= next_heartbeat:
                            log.info(
                                "… %s still running (%.0fs elapsed)", label, now - t0
                            )
                            next_heartbeat = now + 30
                        continue

            stdout = ""
            stderr = ""
            try:
                with open(out_path, "r", encoding="utf-8", errors="replace") as f:
                    stdout = f.read()
            except Exception:
                pass
            try:
                with open(err_path, "r", encoding="utf-8", errors="replace") as f:
                    stderr = f.read()
            except Exception:
                pass
            try:
                os.remove(out_path)
            except Exception:
                pass
            try:
                os.remove(err_path)
            except Exception:
                pass

            if timed_out:
                log.warning("✗ %s — timeout", label)
            elif proc.returncode == 0:
                log.info("✓ %s — %.1fs", label, time.perf_counter() - t0)
                return True
            else:
                log.warning(
                    "✗ %s — exit code %d. Error details:", label, proc.returncode
                )
            if stderr:
                for line in stderr.strip().split("\n"):
                    log.warning("  | %s", line)
        except subprocess.TimeoutExpired:
            log.warning("✗ %s — timeout", label)
        except Exception as exc:
            log.warning("✗ %s — %s", label, exc)
        if attempt < MAX_ATTEMPTS - 1:
            wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            log.info("  ⏳ Retrying in %ds...", wait)
            # Interruptible backoff sleep
            deadline = time.monotonic() + wait
            while time.monotonic() < deadline:
                if shutdown_event and shutdown_event.is_set():
                    log.info("⏹ %s — aborted during backoff (shutdown)", label)
                    return False
                time.sleep(min(5, max(0, deadline - time.monotonic())))
    log.error("✗ %s — FAILED after %d attempts", label, MAX_ATTEMPTS)
    return False
