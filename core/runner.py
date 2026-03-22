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


async def async_run_with_retry(
    cmd: list[str],
    cwd: Path,
    label: str,
    log: logging.Logger,
    *,
    dry_run: bool = False,
    timeout: int = 7200,
    shutdown_event: asyncio.Event | None = None,
) -> bool:
    """
    Native async equivalent of run_with_retry.
    Uses asyncio.create_subprocess_exec to avoid ThreadPoolExecutor overhead.
    """
    if dry_run:
        log.info("[DRY-RUN] Would run: %s", " ".join(cmd[:3]) + " ...")
        return True

    # Lấy environment hiện tại và ép PYTHONIOENCODING=utf-8 để tránh lỗi encoding
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    for attempt, backoff in enumerate([0] + BACKOFF_SECONDS):
        if attempt > 0:
            log.info("⏳ %s: retrying in %ds (attempt %d)", label, backoff, attempt + 1)
            # Interruptible sleep
            if shutdown_event:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=backoff)
                    log.info("⏹ %s: Aborted during backoff (shutdown)", label)
                    return False
                except asyncio.TimeoutError:
                    pass # Continue after backoff
            else:
                await asyncio.sleep(backoff)

        if shutdown_event and shutdown_event.is_set():
            log.info("⏹ %s: Aborted before start (shutdown)", label)
            return False

        log.info("▶ Running: %s", label)
        
        # Create temp files for stdout/stderr
        out_f = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        err_f = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(cwd),
                env=env,
                stdout=out_f,
                stderr=err_f,
            )
        except Exception as exc:
            log.error("❌ %s failed to start: %s", label, exc)
            out_f.close()
            err_f.close()
            os.unlink(out_f.name)
            os.unlink(err_f.name)
            continue

        # Wait for completion or shutdown
        wait_task = asyncio.create_task(proc.wait())
        tasks = [wait_task]
        
        if shutdown_event:
            shutdown_task = asyncio.create_task(shutdown_event.wait())
            tasks.append(shutdown_task)
            
        try:
            done, pending = await asyncio.wait(
                tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            
            if shutdown_event and shutdown_event.is_set():
                log.info("⏹ %s: Shutdown requested. Terminating subprocess...", label)
                try:
                    proc.terminate()
                except Exception as e:
                    log.debug("Terminate failed: %s", e)
                # Ensure we reap the process
                await asyncio.wait_for(proc.wait(), timeout=5.0)
                out_f.close()
                err_f.close()
                os.unlink(out_f.name)
                os.unlink(err_f.name)
                return False
                
            if wait_task not in done:
                log.error("⏳ %s: Timeout after %d seconds. Terminating...", label, timeout)
                try:
                    proc.terminate()
                except Exception as e:
                    log.debug("Terminate failed: %s", e)
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            
        except asyncio.TimeoutError:
             # This block handles the case where asyncio.wait itself timed out (shouldn't happen with FIRST_COMPLETED unless entirely blocked)
             pass
        finally:
            if not wait_task.done():
                wait_task.cancel()
            if shutdown_event and 'shutdown_task' in locals() and not shutdown_task.done():
                shutdown_task.cancel()

        out_f.seek(0)
        stdout_content = out_f.read().strip()
        out_f.close()
        os.unlink(out_f.name)

        err_f.seek(0)
        stderr_content = err_f.read().strip()
        err_f.close()
        os.unlink(err_f.name)
        
        if proc.returncode is None:
            # Proc was terminated due to timeout or shutdown
            log.warning("✗ %s terminated", label)
            continue
            
        if proc.returncode == 0:
            log.info("✓ %s completed", label)
            return True
        else:
            log.warning("✗ %s failed (code %s)", label, proc.returncode)
            if stderr_content:
                # Log last few lines of stderr
                lines = stderr_content.split('\n')
                for line in lines[-5:]:
                    if line.strip():
                        log.warning("    stderr: %s", line.strip())

    log.error("❌ %s completely failed after %d attempts", label, len(BACKOFF_SECONDS) + 1)
    return False
