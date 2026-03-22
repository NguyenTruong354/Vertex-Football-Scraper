"""
Vertex Football - AI Service Router

Provides a unified interface to generate insights using multiple LLM providers.
Primary: Groq Key 1 (llama-3.3-70b-versatile)
Fallback: Groq Key 2 (llama-3.3-70b-versatile)
"""

import os
import time
import logging
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any
from groq import Groq, AsyncGroq
from groq import APIStatusError as GroqAPIError

# --- Multi-Agent Pipeline Configuration ---
TIER1_SEM = asyncio.Semaphore(3)  # Heavy models (Reasoning/Synthesis)
TIER2_SEM = asyncio.Semaphore(5)  # Light models (Data Extraction)

AGENT_MODEL_MAP = {
    "data_miner": "llama-3.1-8b-instant",
    "player_trend": "llama-3.1-8b-instant",  # High volume, numeric trend analysis
    "match_story": "llama-3.1-8b-instant",   # Brief post-match summaries
    "live_badge": "llama-3.1-8b-instant",    # Quick live insights
    "tactical_analyst": "llama-3.3-70b-versatile",
    "scout": "llama-3.3-70b-versatile",
    "editor_in_chief": "llama-3.3-70b-versatile"
}
# ------------------------------------------


try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency guard
    load_dotenv = None

logger = logging.getLogger(__name__)


def _bootstrap_env() -> None:
    """Load workspace .env so LLM keys work without shell-level exports."""
    if load_dotenv is None:
        return
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if env_file.exists():
        load_dotenv(env_file)


_bootstrap_env()

class CircuitBreaker:
    def __init__(self, name: str, provider: str):
        self.name = name
        self.provider = provider  # 'groq'
        self.state = "CLOSED"
        self.consecutive_failures = 0
        self.next_retry_time = 0.0
        self.base_cooldown = 0
        self.current_cooldown = 0
        self._last_open_log_time = 0.0

    @staticmethod
    def _log_transition_to_db(name: str, old_state: str, new_state: str, reason: str = "") -> None:
        """Persist CB state transition to DB for cross-process health monitoring."""
        try:
            from db.config_db import get_connection
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO cb_state_log (breaker_name, old_state, new_state, reason) VALUES (%s, %s, %s, %s)",
                (name, old_state, new_state, reason[:200] if reason else ""),
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            pass  # fail-silent: logging should never break the main flow

    def can_execute(self) -> bool:
        current_time = time.time()
        if self.state == "CLOSED":
            return True
            
        if self.state == "OPEN":
            if current_time >= self.next_retry_time:
                self.state = "HALF_OPEN"
                logger.info("[CircuitBreaker-%s] HALF_OPEN — Testing recovery.", self.name)
                return True
            else:
                remaining = int(self.next_retry_time - current_time)
                mins, secs = divmod(remaining, 60)
                # Throttle noisy OPEN logs to at most once per minute per breaker.
                if (current_time - self._last_open_log_time) >= 60:
                    logger.warning("[CircuitBreaker-%s] OPEN — skipping request. Retry explicitly in %dm:%02ds.", self.name, mins, secs)
                    self._last_open_log_time = current_time
                return False
                
        if self.state == "HALF_OPEN":
            # Nếu đã quá 30s kể từ lần chuyển sang HALF_OPEN mà vẫn chưa có kết quả
            # (tức là request test bị treo/timeout), cho phép retry để tránh bị kẹt mãi.
            if current_time >= self.next_retry_time + 30:
                logger.warning(
                    "[CircuitBreaker-%s] HALF_OPEN test timed out after 30s. Allowing retry.",
                    self.name
                )
                return True
            return False

    def record_success(self):
        if self.state != "CLOSED":
            old = self.state
            logger.info("[CircuitBreaker-%s] SUCCESS — Circuit is now CLOSED.", self.name)
            self._log_transition_to_db(self.name, old, "CLOSED", "recovery_success")
        self.state = "CLOSED"
        self.consecutive_failures = 0
        self.current_cooldown = self.base_cooldown
    def record_error(self, status_code: int):
        self.consecutive_failures += 1
        
        # Determine base cooldown and threshold based on provider and error
        threshold = 3
        
        if self.provider == "groq":
            if status_code == 429:
                self.base_cooldown = 60    # 1 min
                threshold = 1
            elif status_code in (400, 404):
                self.base_cooldown = 86400 # 24 hours
                threshold = 1
            else:
                self.base_cooldown = 300   # 5 mins
        else:
            self.base_cooldown = 300
                
        if self.state == "HALF_OPEN":
            # Exponential backoff on failed recovery test
            self.current_cooldown = min(self.current_cooldown * 2, 86400)
            old = self.state
            self.state = "OPEN"
            self.next_retry_time = time.time() + self.current_cooldown
            logger.warning("[CircuitBreaker-%s] HALF_OPEN test failed (HTTP %s). Back to OPEN. Next retry in %ds.", self.name, status_code, self.current_cooldown)
            self._log_transition_to_db(self.name, old, "OPEN", f"half_open_test_failed_http_{status_code}")
        elif self.state == "CLOSED":
            if self.consecutive_failures >= threshold:
                old = self.state
                self.state = "OPEN"
                self.current_cooldown = self.base_cooldown
                self.next_retry_time = time.time() + self.current_cooldown
                logger.warning("[CircuitBreaker-%s] Tripped OPEN after %d failures (HTTP %s). Cooldown: %ds.", self.name, self.consecutive_failures, status_code, self.current_cooldown)
                self._log_transition_to_db(self.name, old, "OPEN", f"tripped_after_{self.consecutive_failures}_failures_http_{status_code}")

class LLMClient:
    def __init__(self):
        self.groq_key_1 = os.getenv("GROQ_API_KEY")
        self.groq_key_2 = os.getenv("GROQ_API_KEY_2")

        self.groq_client_1 = None
        self.groq_client_2 = None
        self.async_groq_client_1 = None
        self.async_groq_client_2 = None

        self.cb_groq_1 = CircuitBreaker("Groq_1", "groq")
        self.cb_groq_2 = CircuitBreaker("Groq_2", "groq")
        self._last_fallback_log_time = 0.0

        if self.groq_key_1:
            try:
                self.groq_client_1 = Groq(api_key=self.groq_key_1)
                self.async_groq_client_1 = AsyncGroq(api_key=self.groq_key_1)
            except Exception as e:
                logger.error("Failed to init Groq client 1: %s", e)

        if self.groq_key_2:
            try:
                self.groq_client_2 = Groq(api_key=self.groq_key_2)
                self.async_groq_client_2 = AsyncGroq(api_key=self.groq_key_2)
            except Exception as e:
                logger.error("Failed to init Groq client 2: %s", e)

    def _call_groq(self, client: Groq, prompt: str,
                   system_instruction: Optional[str] = None,
                   model: str = "llama-3.3-70b-versatile") -> str:
        messages = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})

        chat_completion = client.chat.completions.create(
            messages=messages,
            model=model,
            temperature=0.2,
        )
        if chat_completion.choices:
            return chat_completion.choices[0].message.content.strip()
        return ""

    def generate_insight(self, prompt: str, system_instruction: Optional[str] = None, agent_name: Optional[str] = None) -> str:
        """
        Generates text using Groq Key 1 first. If it fails, tries Groq Key 2.
        Wraps calls with Circuit Breaker to avoid quota/rate-limit loops.
        """
        if not self.groq_client_1 and not self.groq_client_2:
            logger.warning("No LLM clients configured. Returning empty insight.")
            return ""

        # Determine model
        model = AGENT_MODEL_MAP.get(agent_name or "generic", "llama-3.3-70b-versatile")

        # 1. Try Primary: Groq Key 1
        if self.groq_client_1 and self.cb_groq_1.can_execute():
            try:
                res = self._call_groq(self.groq_client_1, prompt, system_instruction, model=model)
                self.cb_groq_1.record_success()
                logger.debug(
                    "LLM OK | provider=groq_1 | model=%s | words=%d",
                    model, len(res.split())
                )
                return res
            except GroqAPIError as e:
                status_code = e.status_code if hasattr(e, "status_code") else 500
                self.cb_groq_1.record_error(status_code)
                logger.warning("Groq API (Key 1) failed (HTTP %s): %s", status_code, str(e))
            except Exception as e:
                self.cb_groq_1.record_error(500)
                logger.warning("Groq API (Key 1) failed with unexpected error: %s", e)

        # 2. Try Secondary: Groq Key 2
        if self.groq_client_2 and self.cb_groq_2.can_execute():
            try:
                now = time.time()
                if (now - self._last_fallback_log_time) >= 60:
                    logger.info("Groq key 1 unavailable/rate-limited; routing request to Groq key 2.")
                    self._last_fallback_log_time = now

                res = self._call_groq(self.groq_client_2, prompt, system_instruction, model=model)
                self.cb_groq_2.record_success()
                logger.debug(
                    "LLM OK | provider=groq_2_fallback | model=%s | words=%d",
                    model, len(res.split())
                )
                return res
            except GroqAPIError as e:
                status_code = e.status_code if hasattr(e, "status_code") else 500
                self.cb_groq_2.record_error(status_code)
                logger.warning("Groq API (Key 2) failed (HTTP %s): %s", status_code, str(e))
            except Exception as e:
                self.cb_groq_2.record_error(500)
                logger.warning("Groq API (Key 2) failed with unexpected error: %s", e)

        logger.error("All configured Groq clients are unavailable. Returning empty insight.")
        return ""

    async def _async_call_groq(self, client: AsyncGroq, model: str, prompt: str,
                               system_instruction: Optional[str] = None,
                               response_format: Optional[Dict[str, Any]] = None) -> str:
        messages = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})

        kwargs = {
            "messages": messages,
            "model": model,
            "temperature": 0.2, # Default conservative temp
        }
        if response_format:
            kwargs["response_format"] = response_format

        chat_completion = await client.chat.completions.create(**kwargs)
        if chat_completion.choices:
            return chat_completion.choices[0].message.content.strip()
        return ""

    async def async_generate_for_agent(self, agent_name: str, prompt: str,
                                       system_instruction: Optional[str] = None,
                                       response_format: Optional[Dict[str, Any]] = None) -> str:
        """
        Asynchronously generates an insight specifically routed by agent name, 
        respecting tiered semaphores and model mappings.
        """
        if not self.async_groq_client_1 and not self.async_groq_client_2:
            logger.warning("No async LLM clients configured.")
            return ""

        model = AGENT_MODEL_MAP.get(agent_name, "llama-3.3-70b-versatile")
        
        # Determine semaphore tier based on model type
        sem = TIER2_SEM if model == "llama-3.1-8b-instant" else TIER1_SEM

        async with sem:
            # 1. Try Primary: Groq Key 1
            if self.async_groq_client_1 and self.cb_groq_1.can_execute():
                try:
                    res = await self._async_call_groq(
                        self.async_groq_client_1, model, prompt, system_instruction, response_format
                    )
                    self.cb_groq_1.record_success()
                    logger.debug(
                        "Async LLM OK | agent=%s | provider=groq_1 | length=%d",
                        agent_name, len(res)
                    )
                    return res
                except GroqAPIError as e:
                    status_code = getattr(e, "status_code", 500)
                    self.cb_groq_1.record_error(status_code)
                    logger.warning("Async Groq API (Key 1) failed for %s (HTTP %s): %s", agent_name, status_code, e)
                except Exception as e:
                    self.cb_groq_1.record_error(500)
                    logger.warning("Async Groq API (Key 1) unexpected error for %s: %s", agent_name, e)

            # 2. Try Secondary: Groq Key 2
            if self.async_groq_client_2 and self.cb_groq_2.can_execute():
                try:
                    res = await self._async_call_groq(
                        self.async_groq_client_2, model, prompt, system_instruction, response_format
                    )
                    self.cb_groq_2.record_success()
                    logger.debug(
                        "Async LLM OK | agent=%s | provider=groq_2 | length=%d",
                        agent_name, len(res)
                    )
                    return res
                except GroqAPIError as e:
                    status_code = getattr(e, "status_code", 500)
                    self.cb_groq_2.record_error(status_code)
                    logger.warning("Async Groq API (Key 2) failed for %s (HTTP %s): %s", agent_name, status_code, e)
                except Exception as e:
                    self.cb_groq_2.record_error(500)
                    logger.warning("Async Groq API (Key 2) unexpected error for %s: %s", agent_name, e)

            logger.error("All async Groq clients failed for agent %s.", agent_name)
            return ""


if __name__ == "__main__":
    # Quick Test Block
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )
    
    print("\n" + "="*50)
    print("      VERTEX FOOTBALL - LLM QUICK TEST")
    print("="*50)
    
    client = LLMClient()
    
    if not client.groq_key_1:
        print("❌ GROQ_API_KEY không tồn tại trong .env")
    else:
        print(f"✅ Groq Key 1: Found (...{client.groq_key_1[-4:]})")
        
    if not client.groq_key_2:
        print("⚠️  GROQ_API_KEY_2 không tồn tại trong .env (chế độ dự phòng sẽ không hoạt động)")
    else:
        print(f"✅ Groq Key 2: Found (...{client.groq_key_2[-4:]})")

    print("-" * 50)
    test_prompt = "Viết một câu nhận xét ngắn (dưới 20 từ) về việc Erling Haaland ghi hat-trick trong trận derby Manchester."
    print(f"Testing Groq with prompt: '{test_prompt}'")
    
    start_time = time.time()
    result = client.generate_insight(test_prompt)
    duration = time.time() - start_time
    
    print("-" * 50)
    if result:
        print(f"✨ RESULT:\n{result}")
        print(f"\n⏱️ Latency: {duration:.2f}s")
        print("="*50 + "\n")
    else:
        print("❌ TEST FAILED. Vui lòng kiểm tra logs hoặc API Quota.")
        print("="*50 + "\n")

