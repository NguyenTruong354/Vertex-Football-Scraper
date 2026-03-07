"""
Vertex Football - AI Service Router

Provides a unified interface to generate insights using multiple LLM providers.
Primary: Google Gemini (gemini-2.5-flash)
Fallback: Groq (llama-3.3-70b-versatile)
"""

import os
import time
import logging
from pathlib import Path
from typing import Optional
from google import genai
from google.genai import types as genai_types
from google.genai.errors import APIError as GeminiAPIError
from groq import Groq
from groq import APIStatusError as GroqAPIError

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
        self.provider = provider  # 'gemini' or 'groq'
        self.state = "CLOSED"
        self.consecutive_failures = 0
        self.next_retry_time = 0.0
        self.base_cooldown = 0
        self.current_cooldown = 0

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
                logger.warning("[CircuitBreaker-%s] OPEN — skipping request. Retry explicitly in %dm:%02ds.", self.name, mins, secs)
                return False
                
        if self.state == "HALF_OPEN":
            # The test request was already granted during the transition from OPEN -> HALF_OPEN.
            # All subsequent requests should be blocked until the test request completes.
            return False

    def record_success(self):
        if self.state != "CLOSED":
            logger.info("[CircuitBreaker-%s] SUCCESS — Circuit is now CLOSED.", self.name)
        self.state = "CLOSED"
        self.consecutive_failures = 0
        self.current_cooldown = self.base_cooldown

    def record_error(self, status_code: int):
        self.consecutive_failures += 1
        
        # Determine base cooldown and threshold based on provider and error
        threshold = 3
        
        if self.provider == "gemini":
            if status_code == 429:
                self.base_cooldown = 3600  # 60 mins
                threshold = 1
            elif status_code in (400, 404):
                self.base_cooldown = 86400 # 24 hours
                threshold = 1
            else:
                self.base_cooldown = 300   # 5 mins
        else: # groq
            if status_code == 429:
                self.base_cooldown = 60    # 1 min
                threshold = 1
            elif status_code in (400, 404):
                self.base_cooldown = 86400 # 24 hours
                threshold = 1
            else:
                self.base_cooldown = 300   # 5 mins
                
        if self.state == "HALF_OPEN":
            # Exponential backoff on failed recovery test
            self.current_cooldown = min(self.current_cooldown * 2, 86400)
            self.state = "OPEN"
            self.next_retry_time = time.time() + self.current_cooldown
            logger.warning("[CircuitBreaker-%s] HALF_OPEN test failed (HTTP %s). Back to OPEN. Next retry in %ds.", self.name, status_code, self.current_cooldown)
        elif self.state == "CLOSED":
            if self.consecutive_failures >= threshold:
                self.state = "OPEN"
                self.current_cooldown = self.base_cooldown
                self.next_retry_time = time.time() + self.current_cooldown
                logger.warning("[CircuitBreaker-%s] Tripped OPEN after %d failures (HTTP %s). Cooldown: %ds.", self.name, self.consecutive_failures, status_code, self.current_cooldown)

class LLMClient:
    def __init__(self):
        self.gemini_key_1 = os.getenv("GEMINI_API_KEY")
        self.gemini_key_2 = os.getenv("GEMINI_API_KEY_2")
        self.groq_key = os.getenv("GROQ_API_KEY")
        
        self.gemini_client_1 = None
        self.gemini_client_2 = None
        self.groq_client = None
        
        self.cb_gemini_1 = CircuitBreaker("Gemini_1", "gemini")
        self.cb_gemini_2 = CircuitBreaker("Gemini_2", "gemini")
        self.cb_groq = CircuitBreaker("Groq", "groq")
        
        if self.gemini_key_1:
            try:
                self.gemini_client_1 = genai.Client(api_key=self.gemini_key_1)
            except Exception as e:
                logger.error("Failed to init Gemini client 1: %s", e)
                
        if self.gemini_key_2:
            try:
                self.gemini_client_2 = genai.Client(api_key=self.gemini_key_2)
            except Exception as e:
                logger.error("Failed to init Gemini client 2: %s", e)
                
        if self.groq_key:
            try:
                self.groq_client = Groq(api_key=self.groq_key)
            except Exception as e:
                logger.error("Failed to init Groq client: %s", e)

    def _call_gemini(self, client, prompt: str, system_instruction: Optional[str] = None) -> str:
        config = genai_types.GenerateContentConfig()
        if system_instruction:
            config.system_instruction = system_instruction
            
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt,
            config=config
        )
        if response.text:
            return response.text.strip()
        return ""

    def generate_insight(self, prompt: str, system_instruction: Optional[str] = None) -> str:
        """
        Generates text using Gemini 1 first. If it fails, tries Gemini 2.
        If both fail, it seamlessly falls back to Groq.
        Wraps calls with Circuit Breaker to avoid quota exhaustion.
        """
        if not self.gemini_client_1 and not self.gemini_client_2 and not self.groq_client:
            logger.warning("No LLM clients configured. Returning empty insight.")
            return ""

        # 1. Try Primary: Gemini Key 1
        if self.gemini_client_1 and self.cb_gemini_1.can_execute():
            try:
                res = self._call_gemini(self.gemini_client_1, prompt, system_instruction)
                self.cb_gemini_1.record_success()
                return res
            except GeminiAPIError as e:
                status_code = e.code if hasattr(e, 'code') else 500
                self.cb_gemini_1.record_error(status_code)
                logger.warning("Gemini API (Key 1) failed (HTTP %s): %s", status_code, e.message if hasattr(e, 'message') else str(e))
            except Exception as e:
                self.cb_gemini_1.record_error(500)
                logger.warning("Gemini API (Key 1) failed with unexpected error: %s", e)

        # 2. Try Secondary: Gemini Key 2
        if self.gemini_client_2 and self.cb_gemini_2.can_execute():
            try:
                res = self._call_gemini(self.gemini_client_2, prompt, system_instruction)
                self.cb_gemini_2.record_success()
                return res
            except GeminiAPIError as e:
                status_code = e.code if hasattr(e, 'code') else 500
                self.cb_gemini_2.record_error(status_code)
                logger.warning("Gemini API (Key 2) failed (HTTP %s): %s", status_code, e.message if hasattr(e, 'message') else str(e))
            except Exception as e:
                self.cb_gemini_2.record_error(500)
                logger.warning("Gemini API (Key 2) failed with unexpected error: %s", e)

        # 3. Try Fallback: Groq (Llama 3)
        if self.groq_client and self.cb_groq.can_execute():
            try:
                messages = []
                if system_instruction:
                    messages.append({"role": "system", "content": system_instruction})
                messages.append({"role": "user", "content": prompt})

                chat_completion = self.groq_client.chat.completions.create(
                    messages=messages,
                    model="llama-3.3-70b-versatile",
                    temperature=0.7,
                )
                if chat_completion.choices:
                    res = chat_completion.choices[0].message.content.strip()
                    self.cb_groq.record_success()
                    return res
            except GroqAPIError as e:
                status_code = e.status_code if hasattr(e, 'status_code') else 500
                self.cb_groq.record_error(status_code)
                logger.error("Groq API fallback failed (HTTP %s): %s", status_code, str(e))
            except Exception as e:
                self.cb_groq.record_error(500)
                logger.error("Groq API fallback failed with unexpected error: %s", e)
                
        return ""

