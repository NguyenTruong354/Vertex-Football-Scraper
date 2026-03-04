"""
Vertex Football - AI Service Router

Provides a unified interface to generate insights using multiple LLM providers.
Primary: Google Gemini (gemini-2.5-flash)
Fallback: Groq (llama-3.3-70b-versatile)
"""

import os
import logging
from typing import Optional
from google import genai
from google.genai import types as genai_types
from groq import Groq

logger = logging.getLogger(__name__)

class LLMClient:
    def __init__(self):
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        self.groq_key = os.getenv("GROQ_API_KEY")
        
        self.gemini_client = None
        self.groq_client = None
        
        if self.gemini_key:
            try:
                self.gemini_client = genai.Client(api_key=self.gemini_key)
            except Exception as e:
                logger.error("Failed to init Gemini client: %s", e)
                
        if self.groq_key:
            try:
                self.groq_client = Groq(api_key=self.groq_key)
            except Exception as e:
                logger.error("Failed to init Groq client: %s", e)

    def generate_insight(self, prompt: str, system_instruction: Optional[str] = None) -> str:
        """
        Generates text using Gemini first. If it fails (rate limit, error, or missing key),
        it seamlessly falls back to Groq.
        """
        if not self.gemini_client and not self.groq_client:
            logger.warning("No LLM clients configured. Returning empty insight.")
            return ""

        # 1. Try Primary: Gemini 2.5 Flash
        if self.gemini_client:
            try:
                config = genai_types.GenerateContentConfig()
                if system_instruction:
                    config.system_instruction = system_instruction
                    
                response = self.gemini_client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                    config=config
                )
                if response.text:
                    return response.text.strip()
            except Exception as e:
                logger.warning("Gemini API failed: %s. Falling back to Groq...", e)

        # 2. Try Fallback: Groq (Llama 3)
        if self.groq_client:
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
                    return chat_completion.choices[0].message.content.strip()
            except Exception as e:
                logger.error("Groq API fallback also failed: %s", e)
                
        return ""
