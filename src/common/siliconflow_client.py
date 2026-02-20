# === MODULE PURPOSE ===
# Reusable async client for Silicon Flow LLM API (OpenAI-compatible).
# Wraps the chat/completions endpoint with httpx.

# === DEPENDENCIES ===
# - httpx: Async HTTP client (already a project dependency)

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

SILICONFLOW_BASE_URL = "https://api.siliconflow.cn/v1"


@dataclass
class SiliconFlowConfig:
    """Configuration for Silicon Flow API."""

    api_key: str
    model: str = "Qwen/Qwen2.5-72B-Instruct"
    max_tokens: int = 800
    temperature: float = 0.3
    timeout: float = 120.0


class SiliconFlowClient:
    """
    Async client for Silicon Flow chat completions.

    Uses OpenAI-compatible API format (same pattern as Aliyun DashScope
    in announcement_content.py).

    Usage:
        client = SiliconFlowClient(SiliconFlowConfig(api_key="sk-xxx"))
        await client.start()
        answer = await client.chat(system="You are...", user="Analyze...")
        await client.stop()
    """

    def __init__(self, config: SiliconFlowConfig):
        self._config = config
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._config.timeout)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def chat(
        self,
        system: str,
        user: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        """
        Send a chat completion request.

        Args:
            system: System prompt.
            user: User message.
            max_tokens: Override default max_tokens.
            temperature: Override default temperature.

        Returns:
            Assistant's response text.

        Raises:
            httpx.HTTPStatusError on API error.
            RuntimeError if not started.
        """
        if not self._client:
            raise RuntimeError("SiliconFlowClient not started. Call start() first.")

        resp = await self._client.post(
            f"{SILICONFLOW_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {self._config.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self._config.model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "max_tokens": max_tokens or self._config.max_tokens,
                "temperature": temperature if temperature is not None else self._config.temperature,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
