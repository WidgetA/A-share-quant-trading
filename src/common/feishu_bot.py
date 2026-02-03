# === MODULE PURPOSE ===
# Feishu (Lark) bot client for sending alert notifications.
# Used for error alerts and system startup/shutdown notifications.

# === DEPENDENCIES ===
# - httpx: Async HTTP client
# - asyncio: Async sleep for retry delays
# - config: get_feishu_config for environment variable management

# === KEY CONCEPTS ===
# - External Bot Service: Messages sent via intermediary relay service
# - Retry Mechanism: Exponential backoff for reliability
# - Graceful Degradation: Skip if not configured, don't crash

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from src.common.config import get_feishu_config

logger = logging.getLogger(__name__)

# Beijing timezone for timestamps
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class FeishuBot:
    """
    Feishu bot client for sending alert messages via external bot service.

    The bot sends messages through an intermediary relay service that handles
    Feishu API authentication. This avoids embedding Feishu SDK dependencies.

    API Protocol:
        POST /api/send
        Payload: {app_id, app_secret, chat_id, message}
        Response: {code: 0, msg: "success"} or {code: non-zero, msg: "error"}

    Usage:
        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert("Error Title", "Error details...")
            await bot.send_startup_notification()
    """

    def __init__(
        self,
        bot_url: str | None = None,
        app_id: str | None = None,
        app_secret: str | None = None,
        chat_id: str | None = None,
    ):
        """
        Initialize Feishu bot client.

        Args:
            bot_url: Bot relay service URL (default from FEISHU_BOT_URL env)
            app_id: Feishu app ID (default from FEISHU_APP_ID env)
            app_secret: Feishu app secret (default from FEISHU_APP_SECRET env)
            chat_id: Target chat ID (default from FEISHU_CHAT_ID env)
        """
        config = get_feishu_config()
        self.bot_url = bot_url or config["bot_url"]
        self.app_id = app_id or config["app_id"]
        self.app_secret = app_secret or config["app_secret"]
        self.chat_id = chat_id or config["chat_id"]

    def is_configured(self) -> bool:
        """
        Check if all required credentials are configured.

        Returns:
            True if app_id, app_secret, and chat_id are all set
        """
        return bool(self.app_id and self.app_secret and self.chat_id)

    async def send_message(self, message: str, max_retries: int = 20) -> bool:
        """
        Send a text message to the configured Feishu group with retry mechanism.

        Args:
            message: The message text to send
            max_retries: Maximum number of retry attempts (default: 20)

        Returns:
            True if sent successfully, False otherwise

        Note:
            Uses exponential backoff: 1s, 2s, 4s, 8s, ... capped at 60s
        """
        if not self.is_configured():
            logger.warning("Feishu bot not configured, skipping message")
            return False

        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(
                        f"{self.bot_url}/api/send",
                        json={
                            "app_id": self.app_id,
                            "app_secret": self.app_secret,
                            "chat_id": self.chat_id,
                            "message": message,
                        },
                    )
                    response.raise_for_status()
                    data = response.json()

                    if data.get("code") == 0:
                        if attempt > 0:
                            logger.info(
                                f"Message sent successfully after {attempt} retries: "
                                f"{message[:50]}..."
                            )
                        else:
                            logger.debug(f"Message sent successfully: {message[:50]}...")
                        return True
                    else:
                        # API returned error code, treat as failure
                        last_error = Exception(f"Feishu API error: {data}")
                        logger.warning(
                            f"Feishu API error (attempt {attempt + 1}/{max_retries + 1}): {data}"
                        )

            except httpx.HTTPStatusError as e:
                last_error = e
                logger.warning(
                    f"Feishu HTTP error (attempt {attempt + 1}/{max_retries + 1}): "
                    f"{e.response.status_code} - {e.response.text}"
                )
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Failed to send Feishu message (attempt {attempt + 1}/{max_retries + 1}): {e}"
                )

            # If not the last attempt, wait before retrying with exponential backoff
            if attempt < max_retries:
                # Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 60s
                delay = min(2**attempt, 60)
                logger.debug(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)

        # All retries exhausted
        logger.error(
            f"Failed to send Feishu message after {max_retries + 1} attempts. "
            f"Last error: {last_error}"
        )
        return False

    async def send_alert(self, title: str, content: str) -> bool:
        """
        Send an error alert message.

        Args:
            title: Alert title (e.g., "Database Error", "Trading Execution Failed")
            content: Alert content/details

        Returns:
            True if sent successfully, False otherwise
        """
        message = f"🚨 {title}\n\n{content}"
        return await self.send_message(message)

    async def send_startup_notification(self) -> bool:
        """
        Send system startup notification.

        Returns:
            True if sent successfully, False otherwise
        """
        now = datetime.now(BEIJING_TZ)
        message = f"✅ A股交易系统已启动\n\n⏰ 启动时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        return await self.send_message(message)

    async def send_shutdown_notification(self) -> bool:
        """
        Send system shutdown notification.

        Returns:
            True if sent successfully, False otherwise
        """
        now = datetime.now(BEIJING_TZ)
        message = f"⚠️ A股交易系统已停止\n\n⏰ 停止时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        return await self.send_message(message)
