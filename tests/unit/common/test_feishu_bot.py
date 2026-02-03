# === MODULE PURPOSE ===
# Tests for FeishuBot client.
# Verifies message sending, configuration check, and error handling.

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.common.feishu_bot import FeishuBot


class TestFeishuBot:
    """Tests for FeishuBot client."""

    def test_is_configured_with_all_credentials(self):
        """Test is_configured returns True when all credentials set."""
        bot = FeishuBot(
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )
        assert bot.is_configured() is True

    def test_is_configured_missing_app_id(self):
        """Test is_configured returns False when app_id missing."""
        bot = FeishuBot(
            app_id="",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )
        assert bot.is_configured() is False

    def test_is_configured_missing_app_secret(self):
        """Test is_configured returns False when app_secret missing."""
        bot = FeishuBot(
            app_id="test_app_id",
            app_secret="",
            chat_id="test_chat_id",
        )
        assert bot.is_configured() is False

    def test_is_configured_missing_chat_id(self):
        """Test is_configured returns False when chat_id missing."""
        bot = FeishuBot(
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="",
        )
        assert bot.is_configured() is False

    @pytest.mark.asyncio
    async def test_send_message_not_configured(self):
        """Test send_message returns False when not configured."""
        bot = FeishuBot(app_id="", app_secret="", chat_id="")
        result = await bot.send_message("test message")
        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_success(self):
        """Test send_message returns True on successful API call."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"code": 0}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )
            result = await bot.send_message("test message")

        assert result is True

    @pytest.mark.asyncio
    async def test_send_message_api_error(self):
        """Test send_message returns False on API error response."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"code": 1, "msg": "error"}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )
            # Use max_retries=0 to avoid retry delays in test
            result = await bot.send_message("test message", max_retries=0)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_http_error(self):
        """Test send_message returns False on HTTP error."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=httpx.HTTPStatusError(
                    "error", request=MagicMock(), response=mock_response
                )
            )
            # Use max_retries=0 to avoid retry delays in test
            result = await bot.send_message("test message", max_retries=0)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_network_error(self):
        """Test send_message returns False on network error."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=Exception("Network error")
            )
            # Use max_retries=0 to avoid retry delays in test
            result = await bot.send_message("test message", max_retries=0)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_message_retry_success(self):
        """Test send_message retries and succeeds after initial failures."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        # First call fails, second succeeds
        mock_response_fail = MagicMock()
        mock_response_fail.json.return_value = {"code": 1, "msg": "error"}
        mock_response_fail.raise_for_status = MagicMock()

        mock_response_success = MagicMock()
        mock_response_success.json.return_value = {"code": 0}
        mock_response_success.raise_for_status = MagicMock()

        with (
            patch("httpx.AsyncClient") as mock_client,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=[mock_response_fail, mock_response_success]
            )
            result = await bot.send_message("test message", max_retries=1)

            # Should have retried once
            mock_sleep.assert_called_once_with(1)  # First retry delay is 1s

        assert result is True

    @pytest.mark.asyncio
    async def test_send_message_retry_exhausted(self):
        """Test send_message returns False after all retries exhausted."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"code": 1, "msg": "error"}
        mock_response.raise_for_status = MagicMock()

        with (
            patch("httpx.AsyncClient") as mock_client,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )
            result = await bot.send_message("test message", max_retries=2)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_alert_formats_message(self):
        """Test send_alert formats message with title and content."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        with patch.object(bot, "send_message", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = True
            await bot.send_alert("Test Title", "Test Content")

            mock_send.assert_called_once()
            message = mock_send.call_args[0][0]
            assert "🚨" in message
            assert "Test Title" in message
            assert "Test Content" in message

    @pytest.mark.asyncio
    async def test_send_startup_notification(self):
        """Test send_startup_notification sends formatted message."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        with patch.object(bot, "send_message", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = True
            await bot.send_startup_notification()

            mock_send.assert_called_once()
            message = mock_send.call_args[0][0]
            assert "✅" in message
            assert "A股交易系统已启动" in message
            assert "启动时间" in message

    @pytest.mark.asyncio
    async def test_send_shutdown_notification(self):
        """Test send_shutdown_notification sends formatted message."""
        bot = FeishuBot(
            bot_url="http://test.local",
            app_id="test_app_id",
            app_secret="test_secret",
            chat_id="test_chat_id",
        )

        with patch.object(bot, "send_message", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = True
            await bot.send_shutdown_notification()

            mock_send.assert_called_once()
            message = mock_send.call_args[0][0]
            assert "⚠️" in message
            assert "A股交易系统已停止" in message
            assert "停止时间" in message


class TestGetFeishuConfig:
    """Tests for get_feishu_config function."""

    def test_get_feishu_config_from_env(self):
        """Test get_feishu_config reads from environment variables."""
        from src.common.config import get_feishu_config

        with patch.dict(
            "os.environ",
            {
                "FEISHU_BOT_URL": "http://custom.url",
                "FEISHU_APP_ID": "env_app_id",
                "FEISHU_APP_SECRET": "env_secret",
                "FEISHU_CHAT_ID": "env_chat_id",
            },
        ):
            config = get_feishu_config()

            assert config["bot_url"] == "http://custom.url"
            assert config["app_id"] == "env_app_id"
            assert config["app_secret"] == "env_secret"
            assert config["chat_id"] == "env_chat_id"

    def test_get_feishu_config_defaults(self):
        """Test get_feishu_config uses defaults when env not set."""
        from src.common.config import get_feishu_config

        with patch.dict(
            "os.environ",
            {},
            clear=True,
        ):
            config = get_feishu_config()

            # Should have default bot_url and empty strings for others
            assert "leapcell.dev" in config["bot_url"]
            assert config["app_id"] == ""
            assert config["app_secret"] == ""
            assert config["chat_id"] == ""
