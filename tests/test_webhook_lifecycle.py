"""Tests for Telegram Webhook lifecycle management and protection."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from new_agri_bot_backend.config import should_manage_telegram_webhook


def test_should_manage_webhook_in_development():
    """In development/local/test environments, webhook management is strictly disabled."""
    assert should_manage_telegram_webhook(app_env="development", backend_url="https://taurus.pp.ua") is False
    assert should_manage_telegram_webhook(app_env="local", backend_url="https://taurus.pp.ua") is False
    assert should_manage_telegram_webhook(app_env="test", backend_url="https://taurus.pp.ua") is False
    assert should_manage_telegram_webhook(app_env="dev", backend_url="https://taurus.pp.ua") is False


def test_should_manage_webhook_without_backend_url():
    """If BACKEND_URL is missing or empty, webhook cannot be managed."""
    assert should_manage_telegram_webhook(app_env="production", backend_url="") is False
    assert should_manage_telegram_webhook(app_env="production", backend_url="   ") is False


def test_should_manage_webhook_in_production():
    """In production with valid BACKEND_URL, webhook management is enabled by default."""
    assert should_manage_telegram_webhook(app_env="production", backend_url="https://taurus.pp.ua") is True
    assert should_manage_telegram_webhook(app_env="prod", backend_url="https://taurus.pp.ua") is True


def test_should_manage_webhook_with_explicit_flags():
    """Explicit flag MANAGE_TELEGRAM_WEBHOOK can enable or disable management."""
    # Explicit disable in production
    assert should_manage_telegram_webhook(
        app_env="production",
        backend_url="https://taurus.pp.ua",
        manage_flag="false"
    ) is False
    assert should_manage_telegram_webhook(
        app_env="production",
        backend_url="https://taurus.pp.ua",
        manage_flag="0"
    ) is False

    # Development is NEVER allowed even with flag 'true' (hard safety invariant)
    assert should_manage_telegram_webhook(
        app_env="development",
        backend_url="https://taurus.pp.ua",
        manage_flag="true"
    ) is False


@pytest.mark.asyncio
async def test_lifespan_skips_webhook_in_development():
    """Verifies lifespan does NOT call set_webhook or delete_webhook in development."""
    mock_bot = MagicMock()
    mock_bot.set_webhook = AsyncMock()
    mock_bot.delete_webhook = AsyncMock()

    with patch("new_agri_bot_backend.main.bot", mock_bot), \
         patch("new_agri_bot_backend.main.should_manage_telegram_webhook", return_value=False), \
         patch("new_agri_bot_backend.main.setup_scheduler"), \
         patch("new_agri_bot_backend.main.setup_bot_handlers"), \
         patch("new_agri_bot_backend.main.ScheduledDeletions.create_table") as mock_sched_tbl:
        
        mock_sched_tbl.return_value.run = AsyncMock()
        
        from new_agri_bot_backend.main import lifespan, app

        async with lifespan(app):
            # Inside lifespan (running)
            pass

        # Verification: NEITHER set_webhook nor delete_webhook were called!
        mock_bot.set_webhook.assert_not_called()
        mock_bot.delete_webhook.assert_not_called()


@pytest.mark.asyncio
async def test_lifespan_manages_webhook_only_when_registered():
    """Verifies lifespan calls delete_webhook ONLY if set_webhook succeeded."""
    mock_bot = MagicMock()
    mock_bot.set_webhook = AsyncMock(return_value=True)
    mock_bot.delete_webhook = AsyncMock(return_value=True)

    with patch("new_agri_bot_backend.main.bot", mock_bot), \
         patch("new_agri_bot_backend.main.should_manage_telegram_webhook", return_value=True), \
         patch("new_agri_bot_backend.main.BACKEND_URL", "https://taurus.pp.ua"), \
         patch("new_agri_bot_backend.main.setup_scheduler"), \
         patch("new_agri_bot_backend.main.setup_bot_handlers"), \
         patch("new_agri_bot_backend.main.ScheduledDeletions.create_table") as mock_sched_tbl:
        
        mock_sched_tbl.return_value.run = AsyncMock()
        
        from new_agri_bot_backend.main import lifespan, app

        async with lifespan(app):
            mock_bot.set_webhook.assert_awaited_once_with("https://taurus.pp.ua/webhook/bot")

        # Upon exit, delete_webhook was awaited because set_webhook succeeded
        mock_bot.delete_webhook.assert_awaited_once()


@pytest.mark.asyncio
async def test_lifespan_does_not_delete_if_set_failed():
    """If set_webhook raised an exception on startup, delete_webhook is NOT called on shutdown."""
    mock_bot = MagicMock()
    mock_bot.set_webhook = AsyncMock(side_effect=RuntimeError("Network error"))
    mock_bot.delete_webhook = AsyncMock()

    with patch("new_agri_bot_backend.main.bot", mock_bot), \
         patch("new_agri_bot_backend.main.should_manage_telegram_webhook", return_value=True), \
         patch("new_agri_bot_backend.main.BACKEND_URL", "https://taurus.pp.ua"), \
         patch("new_agri_bot_backend.main.setup_scheduler"), \
         patch("new_agri_bot_backend.main.setup_bot_handlers"), \
         patch("new_agri_bot_backend.main.ScheduledDeletions.create_table") as mock_sched_tbl:
        
        mock_sched_tbl.return_value.run = AsyncMock()
        
        from new_agri_bot_backend.main import lifespan, app

        async with lifespan(app):
            pass

        # delete_webhook must NOT be called since registration failed
        mock_bot.delete_webhook.assert_not_called()
