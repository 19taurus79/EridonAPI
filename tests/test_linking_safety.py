"""Tests for Telegram Linking safety invariants in mcp_linking.py and bot_handlers.py."""

import html
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from new_agri_bot_backend.mcp_linking import (
    confirm_mcp_linking,
    cancel_mcp_linking,
    get_mcp_confirmation,
    _pending_mcp_confirmations,
)


@pytest.fixture(autouse=True)
def clean_linking_registry():
    """Clears in-memory linking confirmations before and after each test."""
    _pending_mcp_confirmations.clear()
    yield
    _pending_mcp_confirmations.clear()


@pytest.mark.asyncio
async def test_confirm_mcp_linking_success_and_single_use():
    """Verifies linking confirmation works and rejects second reuse attempt (single-use)."""
    mock_user = MagicMock()
    mock_user.telegram_id = 123456789
    mock_user.full_name_for_orders = "Тестовий Менеджер"
    mock_user.is_allowed = True

    with patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        nonce = "test-valid-nonce-1"
        # 1. First confirmation must succeed
        success_first = await confirm_mcp_linking(nonce, 123456789)
        assert success_first is True

        # Check status recorded
        conf = get_mcp_confirmation(nonce)
        assert conf is not None
        assert conf["status"] == "confirmed"
        assert conf["telegram_id"] == 123456789

        # 2. Replay attempt with same nonce must be rejected!
        success_second = await confirm_mcp_linking(nonce, 123456789)
        assert success_second is False


@pytest.mark.asyncio
async def test_confirm_mcp_linking_rejects_unallowed_user():
    """User with is_allowed=False cannot confirm linking."""
    mock_user = MagicMock()
    mock_user.telegram_id = 555
    mock_user.is_allowed = False  # Blocked / pending

    with patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        success = await confirm_mcp_linking("nonce-unallowed", 555)
        assert success is False
        assert get_mcp_confirmation("nonce-unallowed") is None


@pytest.mark.asyncio
async def test_cancel_mcp_linking():
    """Cancellation records cancelled status and prevents subsequent confirmation."""
    nonce = "nonce-to-cancel"
    cancel_success = await cancel_mcp_linking(nonce)
    assert cancel_success is True

    conf = get_mcp_confirmation(nonce)
    assert conf["status"] == "cancelled"

    # Subsequent confirmation attempt on cancelled nonce must be rejected
    mock_user = MagicMock()
    mock_user.telegram_id = 123
    mock_user.is_allowed = True

    with patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        confirm_attempt = await confirm_mcp_linking(nonce, 123)
        assert confirm_attempt is False


@pytest.mark.asyncio
async def test_html_escaping_in_bot_display():
    """html.escape protects from malicious HTML/XSS in Telegram user names."""
    raw_name = "Иван <CEO & Hacker> Тест"
    escaped = html.escape(raw_name)
    assert "<" not in escaped
    assert ">" not in escaped
    assert "&amp;" in escaped
    assert escaped == "Иван &lt;CEO &amp; Hacker&gt; Тест"


def test_telegram_callback_length_limit():
    """Telegram inline callback_data limit is 64 bytes. Nonce must not exceed 52 bytes."""
    prefix = "mcp_confirm_"
    assert len(prefix) == 12
    max_nonce = 64 - len(prefix)
    assert max_nonce == 52

    # A 52-char nonce generates a valid 64-byte callback_data
    valid_nonce = "a" * 52
    assert len(f"{prefix}{valid_nonce}".encode("utf-8")) == 64

    # A 53-char nonce would exceed Telegram's limit
    invalid_nonce = "a" * 53
    assert len(f"{prefix}{invalid_nonce}".encode("utf-8")) == 65


@pytest.mark.asyncio
async def test_confirm_mcp_linking_forwards_user_data_and_handles_gateway_success(monkeypatch):
    """Phase 2.0.7: confirm_mcp_linking sends complete user_data and succeeds on 200."""
    monkeypatch.setenv("MCP_GATEWAY_URL", "http://127.0.0.1:8002")
    monkeypatch.setenv("MCP_INTERNAL_SECRET", "test-secret-internal")

    mock_user = MagicMock()
    mock_user.telegram_id = 548019148
    mock_user.username = "taurus_user"
    mock_user.full_name_for_orders = "Онищенко Сергій"
    mock_user.is_allowed = True
    mock_user.is_admin = True
    mock_user.is_guest = False

    sent_payload = None

    class MockResponse:
        status_code = 200
        text = "ok"

    async def mock_post(url, headers=None, json=None):
        nonlocal sent_payload
        assert url == "http://127.0.0.1:8002/internal/oauth/linking/confirm"
        assert headers == {"X-Internal-Secret": "test-secret-internal"}
        sent_payload = json
        return MockResponse()

    with patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        with patch("httpx.AsyncClient.post", side_effect=mock_post):
            success = await confirm_mcp_linking("nonce-phase207-success", 548019148)

    assert success is True
    assert sent_payload is not None
    assert sent_payload["nonce"] == "nonce-phase207-success"
    assert sent_payload["telegram_id"] == 548019148
    assert sent_payload["user_data"] == {
        "username": "taurus_user",
        "full_name_for_orders": "Онищенко Сергій",
        "is_admin": True,
        "is_allowed": True,
        "is_guest": False,
    }


@pytest.mark.asyncio
async def test_confirm_mcp_linking_fails_when_gateway_rejects(monkeypatch):
    """Phase 2.0.7: confirm_mcp_linking returns False when Gateway returns 400 or 500."""
    monkeypatch.setenv("MCP_GATEWAY_URL", "http://127.0.0.1:8002")
    monkeypatch.setenv("MCP_INTERNAL_SECRET", "test-secret-internal")

    mock_user = MagicMock()
    mock_user.telegram_id = 548019148
    mock_user.username = "taurus_user"
    mock_user.full_name_for_orders = "Онищенко Сергій"
    mock_user.is_allowed = True
    mock_user.is_admin = True
    mock_user.is_guest = False

    class MockErrorResponse:
        status_code = 400
        text = '{"error": "rejected"}'

    with patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        with patch("httpx.AsyncClient.post", return_value=MockErrorResponse()):
            success = await confirm_mcp_linking("nonce-phase207-rej", 548019148)

    assert success is False
    assert get_mcp_confirmation("nonce-phase207-rej") is None


@pytest.mark.asyncio
async def test_confirm_mcp_linking_fails_on_network_error(monkeypatch):
    """Phase 2.0.7: confirm_mcp_linking returns False when Gateway network fails."""
    monkeypatch.setenv("MCP_GATEWAY_URL", "http://127.0.0.1:8002")
    monkeypatch.setenv("MCP_INTERNAL_SECRET", "test-secret-internal")

    mock_user = MagicMock()
    mock_user.telegram_id = 548019148
    mock_user.username = "taurus_user"
    mock_user.full_name_for_orders = "Онищенко Сергій"
    mock_user.is_allowed = True
    mock_user.is_admin = True
    mock_user.is_guest = False

    with patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        with patch("httpx.AsyncClient.post", side_effect=Exception("Connection refused")):
            success = await confirm_mcp_linking("nonce-phase207-net-err", 548019148)

    assert success is False
    assert get_mcp_confirmation("nonce-phase207-net-err") is None
