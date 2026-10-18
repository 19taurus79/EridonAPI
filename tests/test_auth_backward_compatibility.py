"""Tests for backward compatibility and auth delegation in telegram_auth.py."""

import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException, status
import jwt
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from new_agri_bot_backend.telegram_auth import (
    get_current_telegram_user,
    check_not_guest,
)
from new_agri_bot_backend.jwt_verification import (
    VerificationKeyStore,
    InMemoryJtiStore,
)


@pytest.fixture
def ed25519_keys():
    """Generates an Ed25519 keypair for tests."""
    priv = ed25519.Ed25519PrivateKey.generate()
    pub_pem = priv.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    priv_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return {"kid": "test-kid-1", "pub_pem": pub_pem, "priv_pem": priv_pem}


@pytest.mark.asyncio
async def test_collision_check_rejects_both_headers():
    """If both X-Telegram-Init-Data and Authorization are supplied, returns 400 Bad Request."""
    with pytest.raises(HTTPException) as exc_info:
        await get_current_telegram_user(
            x_telegram_init_data="user_data_here",
            authorization="Bearer test_token",
        )
    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "Одночасне використання декількох методів" in exc_info.value.detail


@pytest.mark.asyncio
async def test_missing_credentials_rejects_with_401():
    """If neither header is supplied, returns 401 Unauthorized."""
    with pytest.raises(HTTPException) as exc_info:
        await get_current_telegram_user(
            x_telegram_init_data=None,
            authorization=None,
        )
    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Відсутній заголовок авторизації" in exc_info.value.detail


@pytest.mark.asyncio
async def test_telegram_mini_app_backward_compatibility():
    """Verifies existing X-Telegram-Init-Data flow works exactly as before."""
    mock_user = MagicMock()
    mock_user.telegram_id = 548019148
    mock_user.is_allowed = True
    mock_user.is_guest = False

    fake_init_data = "user=%7B%22id%22%3A548019148%7D&hash=abc"
    parsed_dict = {"user": json.dumps({"id": 548019148})}

    with patch("new_agri_bot_backend.telegram_auth.check_telegram_auth", return_value=parsed_dict), \
         patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        user = await get_current_telegram_user(
            x_telegram_init_data=fake_init_data,
            authorization=None,
        )
        assert user.telegram_id == 548019148
        assert user.is_allowed is True


@pytest.mark.asyncio
async def test_delegated_bearer_token_authorization(ed25519_keys):
    """Verifies valid Bearer token from MCP Gateway successfully authenticates user."""
    mock_user = MagicMock()
    mock_user.telegram_id = 999888777
    mock_user.is_allowed = True
    mock_user.is_guest = False

    now = int(time.time())
    payload = {
        "iss": "mcp-eridon-gateway",
        "aud": "eridon-api",
        "sub": "999888777",
        "iat": now,
        "nbf": now - 1,
        "exp": now + 60,
        "jti": "jti-auth-test-1",
    }
    token = jwt.encode(
        payload,
        ed25519_keys["priv_pem"],
        algorithm="EdDSA",
        headers={"kid": ed25519_keys["kid"]},
    )

    key_store = VerificationKeyStore()
    key_store.add_public_key(ed25519_keys["kid"], ed25519_keys["pub_pem"])

    with patch("new_agri_bot_backend.jwt_verification.get_verification_key_store", return_value=key_store), \
         patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        user = await get_current_telegram_user(
            x_telegram_init_data=None,
            authorization=f"Bearer {token}",
        )
        assert user.telegram_id == 999888777
        assert user.is_allowed is True


@pytest.mark.asyncio
async def test_delegated_bearer_user_not_allowed_rejected(ed25519_keys):
    """If user in DB has is_allowed=False, access is forbidden with 403."""
    mock_user = MagicMock()
    mock_user.telegram_id = 111222333
    mock_user.is_allowed = False  # Blocked user!

    now = int(time.time())
    payload = {
        "iss": "mcp-eridon-gateway",
        "aud": "eridon-api",
        "sub": "111222333",
        "iat": now,
        "nbf": now,
        "exp": now + 60,
        "jti": "jti-auth-test-blocked",
    }
    token = jwt.encode(
        payload,
        ed25519_keys["priv_pem"],
        algorithm="EdDSA",
        headers={"kid": ed25519_keys["kid"]},
    )

    key_store = VerificationKeyStore()
    key_store.add_public_key(ed25519_keys["kid"], ed25519_keys["pub_pem"])

    with patch("new_agri_bot_backend.jwt_verification.get_verification_key_store", return_value=key_store), \
         patch("new_agri_bot_backend.tables.Users.objects") as mock_users_objs:
        
        mock_filter = MagicMock()
        mock_filter.first.return_value.run = AsyncMock(return_value=mock_user)
        mock_users_objs.return_value.where.return_value = mock_filter

        with pytest.raises(HTTPException) as exc_info:
            await get_current_telegram_user(
                x_telegram_init_data=None,
                authorization=f"Bearer {token}",
            )
        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_check_not_guest_rejects_guest_user():
    """check_not_guest dependency blocks guest users (is_guest=True) with 403."""
    guest_user = MagicMock()
    guest_user.is_guest = True

    with pytest.raises(HTTPException) as exc_info:
        await check_not_guest(current_user=guest_user)
    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN

    # Regular user succeeds
    regular_user = MagicMock()
    regular_user.is_guest = False
    result = await check_not_guest(current_user=regular_user)
    assert result == regular_user
