"""Telegram Linking integration for MCP Gateway."""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from .tables import Users

logger = logging.getLogger("agri_bot")

# In-memory registry of confirmed/cancelled linking proofs with TTL
_pending_mcp_confirmations: dict[str, dict[str, Any]] = {}
NONCE_TTL_SECONDS = 600  # 10 хвилин


def _prune_expired_confirmations() -> None:
    """Очищає застарілі nonces для запобігання витоку пам'яті."""
    now = time.time()
    expired = [
        k for k, v in _pending_mcp_confirmations.items()
        if v.get("created_timestamp", 0) + NONCE_TTL_SECONDS < now
    ]
    for k in expired:
        del _pending_mcp_confirmations[k]


async def confirm_mcp_linking(nonce: str, telegram_id: int) -> bool:
    """
    Підтверджує запит прив'язки до MCP Gateway.
    Забезпечує одноразове використання nonce та перевірку дозволу користувача.
    """
    _prune_expired_confirmations()

    # Захист від повторного використання
    existing = _pending_mcp_confirmations.get(nonce)
    if existing:
        if existing.get("status") in ("confirmed", "cancelled"):
            logger.warning(
                "MCP linking nonce %s already consumed (status=%s)",
                nonce[:8], existing.get("status")
            )
            return False

    user = await Users.objects().where(Users.telegram_id == telegram_id).first().run()
    if not user or not user.is_allowed:
        logger.warning("MCP linking rejected: user %s not allowed", telegram_id)
        return False

    # If MCP Gateway internal webhook is configured, notify gateway
    gateway_url = os.getenv("MCP_GATEWAY_URL")
    internal_secret = os.getenv("MCP_INTERNAL_SECRET")
    if gateway_url and internal_secret:
        user_data = {
            "username": user.username,
            "full_name_for_orders": user.full_name_for_orders,
            "is_admin": bool(user.is_admin),
            "is_allowed": bool(user.is_allowed),
            "is_guest": bool(user.is_guest),
        }
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"{gateway_url.rstrip('/')}/internal/oauth/linking/confirm",
                    headers={"X-Internal-Secret": internal_secret},
                    json={
                        "nonce": nonce,
                        "telegram_id": telegram_id,
                        "user_data": user_data,
                    },
                )
                if resp.status_code != 200:
                    logger.error(
                        "MCP Gateway rejected linking confirmation for nonce %s: HTTP %d %s",
                        nonce[:8],
                        resp.status_code,
                        resp.text[:200],
                    )
                    return False
        except Exception as e:
            logger.error("Failed to notify MCP Gateway via internal webhook: %s", e)
            return False

    now_utc = datetime.now(timezone.utc)
    _pending_mcp_confirmations[nonce] = {
        "telegram_id": telegram_id,
        "full_name": user.full_name_for_orders,
        "confirmed_at": now_utc,
        "created_timestamp": time.time(),
        "status": "confirmed",
    }
    logger.info("MCP linking confirmed for telegram_id=%s, nonce=%s", telegram_id, nonce[:8])
    return True


async def cancel_mcp_linking(nonce: str) -> bool:
    """Скасовує запит прив'язки до MCP Gateway."""
    _prune_expired_confirmations()

    existing = _pending_mcp_confirmations.get(nonce)
    if existing and existing.get("status") == "confirmed":
        logger.warning("Cannot cancel already confirmed MCP linking nonce %s", nonce[:8])
        return False

    _pending_mcp_confirmations[nonce] = {
        "status": "cancelled",
        "cancelled_at": datetime.now(timezone.utc),
        "created_timestamp": time.time(),
    }
    logger.info("MCP linking cancelled for nonce=%s", nonce[:8])
    return True


def get_mcp_confirmation(nonce: str) -> dict[str, Any] | None:
    """Повертає статус підтвердження для nonce (з урахуванням TTL)."""
    _prune_expired_confirmations()
    return _pending_mcp_confirmations.get(nonce)
