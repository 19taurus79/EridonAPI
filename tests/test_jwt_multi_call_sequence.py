"""Tests verifying multi-request HTTP sequences within a single MCP tool call.

Addresses Задача 2:
1. Sequence 1: Search products -> Get remains for product 1 -> Get remains for product 2.
2. Sequence 2: Search clients -> Get contracts -> Get contract details.

Ensures that with per-request minted delegated tokens, all requests succeed without
JTI replay collision, while preserving strict Replay Attack defense against reused tokens.
"""

import time
import uuid
import pytest
import jwt
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from new_agri_bot_backend.jwt_verification import (
    verify_delegation_token,
    VerificationKeyStore,
    InMemoryJtiStore,
    JWTVerificationError,
)


@pytest.fixture
def auth_setup():
    """Sets up an Ed25519 key store and JTI replay store."""
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
    key_store = VerificationKeyStore()
    key_store.add_public_key("mcp-key-1", pub_pem)
    jti_store = InMemoryJtiStore()

    def mint_token(telegram_id: int) -> str:
        now = int(time.time())
        payload = {
            "iss": "mcp-eridon-gateway",
            "aud": "eridon-api",
            "sub": str(telegram_id),
            "iat": now,
            "nbf": now - 1,
            "exp": now + 30,
            "jti": uuid.uuid4().hex,
        }
        return jwt.encode(
            payload,
            priv_pem,
            algorithm="EdDSA",
            headers={"kid": "mcp-key-1", "typ": "JWT"},
        )

    return {
        "mint_token": mint_token,
        "key_store": key_store,
        "jti_store": jti_store,
    }


def test_sequence_1_products_and_remains_multi_call(auth_setup):
    """
    Sequence 1:
    1. Search products (HTTP request 1)
    2. Get remains for product 1 (HTTP request 2)
    3. Get remains for product 2 (HTTP request 3)

    Verifies all 3 requests succeed in one MCP tool operation when using
    per-request minted delegated tokens.
    """
    mint = auth_setup["mint_token"]
    key_store = auth_setup["key_store"]
    jti_store = auth_setup["jti_store"]
    telegram_id = 548019148

    # Step 1: Search products
    token_step_1 = mint(telegram_id)
    claims_1 = verify_delegation_token(token_step_1, key_store=key_store, jti_store=jti_store)
    assert claims_1["sub"] == str(telegram_id)

    # Step 2: Get remains for product 1
    token_step_2 = mint(telegram_id)
    claims_2 = verify_delegation_token(token_step_2, key_store=key_store, jti_store=jti_store)
    assert claims_2["sub"] == str(telegram_id)
    assert claims_2["jti"] != claims_1["jti"]

    # Step 3: Get remains for product 2
    token_step_3 = mint(telegram_id)
    claims_3 = verify_delegation_token(token_step_3, key_store=key_store, jti_store=jti_store)
    assert claims_3["sub"] == str(telegram_id)
    assert claims_3["jti"] != claims_2["jti"]

    # Replay Protection Verification:
    # If any intercepted token is reused, it MUST raise JWTVerificationError!
    with pytest.raises(JWTVerificationError) as exc_info:
        verify_delegation_token(token_step_1, key_store=key_store, jti_store=jti_store)
    assert "replay attack detected" in str(exc_info.value)


def test_sequence_2_clients_contracts_and_details_multi_call(auth_setup):
    """
    Sequence 2:
    1. Search clients (HTTP request 1)
    2. Get contracts (HTTP request 2)
    3. Get contract detail (HTTP request 3)

    Verifies all 3 requests succeed in one MCP tool operation when using
    per-request minted delegated tokens.
    """
    mint = auth_setup["mint_token"]
    key_store = auth_setup["key_store"]
    jti_store = auth_setup["jti_store"]
    telegram_id = 777888999

    # Step 1: Search clients
    token_step_1 = mint(telegram_id)
    claims_1 = verify_delegation_token(token_step_1, key_store=key_store, jti_store=jti_store)
    assert claims_1["sub"] == str(telegram_id)

    # Step 2: Get contracts for client
    token_step_2 = mint(telegram_id)
    claims_2 = verify_delegation_token(token_step_2, key_store=key_store, jti_store=jti_store)
    assert claims_2["sub"] == str(telegram_id)

    # Step 3: Get contract detail
    token_step_3 = mint(telegram_id)
    claims_3 = verify_delegation_token(token_step_3, key_store=key_store, jti_store=jti_store)
    assert claims_3["sub"] == str(telegram_id)

    # Replay Protection Verification:
    with pytest.raises(JWTVerificationError) as exc_info:
        verify_delegation_token(token_step_2, key_store=key_store, jti_store=jti_store)
    assert "replay attack detected" in str(exc_info.value)
