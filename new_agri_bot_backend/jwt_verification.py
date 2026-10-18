"""JWT verification for delegated service tokens from MCP Gateway.

This module provides standalone JWT verification that can be integrated
into EridonAPI's existing Telegram auth flow without replacing it.

Security invariants:
- Only EdDSA algorithm is accepted (algorithm confusion attack prevention).
- Issuer must be 'mcp-eridon-gateway'.
- Audience must be 'eridon-api'.
- All mandatory claims (iss, aud, sub, iat, nbf, exp, jti) are required.
- Token TTL is validated (max 120s to allow clock skew).
- Public keys are identified by `kid` header.
- Replay protection: JTI uniqueness is tracked and enforced.
"""

import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import jwt as pyjwt

logger = logging.getLogger("agri_bot")

ALLOWED_ALGORITHMS = ["EdDSA"]
EXPECTED_ISSUER = "mcp-eridon-gateway"
EXPECTED_AUDIENCE = "eridon-api"
MAX_ACCEPTED_TTL_SECONDS = 120
REQUIRED_CLAIMS = ["iss", "aud", "sub", "iat", "nbf", "exp", "jti"]


class JWTVerificationError(Exception):
    """Raised when JWT verification fails for any reason."""
    pass


@dataclass
class VerificationKeyStore:
    """Holds public keys for verifying delegation tokens from MCP Gateway.

    Keys are indexed by `kid` (key ID) to support rotation.
    """

    _keys: dict[str, bytes] = field(default_factory=dict)

    def add_public_key(self, kid: str, public_key_pem: bytes) -> None:
        """Registers a public key PEM for a given kid."""
        self._keys[kid] = public_key_pem
        logger.info("Registered MCP Gateway verification key kid=%s", kid)

    def get_public_key(self, kid: str) -> bytes | None:
        """Returns public key PEM for the given kid, or None."""
        return self._keys.get(kid)

    def clear(self) -> None:
        """Clears all keys (useful for testing)."""
        self._keys.clear()

    @property
    def is_configured(self) -> bool:
        """Returns True if at least one verification key is registered."""
        return len(self._keys) > 0


_global_key_store = VerificationKeyStore()


def get_verification_key_store() -> VerificationKeyStore:
    """Returns global VerificationKeyStore singleton.

    If not yet populated and environment variables are present,
    attempts to initialize from MCP_GATEWAY_PUBLIC_KEY.
    """
    if not _global_key_store.is_configured:
        env_pem = os.getenv("MCP_GATEWAY_PUBLIC_KEY")
        kid = os.getenv("MCP_GATEWAY_KID", "dev-key-1")
        if env_pem:
            normalized_pem = env_pem.replace("\\n", "\n")
            _global_key_store.add_public_key(kid, normalized_pem.encode("utf-8"))
    return _global_key_store


@dataclass
class InMemoryJtiStore:
    """In-memory JTI replay store with thread locking for testing and single-process mode."""

    _seen_jtis: dict[str, float] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_if_new(self, jti: str, expires_at: float) -> bool:
        """Atomically records JTI if not previously seen. Returns False on replay."""
        now = time.time()
        with self._lock:
            self._prune(now)
            if jti in self._seen_jtis:
                return False
            self._seen_jtis[jti] = expires_at
            return True

    def is_jti_seen(self, jti: str) -> bool:
        """Returns True if jti has already been recorded and not expired."""
        with self._lock:
            self._prune(time.time())
            return jti in self._seen_jtis

    def record_jti(self, jti: str, expires_at: float) -> None:
        """Records jti with its expiration timestamp."""
        with self._lock:
            self._seen_jtis[jti] = expires_at

    def _prune(self, now: float) -> None:
        """Removes expired JTIs."""
        expired = [k for k, exp in self._seen_jtis.items() if exp < now]
        for k in expired:
            del self._seen_jtis[k]

    def clear(self) -> None:
        """Clears all seen JTIs."""
        with self._lock:
            self._seen_jtis.clear()


class SqliteJtiStore:
    """Inter-process and restart-persistent JTI replay store backed by SQLite in WAL mode.

    Guarantees:
    - Multi-process concurrency: WAL mode with busy timeout.
    - Atomic registration: INSERT INTO jti_log with primary key uniqueness catches replay race conditions.
    - Restart persistence: JTI tracking survives service restarts.
    - Fail-closed: DB errors raise JWTVerificationError immediately.
    """

    def __init__(self, db_path: str = "jti_replay.sqlite") -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        try:
            self._conn = sqlite3.connect(self.db_path, timeout=5.0, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._init_db()
        except Exception as e:
            logger.error("Fail-closed: Failed to initialize SQLite JTI store: %s", e)
            raise JWTVerificationError(f"Fail-closed: JTI store initialization error: {e}") from e

    def _init_db(self) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jti_log (
                        jti TEXT PRIMARY KEY,
                        expires_at REAL NOT NULL
                    )
                    """
                )
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_jti_exp ON jti_log(expires_at)"
                )

    def record_if_new(self, jti: str, expires_at: float) -> bool:
        """Atomically inserts JTI. Returns False if already exists (IntegrityError -> replay)."""
        now = time.time()
        try:
            with self._lock:
                with self._conn:
                    # 1. Prune expired entries
                    self._conn.execute("DELETE FROM jti_log WHERE expires_at < ?", (now,))
                    # 2. Atomic insert
                    try:
                        self._conn.execute(
                            "INSERT INTO jti_log (jti, expires_at) VALUES (?, ?)",
                            (jti, expires_at),
                        )
                        return True
                    except sqlite3.IntegrityError:
                        return False
        except Exception as e:
            logger.error("Fail-closed: JTI storage exception on record_if_new: %s", e)
            raise JWTVerificationError(f"Fail-closed: JTI verification store unavailable: {e}") from e

    def is_jti_seen(self, jti: str) -> bool:
        now = time.time()
        try:
            with self._lock:
                with self._conn:
                    self._conn.execute("DELETE FROM jti_log WHERE expires_at < ?", (now,))
                    cursor = self._conn.execute(
                        "SELECT 1 FROM jti_log WHERE jti = ? AND expires_at >= ?",
                        (jti, now),
                    )
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error("Fail-closed: JTI storage exception on is_jti_seen: %s", e)
            raise JWTVerificationError(f"Fail-closed: JTI store check failed: {e}") from e

    def record_jti(self, jti: str, expires_at: float) -> None:
        self.record_if_new(jti, expires_at)

    def clear(self) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute("DELETE FROM jti_log")

    def close(self) -> None:
        with self._lock:
            if getattr(self, "_conn", None):
                self._conn.close()
                self._conn = None


# Alias for backwards compatibility
JtiReplayStore = InMemoryJtiStore
_global_jti_store = InMemoryJtiStore()
_global_sqlite_jti_store: SqliteJtiStore | None = None


def get_jti_replay_store() -> InMemoryJtiStore | SqliteJtiStore:
    """Returns global JTI replay store singleton.

    If MCP_JTI_DB_PATH is specified in environment, returns persistent SqliteJtiStore (cached singleton).
    """
    global _global_sqlite_jti_store
    db_path = os.getenv("MCP_JTI_DB_PATH")
    if db_path:
        if _global_sqlite_jti_store is None or _global_sqlite_jti_store.db_path != db_path:
            _global_sqlite_jti_store = SqliteJtiStore(db_path)
        return _global_sqlite_jti_store
    return _global_jti_store


def verify_delegation_token(
    token: str,
    key_store: VerificationKeyStore | None = None,
    jti_store: Any | None = None,
) -> dict[str, Any]:
    """Verifies an MCP Gateway delegation JWT and returns validated claims.

    Args:
        token: Raw JWT string from Authorization: Bearer header.
        key_store: Store of public keys indexed by kid. Defaults to global store.
        jti_store: Store of seen JTIs for replay protection. Defaults to global store.

    Returns:
        Dictionary of verified claims including 'sub' (telegram_id as string).

    Raises:
        JWTVerificationError: For any verification failure or replay detection.
    """
    active_store = key_store or get_verification_key_store()
    active_jti_store = jti_store or get_jti_replay_store()

    # 1. Extract header without verifying (to get kid)
    try:
        unverified_header = pyjwt.get_unverified_header(token)
    except pyjwt.exceptions.DecodeError as e:
        raise JWTVerificationError(f"Malformed JWT header: {e}") from e

    # 2. Validate algorithm in header against whitelist
    alg = unverified_header.get("alg")
    if alg not in ALLOWED_ALGORITHMS:
        raise JWTVerificationError(
            f"Rejected algorithm '{alg}'. Only {ALLOWED_ALGORITHMS} are accepted."
        )

    # 3. Get kid and look up corresponding public key
    kid = unverified_header.get("kid")
    if not kid:
        raise JWTVerificationError("Missing 'kid' in JWT header")

    public_key_pem = active_store.get_public_key(kid)
    if public_key_pem is None:
        raise JWTVerificationError(f"Unknown key ID '{kid}'. Key not registered.")

    # 4. Full cryptographic verification
    try:
        claims = pyjwt.decode(
            token,
            public_key_pem,
            algorithms=ALLOWED_ALGORITHMS,
            audience=EXPECTED_AUDIENCE,
            issuer=EXPECTED_ISSUER,
            options={
                "require": REQUIRED_CLAIMS,
                "verify_exp": True,
                "verify_nbf": True,
                "verify_iat": True,
                "verify_aud": True,
                "verify_iss": True,
            },
        )
    except pyjwt.ExpiredSignatureError as e:
        raise JWTVerificationError("Token has expired") from e
    except pyjwt.InvalidAudienceError as e:
        raise JWTVerificationError(f"Invalid audience: {e}") from e
    except pyjwt.InvalidIssuerError as e:
        raise JWTVerificationError(f"Invalid issuer: {e}") from e
    except pyjwt.ImmatureSignatureError as e:
        raise JWTVerificationError(f"Token not yet valid: {e}") from e
    except pyjwt.MissingRequiredClaimError as e:
        raise JWTVerificationError(f"Missing required claim: {e}") from e
    except pyjwt.InvalidSignatureError as e:
        raise JWTVerificationError("Invalid token signature") from e
    except pyjwt.DecodeError as e:
        raise JWTVerificationError(f"Token decode error: {e}") from e
    except pyjwt.PyJWTError as e:
        raise JWTVerificationError(f"Token verification failed: {e}") from e

    # 5. Validate sub is present and non-empty
    sub = claims.get("sub")
    if not sub or not str(sub).strip():
        raise JWTVerificationError("Token 'sub' claim is empty or missing")

    # 6. Validate TTL is within acceptable range
    exp = claims.get("exp", 0)
    iat = claims.get("iat", 0)
    token_ttl = exp - iat
    if token_ttl > MAX_ACCEPTED_TTL_SECONDS:
        raise JWTVerificationError(
            f"Token TTL {token_ttl}s exceeds maximum accepted {MAX_ACCEPTED_TTL_SECONDS}s"
        )

    # 7. Enforce JTI replay protection atomically
    jti = claims.get("jti")
    if not jti or not str(jti).strip():
        raise JWTVerificationError("Token 'jti' claim is empty or missing")

    if hasattr(active_jti_store, "record_if_new"):
        is_new = active_jti_store.record_if_new(str(jti), float(exp))
        if not is_new:
            raise JWTVerificationError(
                f"Token JTI '{jti}' has already been used (replay attack detected)"
            )
    else:
        if active_jti_store.is_jti_seen(str(jti)):
            raise JWTVerificationError(
                f"Token JTI '{jti}' has already been used (replay attack detected)"
            )
        active_jti_store.record_jti(str(jti), float(exp))

    logger.info(
        "MCP delegation token verified: sub=%s kid=%s jti=%s",
        sub,
        kid,
        claims.get("jti", "?"),
    )

    return claims
