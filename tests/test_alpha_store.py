from __future__ import annotations

import pytest

from app.alpha_store import AlphaAuthenticationError, AlphaStore, hash_password, normalize_username, verify_password


def test_password_hash_round_trip_and_wrong_password() -> None:
    encoded = hash_password("A-strong-alpha-password", salt=b"0123456789abcdef")

    assert verify_password("A-strong-alpha-password", encoded)
    assert not verify_password("wrong-password", encoded)
    assert "A-strong-alpha-password" not in encoded


def test_username_normalization_and_validation() -> None:
    assert normalize_username("  Tester.One ") == "tester.one"
    with pytest.raises(ValueError):
        normalize_username("bad username")


def test_password_requires_minimum_length() -> None:
    with pytest.raises(ValueError, match="12"):
        hash_password("short")


def test_session_tokens_are_stored_as_stable_sha256_hashes() -> None:
    token_hash = AlphaStore._session_token_hash("opaque-session-token")

    assert len(token_hash) == 64
    assert token_hash == AlphaStore._session_token_hash("opaque-session-token")
    assert "opaque-session-token" not in token_hash
    with pytest.raises(AlphaAuthenticationError):
        AlphaStore._session_token_hash("")
