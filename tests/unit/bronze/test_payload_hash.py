"""Tests for `lol_analytics.bronze.payload_hash`."""

from __future__ import annotations

import pytest

from lol_analytics.bronze.payload_hash import sha256_hex


class TestSha256Hex:
    def test_known_value_empty_string(self) -> None:
        # SHA-256 of "" is the well-known empty-string digest. Locking
        # this down protects against accidental algorithm changes.
        assert sha256_hex("") == (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

    def test_known_value_ascii(self) -> None:
        assert sha256_hex("hello") == (
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )

    def test_known_value_utf8_multibyte(self) -> None:
        # Encodes "ç" as 2 bytes in UTF-8 — verifies we hash bytes, not codepoints.
        assert sha256_hex("ção") == (
            "4ab2d81279c483cb89ff8224683c386fc9ad7837c9fe19a3f6c32af25384bdaf"
        )

    def test_distinct_inputs_produce_distinct_hashes(self) -> None:
        # Whitespace difference must change the hash — we hash the raw
        # bytes Riot sent, not a canonicalized form.
        a = '{"matchId":"BR1_123"}'
        b = '{"matchId": "BR1_123"}'
        assert sha256_hex(a) != sha256_hex(b)

    def test_rejects_bytes(self) -> None:
        with pytest.raises(TypeError, match="must be str"):
            sha256_hex(b"already bytes")  # type: ignore[arg-type]

    def test_rejects_none(self) -> None:
        with pytest.raises(TypeError, match="must be str"):
            sha256_hex(None)  # type: ignore[arg-type]
