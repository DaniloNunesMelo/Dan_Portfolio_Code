"""Hashify API client (injectable)."""

from __future__ import annotations

from typing import Any

import requests


def make_hashify_fn(
    base_url: str = "https://api.hashify.net/hash/md4/hex",
    response_field: str = "Digest",
    timeout: int = 10,
) -> callable:
    """Return a ``(claim_id: str) -> str`` function that calls the Hashify API.

    Parameters come from config so the URL and field name are not hardcoded.
    """

    def _hash(claim_id: str | None) -> str | None:
        if claim_id is None:
            return None
        resp = requests.get(
            base_url,
            params={"value": claim_id},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload: dict[str, Any] = resp.json()
        return payload[response_field]

    return _hash
