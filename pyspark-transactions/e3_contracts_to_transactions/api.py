"""Hashify API client for computing NSE_ID (MD4 hex digest).

The task requires calling:
    https://api.hashify.net/hash/md4/hex?value=<CLAIM_ID>
and extracting the JSON field "Digest".

Design:
    fetch_nse_id() is a plain function with a configurable base URL so that
    tests can point it at a local stub without monkey-patching.
"""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

HASHIFY_URL = "https://api.hashify.net/hash/md4/hex"


def fetch_nse_id(
    claim_id: str,
    *,
    base_url: str = HASHIFY_URL,
    timeout_s: float = 5.0,
) -> str:
    """Return the MD4 hex digest for *claim_id* via the Hashify API.

    Parameters
    ----------
    claim_id:
        The CLAIM_ID value to hash.
    base_url:
        API endpoint. Override in tests to avoid real HTTP calls.
    timeout_s:
        HTTP timeout in seconds.

    Raises
    ------
    ValueError
        If *claim_id* is None or the response has no ``Digest`` field.
    requests.HTTPError
        On non-2xx responses.
    """
    if claim_id is None:
        raise ValueError("claim_id must not be None")

    resp = requests.get(base_url, params={"value": claim_id}, timeout=timeout_s)
    resp.raise_for_status()

    digest = resp.json().get("Digest")
    if not digest:
        raise ValueError(f"API response missing 'Digest': {resp.text[:200]}")

    logger.debug("NSE_ID for %s = %s", claim_id, digest)
    return str(digest)
