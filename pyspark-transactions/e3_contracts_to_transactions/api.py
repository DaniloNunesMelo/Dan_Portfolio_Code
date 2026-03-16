"""Hashify API client (injectable)."""

from __future__ import annotations

import logging
from typing import Any, Callable

import requests
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)


def make_hashify_fn(
    base_url: str = "https://api.hashify.net/hash/md4/hex",
    response_field: str = "Digest",
    timeout: int = 10,
    max_retries: int = 3,
) -> Callable[[str | None], str | None]:
    """Return a ``(claim_id: str) -> str`` function that calls the Hashify API.

    Parameters come from config so the URL and field name are not hardcoded.
    Includes automatic retry logic for transient failures (timeout, connection errors).

    Parameters
    ----------
    base_url : str, default "https://api.hashify.net/hash/md4/hex"
        API endpoint for hashing.
    response_field : str, default "Digest"
        JSON field name containing the hash result.
    timeout : int, default 10
        Request timeout in seconds.
    max_retries : int, default 3
        Maximum number of retry attempts for transient errors.

    Returns
    -------
    Callable[[str | None], str | None]
        A function mapping claim IDs to hash digests, or None for None input.
    """

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(max_retries),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        reraise=True,
    )
    def _hash_with_retry(claim_id: str) -> str:
        """Internal function with retry decorator for transient failures."""
        resp = requests.get(
            base_url,
            params={"value": claim_id},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload: dict[str, Any] = resp.json()
        return payload[response_field]

    def _hash(claim_id: str | None) -> str | None:
        if claim_id is None:
            return None
        try:
            return _hash_with_retry(claim_id)
        except requests.RequestException:
            logger.error(
                f"Failed to hash claim_id={claim_id} after {max_retries} retries",
                exc_info=True,
            )
            raise

    return _hash
