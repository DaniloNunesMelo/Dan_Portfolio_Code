"""Hashify API client (injectable)."""

from __future__ import annotations

from typing import Callable

# ---------------------------------------------------------------------------
# Default retry policy (all primitives — fully picklable)
# ---------------------------------------------------------------------------
DEFAULT_TOTAL_RETRIES: int = 3
DEFAULT_BACKOFF_FACTOR: float = 0.5  # waits 0 s, 0.5 s, 1 s between attempts
DEFAULT_RETRY_ON: tuple[int, ...] = (429, 500, 502, 503, 504)
DEFAULT_TIMEOUT: int = 10


def make_hashify_fn(
    base_url: str = "https://api.hashify.net/hash/md4/hex",
    response_field: str = "Digest",
    timeout: int = DEFAULT_TIMEOUT,
    total_retries: int = DEFAULT_TOTAL_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    retry_on_status: tuple[int, ...] = DEFAULT_RETRY_ON,
) -> Callable[[str | None], str | None]:
    """Return a ``(claim_id: str) -> str`` function that calls the Hashify API.

    All captured values are primitives so the closure is fully picklable by
    Spark's cloudpickle (no top-level ``import requests`` in module scope).

    Retry behaviour
    ---------------
    The returned function mounts a ``urllib3.util.Retry``-backed
    ``HTTPAdapter`` on a fresh ``requests.Session`` per UDF invocation.
    Retries fire on connection errors, read errors, and the HTTP status
    codes listed in *retry_on_status* (default: 429, 5xx).

    Parameters
    ----------
    base_url:
        Hashify endpoint.
    response_field:
        JSON key to extract from the response (default ``"Digest"``).
    timeout:
        Per-request socket timeout in seconds.
    total_retries:
        Maximum number of retry attempts (default 3).
    backoff_factor:
        Exponential back-off multiplier. Sleep between retries is
        ``backoff_factor * 2 ** (attempt - 1)`` seconds.
    retry_on_status:
        HTTP status codes that trigger a retry.
    """

    def _hash(claim_id: str | None) -> str | None:
        if claim_id is None:
            return None

        # Local imports keep the closure free of unpicklable thread-locals.
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        retry_policy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=list(retry_on_status),
            allowed_methods=["GET"],
            raise_on_status=False,  # let raise_for_status() handle it below
        )
        adapter = HTTPAdapter(max_retries=retry_policy)

        with requests.Session() as session:
            session.mount("https://", adapter)
            session.mount("http://", adapter)

            resp = session.get(
                base_url,
                params={"value": claim_id},
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()[response_field]

    return _hash
