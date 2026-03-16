"""API client tests (mocked HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from e3_contracts_to_transactions.api import (
    DEFAULT_BACKOFF_FACTOR,
    DEFAULT_RETRY_ON,
    DEFAULT_TIMEOUT,
    DEFAULT_TOTAL_RETRIES,
    make_hashify_fn,
)

# Patch the Session that is constructed *inside* the closure.
_SESSION_PATCH = "requests.Session"


def _make_mock_session(digest: str = "abc123def") -> MagicMock:
    """Return a mock context-manager Session whose get() returns a good response."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"Digest": digest}
    mock_resp.raise_for_status = MagicMock()

    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp
    # Support `with requests.Session() as session:`
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    return mock_session


class TestMakeHashifyFn:
    def test_returns_digest(self):
        mock_session = _make_mock_session("abc123def")

        with patch(_SESSION_PATCH, return_value=mock_session):
            fn = make_hashify_fn()
            result = fn("CL_123")

        assert result == "abc123def"
        mock_session.get.assert_called_once_with(
            "https://api.hashify.net/hash/md4/hex",
            params={"value": "CL_123"},
            timeout=DEFAULT_TIMEOUT,
        )

    def test_none_input_returns_none(self):
        fn = make_hashify_fn()
        assert fn(None) is None

    def test_custom_base_url_and_field(self):
        mock_session = _make_mock_session()
        mock_session.get.return_value.json.return_value = {"Hash": "custom_hash"}

        with patch(_SESSION_PATCH, return_value=mock_session):
            fn = make_hashify_fn(
                base_url="https://custom.api/hash",
                response_field="Hash",
                timeout=5,
            )
            result = fn("RX_456")

        assert result == "custom_hash"

    def test_http_error_raises(self):
        mock_session = _make_mock_session()
        mock_session.get.return_value.raise_for_status.side_effect = (
            requests.HTTPError("500 Server Error")
        )

        with patch(_SESSION_PATCH, return_value=mock_session):
            fn = make_hashify_fn()
            with pytest.raises(requests.HTTPError):
                fn("CL_999")

    def test_missing_field_raises_key_error(self):
        mock_session = _make_mock_session()
        mock_session.get.return_value.json.return_value = {"OtherField": "value"}

        with patch(_SESSION_PATCH, return_value=mock_session):
            fn = make_hashify_fn()
            with pytest.raises(KeyError):
                fn("CL_1")

    # -- retry configuration ---------------------------------------------------

    def test_default_retry_constants(self):
        assert DEFAULT_TOTAL_RETRIES == 3
        assert DEFAULT_BACKOFF_FACTOR == 0.5
        assert 500 in DEFAULT_RETRY_ON
        assert 429 in DEFAULT_RETRY_ON

    def test_retry_adapter_mounted(self):
        """Verify that an HTTPAdapter is mounted for both http and https."""
        mock_session = _make_mock_session()

        with patch(_SESSION_PATCH, return_value=mock_session):
            fn = make_hashify_fn(total_retries=2, backoff_factor=1.0)
            fn("CL_42")

        mount_calls = [call.args[0] for call in mock_session.mount.call_args_list]
        assert "https://" in mount_calls
        assert "http://" in mount_calls

    def test_custom_retry_parameters_accepted(self):
        """make_hashify_fn accepts custom retry knobs without raising."""
        mock_session = _make_mock_session()

        with patch(_SESSION_PATCH, return_value=mock_session):
            fn = make_hashify_fn(
                total_retries=5,
                backoff_factor=2.0,
                retry_on_status=(503, 504),
            )
            result = fn("CL_7")

        assert result == "abc123def"