"""API client tests (mocked HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from e3_contracts_to_transactions.api import make_hashify_fn

_PATCH_TARGET = (
    "e3_contracts_to_transactions.api.requests.get"
)


class TestMakeHashifyFn:
    def test_returns_digest(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Digest": "abc123def"}
        mock_resp.raise_for_status = MagicMock()

        with patch(
            _PATCH_TARGET, return_value=mock_resp
        ) as mock_get:
            fn = make_hashify_fn()
            result = fn("CL_123")

        assert result == "abc123def"
        mock_get.assert_called_once_with(
            "https://api.hashify.net/hash/md4/hex",
            params={"value": "CL_123"},
            timeout=10,
        )

    def test_none_input_returns_none(self):
        fn = make_hashify_fn()
        assert fn(None) is None

    def test_custom_base_url_and_field(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "Hash": "custom_hash"
        }
        mock_resp.raise_for_status = MagicMock()

        with patch(
            _PATCH_TARGET, return_value=mock_resp
        ):
            fn = make_hashify_fn(
                base_url="https://custom.api/hash",
                response_field="Hash",
                timeout=5,
            )
            result = fn("RX_456")

        assert result == "custom_hash"

    def test_http_error_raises(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = (
            requests.HTTPError("500 Server Error")
        )

        with patch(
            _PATCH_TARGET, return_value=mock_resp
        ):
            fn = make_hashify_fn()
            with pytest.raises(requests.HTTPError):
                fn("CL_999")

    def test_missing_field_raises_key_error(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "OtherField": "value"
        }
        mock_resp.raise_for_status = MagicMock()

        with patch(
            _PATCH_TARGET, return_value=mock_resp
        ):
            fn = make_hashify_fn()
            with pytest.raises(KeyError):
                fn("CL_1")
