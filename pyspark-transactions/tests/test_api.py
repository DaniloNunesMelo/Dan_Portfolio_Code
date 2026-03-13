"""Tests for the Hashify API client.

All HTTP calls are mocked — no network required.
"""

from __future__ import annotations

from unittest.mock import patch, Mock

import pytest

from e3_contracts_to_transactions.api import fetch_nse_id


class TestFetchNseId:

    def test_returns_digest_from_json(self):
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"Digest": "abc123hex"}
        mock_resp.raise_for_status = Mock()

        with patch("e3_contracts_to_transactions.api.requests.get", return_value=mock_resp) as mock_get:
            result = fetch_nse_id("CL_001")

        assert result == "abc123hex"
        mock_get.assert_called_once()

    def test_raises_on_none_claim_id(self):
        with pytest.raises(ValueError, match="must not be None"):
            fetch_nse_id(None)

    def test_raises_on_missing_digest(self):
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"SomeOtherField": "value"}
        mock_resp.text = '{"SomeOtherField": "value"}'
        mock_resp.raise_for_status = Mock()

        with patch("e3_contracts_to_transactions.api.requests.get", return_value=mock_resp):
            with pytest.raises(ValueError, match="missing 'Digest'"):
                fetch_nse_id("CL_001")

    def test_raises_on_http_error(self):
        import requests as req

        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("500 Server Error")

        with patch("e3_contracts_to_transactions.api.requests.get", return_value=mock_resp):
            with pytest.raises(req.HTTPError):
                fetch_nse_id("CL_001")

    def test_uses_custom_base_url(self):
        mock_resp = Mock()
        mock_resp.json.return_value = {"Digest": "deadbeef"}
        mock_resp.raise_for_status = Mock()

        with patch("e3_contracts_to_transactions.api.requests.get", return_value=mock_resp) as mock_get:
            result = fetch_nse_id("CL_X", base_url="http://localhost:9999/hash")

        assert result == "deadbeef"
        mock_get.assert_called_once_with(
            "http://localhost:9999/hash",
            params={"value": "CL_X"},
            timeout=5.0,
        )
