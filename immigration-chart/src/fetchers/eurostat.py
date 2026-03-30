"""
Eurostat REST API fetcher for Italy immigration enrichment.
Uses JSON-STAT 2.0 format.
"""
from datetime import datetime, timezone

import requests
import pandas as pd

from .base import BaseFetcher, FetchError

EUROSTAT_BASE = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}"
)

DATASET_TO_VAR = {
    "migr_imm1ctz": "B11",
    "migr_emi1ctz": "B12",
    "migr_acq": "B16",
}

DATASET_TO_METRIC = {
    "migr_imm1ctz": "Inflows of Foreign Population",
    "migr_emi1ctz": "Outflows of Foreign Population",
    "migr_acq": "Citizenship Acquisition",
}


class EurostatFetcher(BaseFetcher):
    TTL_HOURS = 12

    def fetch_italy(self, dataset: str = "migr_imm1ctz") -> tuple[pd.DataFrame, str]:
        return self.fetch(dataset=dataset, geo="IT", sex="T")

    def _fetch_live(self, dataset: str, geo: str = "IT", sex: str = "T") -> pd.DataFrame:
        url = EUROSTAT_BASE.format(dataset=dataset)
        params: dict = {"format": "JSON", "lang": "en", "geo": geo, "unit": "NR", "sex": sex}
        # migr_acq has different dimensions (no agedef/age); try without agedef
        # The other two datasets accept agedef/age but don't require them
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code in (404, 400):
            # Try without sex parameter (some Eurostat datasets don't have sex dim)
            params2 = {k: v for k, v in params.items() if k != "sex"}
            resp = requests.get(url, params=params2, timeout=30)
        if resp.status_code in (404, 400):
            raise FetchError(f"Eurostat returned {resp.status_code} for {dataset}")
        resp.raise_for_status()
        data = resp.json()
        return self._parse_json_stat(data, dataset, geo, sex)

    def _parse_json_stat(
        self, data: dict, dataset: str, geo: str, sex: str
    ) -> pd.DataFrame:
        """Parse JSON-STAT 2.0 response from Eurostat."""
        try:
            dim_ids: list[str] = data["id"]
            sizes: list[int] = data["size"]
            dimensions: dict = data["dimension"]
            values: dict = data.get("value", {})
        except KeyError as e:
            raise FetchError(f"Unexpected Eurostat JSON-STAT structure: {e}") from e

        # Build lookup: dim_id -> {pos: (code, label)}
        dim_lookup: dict[str, dict[int, tuple[str, str]]] = {}
        for dim_id in dim_ids:
            dim_info = dimensions.get(dim_id, {})
            cat = dim_info.get("category", {})
            index_map: dict[str, int] = cat.get("index", {})
            label_map: dict[str, str] = cat.get("label", {})
            pos_to_val: dict[int, tuple[str, str]] = {}
            for code, pos in index_map.items():
                label = label_map.get(code, code)
                pos_to_val[pos] = (code, label)
            dim_lookup[dim_id] = pos_to_val

        # Find indices for citizen and time dimensions
        try:
            citizen_dim_pos = dim_ids.index("citizen")
        except ValueError:
            # Try alternative names
            for alt in ["cit_go", "partner", "c_birth"]:
                if alt in dim_ids:
                    citizen_dim_pos = dim_ids.index(alt)
                    break
            else:
                citizen_dim_pos = None

        try:
            time_dim_pos = dim_ids.index("time")
        except ValueError:
            time_dim_pos = len(dim_ids) - 1  # usually last

        # Compute strides for decoding flat key
        strides = [1] * len(sizes)
        for i in range(len(sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * sizes[i + 1]

        var_code = DATASET_TO_VAR.get(dataset, "B11")
        metric = DATASET_TO_METRIC.get(dataset, "Inflows of Foreign Population")
        fetch_ts = datetime.now(timezone.utc)

        rows = []
        for k_str, obs_value in values.items():
            if obs_value is None:
                continue
            flat_key = int(k_str)

            # Decode flat key to dimension positions
            positions = []
            remainder = flat_key
            for size in sizes:
                # Recompute using strides
                pass

            # Decode using strides
            positions = []
            remainder = flat_key
            for i, stride in enumerate(strides):
                pos = remainder // stride
                remainder = remainder % stride
                positions.append(pos)

            citizen_code, citizen_label = ("UNK", "Unknown")
            time_code, time_label = ("", "")

            if citizen_dim_pos is not None and citizen_dim_pos < len(positions):
                c_pos = positions[citizen_dim_pos]
                citizen_code, citizen_label = dim_lookup.get(
                    dim_ids[citizen_dim_pos], {}
                ).get(c_pos, ("UNK", "Unknown"))

            if time_dim_pos is not None and time_dim_pos < len(positions):
                t_pos = positions[time_dim_pos]
                time_code, time_label = dim_lookup.get(
                    dim_ids[time_dim_pos], {}
                ).get(t_pos, ("", ""))

            year = _safe_int(time_code)
            rows.append(
                {
                    "ref_area": "ITA",
                    "ref_area_name": "Italy",
                    "counterpart": citizen_code,
                    "counterpart_name": citizen_label,
                    "time_period": time_code,
                    "year": year,
                    "quarter": None,
                    "var_code": var_code,
                    "metric": metric,
                    "sex": sex.upper() if sex else "T",
                    "gender": _sex_to_gender(sex),
                    "area_name": None,
                    "reg_name": None,
                    "province": None,
                    "imm_category": None,
                    "obs_value": float(obs_value),
                    "obs_status": None,
                    "source_dataset": "EUROSTAT",
                    "fetch_ts": fetch_ts,
                }
            )

        if not rows:
            raise FetchError(f"No values parsed from Eurostat response for {dataset}")

        return pd.DataFrame(rows)


def _safe_int(val) -> int | None:
    try:
        if isinstance(val, str) and "-Q" in val:
            return int(val.split("-")[0])
        return int(val)
    except (ValueError, TypeError):
        return None


def _sex_to_gender(sex: str) -> str:
    mapping = {"T": "Total", "M": "Male", "F": "Female"}
    return mapping.get(sex.upper() if sex else "T", "Total")
