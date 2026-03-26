"""
OECD SDMX-JSON API fetcher for immigration data.
Covers ITA (Italy) and CAN (Canada).
Downloads full dataset with time filter, then filters client-side.
Series key format: REF_AREA:CITIZENSHIP:FREQ:MEASURE:SEX:BIRTH_PLACE:EDUCATION_LEV:UNIT_MEASURE
"""
from datetime import datetime, timezone

import requests
import pandas as pd

from .base import BaseFetcher, FetchError

VAR_TO_METRIC = {
    "B11": "Inflows of Foreign Population",
    "B12": "Outflows of Foreign Population",
    "B13": "Asylum Seekers",
    "B14": "Stock of Foreign-Born Population",
    "B15": "Stock of Foreign Nationals",
    "B16": "Citizenship Acquisition",
}

SEX_TO_GENDER = {"_T": "Total", "T": "Total", "M": "Male", "F": "Female"}

AREA_TO_NAME = {
    "ITA": "Italy",
    "CAN": "Canada",
}

OECD_URL = (
    "https://sdmx.oecd.org/public/rest/data/OECD.ELS.IMD,DSD_MIG@DF_MIG/"
)


class OECDFetcher(BaseFetcher):
    TTL_HOURS = 24

    def fetch_country(
        self, ref_area: str, var_code: str = "B11", sex: str = "T"
    ) -> tuple[pd.DataFrame, str]:
        """ref_area: ITA or CAN. Returns canonical-schema DataFrame."""
        return self.fetch(ref_area=ref_area, var_code=var_code, sex=sex)

    def _fetch_live(self, ref_area: str, var_code: str = "B11", sex: str = "T") -> pd.DataFrame:
        params = {
            "format": "jsondata",
            "startPeriod": "1998",
            "endPeriod": str(datetime.now(timezone.utc).year),
        }
        resp = requests.get(OECD_URL, params=params, timeout=60)
        if resp.status_code in (404, 422):
            raise FetchError(f"OECD returned {resp.status_code}")
        resp.raise_for_status()

        data = resp.json()
        return self._parse_series_format(data, ref_area, var_code, sex)

    def _parse_series_format(
        self, data: dict, ref_area: str, var_code: str, sex: str
    ) -> pd.DataFrame:
        """Parse SDMX-JSON series format (new OECD API v2)."""
        try:
            ds = data["data"]["dataSets"][0]
            structure = data["data"]["structures"][0]
        except (KeyError, IndexError) as e:
            raise FetchError(f"Unexpected OECD structure: {e}") from e

        series_dims = structure["dimensions"].get("series", [])
        obs_dims = structure["dimensions"].get("observation", [])

        # Build value maps per dimension
        def _vals(dims):
            return [{v["id"]: (i, v.get("name", v["id"])) for i, v in enumerate(d["values"])}
                    for d in dims]

        s_vals = _vals(series_dims)  # list of {id: (pos, name)} for each series dim
        o_vals = _vals(obs_dims)     # list of {id: (pos, name)} for each obs dim

        # Reverse map: pos → (id, name) for series dims
        s_rev = [{v[0]: (k, v[1]) for k, v in d.items()} for d in s_vals]
        o_rev = [{v[0]: (k, v[1]) for k, v in d.items()} for d in o_vals]

        # Find dimension positions
        s_ids = [d["id"] for d in series_dims]
        try:
            ref_area_dim = s_ids.index("REF_AREA")
            measure_dim = s_ids.index("MEASURE")
            sex_dim = s_ids.index("SEX")
            cit_dim = s_ids.index("CITIZENSHIP") if "CITIZENSHIP" in s_ids else None
        except ValueError as e:
            raise FetchError(f"Missing dimension in OECD response: {e}") from e

        # Find desired positions
        ref_area_pos = s_vals[ref_area_dim].get(ref_area, (None,))[0]
        # Normalize sex codes: T → _T (OECD uses _T)
        sex_code = "_T" if sex in ("T", "_T") else sex
        sex_pos = s_vals[sex_dim].get(sex_code, (None,))[0]
        measure_pos = s_vals[measure_dim].get(var_code, (None,))[0]

        if ref_area_pos is None:
            raise FetchError(f"Country {ref_area} not found in OECD data")
        if measure_pos is None:
            raise FetchError(f"Measure {var_code} not found in OECD data")

        series = ds.get("series", {})
        fetch_ts = datetime.now(timezone.utc)
        rows = []

        for key_str, series_data in series.items():
            positions = [int(p) for p in key_str.split(":")]
            if len(positions) < len(s_ids):
                continue

            # Filter by country, measure, sex
            if positions[ref_area_dim] != ref_area_pos:
                continue
            if positions[measure_dim] != measure_pos:
                continue
            if sex_pos is not None and positions[sex_dim] != sex_pos:
                continue

            # Decode citizenship
            cit_id, cit_name = "UNK", "Unknown"
            if cit_dim is not None:
                cit_id, cit_name = s_rev[cit_dim].get(positions[cit_dim], ("UNK", "Unknown"))

            # Decode sex
            sex_id, _ = s_rev[sex_dim].get(positions[sex_dim], ("_T", "Total"))

            # Iterate observations (TIME_PERIOD)
            for obs_key_str, obs_val in series_data.get("observations", {}).items():
                t_pos = int(obs_key_str)
                time_code, _ = o_rev[0].get(t_pos, ("", "")) if o_rev else ("", "")
                value = obs_val[0] if obs_val and obs_val[0] is not None else None
                status = obs_val[1] if len(obs_val) > 1 else None

                rows.append({
                    "ref_area": ref_area,
                    "ref_area_name": AREA_TO_NAME.get(ref_area, ref_area),
                    "counterpart": cit_id,
                    "counterpart_name": cit_name,
                    "time_period": time_code,
                    "year": _safe_int(time_code),
                    "quarter": None,
                    "var_code": var_code,
                    "metric": VAR_TO_METRIC.get(var_code, var_code),
                    "sex": sex_code,
                    "gender": SEX_TO_GENDER.get(sex_id, "Total"),
                    "area_name": None,
                    "reg_name": None,
                    "province": None,
                    "imm_category": None,
                    "obs_value": float(value) if value is not None else None,
                    "obs_status": str(status) if status is not None else None,
                    "source_dataset": "OECD_MIG",
                    "fetch_ts": fetch_ts,
                })

        if not rows:
            raise FetchError(f"No matching observations for {ref_area}/{var_code}/{sex}")

        df = pd.DataFrame(rows)
        df = df[df["obs_value"].notna()]
        return df


def _safe_int(val) -> int | None:
    try:
        if isinstance(val, str) and "-Q" in val:
            return int(val.split("-")[0])
        return int(val)
    except (ValueError, TypeError):
        return None
