"""
IRCC (Immigration, Refugees and Citizenship Canada) open data fetcher.
File structure: multi-row header (title, blank, year row, quarter row, month row),
then data starting row 5 (0-indexed).
Annual total columns are identified by "YYYY Total" pattern in row 3.
"""
from datetime import datetime, timezone
from io import BytesIO
import re

import pandas as pd
import requests

from .base import BaseFetcher, FetchError

IRCC_BASE = "https://www.ircc.canada.ca/opendata-donneesouvertes/data/"

IRCC_FILES = {
    "citizenship":  "EN_ODP-PR-Citz.xlsx",
    "province_cat": "EN_ODP-PR-ProvImmCat.xlsx",
    "gender":       "EN_ODP-PR-Gen.xlsx",
    "age":          "EN_ODP-PR-Age.xlsx",
}

PROVINCE_ISO = {
    "Ontario": "CA-ON", "Quebec": "CA-QC", "British Columbia": "CA-BC",
    "Alberta": "CA-AB", "Manitoba": "CA-MB", "Saskatchewan": "CA-SK",
    "Nova Scotia": "CA-NS", "New Brunswick": "CA-NB",
    "Newfoundland and Labrador": "CA-NL", "Prince Edward Island": "CA-PE",
    "Northwest Territories": "CA-NT", "Yukon": "CA-YT", "Nunavut": "CA-NU",
}


class IRCCFetcher(BaseFetcher):
    TTL_HOURS = 168  # 7 days

    def fetch_by_citizenship(self) -> tuple[pd.DataFrame, str]:
        return self.fetch(file_key="citizenship")

    def fetch_by_province_category(self) -> tuple[pd.DataFrame, str]:
        return self.fetch(file_key="province_cat")

    def _fetch_live(self, file_key: str = "citizenship") -> pd.DataFrame:
        filename = IRCC_FILES.get(file_key)
        if not filename:
            raise FetchError(f"Unknown IRCC file key: {file_key}")
        url = IRCC_BASE + filename
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            raise FetchError(f"Failed to download IRCC {filename}: {e}") from e

        content = BytesIO(resp.content)
        return self._parse_ircc_xlsx(content, file_key)

    def _parse_ircc_xlsx(self, content: BytesIO, file_key: str) -> pd.DataFrame:
        """
        IRCC XLSX layout:
          Row 0: Title string
          Row 1: Empty
          Row 2: Year header (2015, 2016, ...) in first cell of each year block
          Row 3: Quarter/Annual header ("Q1", "Q2", ..., "YYYY Total")
          Row 4: Month header ("Jan", "Feb", ..., "Q1 Total", ..., "YYYY Total")
          Row 5+: Data (col 0 = country/category name, rest = values)
        Strategy: read all rows without header, find annual-total columns from row 3,
        build (country, year, value) tuples.
        """
        try:
            df_raw = pd.read_excel(content, header=None, engine="openpyxl")
        except Exception as e:
            raise FetchError(f"Failed to read IRCC Excel: {e}") from e

        # Find annual total column positions from row 3
        row3 = df_raw.iloc[3, :].tolist()
        annual_cols: list[tuple[int, int]] = []  # (col_index, year)
        for i, val in enumerate(row3):
            if isinstance(val, str):
                m = re.match(r"^(\d{4})\s*Total$", val.strip(), re.IGNORECASE)
                if m:
                    annual_cols.append((i, int(m.group(1))))

        if not annual_cols:
            raise FetchError("No annual total columns found in IRCC file")

        # Data starts at row 5 (0-indexed)
        data_df = df_raw.iloc[5:].copy()
        id_col = 0  # Country / province / category is always column 0

        # Build long-format rows
        rows = []
        fetch_ts = datetime.now(timezone.utc)

        for _, data_row in data_df.iterrows():
            id_val = data_row.iloc[id_col]
            if pd.isna(id_val) or str(id_val).strip() == "":
                continue
            id_str = str(id_val).strip()
            # Skip footnote or total rows that are clearly metadata
            if id_str.lower().startswith("source") or id_str.lower().startswith("note"):
                continue

            for col_idx, year in annual_cols:
                if col_idx >= len(data_row):
                    continue
                obs_val = data_row.iloc[col_idx]
                if pd.isna(obs_val):
                    continue
                try:
                    obs_float = float(obs_val)
                except (ValueError, TypeError):
                    continue
                if obs_float <= 0:
                    continue

                rows.append({
                    "id_value": id_str,
                    "year": year,
                    "obs_value": obs_float,
                })

        if not rows:
            raise FetchError("No data rows parsed from IRCC file")

        df = pd.DataFrame(rows)
        return self._to_canonical(df, file_key, fetch_ts)

    def _to_canonical(self, df: pd.DataFrame, file_key: str, fetch_ts: datetime) -> pd.DataFrame:
        """Convert parsed rows to canonical schema."""
        id_vals = df["id_value"].astype(str)

        if file_key == "province_cat":
            province_map = PROVINCE_ISO
            province_col = id_vals.map(lambda x: province_map.get(x, x))
        else:
            province_col = pd.Series([None] * len(df), index=df.index)

        return pd.DataFrame({
            "ref_area": "CAN",
            "ref_area_name": "Canada",
            "counterpart": id_vals.map(_name_to_iso3) if file_key == "citizenship" else "_T",
            "counterpart_name": id_vals if file_key == "citizenship" else "All Countries",
            "time_period": df["year"].astype(str),
            "year": df["year"].astype(int),
            "quarter": None,
            "var_code": "PR",
            "metric": "Permanent Residents",
            "sex": "_T",
            "gender": "Total",
            "area_name": None,
            "reg_name": None,
            "province": province_col,
            "imm_category": None,
            "obs_value": df["obs_value"],
            "obs_status": None,
            "source_dataset": "IRCC",
            "fetch_ts": fetch_ts,
        })


def _name_to_iso3(name: str) -> str:
    LOOKUP = {
        "Afghanistan": "AFG", "Albania": "ALB", "Algeria": "DZA",
        "Argentina": "ARG", "Australia": "AUS", "Austria": "AUT",
        "Bangladesh": "BGD", "Belgium": "BEL", "Brazil": "BRA",
        "Bulgaria": "BGR", "Cambodia": "KHM", "Cameroon": "CMR",
        "Chile": "CHL", "China": "CHN", "Colombia": "COL",
        "Croatia": "HRV", "Cuba": "CUB", "Czech Republic": "CZE",
        "Denmark": "DNK", "Ecuador": "ECU", "Egypt": "EGY",
        "El Salvador": "SLV", "Ethiopia": "ETH", "Finland": "FIN",
        "France": "FRA", "Germany": "DEU", "Ghana": "GHA",
        "Greece": "GRC", "Guatemala": "GTM", "Haiti": "HTI",
        "Honduras": "HND", "Hong Kong": "HKG", "Hungary": "HUN",
        "India": "IND", "Indonesia": "IDN", "Iran": "IRN",
        "Iraq": "IRQ", "Ireland": "IRL", "Israel": "ISR",
        "Italy": "ITA", "Jamaica": "JAM", "Japan": "JPN",
        "Jordan": "JOR", "Kenya": "KEN", "Korea, Republic of": "KOR",
        "South Korea": "KOR", "Lebanon": "LBN", "Malaysia": "MYS",
        "Mexico": "MEX", "Morocco": "MAR", "Nepal": "NPL",
        "Netherlands": "NLD", "New Zealand": "NZL", "Nigeria": "NGA",
        "Norway": "NOR", "Pakistan": "PAK", "Peru": "PER",
        "Philippines": "PHL", "Poland": "POL", "Portugal": "PRT",
        "Romania": "ROU", "Russia": "RUS", "Russian Federation": "RUS",
        "Saudi Arabia": "SAU", "Somalia": "SOM", "South Africa": "ZAF",
        "Spain": "ESP", "Sri Lanka": "LKA", "Sudan": "SDN",
        "Sweden": "SWE", "Switzerland": "CHE", "Syria": "SYR",
        "Syrian Arab Republic": "SYR", "Taiwan": "TWN", "Tanzania": "TZA",
        "Thailand": "THA", "Trinidad and Tobago": "TTO", "Tunisia": "TUN",
        "Turkey": "TUR", "Türkiye": "TUR", "Uganda": "UGA",
        "Ukraine": "UKR", "United Kingdom": "GBR", "United States": "USA",
        "Venezuela": "VEN", "Viet Nam": "VNM", "Vietnam": "VNM",
        "Zimbabwe": "ZWE", "All Countries": "_T", "Total": "_T",
    }
    return LOOKUP.get(name.strip(), name.strip()[:3].upper() if len(name.strip()) >= 3 else name.strip())
