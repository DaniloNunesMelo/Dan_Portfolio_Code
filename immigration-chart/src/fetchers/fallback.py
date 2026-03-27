"""
Fallback reader: loads legacy CSV files from data/raw/.
Files must follow the naming pattern {src}_{country}.csv
  e.g. oecd_italy.csv, un_canada.csv
Returns canonical-schema DataFrames.
"""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATA_RAW = Path(__file__).parents[2] / "data" / "raw"

VAR_TO_METRIC = {
    "B11": "Inflows of Foreign Population",
    "B12": "Outflows of Foreign Population",
    "B13": "Asylum Seekers",
    "B14": "Stock of Foreign-Born Population",
    "B15": "Stock of Foreign Nationals",
    "B16": "Citizenship Acquisition",
    "B21": "Outflows of National Population",
}

GEN_TO_GENDER = {"TOT": "Total", "MAS": "Male", "FEM": "Female", "T": "Total", "M": "Male", "F": "Female"}


_SUPPORTED_EXTENSIONS = {".csv"}


def discover_files(src: str | None = None, country: str | None = None) -> list[Path]:
    """
    Return paths in DATA_RAW matching the pattern {src}_{country}.{ext}.
    Pass None for src or country to wildcard that part.
    """
    src_part = src if src else "*"
    country_part = country if country else "*"
    pattern = f"{src_part}_{country_part}.*"
    return sorted(DATA_RAW.glob(pattern))


def load_fallback(src: str, country: str) -> pd.DataFrame:
    """
    Discover and load the fallback CSV for the given source+country pair.
    Dispatches by src prefix: 'oecd' → OECD column format, 'un' → pre-melted canonical format.
    Raises FileNotFoundError if no matching file is found.
    Raises ValueError if the file extension is not supported.
    """
    matches = discover_files(src, country)
    if not matches:
        raise FileNotFoundError(
            f"No fallback file found for src='{src}' country='{country}' in {DATA_RAW}. "
            f"Expected a file matching '{src}_{country}.*'"
        )
    path = matches[0]
    ext = path.suffix.lower()
    if ext == ".csv" and src == "oecd":
        return _load_oecd_csv(path)
    elif ext == ".csv" and src == "un":
        return _load_un_csv(path)
    else:
        raise ValueError(
            f"Unsupported file extension '{ext}' for {path.name}. "
            f"Supported extensions: {', '.join(_SUPPORTED_EXTENSIONS)}"
        )


def _load_oecd_csv(path: Path) -> pd.DataFrame:
    """
    Parse an OECD-format CSV → canonical schema.
    Expected columns: CO2, Country_Nationality, VAR, Variable, GEN, Gender, COU, Country, YEA, Year, Value
    """
    if not path.exists():
        raise FileNotFoundError(f"Italy fallback CSV not found: {path}")

    df = pd.read_csv(path, header=None, names=[
        "CO2", "Country_Nationality", "VAR", "Variable",
        "GEN", "Gender", "COU", "Country", "YEA", "Year", "Value"
    ])
    df = df.dropna(subset=["Value"])
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df[df["Value"].notna()]

    fetch_ts = datetime.now(timezone.utc)
    result = pd.DataFrame({
        "ref_area": "ITA",
        "ref_area_name": "Italy",
        "counterpart": df["COU"].astype(str).str.strip(),
        "counterpart_name": df["Country"].astype(str).str.strip(),
        "time_period": df["Year"].astype(str),
        "year": pd.to_numeric(df["Year"], errors="coerce").astype("Int64"),
        "quarter": None,
        "var_code": df["VAR"].astype(str).str.strip(),
        "metric": df["VAR"].map(VAR_TO_METRIC).fillna(df["Variable"].astype(str)),
        "sex": df["GEN"].astype(str).str.strip().map(
            lambda g: "T" if g in ("TOT", "T") else ("M" if g in ("MAS", "M") else "F")
        ),
        "gender": df["GEN"].astype(str).str.strip().map(
            lambda g: GEN_TO_GENDER.get(g, "Total")
        ),
        "area_name": None,
        "reg_name": None,
        "province": None,
        "imm_category": None,
        "obs_value": df["Value"].astype(float),
        "obs_status": None,
        "source_dataset": "FALLBACK_CSV",
        "fetch_ts": fetch_ts,
    })
    return result


def _load_un_csv(path: Path) -> pd.DataFrame:
    """
    Parse a pre-melted UN canonical CSV → canonical schema.
    The CSV was generated from the original un_canada.xlsx and already has canonical columns.
    """
    if not path.exists():
        raise FileNotFoundError(f"Canada fallback CSV not found: {path}")
    df = pd.read_csv(path, dtype={"time_period": str})
    df["source_dataset"] = "FALLBACK_CSV"
    df["fetch_ts"] = datetime.now(timezone.utc)
    return df


# ---------------------------------------------------------------------------
# Backward-compatible public wrappers (processors import these by name)
# ---------------------------------------------------------------------------

def load_italy_csv() -> pd.DataFrame:
    """Load Italy fallback data from data/raw/oecd_italy.csv."""
    return load_fallback("oecd", "italy")


def load_canada_xlsx() -> pd.DataFrame:
    """Load Canada fallback data from data/raw/un_canada.csv."""
    return load_fallback("un", "canada")
