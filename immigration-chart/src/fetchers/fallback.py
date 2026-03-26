"""
Fallback reader: loads legacy CSV/XLSX files from data/raw/.
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
}

GEN_TO_GENDER = {"TOT": "Total", "MAS": "Male", "FEM": "Female", "T": "Total", "M": "Male", "F": "Female"}

# Partial country-name → ISO 3166-1 alpha-3 lookup for Canada.xlsx
COUNTRY_TO_ISO3 = {
    "Afghanistan": "AFG", "Albania": "ALB", "Algeria": "DZA",
    "Argentina": "ARG", "Australia": "AUS", "Austria": "AUT",
    "Bangladesh": "BGD", "Belgium": "BEL", "Brazil": "BRA",
    "Bulgaria": "BGR", "Cambodia": "KHM", "Cameroon": "CMR",
    "Chile": "CHL", "China": "CHN", "Colombia": "COL",
    "Croatia": "HRV", "Cuba": "CUB", "Czech Republic": "CZE",
    "Denmark": "DNK", "Ecuador": "ECU", "Egypt": "EGY",
    "El Salvador": "SLV", "Ethiopia": "ETH", "Finland": "FIN",
    "France": "FRA", "Germany": "DEU", "Ghana": "GNA",
    "Greece": "GRC", "Guatemala": "GTM", "Haiti": "HTI",
    "Honduras": "HND", "Hong Kong": "HKG", "Hungary": "HUN",
    "India": "IND", "Indonesia": "IDN", "Iran": "IRN",
    "Iraq": "IRQ", "Ireland": "IRL", "Israel": "ISR",
    "Italy": "ITA", "Jamaica": "JAM", "Japan": "JPN",
    "Jordan": "JOR", "Kenya": "KEN", "Korea": "KOR",
    "Lebanon": "LBN", "Malaysia": "MYS", "Mexico": "MEX",
    "Morocco": "MAR", "Nepal": "NPL", "Netherlands": "NLD",
    "New Zealand": "NZL", "Nigeria": "NGA", "Norway": "NOR",
    "Pakistan": "PAK", "Peru": "PER", "Philippines": "PHL",
    "Poland": "POL", "Portugal": "PRT", "Romania": "ROU",
    "Russia": "RUS", "Saudi Arabia": "SAU", "Somalia": "SOM",
    "South Africa": "ZAF", "Spain": "ESP", "Sri Lanka": "LKA",
    "Sudan": "SDN", "Sweden": "SWE", "Switzerland": "CHE",
    "Syria": "SYR", "Taiwan": "TWN", "Tanzania": "TZA",
    "Thailand": "THA", "Trinidad and Tobago": "TTO",
    "Turkey": "TUR", "Uganda": "UGA", "Ukraine": "UKR",
    "United Kingdom": "GBR", "United States": "USA",
    "Venezuela": "VEN", "Vietnam": "VNM", "Zimbabwe": "ZWE",
}

AREA_MAP = {
    "Africa": "Africa", "Asia": "Asia", "Europe": "Europe",
    "Northern America": "Northern America", "Latin America and the Caribbean": "Latin America",
    "Oceania": "Oceania", "Unknown": None,
}


def load_italy_csv() -> pd.DataFrame:
    """
    Reads MIG_ITALY_NO_QUOTE.csv → canonical schema.
    Columns: CO2, Country_Nationality, VAR, Variable, GEN, Gender, COU, Country, YEA, Year, Value
    """
    csv_path = DATA_RAW / "MIG_ITALY_NO_QUOTE.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Italy fallback CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, header=None, names=[
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


def load_canada_xlsx() -> pd.DataFrame:
    """
    Reads Canada.xlsx (two sheets) → canonical schema.
    Sheet 'Canada by Citizenship': wide format with year columns 1980-2013.
    Sheet 'Regions by Citizenship': wide format with year columns 1980-2013.
    """
    xlsx_path = DATA_RAW / "Canada.xlsx"
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Canada fallback XLSX not found: {xlsx_path}")

    fetch_ts = datetime.now(timezone.utc)
    frames = []

    # --- Sheet 1: Canada by Citizenship ---
    try:
        df_cit = pd.read_excel(xlsx_path, sheet_name="Canada by Citizenship",
                               skiprows=1, engine="openpyxl")
        id_cols = ["Type", "Coverage", "OdName", "AREA", "AreaName", "REG", "RegName", "DEV", "DevName"]
        id_cols_present = [c for c in id_cols if c in df_cit.columns]
        year_cols = [c for c in df_cit.columns if isinstance(c, int) and 1900 <= c <= 2030]
        if not year_cols:
            # Try string year columns
            year_cols = [c for c in df_cit.columns if str(c).isdigit() and 1900 <= int(c) <= 2030]

        df_melt = pd.melt(df_cit, id_vars=id_cols_present, value_vars=year_cols,
                          var_name="Year", value_name="obs_value")
        df_melt["obs_value"] = pd.to_numeric(df_melt["obs_value"], errors="coerce")
        df_melt = df_melt[df_melt["obs_value"].notna() & (df_melt["obs_value"] > 0)].copy()
        df_melt["Year"] = pd.to_numeric(df_melt["Year"], errors="coerce").astype("Int64")

        origin_col = "OdName" if "OdName" in df_melt.columns else id_cols_present[0]
        area_col = "AreaName" if "AreaName" in df_melt.columns else None
        reg_col = "RegName" if "RegName" in df_melt.columns else None

        frame = pd.DataFrame({
            "ref_area": "CAN",
            "ref_area_name": "Canada",
            "counterpart": df_melt[origin_col].map(
                lambda x: COUNTRY_TO_ISO3.get(str(x).strip(), str(x).strip()[:3].upper())
            ),
            "counterpart_name": df_melt[origin_col].astype(str),
            "time_period": df_melt["Year"].astype(str),
            "year": df_melt["Year"],
            "quarter": None,
            "var_code": "B11",
            "metric": "Inflows of Foreign Population",
            "sex": "T",
            "gender": "Total",
            "area_name": df_melt[area_col].astype(str) if area_col else None,
            "reg_name": df_melt[reg_col].astype(str) if reg_col else None,
            "province": None,
            "imm_category": None,
            "obs_value": df_melt["obs_value"].astype(float),
            "obs_status": None,
            "source_dataset": "FALLBACK_XLSX",
            "fetch_ts": fetch_ts,
        })
        frames.append(frame)
    except Exception as e:
        print(f"[fallback] Warning: Could not load Canada by Citizenship sheet: {e}")

    # --- Sheet 2: Regions by Citizenship ---
    try:
        df_reg = pd.read_excel(xlsx_path, sheet_name="Regions by Citizenship",
                               skiprows=20, engine="openpyxl")
        id_cols_r = ["Type", "Coverage", "AreaName", "RegName"]
        id_cols_r_present = [c for c in id_cols_r if c in df_reg.columns]
        year_cols_r = [c for c in df_reg.columns if isinstance(c, int) and 1900 <= c <= 2030]
        if not year_cols_r:
            year_cols_r = [c for c in df_reg.columns if str(c).isdigit() and 1900 <= int(c) <= 2030]

        df_melt_r = pd.melt(df_reg, id_vars=id_cols_r_present, value_vars=year_cols_r,
                            var_name="Year", value_name="obs_value")
        df_melt_r["obs_value"] = pd.to_numeric(df_melt_r["obs_value"], errors="coerce")
        df_melt_r = df_melt_r[df_melt_r["obs_value"].notna() & (df_melt_r["obs_value"] > 0)].copy()
        df_melt_r["Year"] = pd.to_numeric(df_melt_r["Year"], errors="coerce").astype("Int64")

        area_col_r = "AreaName" if "AreaName" in df_melt_r.columns else None
        reg_col_r = "RegName" if "RegName" in df_melt_r.columns else None

        origin_name = (df_melt_r[reg_col_r].astype(str)
                       if reg_col_r else pd.Series("Unknown", index=df_melt_r.index))

        frame_r = pd.DataFrame({
            "ref_area": "CAN",
            "ref_area_name": "Canada",
            "counterpart": origin_name.map(
                lambda x: COUNTRY_TO_ISO3.get(str(x).strip(), "_R")
            ),
            "counterpart_name": origin_name,
            "time_period": df_melt_r["Year"].astype(str),
            "year": df_melt_r["Year"],
            "quarter": None,
            "var_code": "B11",
            "metric": "Inflows of Foreign Population",
            "sex": "T",
            "gender": "Total",
            "area_name": df_melt_r[area_col_r].astype(str) if area_col_r else None,
            "reg_name": origin_name,
            "province": None,
            "imm_category": None,
            "obs_value": df_melt_r["obs_value"].astype(float),
            "obs_status": None,
            "source_dataset": "FALLBACK_XLSX",
            "fetch_ts": fetch_ts,
        })
        frames.append(frame_r)
    except Exception as e:
        print(f"[fallback] Warning: Could not load Regions by Citizenship sheet: {e}")

    if not frames:
        raise RuntimeError("Both Canada.xlsx sheets failed to load")

    return pd.concat(frames, ignore_index=True)
