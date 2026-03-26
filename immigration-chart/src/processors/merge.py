"""
History merge: deduplicates and stitches DataFrames from multiple sources.
Source priority: OECD_MIG > EUROSTAT > IRCC > FALLBACK_CSV > FALLBACK_XLSX
"""
import pandas as pd

DEDUP_KEY = ["ref_area", "counterpart", "time_period", "var_code", "sex"]

SOURCE_PRIORITY = {
    "OECD_MIG": 0,
    "EUROSTAT": 1,
    "IRCC": 2,
    "FALLBACK_CSV": 3,
    "FALLBACK_XLSX": 4,
}

CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


def merge_sources(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concat all DataFrames, deduplicate by DEDUP_KEY keeping highest-priority source.
    Returns merged DataFrame in canonical schema.
    """
    if not dfs:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    # Ensure all have required columns
    clean = []
    for df in dfs:
        if df is None or len(df) == 0:
            continue
        # Add missing columns as None
        for col in CANONICAL_COLUMNS:
            if col not in df.columns:
                df = df.copy()
                df[col] = None
        clean.append(df[CANONICAL_COLUMNS])

    if not clean:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    combined = pd.concat(clean, ignore_index=True)

    # Add priority column for deduplication
    combined["_priority"] = combined["source_dataset"].map(
        lambda s: SOURCE_PRIORITY.get(s, 99)
    )

    # Sort by key + priority (ascending = best first)
    combined = combined.sort_values(DEDUP_KEY + ["_priority"])

    # Drop duplicates, keep first (highest priority)
    # Fill NaN in key columns with empty string for comparison
    for col in DEDUP_KEY:
        combined[col] = combined[col].fillna("").astype(str)

    combined = combined.drop_duplicates(subset=DEDUP_KEY, keep="first")
    combined = combined.drop(columns=["_priority"])

    # Restore numeric types
    combined["year"] = pd.to_numeric(combined["year"], errors="coerce").astype("Int64")
    combined["obs_value"] = pd.to_numeric(combined["obs_value"], errors="coerce")

    return combined.reset_index(drop=True)


def get_coverage_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a summary DataFrame for the History tab.
    Columns: source_dataset, min_year, max_year, row_count, ref_area
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["source_dataset", "ref_area", "min_year", "max_year", "row_count"])

    df_copy = df.copy()
    df_copy["year_num"] = pd.to_numeric(df_copy["year"], errors="coerce")

    groups = (
        df_copy
        .groupby(["source_dataset", "ref_area"], dropna=False)
        .agg(
            min_year=("year_num", "min"),
            max_year=("year_num", "max"),
            row_count=("obs_value", "count"),
        )
        .reset_index()
    )
    return groups
