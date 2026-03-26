"""
Italy data orchestrator.
Priority: OECD_MIG → Eurostat → FALLBACK_CSV
"""
import pandas as pd

from ..fetchers.base import FetchError
from ..fetchers.oecd import OECDFetcher
from ..fetchers.eurostat import EurostatFetcher
from ..fetchers.fallback import load_italy_csv
from .merge import merge_sources

_oecd = OECDFetcher()
_eurostat = EurostatFetcher()


def load_italy(
    var_codes: list[str] | None = None,
    sex: str = "T",
    use_live: bool = True,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Load Italy immigration data from available sources.
    Returns: (merged_df, [source_labels_used])
    """
    if var_codes is None:
        var_codes = ["B11", "B12", "B16"]

    frames: list[pd.DataFrame] = []
    sources_used: list[str] = []

    if use_live:
        # Try OECD for each variable
        for var in var_codes:
            try:
                df, src = _oecd.fetch_country(ref_area="ITA", var_code=var, sex=sex)
                frames.append(df)
                if "OECD_MIG" not in sources_used:
                    sources_used.append(f"OECD_MIG ({src})")
            except FetchError as e:
                print(f"[italy] OECD fetch failed for {var}: {e}")

        # Eurostat enrichment (B11 additional detail)
        for dataset in ["migr_imm1ctz", "migr_emi1ctz", "migr_acqctz"]:
            try:
                df, src = _eurostat.fetch_italy(dataset=dataset)
                frames.append(df)
                if "EUROSTAT" not in sources_used:
                    sources_used.append(f"EUROSTAT ({src})")
            except FetchError as e:
                print(f"[italy] Eurostat fetch failed for {dataset}: {e}")

    # Fallback CSV
    try:
        df_fallback = load_italy_csv()
        # Filter to requested var_codes
        df_fallback = df_fallback[df_fallback["var_code"].isin(var_codes)].copy()
        frames.append(df_fallback)
        sources_used.append("FALLBACK_CSV")
    except Exception as e:
        print(f"[italy] Fallback CSV failed: {e}")

    if not frames:
        raise RuntimeError("All Italy data sources failed")

    merged = merge_sources(frames)
    return merged, sources_used
