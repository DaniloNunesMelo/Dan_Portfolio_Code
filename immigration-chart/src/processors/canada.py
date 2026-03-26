"""
Canada data orchestrator.
Priority: OECD_MIG (B11/B12) + IRCC (PR) → FALLBACK_XLSX
Stitches XLSX history (1980-2013) with IRCC live (2015+).
"""
import pandas as pd

from ..fetchers.base import FetchError
from ..fetchers.oecd import OECDFetcher
from ..fetchers.ircc import IRCCFetcher
from ..fetchers.fallback import load_canada_xlsx
from .merge import merge_sources

_oecd = OECDFetcher()
_ircc = IRCCFetcher()


def load_canada(
    var_codes: list[str] | None = None,
    sex: str = "T",
    use_live: bool = True,
    include_historical: bool = True,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Load Canada immigration data from available sources.
    Returns: (merged_df, [source_labels_used])
    """
    if var_codes is None:
        var_codes = ["B11", "PR"]

    frames: list[pd.DataFrame] = []
    sources_used: list[str] = []

    oecd_vars = [v for v in var_codes if v != "PR"]
    pr_requested = "PR" in var_codes

    if use_live:
        # OECD for standard migration variables
        for var in oecd_vars:
            try:
                df, src = _oecd.fetch_country(ref_area="CAN", var_code=var, sex=sex)
                frames.append(df)
                if "OECD_MIG" not in sources_used:
                    sources_used.append(f"OECD_MIG ({src})")
            except FetchError as e:
                print(f"[canada] OECD fetch failed for {var}: {e}")

        # IRCC for permanent residents
        if pr_requested:
            try:
                df_ircc, src = _ircc.fetch_by_citizenship()
                frames.append(df_ircc)
                sources_used.append(f"IRCC ({src})")
            except FetchError as e:
                print(f"[canada] IRCC citizenship fetch failed: {e}")

            try:
                df_prov, src = _ircc.fetch_by_province_category()
                frames.append(df_prov)
                if not any("IRCC" in s for s in sources_used):
                    sources_used.append(f"IRCC ({src})")
            except FetchError as e:
                print(f"[canada] IRCC province/category fetch failed: {e}")

    # Historical fallback XLSX (1980-2013)
    if include_historical:
        try:
            df_hist = load_canada_xlsx()
            frames.append(df_hist)
            sources_used.append("FALLBACK_XLSX (1980-2013)")
        except Exception as e:
            print(f"[canada] Fallback XLSX failed: {e}")

    if not frames:
        raise RuntimeError("All Canada data sources failed")

    merged = merge_sources(frames)
    return merged, sources_used
