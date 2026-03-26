"""Pivot table builder for the UI."""
import pandas as pd


def build_pivot_table(
    df: pd.DataFrame,
    rows: str,
    cols: str,
    aggfunc: str = "sum",
    fill_value: float = 0,
) -> pd.DataFrame:
    """
    Build a pivot table from the canonical DataFrame.
    Returns a formatted DataFrame suitable for gr.Dataframe.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame({"Info": ["No data available"]})

    # Ensure columns exist
    if rows not in df.columns or cols not in df.columns:
        available = [c for c in [rows, cols] if c in df.columns]
        if len(available) < 2:
            return pd.DataFrame({"Info": [f"Columns '{rows}' or '{cols}' not available in data"]})

    # Drop rows where key columns are null
    df_clean = df[[rows, cols, "obs_value"]].dropna(subset=[rows, cols, "obs_value"])

    if len(df_clean) == 0:
        return pd.DataFrame({"Info": ["No data after filtering nulls"]})

    try:
        pivot = pd.pivot_table(
            df_clean,
            index=rows,
            columns=cols,
            values="obs_value",
            aggfunc=aggfunc,
            fill_value=fill_value,
        )
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

    # Format numbers
    def fmt(v):
        try:
            return f"{int(v):,}"
        except (ValueError, TypeError):
            return str(v)

    pivot_fmt = pivot.map(fmt) if hasattr(pivot, "map") else pivot.applymap(fmt)
    pivot_fmt.columns = [str(c) for c in pivot_fmt.columns]
    pivot_fmt = pivot_fmt.reset_index()
    pivot_fmt.columns.name = None
    return pivot_fmt
