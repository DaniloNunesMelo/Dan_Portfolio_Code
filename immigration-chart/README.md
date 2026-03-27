---
title: Immigration Chart
emoji: 🌍
colorFrom: blue
colorTo: green
sdk: gradio
sdk_version: "4.44.1"
python_version: "3.11"
app_file: app.py
pinned: false
---

# Immigration Analysis Dashboard

Interactive web application for analyzing international migration data for **Italy** and **Canada**, built with modern Python, Gradio, and Plotly.

Modernized replacement for the legacy `python-data-analysis` and `spark-data-analysis` pipeline projects. Static HTML reports and PySpark RDD jobs are replaced by a live, interactive dashboard that fetches official data directly from government and international APIs.

---

## Quick Start

```bash
cd immigration-chart
pip install -r requirements.txt
python app.py
```

Open **http://localhost:7860** in your browser.

---

## Features

| Feature | Details |
|---------|---------|
| **Countries** | Italy (`ITA`), Canada (`CAN`), or both simultaneously |
| **Chart types** | Line, Bar, Heatmap, Choropleth (world map), Pie, Bubble |
| **Group by** | Origin Country, Area, Region, Province, Immigration Category, Gender |
| **Pivot table** | Configurable rows × columns with formatted counts |
| **History tab** | Gantt-style coverage chart showing which years each source covers |
| **Live refresh** | Button to clear cache and re-fetch from APIs |
| **Offline mode** | Automatic fallback to bundled CSV/XLSX when APIs are unavailable |

---

## Data Sources

All country codes follow **ISO 3166-1 alpha-3**, variable codes follow **OECD MIG** standards, and time periods use **ISO 8601**.

### Italy

| Priority | Source | Coverage | Variables |
|----------|--------|----------|-----------|
| 1 | [OECD SDMX API](https://sdmx.oecd.org/public/rest/data/OECD.ELS.IMD,DSD_MIG@DF_MIG/) | 2000–present | B11 Inflows, B12 Outflows, B13 Asylum Seekers, B14 Stock Foreign-Born, B15 Stock Foreign Nationals, B16 Citizenship Acquisition |
| 2 | [Eurostat REST API](https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/) | 1998–present | `migr_imm1ctz`, `migr_emi1ctz` (citizenship breakdown) |
| 3 | `data/raw/MIG_ITALY_NO_QUOTE.csv` | 2000–2020 | OECD format fallback |

### Canada

| Priority | Source | Coverage | Variables |
|----------|--------|----------|-----------|
| 1 | [OECD SDMX API](https://sdmx.oecd.org/public/rest/data/OECD.ELS.IMD,DSD_MIG@DF_MIG/) | 2000–present | B11 Inflows, B12 Outflows |
| 2 | [IRCC Open Data](https://www.ircc.canada.ca/opendata-donneesouvertes/data/) | 2015–present | Permanent Residents by citizenship, 2015–present |
| 3 | `data/raw/Canada.xlsx` | 1980–2013 | UN database (historical fallback) |

### Variable Code Reference

| Code | Metric |
|------|--------|
| `B11` | Inflows of Foreign Population |
| `B12` | Outflows of Foreign Population |
| `B13` | Asylum Seekers |
| `B14` | Stock of Foreign-Born Population |
| `B15` | Stock of Foreign Nationals |
| `B16` | Citizenship Acquisition |
| `PR` | Permanent Residents (IRCC-specific) |

---

## Project Structure

```
immigration-chart/
├── app.py                    # Entry point — run this
├── requirements.txt
│
├── data/
│   ├── raw/                  # Bundled fallback data files
│   │   ├── MIG_ITALY_NO_QUOTE.csv
│   │   └── Canada.xlsx
│   └── cache/                # Parquet cache (auto-managed, TTL-based)
│
└── src/
    ├── fetchers/             # Data acquisition layer
    │   ├── base.py           # Two-tier cache: memory → Parquet on disk
    │   ├── oecd.py           # OECD SDMX API (Italy + Canada)
    │   ├── eurostat.py       # Eurostat REST API (Italy enrichment)
    │   ├── ircc.py           # IRCC open data XLSX (Canada)
    │   └── fallback.py       # Legacy CSV/XLSX reader
    │
    ├── processors/           # Normalization layer
    │   ├── italy.py          # Italy orchestrator → canonical schema
    │   ├── canada.py         # Canada orchestrator → canonical schema
    │   └── merge.py          # History merge + deduplication
    │
    ├── charts/               # Plotly chart builders
    │   ├── registry.py       # Dispatch table + filtering
    │   ├── line.py
    │   ├── bar.py
    │   ├── heatmap.py
    │   ├── choropleth.py
    │   ├── pie.py
    │   └── bubble.py
    │
    └── ui/                   # Gradio interface
        ├── layout.py         # gr.Blocks layout + event wiring
        ├── controls.py       # Component choices and mappings
        ├── callbacks.py      # Event handler functions
        └── pivot.py          # Pivot table builder
```

---

## How to Use the Dashboard

### 1 — Select a Country
Use the **Destination Country** checkboxes to select Italy, Canada, or both. The **Metric** dropdown updates automatically to show metrics available for your selection.

### 2 — Choose a Metric
- Italy metrics: Inflows, Outflows, Asylum Seekers, Stock (Foreign-Born / Nationals), Citizenship Acquisition
- Canada metrics: Inflows of Foreign Population, Permanent Residents
- When both countries are selected, shared metrics (e.g. Inflows) can be compared side by side.

### 3 — Set the Year Range
Drag the **From Year** and **To Year** sliders. Italy data is available from 1998 (Eurostat) or 2000 (OECD/CSV). Canada data goes back to 1980 (XLSX fallback) or 2000 (OECD).

### 4 — Pick a Chart Type and Grouping

| Chart Type | Best for |
|------------|---------|
| **Line** | Trends over time per country of origin, region, or gender |
| **Bar** | Year-over-year comparison across groups |
| **Heatmap** | Origin × Year matrix, spot patterns at a glance |
| **Choropleth** | World map coloured by migration volume |
| **Pie** | Top-N share by origin country or region |
| **Bubble** | Size-encoded scatter over time |

The **Group By** dropdown controls what dimension splits the data (origin country, area, region, province, immigration category, gender, or data source).

Use **Top N Groups** to limit how many categories appear (avoids clutter with 200+ countries).

### 5 — Pivot Table Tab
Select **Rows** and **Columns** dimensions to build a cross-tabulation. Values are aggregated sums of the current metric, formatted with comma separators. Useful for quick numerical comparisons (e.g. origin country × year).

### 6 — History Tab
Shows a Gantt-style bar chart of which years are available per source, along with a summary table of row counts. Helps understand data coverage gaps (e.g. no IRCC data before 2015).

### 7 — Data Source Info Tab
Displays the sources that were actually used for the current selection, including whether data came from a live API call, disk cache, or the bundled fallback files.

### 8 — Refresh Live Data
Click **Refresh Live Data** to clear the in-memory cache for selected countries and re-fetch from APIs. Useful when you want the latest figures (OECD and Eurostat publish updates periodically).

---

## Cache Behaviour

Data is cached at two levels to avoid repeated API calls:

| Level | Storage | TTL |
|-------|---------|-----|
| Memory | Python dict (process lifetime) | Per-session |
| Disk | Parquet files in `data/cache/` | OECD: 24 h · Eurostat: 12 h · IRCC: 7 days |

Cache files are named by a SHA-256 hash of the request parameters. They are read automatically on startup if still within TTL. The **Refresh** button clears only the in-memory layer; disk cache expires naturally.

---

## Canonical Data Schema

All data sources are normalized to this schema before charting:

| Column | Type | Description |
|--------|------|-------------|
| `ref_area` | `str` | ISO 3166-1 alpha-3 destination country (`ITA`, `CAN`) |
| `ref_area_name` | `str` | Full country name |
| `counterpart` | `str` | ISO 3166-1 alpha-3 origin country code |
| `counterpart_name` | `str` | Full origin country name |
| `time_period` | `str` | ISO 8601 year (`"2023"`) or quarter (`"2023-Q1"`) |
| `year` | `int` | Calendar year |
| `quarter` | `str\|None` | `"Q1"`–`"Q4"` or `None` for annual |
| `var_code` | `str` | OECD variable code (`B11`–`B16`, `PR`) |
| `metric` | `str` | Human-readable metric name |
| `sex` | `str` | `_T` (Total) · `M` (Male) · `F` (Female) |
| `gender` | `str` | Display label: Total / Male / Female |
| `area_name` | `str\|None` | UN macro-region (Africa, Asia, …) |
| `reg_name` | `str\|None` | UN sub-region |
| `province` | `str\|None` | Canadian province (ISO 3166-2:CA) |
| `imm_category` | `str\|None` | Economic / Family / Refugee / Other |
| `obs_value` | `float` | Count of persons |
| `obs_status` | `str\|None` | Observation status flag |
| `source_dataset` | `str` | `OECD_MIG` · `EUROSTAT` · `IRCC` · `FALLBACK_CSV` · `FALLBACK_XLSX` |
| `fetch_ts` | `datetime` | UTC timestamp of fetch |

---

## Requirements

```
pandas>=2.1.0
numpy>=1.26.0
openpyxl>=3.1.2
requests>=2.31.0
plotly>=5.18.0
gradio>=4.7.1
pyarrow>=14.0.0
python-dateutil>=2.8.2
beautifulsoup4>=4.12.0
lxml>=4.9.0
```

Python 3.11+ recommended.

---

## References

- OECD International Migration Database — https://www.oecd.org/en/data/datasets/international-migration-database.html
- Eurostat Migration Statistics — https://ec.europa.eu/eurostat/statistics-explained/index.php/Migration_and_migrant_population_statistics
- IRCC Open Data — https://www.ircc.canada.ca/english/resources/statistics/index.asp
- ISTAT — https://www.istat.it/en/
- UN International Migration Flows (source of legacy Canada.xlsx) — https://www.un.org/development/desa/pd/
