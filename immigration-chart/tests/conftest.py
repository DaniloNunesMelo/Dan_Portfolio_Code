"""Shared fixtures for the immigration-chart test suite."""
from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import openpyxl
import pandas as pd
import pytest


CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


@pytest.fixture
def canonical_df() -> pd.DataFrame:
    """Minimal valid 19-column canonical DataFrame."""
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return pd.DataFrame({
        "ref_area": ["ITA", "ITA", "CAN"],
        "ref_area_name": ["Italy", "Italy", "Canada"],
        "counterpart": ["DEU", "FRA", "IND"],
        "counterpart_name": ["Germany", "France", "India"],
        "time_period": ["2010", "2011", "2015"],
        "year": pd.array([2010, 2011, 2015], dtype="Int64"),
        "quarter": [None, None, None],
        "var_code": ["B11", "B11", "B11"],
        "metric": ["Inflows of Foreign Population"] * 3,
        "sex": ["T", "T", "T"],
        "gender": ["Total", "Total", "Total"],
        "area_name": ["Europe", "Europe", "Asia"],
        "reg_name": [None, None, None],
        "province": [None, None, None],
        "imm_category": [None, None, None],
        "obs_value": [1000.0, 1200.0, 5000.0],
        "obs_status": [None, None, None],
        "source_dataset": ["OECD_MIG", "OECD_MIG", "FALLBACK_XLSX"],
        "fetch_ts": [ts, ts, ts],
    })


@pytest.fixture
def italy_csv_tmp(tmp_path: Path, monkeypatch):
    """
    Write a minimal oecd_italy.csv to tmp_path and monkeypatch DATA_RAW.
    Returns the path to the CSV.
    """
    csv_content = (
        "ITA,Italy,B11,Inflows of foreign population by nationality,TOT,Total,DEU,Germany,2010,2010,1000\n"
        "ITA,Italy,B11,Inflows of foreign population by nationality,TOT,Total,FRA,France,2011,2011,1200\n"
        "ITA,Italy,B12,Outflows of foreign population by nationality,TOT,Total,DEU,Germany,2010,2010,500\n"
    )
    csv_path = tmp_path / "oecd_italy.csv"
    csv_path.write_text(csv_content)

    import src.fetchers.fallback as fb
    monkeypatch.setattr(fb, "DATA_RAW", tmp_path)
    return csv_path


@pytest.fixture
def canada_xlsx_tmp(tmp_path: Path, monkeypatch):
    """
    Write a minimal un_canada.xlsx to tmp_path and monkeypatch DATA_RAW.
    Returns the path to the XLSX.
    """
    wb = openpyxl.Workbook()

    # Sheet 1: Canada by Citizenship
    ws1 = wb.active
    ws1.title = "Canada by Citizenship"
    # Header row (skiprows=1 means row 2 is the column header)
    ws1.append(["(ignored title row)"])
    ws1.append(["Type", "Coverage", "OdName", "AREA", "AreaName", "REG", "RegName", "DEV", "DevName", 1990, 2000])
    ws1.append(["Immigrants", "Country", "India", 5, "Asia", 922, "Southern Asia", 2, "Developing", 2000, 5000])
    ws1.append(["Immigrants", "Country", "Germany", 5, "Europe", 150, "Western Europe", 1, "Developed", 800, 1500])

    # Sheet 2: Regions by Citizenship
    ws2 = wb.create_sheet("Regions by Citizenship")
    # skiprows=20 means we need at least 21 rows; rows 0-19 are skipped
    for _ in range(20):
        ws2.append(["(skip)"])
    ws2.append(["Type", "Coverage", "AreaName", "RegName", 1990, 2000])
    ws2.append(["Immigrants", "Region", "Asia", "Southern Asia", 5000, 12000])

    xlsx_path = tmp_path / "un_canada.xlsx"
    wb.save(xlsx_path)

    import src.fetchers.fallback as fb
    monkeypatch.setattr(fb, "DATA_RAW", tmp_path)
    return xlsx_path


@pytest.fixture
def oecd_json_response() -> dict:
    """
    Minimal SDMX-JSON v2 response for ITA/B11/_T.
    Series key positions: REF_AREA:CITIZENSHIP:FREQ:MEASURE:SEX:...
    Positions: 0=REF_AREA, 1=CITIZENSHIP, 2=FREQ, 3=MEASURE, 4=SEX
    """
    return {
        "data": {
            "dataSets": [{
                "series": {
                    "0:0:0:0:0": {  # ITA:DEU:A:B11:_T
                        "observations": {
                            "0": [1000.0, None],
                            "1": [1200.0, None],
                        }
                    },
                    "0:1:0:0:0": {  # ITA:FRA:A:B11:_T
                        "observations": {
                            "0": [500.0, None],
                        }
                    },
                }
            }],
            "structures": [{
                "dimensions": {
                    "series": [
                        {
                            "id": "REF_AREA",
                            "values": [{"id": "ITA", "name": "Italy"}]
                        },
                        {
                            "id": "CITIZENSHIP",
                            "values": [
                                {"id": "DEU", "name": "Germany"},
                                {"id": "FRA", "name": "France"},
                            ]
                        },
                        {
                            "id": "FREQ",
                            "values": [{"id": "A", "name": "Annual"}]
                        },
                        {
                            "id": "MEASURE",
                            "values": [{"id": "B11", "name": "Inflows of Foreign Population"}]
                        },
                        {
                            "id": "SEX",
                            "values": [{"id": "_T", "name": "Total"}]
                        },
                    ],
                    "observation": [
                        {
                            "id": "TIME_PERIOD",
                            "values": [
                                {"id": "2010", "name": "2010"},
                                {"id": "2011", "name": "2011"},
                            ]
                        }
                    ]
                }
            }]
        }
    }


@pytest.fixture
def eurostat_json_stat_response() -> dict:
    """
    Minimal JSON-STAT 2.0 response for migr_imm1ctz/IT.
    Dimensions: sex, citizen, time  (sizes 1, 2, 2)
    Values: {flat_key: value}
    """
    return {
        "id": ["sex", "citizen", "time"],
        "size": [1, 2, 2],
        "dimension": {
            "sex": {
                "category": {
                    "index": {"T": 0},
                    "label": {"T": "Total"},
                }
            },
            "citizen": {
                "category": {
                    "index": {"DEU": 0, "FRA": 1},
                    "label": {"DEU": "Germany", "FRA": "France"},
                }
            },
            "time": {
                "category": {
                    "index": {"2010": 0, "2011": 1},
                    "label": {"2010": "2010", "2011": "2011"},
                }
            },
        },
        # Flat key = sex_pos*4 + citizen_pos*2 + time_pos
        "value": {
            "0": 1000,   # sex=0, citizen=0(DEU), time=0(2010)
            "1": 1200,   # sex=0, citizen=0(DEU), time=1(2011)
            "2": 500,    # sex=0, citizen=1(FRA), time=0(2010)
            "3": 600,    # sex=0, citizen=1(FRA), time=1(2011)
        },
    }


@pytest.fixture
def ircc_xlsx_bytes() -> BytesIO:
    """
    Minimal IRCC XLSX BytesIO with correct 5-row header + data.
    Row 0: title, Row 1: empty, Row 2: year,
    Row 3: "YYYY Total" headers, Row 4: month headers, Row 5+: data
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"

    # Row 0: title
    ws.append(["Permanent Residents by Country of Citizenship"])
    # Row 1: empty
    ws.append([""])
    # Row 2: year row
    ws.append(["Country", 2020, None, 2021, None])
    # Row 3: quarter/annual headers — col 1 = "2020 Total", col 3 = "2021 Total"
    ws.append(["Country", "2020 Total", "Q1", "2021 Total", "Q1"])
    # Row 4: month headers
    ws.append(["Country", "Jan", "Feb", "Jan", "Feb"])
    # Row 5+: data
    ws.append(["India", 5000, 1200, 5500, 1300])
    ws.append(["China", 3000, 750, 3200, 800])
    ws.append(["Source: IRCC"])  # footnote row — should be skipped

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf
