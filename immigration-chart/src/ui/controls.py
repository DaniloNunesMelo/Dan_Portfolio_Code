"""UI component configuration — choices, labels, mappings."""

COUNTRIES = ["Italy", "Canada"]

COUNTRY_TO_ISO = {"Italy": "ITA", "Canada": "CAN"}
ISO_TO_COUNTRY = {"ITA": "Italy", "CAN": "Canada"}

METRICS_BY_COUNTRY: dict[str, list[str]] = {
    "Italy": [
        "Inflows of Foreign Population",
        "Outflows of Foreign Population",
        "Asylum Seekers",
        "Stock of Foreign-Born Population",
        "Stock of Foreign Nationals",
        "Citizenship Acquisition",
        "Outflows of National Population",
    ],
    "Canada": [
        "Inflows of Foreign Population",
        "Outflows of National Population",
        "Permanent Residents",
    ],
}

COMMON_METRICS = ["Inflows of Foreign Population"]

GROUP_BY_OPTIONS = [
    "By Origin Country",
    "By Area",
    "By Region",
    "By Province",
    "By Immigration Category",
    "By Gender",
    "By Source",
]

CHART_TYPES = ["Line", "Bar", "Heatmap", "Choropleth", "Pie", "Bubble"]

GENDER_OPTIONS = ["Total", "Male", "Female"]

PIVOT_DIMENSIONS = [
    "counterpart_name",
    "year",
    "area_name",
    "reg_name",
    "province",
    "imm_category",
    "gender",
    "source_dataset",
]

DEFAULT_YEAR_START = 2000
DEFAULT_YEAR_END = 2024
MIN_YEAR = 1980
MAX_YEAR = 2024
