"""Great Expectations 1.x validation for the multi-region pipeline.

Uses an ephemeral context with pandas DataFrame assets — no filesystem writes,
no great_expectations/ directory required.

GX 1.x API used here
---------------------
context.data_sources.add_pandas()                      (replaces context.sources)
data_source.add_dataframe_asset()                      (CSV loaded by pandas first)
asset.add_batch_definition_whole_dataframe()
context.suites.add(ExpectationSuite(...))
suite.add_expectation(gxe.<Expectation>(...))
context.validation_definitions.add(ValidationDefinition(...))
validation_def.run(batch_parameters={"dataframe": df})
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Valid domain values — keep in sync with config/*/parameters.yaml
_VALID_TRANSACTION_TYPES = [
    "Private", "Corporate", "SME", "Government", "NonProfit",
    "GroupPolicy", "BrokerSubmitted", "PartnerNetwork", "Reinsurance",
    "Internal", "Unknown",
]
_VALID_TRANSACTION_DIRECTIONS = [
    "COINSURANCE", "REINSURANCE", "INWARD_REINSURANCE", "DIRECT", "RECOVERY",
]
_VALID_TRANSACTION_CATEGORIES = ["CHARGE", "REFUND"]

_TRANSACTIONS_COLUMNS = [
    "CONTRACT_SOURCE_SYSTEM", "CONTRACT_SOURCE_SYSTEM_ID", "SOURCE_SYSTEM_ID",
    "TRANSACTION_TYPE", "TRANSACTION_DIRECTION", "CONFORMED_VALUE",
    "BUSINESS_DATE", "CREATION_DATE", "SYSTEM_TIMESTAMP",
    "NSE_ID", "TRANSACTION_CATEGORY",
]


class ValidationError(RuntimeError):
    """Raised when a GX validation suite fails."""


def _run_suite(df: pd.DataFrame, suite_name: str, expectations: list) -> None:
    """Build an ephemeral GX 1.x context, run expectations, raise on failure."""
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(name=f"{suite_name}_source")
    data_asset = data_source.add_dataframe_asset(name=f"{suite_name}_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")

    suite = context.suites.add(ExpectationSuite(name=suite_name))
    for exp in expectations:
        suite.add_expectation(exp)

    validation_def = context.validation_definitions.add(
        ValidationDefinition(name=suite_name, data=batch_def, suite=suite)
    )

    results = validation_def.run(batch_parameters={"dataframe": df})

    if not results.success:
        failed = [r for r in results.results if not r.success]
        messages = "\n".join(
            f"  - {r.expectation_config.type}({r.expectation_config.kwargs})"
            for r in failed
        )
        raise ValidationError(
            f"GX suite '{suite_name}' failed with {len(failed)} expectation(s):\n{messages}"
        )

    logger.info("GX suite '%s' passed.", suite_name)


def validate_claims(claims_path: str) -> None:
    """Validate raw claims CSV before the ETL runs.

    Expectations
    ------------
    * All 8 source columns exist.
    * ``CLAIM_ID``: not null, unique, matches ``^[A-Z]{2}_\\d+$``.
    * ``AMOUNT``: not null.

    Raises
    ------
    ValidationError
        If any expectation fails.
    """
    import great_expectations.expectations as gxe

    df = pd.read_csv(claims_path)

    expectations = [
        *[gxe.ExpectColumnToExist(column=c) for c in [
            "SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM",
            "CONTRACT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE",
        ]],
        gxe.ExpectColumnValuesToNotBeNull(column="CLAIM_ID"),
        gxe.ExpectColumnValuesToBeUnique(column="CLAIM_ID"),
        gxe.ExpectColumnValuesToMatchRegex(column="CLAIM_ID", regex=r"^[A-Z]{2}_\d+$"),
        gxe.ExpectColumnValuesToNotBeNull(column="AMOUNT"),
    ]

    _run_suite(df, "claims_input_suite", expectations)


def validate_transactions(output_path: str) -> None:
    """Validate output TRANSACTIONS CSV after the ETL completes.

    Expectations
    ------------
    * All 11 output columns exist.
    * ``NSE_ID``: not null, unique.
    * ``TRANSACTION_TYPE``, ``TRANSACTION_DIRECTION``, ``TRANSACTION_CATEGORY``:
      values in known domain sets (nulls skipped for DIRECTION by default).

    Raises
    ------
    ValidationError
        If any expectation fails.
    """
    import great_expectations.expectations as gxe

    df = pd.read_csv(output_path)

    expectations = [
        *[gxe.ExpectColumnToExist(column=c) for c in _TRANSACTIONS_COLUMNS],
        gxe.ExpectColumnValuesToNotBeNull(column="NSE_ID"),
        gxe.ExpectColumnValuesToBeUnique(column="NSE_ID"),
        gxe.ExpectColumnValuesToBeInSet(
            column="TRANSACTION_TYPE", value_set=_VALID_TRANSACTION_TYPES
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="TRANSACTION_DIRECTION", value_set=_VALID_TRANSACTION_DIRECTIONS
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="TRANSACTION_CATEGORY", value_set=_VALID_TRANSACTION_CATEGORIES
        ),
    ]

    _run_suite(df, "transactions_output_suite", expectations)
