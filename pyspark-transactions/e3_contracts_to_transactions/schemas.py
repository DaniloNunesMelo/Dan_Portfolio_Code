"""Shared schema definitions for CONTRACT and CLAIM CSVs."""

from pyspark.sql.types import LongType, StringType, StructField, StructType

CONTRACT_SCHEMA = StructType(
    [
        StructField("SOURCE_SYSTEM", StringType(), True),
        StructField("CONTRACT_ID", LongType(), True),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True),
    ]
)

CLAIM_SCHEMA = StructType(
    [
        StructField("SOURCE_SYSTEM", StringType(), True),
        StructField("CLAIM_ID", StringType(), True),
        StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
        StructField("CONTRACT_ID", LongType(), True),
        StructField("CLAIM_TYPE", StringType(), True),
        StructField("DATE_OF_LOSS", StringType(), True),
        StructField("AMOUNT", StringType(), True),
        StructField("CREATION_DATE", StringType(), True),
    ]
)

NSE_LOOKUP_SCHEMA = StructType(
    [
        StructField("CLAIM_ID", StringType(), False),
        StructField("NSE_ID", StringType(), False),
    ]
)
