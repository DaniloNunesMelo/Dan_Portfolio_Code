"""Shared StructType definitions for the Europe 3 pipeline."""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("CONTRACT_SOURCE_SYSTEM", StringType(), nullable=True),
        StructField("CONTRACT_SOURCE_SYSTEM_ID", LongType(), nullable=True),
        StructField("SOURCE_SYSTEM_ID", IntegerType(), nullable=True),
        StructField("TRANSACTION_TYPE", StringType(), nullable=False),
        StructField("TRANSACTION_DIRECTION", StringType(), nullable=True),
        StructField("CONFORMED_VALUE", DecimalType(16, 5), nullable=True),
        StructField("BUSINESS_DATE", DateType(), nullable=True),
        StructField("CREATION_DATE", TimestampType(), nullable=True),
        StructField("SYSTEM_TIMESTAMP", TimestampType(), nullable=True),
        StructField("NSE_ID", StringType(), nullable=False),
        StructField("TRANSACTION_CATEGORY", StringType(), nullable=False),
    ]
)
