import dlt
from pyspark.sql.functions import *
from pyspark. sql.types import *

############### DATA CLEANING - CUSTOMERS ##########
#create streaming table for landing customer inc
@dlt.table(
    name="bronze_customers_ingestion_cleaned",
    comment="This table contains the cleaned data from the customers ingested"
)

@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
@dlt.expect_or_drop("valid_city", "city IS NOT NULL")
@dlt.expect_or_drop("valid_join_date", "join_date IS NOT NULL")
@dlt.expect_or_drop(
    "valid_email",
    "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'"
)
@dlt.expect_or_drop(
    "valid_phone",
    "phone_number IS NOT NULL AND phone_number RLIKE '^\\\\+44[0-9]{10}$'"
)
@dlt.expect_or_drop("valid_channel", "preferred_channel IS NOT NULL")
@dlt.expect_or_drop("valid_occupation", "occupation IS NOT NULL")
@dlt.expect_or_drop("valid_income", "income_range IS NOT NULL")
@dlt.expect_or_drop("valid_risk_segment", "risk_segment IS NOT NULL")
@dlt.expect("valid_gender", "gender IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IS NOT NULL")


def bronze_customers_ingestion_cleaned():

    df = dlt.read_stream("landing_customers_incremental")


    # Standardize text columns
    df = (
        df.withColumn("name", upper(col("name")))
        .withColumn("email", lower(col("email")))
        .withColumn("occupation", upper(col("occupation")))
        .withColumn("city", upper(col("city")))
        .withColumn("income_range", upper(col("income_range")))
        .withColumn("risk_segment", upper(col("risk_segment")))
        .withColumn("preferred_channel", upper(col("preferred_channel")))
    )

    # Standardize gender values
    df = df.withColumn(
        "gender",
        when(col("gender") == "M", "MALE")
        .when(col("gender") == "F", "FEMALE")
        .otherwise("UNKNOWN")
    )

    # Handle null or blank status
    df = df.withColumn(
        "status",
        when(
            col("status").isNull() | (trim(col("status")) == ""),
            lit("UNKNOWN")
        ).otherwise(col("status"))
    )

    # Clean phone numbers
    df = (
        df.withColumn("phone_number", trim(col("phone_number")))
        .withColumn(
            "phone_number",
            regexp_replace(col("phone_number"), r"[^0-9+]", "")
        )
    )

    # Keep only valid UK phone numbers
    df = df.filter(
        col("phone_number").rlike(r"^\+44\d{10}$")
    )

    # Filter valid preferred channels
    df = df.filter(
        col("preferred_channel").isin(
            "ONLINE",
            "MOBILE",
            "BRANCH",
            "ATM"
        )
    )

    # Filter valid income ranges
    df = df.filter(
        col("income_range").isin(
            "HIGH",
            "MEDIUM",
            "LOW",
            "VERY HIGH"
        )
    )

    # Filter valid risk segments
    df = df.filter(
        col("risk_segment").isin(
            "LOW",
            "MEDIUM",
            "HIGH",
            "UNKNOWN"
        )
    )

    return df

#################################Streaming table for transaction layer###########################

@dlt.table(
    name="bronze_accounts_transactions_ingestion_cleaned",
    comment="This table contains the cleaned data from the transactions ingestion"
)

@dlt.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_txn_id","txn_id IS NOT NULL")
@dlt.expect_or_drop("account_type", "account_type IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_type", "txn_type IS NOT NULL")
@dlt.expect_or_drop("valid_txn_channel", "txn_channel IS NOT NULL")

def bronze_accounts_transactions_ingestion_cleaned():
    df = dlt.read_stream("landing_accounts_transactions_incremental")
    #### cleaning
    df = df.withColumn("txn_type",when(col("txn_type") == "DEBITT","DEBIT").when(col("txn_type") == "CREDITT","CREDIT").otherwise(col("txn_type")))
    #df = df.withColumn("txn_type",when(col("txn_type") != "DEBIT","DEBIT").when(col("txn_type") != "CREDIT","CREDIT").otherwise(col("txn_type")))
    return df   