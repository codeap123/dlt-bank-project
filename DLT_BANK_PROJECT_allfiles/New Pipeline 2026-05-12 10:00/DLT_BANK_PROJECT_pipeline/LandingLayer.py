############ Customers 2023 Data Ingestion #############

import dlt
from pyspark. sql. functions import *
from pyspark. sql.types import *

customer_schema = """
customer_id INT,
name STRING,
dob DATE,
gender STRING,
city STRING,
join_date DATE,
status STRING,
email STRING,
phone_number STRING,
preferred_channel STRING,
occupation STRING,
income_range STRING,
risk_segment STRING
"""

@dlt.table(
name="landing_customers_incremental",
comment="landing customers data"
)

def landing_customers_incremental():     #auto loader
    return(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "true")
    .schema(customer_schema)
    .load("/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema/dlt_bank_project_volume/customers/")
    )



############Accounts-Transactions 2023 Data Ingestion#############
##########straming table creation for accounts###########

accounts_schema = """
account_id BIGINT,
customer_id BIGINT,
account_type STRING,
balance DOUBLE,
txn_id BIGINT,
txn_date DATE,
txn_type STRING,
txn_amount DOUBLE,
txn_channel STRING
"""

@dlt.table(
name="landing_accounts_transactions_incremental",
comment="landing customers data"
)
def landing_accounts_transactions_incremental():
    return(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "true")
    .schema(accounts_schema)
    .load("/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema/dlt_bank_project_volume/accounts/")
)