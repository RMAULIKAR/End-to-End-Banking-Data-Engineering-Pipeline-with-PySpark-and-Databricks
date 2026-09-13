# Databricks notebook source
# MAGIC %md
# MAGIC # ingest_customers

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"


customer_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("dob", StringType()),
    StructField("kyc_status", StringType()),
    StructField("address", StringType()),
    StructField("updated_at", StringType())
])


customers_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .schema(customer_schema)
    .load(f"{BASE_PATH}/src_dataset/landing/customers")
)


customers_write = (
    customers_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{BASE_PATH}/checkpoints/customers"
    )
    .trigger(availableNow=True)
    .start(f"{BASE_PATH}/bronze/customers")
)

print("Customers ingestion completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # ingest_accounts.py

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"


schema = StructType([
    StructField("account_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("account_type", StringType()),
    StructField("balance", DoubleType())
])


accounts_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .schema(schema)
    .load(f"{BASE_PATH}/src_dataset/landing/accounts")
)


accounts_write = (
    accounts_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/accounts")
    .trigger(availableNow=True)
    .start(f"{BASE_PATH}/bronze/accounts")
)

print("Accounts ingestion completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # ingest_transactions.py

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)


BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"


transaction_schema = StructType([
    StructField("txn_id", IntegerType()),
    StructField("account_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("txn_type", StringType()),
    StructField("txn_date", StringType())
])


transactions_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .schema(transaction_schema)
    .load(f"{BASE_PATH}/src_dataset/landing/transactions")
)


transactions_write = (
    transactions_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{BASE_PATH}/checkpoints/transactions"
    )
    .trigger(availableNow=True)
    .start(f"{BASE_PATH}/bronze/transactions")
)

print("Transactions ingestion completed successfully!")