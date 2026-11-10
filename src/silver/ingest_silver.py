# Databricks notebook source
# MAGIC %md
# MAGIC # enr_customers.py

# COMMAND ----------

from pyspark.sql.functions import col, to_date

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

df = (
    spark.readStream
    .format("delta")
    .load(f"{BASE_PATH}/bronze/customers")
)

df_clean = (
    df
    .withColumn("dob", to_date(col("dob"), "dd-MM-yyyy"))
    .dropDuplicates(["customer_id", "updated_at"])
    .coalesce(1)
)

(
    df_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{BASE_PATH}/checkpoints/silver_customers"
    )
    .trigger(availableNow=True)
    .start(f"{BASE_PATH}/silver/customers")
)

print("Customers Silver transformation completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # enr_accounts.py

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, upper, trim 

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

SOURCE_PATH = f"{BASE_PATH}/bronze/accounts"
TARGET_PATH = f"{BASE_PATH}/silver/accounts"


df = (
    spark.read
    .format("delta")
    .load(SOURCE_PATH)
)


df_clean = (
    df
    .withColumn("account_type", upper(trim(col("account_type"))))
    .filter(
        col("account_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("account_type").isin("SAVINGS", "CURRENT") &
        col("balance").isNotNull()
    )
)

df_clean = df_clean.dropDuplicates(["account_id"])


if not DeltaTable.isDeltaTable(spark, TARGET_PATH):

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .save(TARGET_PATH)
    )

    print("Silver Accounts table created successfully!")


else:

    target = DeltaTable.forPath(
        spark,
        TARGET_PATH
    )

    (
        target.alias("target")
        .merge(
            df_clean.alias("source"),
            "target.account_id = source.account_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver Accounts SCD1 upsert completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # enr_transaction.py

# COMMAND ----------

from pyspark.sql.functions import col, to_date

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

df = (
    spark.readStream
    .format("delta")
    .load(f"{BASE_PATH}/bronze/transactions")
)

df_clean = (
    df
    .withColumn("txn_date", to_date(col("txn_date"), "dd-MM-yyyy"))
    .filter(col("amount") > 0)
    .dropDuplicates(["txn_id"])
    .coalesce(1)
)

(
    df_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{BASE_PATH}/checkpoints/silver_transactions"
    )
    .trigger(availableNow=True)
    .start(f"{BASE_PATH}/silver/transactions")
)

print("Transaction Silver transformation completed successfully!")