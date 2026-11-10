# Databricks notebook source
# MAGIC %md
# MAGIC # scd2_customers.py

# COMMAND ----------

from pyspark.sql.functions import col, lead
from pyspark.sql.window import Window

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

df = (
    spark.read
    .format("delta")
    .load(f"{BASE_PATH}/silver/customers")
)

window = Window.partitionBy("customer_id").orderBy("updated_at")

scd2 = (
    df
    .withColumn("effective_from", col("updated_at"))
    .withColumn("effective_to", lead("updated_at").over(window))
    .withColumn("is_current", col("effective_to").isNull())
)

(
    scd2.write
    .format("delta")
    .mode("overwrite")
    .save(f"{BASE_PATH}/silver/customers_scd2")
)