from pyspark.sql.functions import sum, count, avg, date_format, round

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

transactions =  (
     spark.read
    .format("delta")
    .load(f"{BASE_PATH}/silver/transactions")
)

monthly_kpis = (
    transactions
    .withColumn(
        "transaction_month",
        date_format("txn_date", "yyyy-MM")
    )
    .groupBy("transaction_month")
    .agg(
        sum("amount").alias("total_transaction_amount"),
        count("txn_id").alias("transaction_count"),
        round(avg("amount"),1).alias("average_transaction_amount")
    )
)

(    
    monthly_kpis.write
    .format("delta")
    .mode("overwrite")
    .save(f"{BASE_PATH}/gold/monthly_transaction_kpis")
)

print("Monthly transaction KPIs generated successfully!")