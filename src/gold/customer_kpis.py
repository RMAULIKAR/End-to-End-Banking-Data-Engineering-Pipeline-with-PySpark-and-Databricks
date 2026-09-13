from pyspark.sql.functions import sum, count, avg, round

BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

transactions = (
    spark.read
    .format("delta")
    .load(f"{BASE_PATH}/silver/transactions")
)

customer_kpis = (
    transactions
    .groupBy("customer_id")
    .agg(
        sum("amount").alias("total_transaction_amount"),
        count("txn_id").alias("transaction_count"),
        round(avg("amount"),1).alias("average_transaction_amount")
    )
)


(    
    customer_kpis.write
    .format("delta")
    .mode("overwrite")
    .save(f"{BASE_PATH}/gold/customer_kpis")
)

print("Customer KPIs generated successfully!")