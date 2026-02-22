# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_demo")

spark.sql("""
CREATE TABLE IF NOT EXISTS fraud_demo.bronze_transactions (
  event_time TIMESTAMP,
  transaction_id BIGINT,
  user_id INT,
  amount DOUBLE,
  country STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS fraud_demo.fraud_predictions (
  event_time TIMESTAMP,
  transaction_id BIGINT,
  user_id INT,
  amount DOUBLE,
  country STRING,
  fraud_flag INT,
  fraud_reason STRING
) USING DELTA
""")

print("Schemas/tables ready.")

# COMMAND ----------

from pyspark.sql import functions as F

def generate_batch(n_rows: int):
    df = (spark.range(n_rows)
          .withColumn("event_time", F.current_timestamp())
          .withColumnRenamed("id", "transaction_id")
          .withColumn("user_id", (F.col("transaction_id") % 50).cast("int"))
          .withColumn("amount", (F.rand() * 500).cast("double"))
          .withColumn("country", F.when(F.rand() < 0.8, F.lit("DE")).otherwise(F.lit("FR")))
         )
    return df.select("event_time","transaction_id","user_id","amount","country")

batch = generate_batch(200)   # change 200 to 1000 later if you want
batch.write.mode("append").saveAsTable("fraud_demo.bronze_transactions")

print("Inserted one live batch into bronze.")

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("fraud_demo.bronze_transactions")

pred = (bronze
        .withColumn(
            "fraud_flag",
            F.when((F.col("amount") > 300) | (F.col("country") != "DE"), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "fraud_reason",
            F.when((F.col("amount") > 300) & (F.col("country") != "DE"), F.lit("high_amount_and_foreign"))
             .when(F.col("amount") > 300, F.lit("high_amount"))
             .when(F.col("country") != "DE", F.lit("foreign"))
             .otherwise(F.lit("ok"))
        )
       )

# Only insert new rows (simple approach): overwrite predictions each time for now
pred.write.mode("overwrite").saveAsTable("fraud_demo.fraud_predictions")

print("Predictions table updated.")

# COMMAND ----------

display(spark.table("fraud_demo.bronze_transactions").orderBy(F.col("event_time").desc()))
display(spark.table("fraud_demo.fraud_predictions").orderBy(F.col("event_time").desc()))