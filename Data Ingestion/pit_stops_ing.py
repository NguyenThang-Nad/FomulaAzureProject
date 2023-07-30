# Databricks notebook source
#pit_stops.csv to spark dataframe
pit_stops_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/pit_stops.csv")

# COMMAND ----------

display(pit_stops_df)
pit_stops_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp,col

# COMMAND ----------

pit_final_stops_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_final_stops_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/pit_stops")

# COMMAND ----------


