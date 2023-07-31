# Databricks notebook source
#lap_times.csv to spark dataframe
lap_times_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/lap_times.csv") 

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

#save dataframe as Parquet format
lap_times_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/lap_times")
