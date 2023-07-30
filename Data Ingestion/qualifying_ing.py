# Databricks notebook source
#qualifying.csv to spark dataframe
qualifying_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/qualifying.csv") 

# COMMAND ----------

display(qualifying_df)
qualifying_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp,col

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

#save dataframe as Parquet format
qualifying_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/qualifying")

# COMMAND ----------


