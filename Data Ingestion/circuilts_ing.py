# Databricks notebook source
#circuits.csv to spark dataframe
circuits_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/circuits.csv") 

# COMMAND ----------

display(circuits_df)
circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

#drop colum url
circuits_selected_df = circuits_df.drop(col("url"))

# COMMAND ----------

#rename required columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

#save dataframe as Parquet format
circuits_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/circuits")

# COMMAND ----------


