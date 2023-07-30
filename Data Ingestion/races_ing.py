# Databricks notebook source
#races.csv to spark dataframe
races_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/races.csv") 

# COMMAND ----------

display(races_df)
races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#drop colum fp1_date,fp1_time,fp2_date,fp2_time,fp3_date,fp3_time,quali_date,quali_time,sprint_date,sprint_time
columns_to_drop = ["fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time", "quali_date", "quali_time", "sprint_date", "sprint_time"]
races_selected_df = races_df.drop(*columns_to_drop)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit,current_timestamp

# COMMAND ----------

races_final_df = races_selected_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitld","circuit_id") \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/races")

# COMMAND ----------


