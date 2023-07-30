# Databricks notebook source
#results.csv to spark dataframe
results_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/results.csv") 

# COMMAND ----------

display(results_df)
results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp,col

# COMMAND ----------


results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

duplicate_counts =results_final_df.groupBy("race_id", "driver_id").count().filter(col("count") > 1)

# Hiển thị các giá trị trùng lặp
print("Các giá trị trùng lặp:")
duplicate_counts.show()

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

#save dataframe as Parquet format
results_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/results")

# COMMAND ----------


