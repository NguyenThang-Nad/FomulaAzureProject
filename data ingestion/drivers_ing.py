# Databricks notebook source
#drivers.csv to spark dataframe
drivers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake123/raw/drivers.csv") 

# COMMAND ----------

display(drivers_df)
drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit,concat

# COMMAND ----------

drivers_select_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("forename"), lit(" "), col("surname"))) \
                                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

columns_to_drop = ["url", "forename", "surname"]

# Xóa các cột và lưu kết quả vào DataFrame mới
drivers_final_df= drivers_select_df.drop(*columns_to_drop)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

#save dataframe as Parquet format
drivers_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake123/processed/drivers")
