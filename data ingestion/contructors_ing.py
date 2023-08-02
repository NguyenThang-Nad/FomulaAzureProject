# Databricks notebook source
#constructors.csv to spark dataframe
constructors_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/formuladatalake12/raw/constructors.csv") 

# COMMAND ----------

display(constructors_df)
constructors_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

constructors_df = constructors_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructors_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date",current_timestamp())           

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

#save dataframe as Parquet format
constructor_final_df.write.mode("overwrite").parquet("/mnt/formuladatalake12/processed/constructors")

# COMMAND ----------


