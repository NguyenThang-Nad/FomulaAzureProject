# Databricks notebook source
raw_folder_path = "/mnt/formuladatalake123/raw"
processed_folder_path = "/mnt/formuladatalake123/processed"
presentation_folder_path = "/mnt/formuladatalake123/presentation"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

distinct_race_years_df = race_results_df.select("race_year").distinct()
race_year_list = distinct_race_years_df.select("race_year").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

from pyspark.sql.functions import col
race_yeasr_results_df =race_results_df.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy("race_year", "team_name") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_contructor_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_contructor_df)
