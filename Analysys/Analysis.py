# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists formula_db;

# COMMAND ----------

presentation_folder_path = "/mnt/formuladatalake12/presentation"
raceresults_df=spark.read.parquet(f"{presentation_folder_path}/race_results")
raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula_db.race_results")

# COMMAND ----------

drivers_results_df=spark.read.parquet(f"{presentation_folder_path}/drivers_results")
drivers_results_df.write.mode("overwrite").format("parquet").saveAsTable("formula_db.drivers_results")

# COMMAND ----------

constructors_results_df=spark.read.parquet(f"{presentation_folder_path}/constructors_results")
constructors_results_df.write.mode("overwrite").format("parquet").saveAsTable("formula_db.constructors_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in formula_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula_db.constructors_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula_db.race_results;
