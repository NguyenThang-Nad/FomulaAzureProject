# Databricks notebook source
# MAGIC %sql
# MAGIC show tables in formula_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula_db.drivers_results
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name, total_points
# MAGIC FROM formula_db.drivers_results
# MAGIC WHERE race_year = 2023
# MAGIC ORDER BY total_points DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(points),
# MAGIC        AVG(points) AS avg_points
# MAGIC   FROM formula_db.race_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC
