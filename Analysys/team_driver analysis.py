# Databricks notebook source
# MAGIC %sql
# MAGIC select * from formula_db.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     driver_name,
# MAGIC     SUM(points) AS total_points,
# MAGIC     AVG(points) AS avg_points,
# MAGIC     COUNT(1) AS total_races
# MAGIC FROM
# MAGIC     formula_db.race_results
# MAGIC WHERE 
# MAGIC race_year = 2023
# MAGIC GROUP BY
# MAGIC     driver_name
# MAGIC ORDER BY
# MAGIC     total_points DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula_db.constructors_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,SUM(points) as total_points, COUNT(1) AS total_races
# MAGIC FROM
# MAGIC     formula_db.race_results
# MAGIC GROUP BY team_name
# MAGIC HAVING
# MAGIC     COUNT(1) >= 100
# MAGIC ORDER BY
# MAGIC     total_points DESC;
# MAGIC

# COMMAND ----------

    
