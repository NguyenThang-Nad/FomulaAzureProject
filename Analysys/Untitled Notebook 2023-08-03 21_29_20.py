# Databricks notebook source
# MAGIC %sql
# MAGIC select * from formula_db.constructors_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, 
# MAGIC        team_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(points) AS total_points,
# MAGIC        AVG(points) AS avg_points
# MAGIC   FROM formula_db.race_results
# MAGIC  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
# MAGIC GROUP BY race_year, team_name
# MAGIC ORDER BY race_year, avg_points DESC
# MAGIC
