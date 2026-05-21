# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formula1.gold.v_driving_standing
# MAGIC AS
# MAGIC WITH driver_session_summary 
# MAGIC AS(
# MAGIC SELECT r.season,
# MAGIC d.driver_id,
# MAGIC d.driver_name,
# MAGIC d.nationality,
# MAGIC COUNT(*) AS race_starts,
# MAGIC SUM(points) AS total_points,
# MAGIC COUNT_IF(r.is_win) AS number_of_wins,
# MAGIC COUNT_IF(r.is_podium) AS number_of_podiums
# MAGIC FROM formula1.gold.fact_session_results r 
# MAGIC JOIN formula1.gold.dim_drivers d  
# MAGIC ON r.driver_id = d.driver_id
# MAGIC GROUP BY r.season,
# MAGIC d.driver_id,
# MAGIC d.driver_name,
# MAGIC d.nationality
# MAGIC )
# MAGIC SELECT season,
# MAGIC driver_id,
# MAGIC RANK() OVER (PARTITION by season ORDER BY total_points DESC,number_of_wins) AS standing,
# MAGIC driver_name,
# MAGIC nationality,
# MAGIC race_starts,
# MAGIC total_points,
# MAGIC number_of_wins,
# MAGIC number_of_podiums
# MAGIC FROM driver_session_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.gold.v_driving_standing WHERE season = "2020"