# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/04-goldhelper"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .filter(F.col("batch_id") == v_batch_id)
    .withColumn("session_type",F.lit("RACE"))
    .drop("race_name","race_date","ingestion_timestamp","source_file")
)
sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .filter(F.col("batch_id") == v_batch_id)
    .withColumn("session_type",F.lit("SPRINT"))
    .drop("race_name","race_date","ingestion_timestamp","source_file")
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

fact_session_results_df= (
    results_sprints_df
    .withColumn("is_win",F.col("final_position")== 1)
    .withColumn("is_podium",F.col("final_position").between(1,3))
    .withColumn("has_points",F.col("points") > 0 )
)

# COMMAND ----------

display(fact_session_results_df.filter("season = 2025"))

# COMMAND ----------

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition="""
        t.season = s.season
        AND t.round = s.round
        AND t.constructor_id = s.constructor_id
        AND t.driver_id = s.driver_id
        AND t.session_type = s.session_type
    """,
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_points"
    ]
)


# COMMAND ----------

display(spark.table(target_table))