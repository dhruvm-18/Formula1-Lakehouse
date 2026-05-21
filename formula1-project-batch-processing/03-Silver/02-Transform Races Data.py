# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/03-silverhelper"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = (
    spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_select_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("source_file"),
    F.col("ingestion_timestamp"),
    F.col("batch_id")
)

# COMMAND ----------

races_renamed_df = (
    races_select_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("raceName", "race_name")
    .withColumnRenamed("date", "race_date")
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

races_valid_df = races_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

display(races_valid_df)

# COMMAND ----------

races_distinct_df = races_valid_df.dropDuplicates(
    ["season","round"]
)

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

races_final_df = (
    races_distinct_df
    .withColumn('race_name',F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))