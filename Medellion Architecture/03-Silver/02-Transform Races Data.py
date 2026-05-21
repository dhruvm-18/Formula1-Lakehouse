# Databricks notebook source
# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = spark.read.option('versionAsOf',0).table(bronze_table)

# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_select_df = races_df.select(
    "season",
    "round",
    "raceName",
    "date",
    "circuitId",
    "source_file",
    "ingestion_timestamp"
)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_select_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("source_file"),
    F.col("ingestion_timestamp")
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

(
    races_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))