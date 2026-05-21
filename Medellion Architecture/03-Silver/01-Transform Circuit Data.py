# Databricks notebook source
# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_select_df = circuits_df.select(
    F.col("circuitId").alias("circuit_id"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("source_file"),
    F.col("ingestion_timestamp")
)

# COMMAND ----------

circuits_renamed_df = (
    circuits_select_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitName", "circuit_name")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("long", "longitude")
)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    "circuit_id IS NOT NULL"
)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(
    ["circuit_id"]
)

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn('circuit_name',F.initcap(F.col("circuit_name")))
    .withColumn('locality',F.initcap(F.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

(
    circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))