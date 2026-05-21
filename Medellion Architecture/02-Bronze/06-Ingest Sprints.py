# Databricks notebook source
# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/02-bronzehelper"

# COMMAND ----------

source_file = f"{landings_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

from pyspark.sql.types import *

sprint_schema = StructType([
    StructField("date", DateType(), True),
    StructField("raceName", StringType(), True),
    StructField("round", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("constructorId", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

sprints_df = (
    spark.read
    .format('json')
    .option('mode','FAILFAST')
    .option('multiLine',True)
    .schema(sprint_schema)
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

(
    sprints_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season,count(*)
# MAGIC FROM formula1.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season;