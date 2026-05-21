# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,DateType

races_schema= StructType([
    StructField('season', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('url', StringType(), True),
    StructField('raceName', StringType(), True),
    StructField('date', DateType(), True),
    StructField('circuitId', StringType(), True),
])

# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(races_schema)
    .load('/Volumes/formula1/landing/files/races.csv')
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql import functions as F
races_final_df = (
    races_df
    .withColumn('source_file',F.col('_metadata.file_path'))
    .withColumn('ingestion_timestamp',F.current_timestamp())
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

(
    races_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("formula1.bronze.races")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.races;

# COMMAND ----------

display(spark.table('formula1.bronze.races'))

# COMMAND ----------

