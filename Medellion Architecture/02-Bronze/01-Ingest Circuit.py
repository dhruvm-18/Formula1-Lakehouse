# Databricks notebook source
# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/02-bronzehelper"

# COMMAND ----------

source_file = f"{landings_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

source_file
table_name

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DoubleType

circuits_schema= StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)
])

# COMMAND ----------

circuts_df = (
    spark.read
    .format('csv')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(circuits_schema)
    .load('/Volumes/formula1/landing/files/circuits.csv')
)

# COMMAND ----------

display(circuts_df)

# COMMAND ----------

from pyspark.sql import functions as F
circuits_final_df = (
    circuts_df
    .withColumn('source_file',F.col('_metadata.file_path'))
    .withColumn('ingestion_timestamp',F.current_timestamp())
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

(
    circuits_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("formula1.bronze.circuits")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.circuits;

# COMMAND ----------

display(spark.table('formula1.bronze.circuits'))

# COMMAND ----------

