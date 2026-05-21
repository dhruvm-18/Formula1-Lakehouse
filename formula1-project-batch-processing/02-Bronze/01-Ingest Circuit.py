# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/02-bronzehelper"

# COMMAND ----------

source_file = f"{landings_folder_path}/{v_batch_id}/circuits.csv"
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
    .load(source_file)
)

# COMMAND ----------

display(circuts_df)

# COMMAND ----------


circuits_final_df = add_ingestion_metadata(circuts_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

write_to_bronze(
    input_df = circuits_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))