# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/02-bronzehelper"

# COMMAND ----------

source_file = f"{landings_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DateType

name_schema= StructType([
    StructField('givenName', StringType(), True),
    StructField('familyName', StringType(), True)
])
drivers_schema= StructType([
    StructField('driverId', StringType(), True),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = (
    spark.read
    .format('json')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(drivers_schema)
    .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

write_to_bronze(
    input_df = drivers_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

