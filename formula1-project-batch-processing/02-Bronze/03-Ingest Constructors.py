# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/02-bronzehelper"

# COMMAND ----------

source_file = f"{landings_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

constructors_schema = """constructorId STRING ,
                         name STRING , 
                         nationality STRING , 
                         url STRING
                         """

# COMMAND ----------

constructors_df = (
    spark.read
    .format('json')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(constructors_schema)
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

write_to_bronze(
    input_df = constructors_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

