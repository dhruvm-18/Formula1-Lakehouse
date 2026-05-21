# Databricks notebook source
# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/02-bronzehelper"

# COMMAND ----------

source_file = f"{landings_folder_path}/constructors.json"
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

(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.constructors;

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

