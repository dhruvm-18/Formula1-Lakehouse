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

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = (
    spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("dateOfBirth", "date_of_birth")
)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

drivers_concatenated_df = (
     drivers_renamed_df
     .withColumn("driver_name", 
                 F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
     .drop("name")
)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

drivers_case_df = (
    drivers_concatenated_df
    .withColumn('nationality',F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(drivers_case_df)

# COMMAND ----------

drivers_final_df = drivers_case_df.dropDuplicates(
    ["driver_id"]
)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))