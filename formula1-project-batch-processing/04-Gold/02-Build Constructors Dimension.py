# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00-common/01-Environment Config"

# COMMAND ----------

# MAGIC %run "../00-common/04-goldhelper"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructors_df = (
    spark.table(f"{catalog_name}.{silver_schema}.constructors")
    .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

ref_nationality_region_df = (
    spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")
)

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(
        ref_nationality_region_df,
        constructors_df.nationality == ref_nationality_region_df.nationality,
        "left"
    )
    .select(
        constructors_df.constructor_id,
        constructors_df.consutructor_name,
        constructors_df.nationality,
        ref_nationality_region_df.region,
    )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "consutructor_name",
        "nationality",
        "region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))