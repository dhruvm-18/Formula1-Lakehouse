# Databricks notebook source
from pyspark.sql import functions as F
def add_ingestion_metadata(df):
    return (    
    df
    .withColumn('source_file',F.col('_metadata.file_path'))
    .withColumn('ingestion_timestamp',F.current_timestamp())
)