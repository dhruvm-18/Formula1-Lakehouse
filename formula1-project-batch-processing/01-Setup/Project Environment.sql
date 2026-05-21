-- Databricks notebook source
-- MAGIC %fs ls 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula_incr
    URL 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databricks-course-sc`)
    COMMENT 'External Location for the formula1 container'

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS  formula1_incr
      MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/'
      COMMENT 'Main catalog for the formula1 database'


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing; 
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
      MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
      MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
      MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/';

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG formula1_incr;

-- COMMAND ----------

Show schemas;

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.files
    LOCATION 'abfss://formula1-incr@databrickscourseextdl187.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files