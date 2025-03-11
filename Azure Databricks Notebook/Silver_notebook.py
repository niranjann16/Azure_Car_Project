# Databricks notebook source
# MAGIC %md
# MAGIC #Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .option('inferSchema',True)\
        .load('abfss://bronze@carsdatalake.dfs.core.windows.net/Raw_Data')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_ID'), '-')[0])
display(df)

# COMMAND ----------

df = df.withColumn('Units_Sold', col('Units_Sold').cast(StringType()))
display(df)

# COMMAND ----------

df = df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC

# COMMAND ----------

display(df.groupBy('Year', 'BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold',ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
        .option('path','abfss://silver@carsdatalake.dfs.core.windows.net/carsales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@carsdatalake.dfs.core.windows.net/carsales`