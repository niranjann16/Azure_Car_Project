# Databricks notebook source
# MAGIC %md
# MAGIC #Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC #CREATE SCHEMA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cars_catalog.gold;