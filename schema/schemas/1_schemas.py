# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Create Schemas
# MAGIC Create the schemas if not exists
# MAGIC
# MAGIC  
# MAGIC #### History
# MAGIC  
# MAGIC | Date       | By  | Reason           |
# MAGIC |------------|-----|------------------|
# MAGIC | 2024-03-17 | Shamen Paris | Create schemas scripts. |

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os
import sys
import json

# COMMAND ----------

# DBTITLE 1,Get external locations
# MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/Global.Configs

# COMMAND ----------

# DBTITLE 1,Setup the path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','').replace('schema','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Create Schemas
conf_json = open('./config/schemas_tables.json',"r")
conf_sc_tbl = json.load(conf_json)

# Create Schemas
for i in conf_sc_tbl:
    schema = conf_sc_tbl[i]['schema']
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema}')

print('Schema(s) created successfully.')
conf_json.close()
