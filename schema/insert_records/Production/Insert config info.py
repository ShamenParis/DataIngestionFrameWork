# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Autoloader
# MAGIC Insert Records to configuration tables
# MAGIC  
# MAGIC #### History
# MAGIC  
# MAGIC | Date       | By  | Reason           |
# MAGIC |------------|-----|------------------|
# MAGIC | 2024-03-05 | Shamen Paris | Notebook Created. |

# COMMAND ----------

# DBTITLE 1,Setup root path
import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root =  os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(notebook_path))))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Install Libraries
!cp ./schema/insert_records/config_requirements.txt ~/.
%pip install -r ~/config_requirements.txt

# COMMAND ----------

# DBTITLE 1,Restart the kernel
dbutils.library.restartPython()

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
repo_root =  os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(notebook_path))))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Import User Defined Modules
from modules.insert_config import *

# COMMAND ----------

# DBTITLE 1,Validatation Step
config_files_location = './config/Production/main_config'
v_config = InsertConfig(config_files_location)
    
# Validation
v_config.validate_json_file()

# COMMAND ----------

# DBTITLE 1,Insert Configurations To Tables
v_config.insert_config()
