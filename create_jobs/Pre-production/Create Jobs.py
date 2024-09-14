# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Autoloader
# MAGIC This notebook facilitates the creation of jobs in the Databricks environment by leveraging configurable settings. With this tool, users can easily define job parameters, such as data sources, scheduling options, and notification settings, through header configuration table.
# MAGIC
# MAGIC ### Frequency: 
# MAGIC Daily, Adhoc, Continuous
# MAGIC  
# MAGIC #### History
# MAGIC  
# MAGIC | Date       | By  | Reason           |
# MAGIC |------------|-----|------------------|
# MAGIC | 2024-07-22 | Shamen Paris | Auto Loader Jobs Notebook Created. |

# COMMAND ----------

# DBTITLE 1,Set up root path
import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root =  os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Install Databricks Python SDK
!cp ./create_jobs/jobs_requirements.txt ~/.
%pip install -r ~/jobs_requirements.txt

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Restart Libraries
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set up notebook paths
import os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import col
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *
from databricks.sdk.service import iam
import sys
import os
import json
import time

# COMMAND ----------

# DBTITLE 1,Set TimeZone
spark.conf.set("spark.sql.session.timeZone", "Europe/London")

# COMMAND ----------

# DBTITLE 1,Import Config Prepare Modules
from modules.create_workflow_jobs import *

# COMMAND ----------

# DBTITLE 1,Get Datalake Config paths
# MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/Global.Configs

# COMMAND ----------

# DBTITLE 1,Get the object to create jobs and validate the configurations
env = 'Pre-production'
workflow_jobs = CreateWorkFlowJobs(environment = env,files_path = './config/{env}/main_config')

# COMMAND ----------

# DBTITLE 1,Validatation Step
workflow_jobs.validate_json_file()

# COMMAND ----------

# DBTITLE 1,Create Jobs Step
workflow_jobs.create_and_reset_jobs()
