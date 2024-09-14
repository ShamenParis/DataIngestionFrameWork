# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Autoloader
# MAGIC The Run Jobs notebook is used to execute the main job and then run the child jobs.
# MAGIC
# MAGIC ### Frequency: 
# MAGIC Daily, Adhoc, Continuous
# MAGIC  
# MAGIC #### History
# MAGIC  
# MAGIC | Date       | By  | Reason           |
# MAGIC |------------|-----|------------------|
# MAGIC | 2024-03-05 | Shamen Paris | Run Jobs Notebook Created. |

# COMMAND ----------

# DBTITLE 1,Install Databricks Python SDK
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

!cp ../requirements.txt ~/.
%pip install -r ~/requirements.txt

# COMMAND ----------

# DBTITLE 1,Restart Libararies
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
import sys
import os
import json
import time
from datetime import datetime, time as t
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import  Task, NotebookTask, Source

# COMMAND ----------

# DBTITLE 1,Set up notebook paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','')}")
%pwd

# COMMAND ----------

# MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/WebhookURL.Configs

# COMMAND ----------

from modules.send_google_chat import *
WEBHOOK_URL = spark.conf.get('conf.autoloader_alerts')
SendGoogleChat = SendGChat(WEBHOOK_URL) 

# COMMAND ----------

# DBTITLE 1,Set TimeZone
##spark.conf.set("spark.sql.session.timeZone", "Europe/London")
##This code blocked job execution in production & had a discussion with developer (Shamen Paris ) and decided to remove it .

# COMMAND ----------

# DBTITLE 1,Configuration tables
conf_json = open('./config/schemas_tables.json',"r")
conf_sc_tbl = json.load(conf_json)

# Header Table Info
header_table                = f"{conf_sc_tbl['header_table']['schema']}.{conf_sc_tbl['header_table']['table_name']}"
# Control Table Info
control_table               = f"{conf_sc_tbl['control_table']['schema']}.{conf_sc_tbl['control_table']['table_name']}"

conf_json.close()

# COMMAND ----------

# DBTITLE 1,Get configurations to run the jobs
need_to_run_jobs = f"""
WITH RestartConfig AS (
  SELECT
    CONCAT(H.DeltaTableSchema,CONCAT(".",H.DeltaTableName)) as TableName,
    C.JobID,
    H.RunFrequency,
    H.Mon,
    H.Tue,
    H.Wed,
    H.Thu,
    H.Fri,
    H.Sat,
    H.Sun,
    CASE 
      WHEN 
        CAST(C.LastUpdateTime AS DATE) = current_date() 
        AND 
        StatusID IN (1,2)
      THEN 0
      ELSE 
        CASE
          WHEN
            StatusID = 1
          THEN 0
          ELSE 1
        END
    END AS NeedToRun,
    H.SLA
  FROM 
    {control_table} C
  LEFT JOIN
    {header_table} H
  ON
    C.HeaderID = H.HeaderID
)
SELECT
  JobID,
  TableName,
  RunFrequency,
  Mon,
  Tue,
  Wed,
  Thu,
  Fri,
  Sat,
  Sun,
  SLA
FROM 
  RestartConfig
WHERE
  NeedToRun = 1
"""

# COMMAND ----------

# DBTITLE 1,Run Active/Restart/Backfill Jobs


# Get Day Name for run the process per scheduled day
now = datetime.now()
short_day = now.strftime("%a")

# Get workspace clent --- WorkspaceClent allow to access the create, run and edit jobs etc.
w = WorkspaceClient()

# Get Job Id and Schedule days
jobs = spark.sql(need_to_run_jobs).collect()
print(f"All Jobs - {jobs}")

for job in jobs:
    # Get SLA
    hours,mins = map(int,job.SLA.split(':'))
    timeout_time = t(hours,mins)
    current_time = datetime.now().time()
    
    # current job run
    run_list = w.jobs.list_runs(job_id=job.JobID,active_only=True)
    rerun = True
    for run in run_list:
        if run.state.life_cycle_state == 'RunLifeCycleState.RUNNING':
            rerun = False
        else:
            rerun = True

    if current_time <= timeout_time and rerun: # Check SLA
        # Run the jobs
        if job.RunFrequency == 1:
            w.jobs.run_now(job.JobID)
            print(f'Job - {job.JobID} started.')
        elif job.RunFrequency == 2 and job[short_day] == 1:
            w.jobs.run_now(job.JobID)
            print(f'Job - {job.JobID} started.')
    else: # Check SLA
        # Cancel the job
        w.jobs.cancel_all_runs(job_id=job.JobID)
        spark.sql(f"UPDATE {control_table} SET StatusID = 2, LastUpdateTime = CURRENT_TIMESTAMP() WHERE JobID = {job.JobID}")
        print(f'Job - {job.JobID} canceled.')

# COMMAND ----------

# DBTITLE 1,Cancel all the pending jobs with SLA
jobs = spark.sql(f'SELECT C.JobID,H.SLA,CONCAT(H.DeltaTableSchema,CONCAT(".",H.DeltaTableName)) as TableName FROM {control_table} C LEFT JOIN {header_table} H ON C.HeaderID = H.HeaderID WHERE C.StatusID = 1').collect()

# Get workspace clent --- WorkspaceClent allow to access the create, run and edit jobs etc.
w = WorkspaceClient()

for job in jobs:
    # Get SLA
    hours,mins = map(int,job.SLA.split(':'))
    timeout_time = t(hours,mins)
    current_time = datetime.now().time()

    if current_time > timeout_time: # Check SLA
        # Cancel the job
        w.jobs.cancel_all_runs(job_id=job.JobID)
        # Send a message
        message = f'<b>SLA </b> breach for {job.TableName}'
        c_datetime = datetime.now()
        current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
        SendGoogleChat.warning_message(job.TableName,message,current_time)
        spark.sql(f"UPDATE {control_table} SET StatusID = 2, LastUpdateTime = CURRENT_TIMESTAMP() WHERE JobID = {job.JobID}")
        print(f'Job - {job.JobID} canceled.')
