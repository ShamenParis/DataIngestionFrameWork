# Databricks notebook source
# MAGIC %md
# MAGIC `This notebook performs checks on source files and destination tables, validates their integrity, and imports necessary modules and parameters.`

# COMMAND ----------

# DBTITLE 1,Install the libraries
!cp ../requirements.txt ~/.
%pip install -r ~/requirements.txt

# COMMAND ----------

# DBTITLE 1,Restart libraries
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
import json
import time
import os
import sys
from pyspark.sql.streaming import StreamingQuery
from datetime import datetime ,time as t
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.errors import PySparkException, AnalysisException,StreamingQueryException
from json import dumps
from httplib2 import Http
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Set up notebook paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Set TimeZone
spark.conf.set("spark.sql.session.timeZone", "Europe/London")

# COMMAND ----------

# DBTITLE 1,Import Data Ingestion Modules
from modules.delta_table_create_tables import *
from modules.log_table_control_table_upsert import *
from modules.send_google_chat import *
from modules.auto_loader_steps import *

# COMMAND ----------

# DBTITLE 1,Get external locations
# MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/Global.Configs

# COMMAND ----------

# DBTITLE 1,Webhook URL
# MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/WebhookURL.Configs

# COMMAND ----------

# DBTITLE 1,Google Chat
WEBHOOK_URL = spark.conf.get('conf.autoloader_alerts')
SendGChat = SendGChat(WEBHOOK_URL)

# COMMAND ----------

# DBTITLE 1,Configuration tables
conf_json = open('./config/schemas_tables.json',"r")
conf_sc_tbl = json.load(conf_json)

# Column Table Info
column_details_table                = f"{conf_sc_tbl['column_details_table']['schema']}.{conf_sc_tbl['column_details_table']['table_name']}"
# Control Table Info
control_table                       = f"{conf_sc_tbl['control_table']['schema']}.{conf_sc_tbl['control_table']['table_name']}"
# Log Table Info
log_table                           = f"{conf_sc_tbl['log_table']['schema']}.{conf_sc_tbl['log_table']['table_name']}"

conf_json.close()

# COMMAND ----------

# DBTITLE 1,Get and setup parameters from the main Notebook
header_id = int(dbutils.widgets.get('header_id'))
file_format = dbutils.widgets.get('file_format')
source_file_header = int(dbutils.widgets.get('source_file_header'))
source_file_delimiter = dbutils.widgets.get('source_file_delimiter')
source_file_path = dbutils.widgets.get('source_file_path')
schema_location = dbutils.widgets.get('schema_location')
checkpoint_path = dbutils.widgets.get('checkpoint_path')
staging_path = dbutils.widgets.get('staging_path')
target_table_schema = dbutils.widgets.get('target_table_schema')
target_table_name = dbutils.widgets.get('target_table_name')
is_pii = int(dbutils.widgets.get('is_pii'))
pii_table_schema = dbutils.widgets.get('pii_table_schema')
pii_table_name = dbutils.widgets.get('pii_table_name')
overwrite_flag = int(dbutils.widgets.get('overwrite_flag'))
file_count = int(dbutils.widgets.get('file_count'))
archive_path = dbutils.widgets.get('archive_path')
corrupt_delta_location = dbutils.widgets.get('corrupt_delta_location')
error_file_location = dbutils.widgets.get('error_file_location')
continue_run_flag = int(dbutils.widgets.get('continue_run_flag'))

target_table = f"{target_table_schema}.{target_table_name}"
dbk_table_external_location = spark.conf.get('conf.dbktables')
external_location = f"{dbk_table_external_location}/{target_table_schema}/{target_table_name}"
pii_target_table  = f"{pii_table_schema}.{pii_table_name}"
pii_external_location = f"{dbk_table_external_location}/{pii_table_schema}/{pii_table_name}"

# Get last status id
status_id = spark.sql(f"SELECT StatusID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
# Get last batch id
batch_id = spark.sql(f"SELECT LatestBatchID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
# Initial batch id
initial_batch_id = batch_id
# Initial batch id
initial_status_id = status_id
# Current Datetime
current_datetime = spark.sql('select current_timestamp() as current_datetime').collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Call the objects
AutoloaderDataLoad_ = AutoloaderDataLoad(file_format,source_file_header,source_file_delimiter,file_count,schema_location,source_file_path,target_table,is_pii,pii_target_table,checkpoint_path,initial_batch_id,header_id,column_details_table,control_table,log_table,corrupt_delta_location,error_file_location,WEBHOOK_URL,continue_run_flag)

# COMMAND ----------

# DBTITLE 1,Delete table Records and Create tables if not exists
def delete_table_records_step(target_table,external_location,source_file_path,column_details_table,log_table,control_table,header_id,initial_batch_id,continue_run_flag) -> None:
    """
    Description: 
        Delete the records and check table is available or not.
    """
    try:
        print('Delete tables step started.')
        if continue_run_flag == 0:
            spark.sql(f"DELETE FROM {target_table} WHERE InsertDate = CURRENT_DATE()")
        else:
            spark.sql(f"DESCRIBE {target_table}")

        # Logging
        log_entry_type = 'START'
        log_entry_des = 'Delete current day records from the delta table'
        error_des = ''
        status_id = 1
        update_insert_log_control(header_id,source_file_path,initial_batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)
    except PySparkException as e:
        if e.getErrorClass() == 'TABLE_OR_VIEW_NOT_FOUND':
            DeltaTableOperations_ = DeltaTableOperations(target_table,external_location,column_details_table,header_id)
            DeltaTableOperations_.create_table()

            # Logging
            log_entry_type = 'CREATE_TABLE'
            log_entry_des = 'Delta table not found. Create a new delta table'
            error_des = ''
            status_id = 1
            update_insert_log_control(header_id,source_file_path,initial_batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)

            # Send Google Chat
            message = f"<b>CREATE_DELTA_TALBE</b> - Table not found when try to insert data into the {target_table}. Created the table with all the columns available in the configuration table."
            job_id = spark.sql(f"SELECT JobID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
            job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
            c_datetime = datetime.now()
            current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
            SendGChat.warning_message(target_table,message,current_time)
            print('Table has been created.')
            
    except Exception as e:
        print('Delete table ERROR:')
        # Logging
        log_entry_type = 'DELETE_RECORDS'
        log_entry_des = 'Delete current day records from the delta table'
        error_des = str(e).replace("'",'"')
        status_id = 3
        update_insert_log_control(header_id,source_file_path,initial_batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)

        # Send Google Chat
        error_message = f"<b>DELETE_RECORDS</b> - Error when try to delete the current records from the delta table."
        job_id = spark.sql(f"SELECT JobID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
        job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
        c_datetime = datetime.now()
        current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
        SendGChat.failed_message(target_table,error_message,job_url,current_time)

        raise Exception(e)

# COMMAND ----------

# DBTITLE 1,Check the new columns added to delta table
def check_delta_table_columns(column_details_table,header_id,target_table,source_file_path,batch_id,log_table,control_table):
    """
    Description:
        Check the all the columns available in the delta table
    """
    try:
        print('Check delta table column step started.')
        # Get column info
        col_query = f"""
                    SELECT
                        DeltaColumnName,
                        SourceColumnName,
                        DeltaDataType,
                        ZOrder,
                        IsPII
                    FROM
                        {column_details_table}
                    WHERE
                        HeaderID = {header_id}
                        AND IsCurrent = 1
                    ORDER BY
                        ColumnOrder ASC
                    """
        col_configurations = spark.sql(col_query).collect()

        column_table_info = {}
        for column_info in col_configurations:
            column_name = column_info['SourceColumnName']
            data_type = column_info['DeltaDataType']
            column_table_info[column_name] = data_type

        # Delta talbe info
        df_delta_table = DeltaTable.forName(spark,tableOrViewName=target_table).toDF()
        column_object = df_delta_table.dtypes
        delta_table_info = {}
        for column_name , data_type in column_object:
            delta_table_info[column_name] = data_type

        extra_columns = set(column_table_info.keys()).difference(delta_table_info.keys())
        extra_columns_dict = {key: column_table_info[key] for key in extra_columns}

        if extra_columns:
            print('Extra columns found.')
            for extra_col in extra_columns_dict:
                column_order_q = f"SELECT MAX(ColumnOrder) max_column_order FROM {column_details_table} WHERE HeaderID = {header_id}"
                max_column_order = spark.sql(column_order_q).collect()
                max_column_order_no = int(max_column_order[0]['max_column_order']) - 1
                max_column_name_q = f"SELECT DeltaColumnName FROM {column_details_table} WHERE HeaderID = {header_id} AND ColumnOrder = {max_column_order_no}"
                max_column_name = spark.sql(max_column_name_q).collect()
                last_column_name = max_column_name[0]['DeltaColumnName']

                alter_table_q = f"ALTER TABLE {target_table} ADD COLUMNS ({extra_col} STRING)"
                spark.sql(alter_table_q)

                change_column_position_q = f"ALTER TABLE {target_table} CHANGE COLUMN {extra_col} AFTER {last_column_name};"
                spark.sql(change_column_position_q)


                # Logging
                log_entry_type = 'TABLE_CHECK'
                log_entry_des = 'New cloumn(s) detected and added.'
                error_des = ''
                status_id = 1
                update_insert_log_control(header_id,source_file_path,batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)

                # Send message about table creation
                message = f"<b>ADD_NEW_COLUMNS</b> - New column(s) has been added to {target_table}. Column name - {extra_col}"
                job_id = spark.sql(f"SELECT JobID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
                job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
                c_datetime = datetime.now()
                current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
                SendGChat.warning_message(target_table,message,current_time)
        else:
            print('No Extra Column(s) detect')
    except Exception as e:
        print('Check delta table column step ERROR:')
        # Logging
        log_entry_type = 'TABLE_CHECK'
        log_entry_des = 'New cloumn(s) detected and added.'
        error_des = str(e).replace("'",'"')
        status_id = 3
        update_insert_log_control(header_id,source_file_path,batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)

        # Send message about table creation
        error_message = f"<b>ADD_NEW_COLUMNS</b> - Error when adding new column(s) to the {target_table}"
        job_id = spark.sql(f"SELECT JobID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
        job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
        c_datetime = datetime.now()
        current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
        SendGChat.failed_message(target_table,error_message,job_url,current_time)
        raise Exception(e)

# COMMAND ----------

# DBTITLE 1,Loggings
def start_logging() -> None:
    log_entry_type = 'AUTO_LOADER'
    log_entry_des = 'Data ingestion process started'
    error_des = ''
    status_id = 1
    update_insert_log_control(header_id,source_file_path,initial_batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)


def bad_records_error_logging() -> None:
    log_entry_type = 'AUTO_LOADER'
    log_entry_des = 'Bad records found in the file(s)'
    error_des = str(e).replace("'",'"')
    status_id = 3
    update_insert_log_control(header_id,source_file_path,initial_batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)


def erro_logging() -> None:
    log_entry_type = 'AUTO_LOADER'
    log_entry_des = 'Auto Loader process failed.'
    error_des = str(e).replace("'",'"')
    status_id = 3
    update_insert_log_control(header_id,source_file_path,initial_batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table)

# COMMAND ----------

# DBTITLE 1,Send Notifications
def bad_records_notification() -> None:
    error_message = f"<b>AUTO_LOADER</b> - Error when try to insert the data into the delta table. \n<b>Error File Location:</b> {error_file_location}\n<b>Corrupt Records (Delta format):</b> {corrupt_delta_location}"
    job_id = spark.sql(f"SELECT JobID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
    job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
    c_datetime = datetime.now()
    current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
    SendGChat.failed_message(target_table,error_message,job_url,current_time)


def error_notification() -> None:
    error_message = f"<b>AUTO_LOADER</b> - Error on the Auto Loader process."
    job_id = spark.sql(f"SELECT JobID FROM {control_table} WHERE HeaderID = {header_id}").collect()[0][0]
    job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
    c_datetime = datetime.now()
    current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
    SendGChat.failed_message(target_table,error_message,job_url,current_time)

# COMMAND ----------

# DBTITLE 1,Check the file path
def file_exists(path) -> bool:
    """
    Description: 
        Check the file path and return true boolean value if the files are available.

    Return: 
        Boolean
    """
    try:
        files_paths = dbutils.fs.ls(path)
        if files_paths:
            return True
        else:
            return False
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Delete Records and Check the delta table
if is_pii == 1:
    delete_table_records_step(target_table,external_location,source_file_path,column_details_table,log_table,control_table,header_id,initial_batch_id,continue_run_flag)
    delete_table_records_step(pii_target_table,pii_external_location,source_file_path,column_details_table,log_table,control_table,header_id,initial_batch_id,continue_run_flag)
    check_delta_table_columns(column_details_table,header_id,pii_target_table,source_file_path,batch_id,log_table,control_table)
    check_delta_table_columns(column_details_table,header_id,target_table,source_file_path,batch_id,log_table,control_table)
else:
    delete_table_records_step(target_table,external_location,source_file_path,column_details_table,log_table,control_table,header_id,initial_batch_id,continue_run_flag)
    check_delta_table_columns(column_details_table,header_id,target_table,source_file_path,batch_id,log_table,control_table)

# COMMAND ----------

# DBTITLE 1,Check path empty or not
print_flag = True
while not file_exists(source_file_path):
    time.sleep(1)
    if print_flag:
        print('Waiting for the source file availability.')
        print_flag = False
