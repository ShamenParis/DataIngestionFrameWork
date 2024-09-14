# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Autoloader
# MAGIC This is the main notebook which used to run the Databricks - Auto Loader main process
# MAGIC
# MAGIC ### Frequency: Daily, Adhoc, Continuous
# MAGIC
# MAGIC ### Notes
# MAGIC Refer the README.md for more details 
# MAGIC  
# MAGIC #### History
# MAGIC  
# MAGIC | Date       | By  | Reason           |
# MAGIC |------------|-----|------------------|
# MAGIC | 2024-03-05 | Shamen Paris | Auto Loader Process Created. |

# COMMAND ----------

# DBTITLE 1,Get Data Ingestion Helper
# MAGIC %run "./Data Ingestion Helper"

# COMMAND ----------

# DBTITLE 1,Auto Loader Process
# Start logging
start_logging()

# Create a streaming DataFrame
streaming_dataframe = AutoloaderDataLoad_.read_streaming()

if continue_run_flag == 0:
    trigger_time = '30 seconds'
else:
    trigger_time = '0.5 seconds'

query = (
    streaming_dataframe
    .selectExpr('*', '_metadata.file_path as source_file_path')
    .writeStream
    .foreachBatch(lambda df, batch_id: AutoloaderDataLoad_.microbatch_process(df, batch_id, initial_batch_id, overwrite_flag))
    .trigger(processingTime=trigger_time)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .option("retryCount", 2)
    .start()
)

# COMMAND ----------

# DBTITLE 1,Stop the batch
if continue_run_flag == 0:
    AutoloaderDataLoad_.stop_batch(query)
