-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Ingestion - auto loader daily log report notebook
-- MAGIC Create the daily log report notebook to ingest daily comparison with sources and autoloader inserted record count into autoloader.daily_log_report 
-- MAGIC
-- MAGIC #### Note
-- MAGIC If you add a new column or calculation, please ensure that all previous rows are updated manually.
-- MAGIC  
-- MAGIC #### History
-- MAGIC  
-- MAGIC | Date       | By  | Reason           |
-- MAGIC |------------|-----|------------------|
-- MAGIC | 2024-08-08 | Muditha Pelpola | daily log report notebook creation. |
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Run config get environment variables
-- MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/Global.Configs

-- COMMAND ----------

-- DBTITLE 1,Get job/header config information
CREATE OR REPLACE TEMPORARY VIEW vw_config_data 
AS
SELECT
  A.HeaderID,
  concat(
    '${conf.raw}/',(
      CASE
        WHEN SourceFilePath like '%current%' THEN SourceFilePath
        ELSE concat(
          SourceFilePath,
          substring(current_date(), 0, 4),
          '/',
          substring(current_date(), 6, 2),
          '/',
          substring(current_date(), 9, 2)
        )
      END
    ),
    '/*.parquet'
  ) As FilePath,
  concat(trim(DeltaTableSchema), '.', trim(DeltaTableName)) DeltaTabelName,
  concat(
    'select count(*) DeltaCount from ',
    concat(trim(DeltaTableSchema), '.', trim(DeltaTableName)),
    ' Where InsertDate=cast(current_date() as date)'
  ) sqlcmd,WarningDuration
FROM
  autoloader.header_config A 
INNER JOIN autoloader.job_config B ON B.HeaderID=A.HeaderID

-- COMMAND ----------

-- DBTITLE 1,Read parquet file row count from source & delta table row count
-- MAGIC %python
-- MAGIC import pyspark
-- MAGIC # importing sparksession from pyspark.sql module
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC df = spark.sql("select * from vw_config_data")
-- MAGIC #display(df)
-- MAGIC data_list = list()
-- MAGIC
-- MAGIC for x in df.collect():
-- MAGIC     try:
-- MAGIC         count = spark.read.parquet(x["FilePath"]).count()
-- MAGIC         FileCT=spark.read.parquet(x["FilePath"]).select (date_format("_metadata.file_modification_time","MM/dd/yyyy hh:mm")).head()[0]
-- MAGIC     except Exception as e:
-- MAGIC         count=0
-- MAGIC         filetime=''
-- MAGIC         FileCT=''
-- MAGIC     dfc = spark.sql(x["sqlcmd"])
-- MAGIC     deltac=dfc.head()[0]
-- MAGIC     data_list.append(dict(HeaderID=x['HeaderID'],Source_file_path=x['FilePath'],Destination_Tabel_Name=x["DeltaTabelName"],Parquet_Row_Count=count,Delta_Count=deltac,WarningDuration=x['WarningDuration'],FileCreatedTime=FileCT))
-- MAGIC df2=spark.createDataFrame(data_list)
-- MAGIC df2.createOrReplaceTempView("vw_parquet_file_row_count")
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Read Log start end times
CREATE OR REPLACE TEMPORARY VIEW vw_process_start_end_time 
AS
SELECT
  HeaderID,
  MAX(Start_LogDateTime) Start_LogDateTime,
  COALESCE(MAX(End_LogDateTime), current_timestamp()) End_LogDateTime,
  timediff(
    minute,
    MAX(Start_LogDateTime),
    COALESCE(MAX(End_LogDateTime), current_timestamp())
  ) Job_Duration
FROM
  (
    SELECT
      HeaderID,
      MAX(LogDateTime) Start_LogDateTime,
      NULL End_LogDateTime
    FROM
      autoloader.logs
    WHERE
      cast(LogDateTime AS DATE) = current_date()
      AND LogEntryType = 'START'
    GROUP BY
      HeaderID
    UNION
    SELECT
      HeaderID,
      NULL Start_LogDateTime,
      MAX(LogDateTime) End_LogDateTime
    FROM
      autoloader.logs
    WHERE
      cast(LogDateTime AS DATE) = current_date()
      AND LogEntryType = 'END'
    GROUP BY
      HeaderID
  ) AS D
GROUP BY
  HeaderID

-- COMMAND ----------

-- DBTITLE 1,Read Log start end times
CREATE OR REPLACE TEMPORARY VIEW vw_process_all_steps 
AS
SELECT
  A.*,
  B.Job_Duration
FROM
  autoloader.logs A
  INNER JOIN vw_process_start_end_time B ON A.HeaderID = B.HeaderID
  AND A.LogDateTime BETWEEN B.Start_LogDateTime
  AND End_LogDateTime

-- COMMAND ----------

-- DBTITLE 1,Calculate Job duration
CREATE OR REPLACE TEMPORARY VIEW vw_job_duration 
AS
SELECT
  HeaderID,
  ST_LogDateTime Job_Start_Time,
  Wating_End Data_Loading_Started_Time,
  Process_End,
  timediff(minute, ST_LogDateTime, Wating_End) File_Waiting_Duration_Min,
  timediff(minute, ST_LogDateTime, Process_End) - timediff(minute, ST_LogDateTime, Wating_End) File_Loading_Duration_Min,
  timediff(minute, ST_LogDateTime, Process_End) Total_Job_Duration_Min
FROM
  (
    SELECT
      HeaderID,
      MAX(ST_LogDateTime) ST_LogDateTime,
      COALESCE(MAX(Wating_End), current_timestamp()) Wating_End,
      COALESCE(MAX(Process_End), current_timestamp()) Process_End
    FROM
      (
        SELECT
          HeaderID,
          MAX(LogDateTime) ST_LogDateTime,
          NULL Wating_End,
          NULL Process_End
        FROM
          vw_process_all_steps
        WHERE
          LogEntryType IN ('START')
        GROUP BY
          HeaderID
        UNION
        SELECT
          HeaderID,
          NULL ST_LogDateTime,
          MAX(LogDateTime) Wating_End,
          NULL Process_End
        FROM
          vw_process_all_steps
        WHERE
          LogEntryType IN ('AUTO_LOADER')
        GROUP BY
          HeaderID
        UNION
        SELECT
          HeaderID,
          NULL ST_LogDateTime,
          NULL Wating_End,
          MAX(LogDateTime) Process_End
        FROM
          vw_process_all_steps
        WHERE
          LogEntryType IN ('END')
        GROUP BY
          HeaderID
      ) AS F
    GROUP BY
      HeaderID
  ) AS G

-- COMMAND ----------

-- DBTITLE 1,Get log row counts
CREATE OR REPLACE TEMPORARY VIEW vw_job_row_count 
AS
SELECT
  A.HeaderID,
  SUM( CAST(COALESCE(LogEntryDescription, '0') AS INT) ) RowCount
FROM
  vw_process_all_steps A
WHERE
  LogEntryType = 'ROW_COUNT'
GROUP BY A.HeaderID;

-- COMMAND ----------

-- DBTITLE 1,Final output
CREATE OR REPLACE TEMPORARY VIEW vw_final
AS
SELECT A.HeaderID,Source_file_path,FileCreatedTime,Parquet_Row_Count,Destination_Tabel_Name,Delta_Count,B.Job_Start_Time,B.Data_Loading_Started_Time,B.Process_End,B.Total_Job_Duration_Min,
timestampadd(SECOND, WarningDuration,Job_Start_Time) Job_thresholds_End_Time ,C.RowCount LogRowCount,
CASE WHEN timestampadd(SECOND, WarningDuration,Job_Start_Time) < Process_End THEN 'Job Timeout' ELSE 'NO Timeout' END Job_Timeout_Status,
CASE 
WHEN (COALESCE(Parquet_Row_Count,0)-COALESCE(Delta_Count,0) =0) AND (COALESCE(Parquet_Row_Count,0)-COALESCE(C.RowCount,0) =0)   THEN 'PASS (Row count match)' 
WHEN COALESCE(Parquet_Row_Count,0)=0 AND COALESCE(Delta_Count,0) =0  THEN 'In Progress' 
ELSE  'FAIL (Row count match)' END RowCountMatchFlag,
CURRENT_DATE() LogDate
FROM vw_parquet_file_row_count A 
LEFT JOIN vw_job_duration B ON A.HeaderID=B.HeaderID
LEFT JOIN vw_job_row_count C ON A.HeaderID=C.HeaderID
ORDER BY  A.HeaderID

-- COMMAND ----------

-- DBTITLE 1,Delete if records already exists for current date
DELETE FROM autoloader.daily_log_report WHERE  LogCreatedDate=CURRENT_DATE();

-- COMMAND ----------

-- DBTITLE 1,Insert final log output
INSERT INTO autoloader.daily_log_report 
(
HeaderID		,
Source_file_path		,
FileCreatedTime		,
Parquet_Row_Count		,
Destination_Tabel_Name		,
Delta_Count		,
Job_Start_Time		,
Data_Loading_Started_Time		,
Process_End		,
Total_Job_Duration_Min		,
Job_thresholds_End_Time		,
LogRowCount		,
Job_Timeout_Status		,
RowCountMatchFlag		,
LogCreatedDate  
)
SELECT 
HeaderID,
Source_file_path,
FileCreatedTime,
Parquet_Row_Count,
Destination_Tabel_Name,
Delta_Count,
Job_Start_Time,
Data_Loading_Started_Time,
Process_End,
Total_Job_Duration_Min,
Job_thresholds_End_Time,
LogRowCount,
Job_Timeout_Status,
RowCountMatchFlag,
LogDate
FROM 
vw_final
