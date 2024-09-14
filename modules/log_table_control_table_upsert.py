from datetime import datetime
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('availability_check').getOrCreate()
dbutils = DBUtils(spark)


def update_insert_log_control(header_id,source_file_path,batch_id,log_entry_type,log_entry_des,error_des,status_id,log_table,control_table) -> None:
    """
    Creator:
    --------
    Created by: Shamen Paris
    Email: shamen_paris@next.co.uk
    
    This function inserts log entries into a specified log table. It is designed to keep track of the steps and status of a process, 
    which can be useful for debugging and monitoring purposes.

    Parameters:
    header_id (int): The Header ID from configurations. This is used to identify the process or task.
    source_file_path (str): The directory path of the source file being processed.
    batch_id (int): The ID of the current batch being processed.
    log_entry_type (str): The type of log entry. This could be 'Error', 'Info', 'Warning', etc.
    log_entry_des (str): A brief description of the log entry.
    error_des (str): A description of any errors that occurred. If no errors occurred, this could be 'None' or 'N/A'.
    status_id (int): The status of the process. This could be an integer code representing different stages of the process.
    log_table (str): The name of the log table where the log entries are to be inserted.
    control_table (str): The name of the control table associated with the process.

    Returns:
    None. The function performs an update operation on the database, and does not return any value.
    """

    log_id = int(datetime.now().strftime('%Y%m%d%H%M%S') + str(header_id))

    try:
        notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        job_id = notebook_info["tags"]["jobId"]
    except: 
        job_id = -1 

    log_entry_query = f"""
    INSERT INTO {log_table} (
        LogID,
        HeaderID ,
        SourceFilePath ,
        BatchID ,
        JobID ,
        LogEntryType ,
        LogEntryDescription ,
        ErrorDescription , 
        StatusID ,
        LogDateTime  
    )
    VALUES (
        CAST({log_id} AS BIGINT),
        {header_id},
        '{source_file_path}',
        {batch_id},
        {job_id},
        '{log_entry_type}',
        '{log_entry_des}',
        '{error_des}',
        {status_id},
        CURRENT_TIMESTAMP()
    )
    """

    spark.sql(log_entry_query)

    control_update_q = f"""
    UPDATE {control_table} SET StatusID = {status_id}, LastUpdateTime = CURRENT_TIMESTAMP() WHERE HeaderID = {header_id}
    """
    
    spark.sql(control_update_q)
