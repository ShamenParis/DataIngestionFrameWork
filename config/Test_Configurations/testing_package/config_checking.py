import sys
import os
from modules.validate_configurations import *
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

sys.path.append("/Workspace/Shared")
spark = SparkSession.builder.appName('availability_check').getOrCreate()
dbutils = DBUtils(spark)

def check_results(config_files_location):
    dbutils.library.restartPython()
    v_config = ValidateConfig(config_files_location)
    
    # Validation
    v_config.validate_json_file()
    header_id = input("Enter Header IDs as 1,2,3 or type 'all' to get all Header IDs:")

    # Header Config
    print('\n' + '='*30)
    print('Header Config'.center(30))
    print('='*30)
    df_header = v_config.get_header_config()
    if header_id.lower() == 'all':
        df_header.display()
    else:
        header_id_list = [int(id.strip()) for id in header_id.split(',')]
        df_header.filter(df_header.HeaderID.isin(header_id_list)).display()
    
    # Column Config
    print('\n' + '='*30)
    print('Column Config'.center(30))
    print('='*30)
    df_column = v_config.get_column_config()
    if header_id.lower() == 'all':
        df_column.display()
    else:
        header_id_list = [int(id.strip()) for id in header_id.split(',')]
        df_column.filter(df_column.HeaderID.isin(header_id_list)).display()
    
    # Job Config
    print('\n' + '='*30)
    print('Job Config'.center(30))
    print('='*30)
    df_job = v_config.get_job_config()
    if header_id.lower() == 'all':
        df_job.display()
    else:
        header_id_list = [int(id.strip()) for id in header_id.split(',')]
        df_job.filter(df_job.HeaderID.isin(header_id_list)).display()