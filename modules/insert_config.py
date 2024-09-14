import json
import os
import glob
import pandas as pd
import cron_descriptor
from jsonschema import validate, ValidationError, SchemaError
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from modules.validate_configurations import *
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('availability_check').getOrCreate()
dbutils = DBUtils(spark)

class InsertConfig(ValidateConfig):
    """
    Creator:
    --------
    Created by: Shamen Paris
    Email: shamen_paris@next.co.uk
    
    Description:
        Insert configurations into configuration tables
    Parameter:
        
    """
    def __init__(self,files_path):
        self.files_path = files_path
        super().__init__(files_path)

    def get_config_tables(self):
        conf_json = open('./config/schemas_tables.json',"r")
        conf_sc_tbl = json.load(conf_json)

        # Header Table Info
        self.header_table_schema                 = f"{conf_sc_tbl['header_table']['schema']}"
        self.header_table                        = f"{conf_sc_tbl['header_table']['table_name']}"

        # Column Table Info
        self.column_details_table_schema         = f"{conf_sc_tbl['column_details_table']['schema']}"
        self.column_details_table                = f"{conf_sc_tbl['column_details_table']['table_name']}"

        # Control Table Info
        self.control_table_schema                = f"{conf_sc_tbl['control_table']['schema']}"
        self.control_table                       = f"{conf_sc_tbl['control_table']['table_name']}"

        # Log Table Info
        self.log_table_schema                    = f"{conf_sc_tbl['log_table']['schema']}"
        self.log_table                           = f"{conf_sc_tbl['log_table']['table_name']}"

        # Status Table Info
        self.status_schema                       = f"{conf_sc_tbl['status']['schema']}"
        self.status                              = f"{conf_sc_tbl['status']['table_name']}"

        # Error Table Info
        self.error_description_schema            = f"{conf_sc_tbl['error_description']['schema']}"
        self.error_description                   = f"{conf_sc_tbl['error_description']['table_name']}"

        # Header Staging Table Info
        self.header_staging_table_schema         = f"{conf_sc_tbl['header_staging_table']['schema']}"
        self.header_staging_table                = f"{conf_sc_tbl['header_staging_table']['table_name']}"

        # Column Staging Table Info
        self.column_details_staging_table_schema = f"{conf_sc_tbl['column_details_staging_table']['schema']}"
        self.column_details_staging_table        = f"{conf_sc_tbl['column_details_staging_table']['table_name']}"

        # Schedule Table Info
        self.schedule_description_schema         = f"{conf_sc_tbl['schedule_description']['schema']}"
        self.schedule_description                = f"{conf_sc_tbl['schedule_description']['table_name']}"

        # Other Config Table Info
        self.other_config_schema                 = f"{conf_sc_tbl['other_config']['schema']}"
        self.other_config_table                  = f"{conf_sc_tbl['other_config']['table_name']}"

        # Job Config Table Info
        self.job_config_schema                 = f"{conf_sc_tbl['job_table']['schema']}"
        self.job_config_table                  = f"{conf_sc_tbl['job_table']['table_name']}"

        # Job Config Staging Table Info
        self.job_staging_config_schema         = f"{conf_sc_tbl['job_staging_table']['schema']}"
        self.job_staging_config_table          = f"{conf_sc_tbl['job_staging_table']['table_name']}"
    def get_duplicates_as_string(self,arr):
        seen = set()
        duplicates = set()
        for item in arr:
            if item in seen:
                duplicates.add(item)
            else:
                seen.add(item)
        return ', '.join(map(str, duplicates))
    
    def validate_header_ids_configs(self):
        self.get_config_tables()
        main_config = spark.sql(f'SELECT HeaderID, ConfigFile FROM {self.header_table_schema}.{self.header_table}').collect()
        max_header_id = spark.sql(f'SELECT COALESCE(MAX(HeaderID), 0) AS HeaderID FROM {self.header_table_schema}.{self.header_table}').collect()[0][0]
        used_header = []

        for row in main_config:
            json_config = spark.sql(f"SELECT HeaderID, ConfigFile FROM {self.header_staging_table_schema}.{self.header_staging_table} WHERE HeaderID = {row.HeaderID}").collect()
            for j_row in json_config:
                if row.ConfigFile != j_row.ConfigFile:
                    max_header_id += 1
                    msg = f"Header ID {row.HeaderID} already used in config table using {row.ConfigFile}. Please use a new Header ID in {j_row.ConfigFile}. (Suggestion: Use Header ID as {max_header_id})"
                    used_header.append(msg)

        if used_header:
            raise AssertionError(', \n'.join(map(str, used_header)))

        df_header_config = spark.sql(f'SELECT * FROM {self.header_staging_table_schema}.{self.header_staging_table}')
        empty_header_ids = [row.SourceFile for row in df_header_config.collect() if row.HeaderID == "" or row.HeaderID is None]
        header_ids = [row.HeaderID for row in df_header_config.collect()]

        if empty_header_ids:
            raise AssertionError(f"Empty Header Id files {', '.join(empty_header_ids)}")
        elif self.get_duplicates_as_string(header_ids):
            raise AssertionError(f"Duplicate Header Id entries in: {self.get_duplicates_as_string(header_ids)}")
        else:
            print('HeaderID Validation Successful.')

    def insert_status_config(self):
        try:
            self.get_config_tables()
            status_config_temp_view = f"""
            CREATE OR REPLACE TEMP VIEW status_view AS
            SELECT
                0 as StatusID,
                'Not Started' as StatusDescription
            UNION ALL
            SELECT
                1 as StatusID,
                'In Progress' as StatusDescription
            UNION ALL
            SELECT
                2 as StatusID,
                'Succeeded' as StatusDescription
            UNION ALL
            SELECT
                3 as StatusID,
                'Failed' as StatusDescription
            """

            spark.sql(status_config_temp_view)

            insert_status_config_q = f"""
            MERGE INTO {self.status_schema}.{self.status} t
            USING status_view s
            ON  t.StatusID = s.StatusID
            WHEN NOT MATCHED THEN 
            INSERT(
                StatusID ,
                StatusDescription
            )
            VALUES(
                s.StatusID ,
                s.StatusDescription
            )
            """

            spark.sql(insert_status_config_q)
            print(f'Configurations load into {self.status_schema}.{self.status}')
        except Exception as e:
            print(f'Configurations load ERROR {self.status_schema}.{self.status}', e)
            raise
    
    def insert_header_config(self):
        try:
            self.get_config_tables()
            df_header_config = self.get_header_config()
            df_header_config.createOrReplaceTempView('temp_header_config')
            spark.sql(f'TRUNCATE TABLE {self.header_staging_table_schema}.{self.header_staging_table}')
            spark.sql(f"""
                    INSERT INTO {self.header_staging_table_schema}.{self.header_staging_table}
                    SELECT
                        HeaderID,
                        SourceContainer,
                        SourceFilePath,
                        SourceFileFormat,
                        SourceFileHeader,
                        SourceFileDelimiter,
                        DeltaTableSchema,
                        DeltaTableName,
                        IsPII,
                        PIISchema,
                        PIITableName,
                        OverWriteFlag,
                        BatchFileCount,
                        ContinuousRunFlag,
                        ConfigFile,
                        IsCurrent,
                        CURRENT_TIMESTAMP() AS CreatedDateTime,
                        CURRENT_TIMESTAMP() AS LastUpdatedDateTime
                    FROM
                        temp_header_config
                    """)
            
            print(f'Configurations load into {self.header_staging_table_schema}.{self.header_staging_table}')

            self.validate_header_ids_configs()

            insert_q_header = f"""
            MERGE INTO {self.header_table_schema}.{self.header_table} t
            USING {self.header_staging_table_schema}.{self.header_staging_table} s
            ON  t.HeaderID = s.HeaderID
            WHEN  MATCHED THEN UPDATE SET
                t.HeaderID = s.HeaderID,
                t.SourceContainer = s.SourceContainer,
                t.SourceFilePath = s.SourceFilePath,
                t.SourceFileFormat = s.SourceFileFormat,
                t.SourceFileHeader = s.SourceFileHeader,
                t.SourceFileDelimiter = s.SourceFileDelimiter,
                t.DeltaTableSchema = s.DeltaTableSchema,
                t.DeltaTableName = s.DeltaTableName,
                t.IsPII = s.IsPII,
                t.PIISchema = s.PIISchema,
                t.PIITableName = s.PIITableName,
                t.OverWriteFlag = s.OverWriteFlag,
                t.BatchFileCount = s.BatchFileCount,
                t.ContinuousRunFlag = s.ContinuousRunFlag,
                t.ConfigFile = s.ConfigFile,
                t.IsCurrent = s.IsCurrent,
                t.LastUpdatedDateTime = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN 
            INSERT(
                HeaderID,
                SourceContainer,
                SourceFilePath,
                SourceFileFormat,
                SourceFileHeader,
                SourceFileDelimiter,
                DeltaTableSchema,
                DeltaTableName,
                IsPII,
                PIISchema,
                PIITableName,
                OverWriteFlag,
                BatchFileCount,
                IsCurrent,
                ContinuousRunFlag,
                ConfigFile,
                CreatedDateTime,
                LastUpdatedDateTime 
            )
            VALUES(
                s.HeaderID,
                s.SourceContainer,
                s.SourceFilePath,
                s.SourceFileFormat,
                s.SourceFileHeader,
                s.SourceFileDelimiter,
                s.DeltaTableSchema,
                s.DeltaTableName,
                s.IsPII,
                s.PIISchema,
                s.PIITableName,
                s.OverWriteFlag,
                s.BatchFileCount,
                s.IsCurrent,
                s.ContinuousRunFlag,
                s.ConfigFile,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
                t.IsCurrent = 0,
                t.LastUpdatedDateTime = CURRENT_TIMESTAMP()
            """

            spark.sql(insert_q_header)
            print(f'Configurations load into {self.header_table_schema}.{self.header_table}')
        except Exception as e:
            print(f'Configurations load ERROR {self.header_table_schema}.{self.header_table}', e)
            raise

    def insert_column_config(self):
        try:
            self.get_config_tables()
            df_column_config = self.get_column_config()
            df_column_config.createOrReplaceTempView('temp_column_config')
            spark.sql(f'TRUNCATE TABLE {self.column_details_staging_table_schema}.{self.column_details_staging_table}')
            spark.sql(f"""
                    INSERT INTO {self.column_details_staging_table_schema}.{self.column_details_staging_table}
                    SELECT
                        HeaderID,
                        SourceColumnName,
                        DeltaColumnName,
                        DeltaDataType,
                        ColumnOrder,
                        ZOrder,
                        IsPII,
                        1 IsCurrent,
                        CURRENT_TIMESTAMP() AS CreatedDateTime,
                        CURRENT_TIMESTAMP() AS LastUpdatedDateTime
                    FROM
                        temp_column_config
                    """)
            
            print(f'Configurations load into {self.column_details_staging_table_schema}.{self.column_details_staging_table}')

            col_query = f"""
            MERGE INTO {self.column_details_table_schema}.{self.column_details_table} t
            USING {self.column_details_staging_table_schema}.{self.column_details_staging_table} s
            ON  t.HeaderID = s.HeaderID AND t.ColumnOrder = s.ColumnOrder
            WHEN  MATCHED THEN UPDATE SET
                t.HeaderID = s.HeaderID,
                t.SourceColumnName = s.SourceColumnName,
                t.DeltaColumnName = s.DeltaColumnName,
                t.DeltaDataType = s.DeltaDataType,
                t.ColumnOrder = s.ColumnOrder,
                t.ZOrder = s.ZOrder,
                t.IsPII = s.IsPII,
                t.IsCurrent = s.IsCurrent,
                t.LastUpdatedDateTime = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN 
            INSERT(
                HeaderID,
                SourceColumnName,
                DeltaColumnName,
                DeltaDataType,
                ColumnOrder,
                ZOrder,
                IsPII,
                IsCurrent,
                CreatedDateTime,
                LastUpdatedDateTime
            )
            VALUES(
                s.HeaderID,
                s.SourceColumnName,
                s.DeltaColumnName,
                s.DeltaDataType,
                s.ColumnOrder,
                s.ZOrder,
                s.IsPII,
                s.IsCurrent,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
                t.IsCurrent = 0,
                t.LastUpdatedDateTime = CURRENT_TIMESTAMP()
            """

            spark.sql(col_query)

            print(f'Configurations load into {self.column_details_table_schema}.{self.column_details_table}')
        except Exception as e:
            print(f'Configurations load ERROR {self.column_details_table_schema}.{self.column_details_table}', e)
            raise
    
    def insert_job_config(self):
        try:
            self.get_config_tables()
            df_column_config = self.get_job_config()
            df_column_config.createOrReplaceTempView('temp_job_config')
            spark.sql(f'TRUNCATE TABLE {self.job_staging_config_schema}.{self.job_staging_config_table}')
            spark.sql(f"""
                    INSERT INTO {self.job_staging_config_schema}.{self.job_staging_config_table}
                    SELECT
                        HeaderID,
                        Alert,
                        Emails,
                        WarningDuration,
                        TimeOut,
                        Retries,
                        ClusterMaxWorkers,
                        SparkConf,
                        CronSyntax,
                        CronSyntaxDescription,
                        CURRENT_TIMESTAMP() AS CreatedDateTime,
                        CURRENT_TIMESTAMP() AS LastUpdatedDateTime
                    FROM
                        temp_job_config
                    """)

            print(f'Configurations load into {self.job_staging_config_schema}.{self.job_staging_config_table}')

            job_query = f"""
            MERGE INTO {self.job_config_schema}.{self.job_config_table} t
            USING  {self.job_staging_config_schema}.{self.job_staging_config_table} s
            ON  t.HeaderID = s.HeaderID
            WHEN  MATCHED THEN UPDATE SET
                t.HeaderID = s.HeaderID,
                t.Alert = s.Alert,
                t.Emails = s.Emails,
                t.WarningDuration = s.WarningDuration,
                t.TimeOut = s.TimeOut,
                t.Retries = s.Retries,
                t.ClusterMaxWorkers = s.ClusterMaxWorkers,
                t.SparkConf = s.SparkConf,
                t.CronSyntax = s.CronSyntax,
                t.CronSyntaxDescription = s.CronSyntaxDescription,
                t.LastUpdatedDateTime = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN 
            INSERT(
                HeaderID,
                Alert,
                Emails,
                WarningDuration,
                TimeOut,
                Retries,
                ClusterMaxWorkers,
                SparkConf,
                CronSyntax,
                CronSyntaxDescription,
                CreatedDateTime,
                LastUpdatedDateTime
            )
            VALUES(
                s.HeaderID,
                s.Alert,
                s.Emails,
                s.WarningDuration,
                s.TimeOut,
                s.Retries,
                s.ClusterMaxWorkers,
                s.SparkConf,
                s.CronSyntax,
                s.CronSyntaxDescription,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
            """

            spark.sql(job_query)

            print(f'Configurations load into {self.job_config_schema}.{self.job_config_table}')

        except Exception as e:
            print(f'Configurations load ERROR {self.job_config_schema}.{self.job_config_table}', e)
            raise
    def insert_control_table(self):
        try:
            self.get_config_tables()
            header_query = f"""
            CREATE OR REPLACE TEMP VIEW vw_header AS
            SELECT HeaderID FROM {self.header_table_schema}.{self.header_table}
            """

            spark.sql(header_query)

            control_query = f"""
            MERGE INTO {self.control_table_schema}.{self.control_table} t
            USING vw_header s
            ON  t.HeaderID = s.HeaderID
            WHEN NOT MATCHED THEN 
            INSERT(
                HeaderID,
                StatusID,
                ErrorID,
                PreviousBatchID,
                LatestBatchID,
                JobID,
                LastUpdateTime
            )
            VALUES(
                s.HeaderID,
                0,
                0,
                0,
                0,
                NULL,
                CURRENT_TIMESTAMP()
            )
            """

            spark.sql(control_query)

            print(f'Configurations load into {self.control_table_schema}.{self.control_table}')

        except Exception as e:
            print(f'Configurations load ERROR {self.control_table_schema}.{self.control_table}', e)
            raise
    
    def OptimizeTables(self):
        self.get_config_tables()
        spark.sql(f'OPTIMIZE {self.header_table_schema}.{self.header_table} ZORDER BY (HeaderID)')
        spark.sql(f'OPTIMIZE {self.column_details_table_schema}.{self.column_details_table} ZORDER BY (HeaderID)')
        spark.sql(f'OPTIMIZE {self.job_config_schema}.{self.job_config_table} ZORDER BY (HeaderID)')

    def insert_config(self):
        # Insert Status
        self.insert_status_config()
        # Insert Header Config
        self.insert_header_config()
        # Insert Column Config
        self.insert_column_config()
        # Insert Job Config
        self.insert_job_config()
        # Insert Control Table
        self.insert_control_table()
        # Optimize Table
        self.OptimizeTables()
