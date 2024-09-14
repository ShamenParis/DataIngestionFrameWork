from pyspark.sql.streaming import StreamingQuery
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.errors import PySparkException, AnalysisException
import json
from httplib2 import Http
from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from modules.send_google_chat import *
from modules.log_table_control_table_upsert import *

spark = SparkSession.builder.appName('availability_check').getOrCreate()
dbutils = DBUtils(spark)

class AutoloaderDataLoad:
    """
    Creator:
    --------
    Created by: Shamen Paris
    Email: shamen_paris@next.co.uk

    Description:
    ------------
    This class handles data loading using autoloader.

    Functions:
    ----------
    - read_streaming: Reads streaming data using autoloader.
    - microbatch_process: Processes each micro batch.
    - stop_batch: Stops the stream after batch processing is complete.

    Parameters:
    -----------
    - data_format: Format of the data files.
    - file_count: Number of files to process.
    - schema_location: Location of the schema definition.
    - source_file_path: Path to the source data files.
    - table_name: Name of the target database table.
    - is_pii: Flag indicating whether the data contains personally identifiable information.
    - target_pii_table: Name of the PII target table.
    - checkpoint_path: Path for checkpointing.
    - batch_id: Batch identifier.
    - header_id: Header identifier.
    - column_details_table: Table containing column details.
    - control_table: Control table for data loading.
    - log_table: Table for logging.
    - corrupt_location: Location for storing corrupt data.
    - error_location: Location for storing error data.
    - webhook_url: URL for webhook notifications.

    Attributes:
    -----------
    - cloud_files: Dictionary containing cloud file configuration.
    """
    def __init__(self, file_format,source_file_header,source_file_delimiter, 
                 file_count, schema_location, source_file_path,
                 table_name, is_pii, pii_target_table, checkpoint_path, batch_id,
                 header_id, column_details_table, control_table, log_table,
                 corrupt_location, error_location, webhook_url,continue_run_flag):
        self.file_format = file_format
        self.source_file_header = source_file_header
        self.source_file_delimiter = source_file_delimiter
        self.file_count = file_count
        self.schema_location = schema_location
        self.source_file_path = source_file_path
        self.column_details_table = column_details_table
        self.control_table = control_table
        self.log_table = log_table
        self.batch_id = batch_id
        self.header_id = header_id
        self.table_name = table_name
        self.is_pii = is_pii
        self.pii_target_table = pii_target_table
        self.corrupt_location = corrupt_location
        self.error_location = error_location
        self.webhook_url = webhook_url
        self.error_file_name = self.generate_error_file_name()
        if continue_run_flag == 1:
            max_file_per_trigger = 100
        else:
            max_file_per_trigger = 1
        self.cloud_files = {
            "cloudFiles.format": self.file_format,
            "cloudFiles.allowOverwrites": "true",
            "cloudFiles.maxFilesPerTrigger":max_file_per_trigger,
            "cloudFiles.schemaLocation": self.schema_location,
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.schemaHints": self.schema_hints(),
            "header": "true" if self.source_file_header == 1 else "false",
            "ignoreMissingFiles": "true",
            "ignoreCorruptFiles": "true",
            "rescuedDataColumn": "_rescued_data"
        }

    def read_streaming(self) -> DataFrame:
        """
        Reads streaming data using autoloader.

        Returns:
            DataFrame: Streaming data frame.
        """
        try:
            if self.source_file_delimiter != "" or self.source_file_delimiter != None:
                df = spark.readStream.format("cloudFiles").options(**self.cloud_files).option('sep',self.source_file_delimiter).load(self.source_file_path)
            else:
                df = spark.readStream.format("cloudFiles").options(**self.cloud_files).load(self.source_file_path)
            
            return df
        
        except Exception as e:
            raise Exception(f"Error reading streaming data: {e}")

    
    def column_info(self) -> list:
        """
        Retrieves column configurations from the specified table.

        Returns:
            list: List of dictionaries containing column information.
        """
        try:
            col_query = f"""
                SELECT
                    DeltaColumnName,
                    SourceColumnName,
                    DeltaDataType,
                    ZOrder,
                    IsPII
                FROM
                    {self.column_details_table}
                WHERE
                    HeaderID = {self.header_id}
                    AND IsCurrent = 1
                ORDER BY
                    ColumnOrder ASC
            """
            col_configurations = spark.sql(col_query).collect()
            return col_configurations
        except Exception as e:
            raise Exception(f"Error fetching column information: {e}")
    
    def get_schema(self) -> StructType:
        """
        Retrieves the schema based on the configuration table.

        Returns:
            StructType: Schema information.
        """
        fields_list = list()
        for cdetails in self.column_info():
            col_name = cdetails["SourceColumnName"]
            data_type = cdetails["DeltaDataType"]
            fields_list.append(dict(metadata={},name=col_name,nullable=True,type=data_type))
        
        
        # Create the schema in json format
        jschema = {"fields":fields_list,"type":"struct"}
        jdump = json.dumps(jschema)
        json_schema_to_str = str(jdump)
        schema = StructType.fromJson(json.loads(json_schema_to_str))
        return schema

    def get_selected_columns(self) -> list:
        """
        Retrieves the selected columns to include in the dataframe.

        Returns:
            list: List of selected column names.
        """
        columns = [col['SourceColumnName'] for col in self.column_info()]

        # Additional columns to include
        columns += ['BatchId', 'InsertDate', 'ModifiedDateTime']
        return columns

    def schema_hints(self) -> str:
        """
        Generates schema hints for cloud_files.

        Returns:
            str: Comma-separated list of column hints.
        """
        hints = ", ".join(f"{col['SourceColumnName']} {col['DeltaDataType']}" for col in self.column_info())
        return hints

    def check_extra_columns_and_add_to_delta_config_tables(self, batch_df, batch_id) -> str:
        """
        Description:
            Check extra columns in the data file and delta table
        Return:
            extra_column_names -> Names of extra column names
        """
        try:
            # Logging
            log_entry_type = 'AUTO_LOADER'
            log_entry_des = 'Checking extra columns.'
            error_des = ''
            status_id = 1
            update_insert_log_control(self.header_id, self.source_file_path, batch_id, log_entry_type, log_entry_des, error_des, status_id, self.log_table, self.control_table)

            # Extracting target table name and staging columns data types
            target_table = self.table_name
            staging_cols = dict(batch_df.drop('_rescued_data', 'source_file_path').dtypes)

            # Extracting main columns data types from delta table configuration
            main_cols = {col_info['SourceColumnName']: col_info['DeltaDataType'] for col_info in self.column_info()}
            main_cols.update({'BatchId': 'integer', 'InsertDate': 'date', 'ModifiedDateTime': 'timestamp'})

            # Finding extra columns present in staging but not in main
            extra_columns = set(staging_cols.keys()).difference(main_cols.keys())

            SendGoogleChat = SendGChat(self.webhook_url)

            if extra_columns:
                extra_column_names = ""
                for extra_col in extra_columns:
                    extra_column_names += f"{extra_col}, "
                    
                    # Retrieving next available column order and the name of the last column
                    next_column_order_no = spark.sql(f"SELECT MAX(ColumnOrder) max_column_order FROM {self.column_details_table} WHERE HeaderID = {self.header_id}").collect()[0]['max_column_order'] + 1
                    last_column_name = spark.sql(f"SELECT DeltaColumnName FROM {self.column_details_table} WHERE HeaderID = {self.header_id} AND ColumnOrder = (SELECT MAX(ColumnOrder) FROM {self.column_details_table} WHERE HeaderID = {self.header_id})").collect()[0]['DeltaColumnName']
                    
                    # Adding extra column to column details table and altering delta table
                    add_col_info_q = f""" 
                    INSERT INTO {self.column_details_table}
                    (HeaderID, SourceColumnName, DeltaColumnName, DeltaDataType, ColumnOrder, ZOrder, IsPII, IsCurrent, CreatedDateTime, LastUpdatedDateTime)
                    VALUES
                    ({self.header_id}, '{extra_col}', '{extra_col}', 'string', {next_column_order_no}, 0, 0, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                    """
                    spark.sql(add_col_info_q)

                    alter_table_q = f"ALTER TABLE {self.table_name} ADD COLUMNS ({extra_col} STRING)"
                    spark.sql(alter_table_q)

                    change_column_position_q = f"ALTER TABLE {self.table_name} CHANGE COLUMN {extra_col} AFTER {last_column_name}"
                    spark.sql(change_column_position_q)

                    # Add extra column to PII table if required
                    if self.is_pii == 1:
                        alter_table_q = f"ALTER TABLE {self.pii_target_table} ADD COLUMNS ({extra_col} STRING)"
                        spark.sql(alter_table_q)

                        change_column_position_q = f"ALTER TABLE {self.pii_target_table} CHANGE COLUMN {extra_col} AFTER {last_column_name}"
                        spark.sql(change_column_position_q)

                    # Logging
                    log_entry_type = 'AUTO_LOADER'
                    log_entry_des = 'New column(s) detected and added.'
                    error_des = ''
                    status_id = 1
                    update_insert_log_control(self.header_id, self.source_file_path, batch_id, log_entry_type, log_entry_des, error_des, status_id, self.log_table, self.control_table)

                    # Send message about table creation
                    message = f"<b>ADD_NEW_COLUMNS</b> - New column(s) has been added to {self.table_name}."
                    job_id = spark.sql(f"SELECT JobID FROM {self.control_table} WHERE HeaderID = {self.header_id}").collect()[0][0]
                    job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
                    c_datetime = datetime.now()
                    current_time = c_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    SendGoogleChat.warning_message(target_table, message, current_time)

                return extra_column_names.rstrip(", ")
            else:
                print('No Extra Column(s) detect')
                return False
        except Exception as e:
            # Logging
            log_entry_type = 'AUTO_LOADER'
            log_entry_des = 'Checking extra columns errors.'
            error_des = str(e).replace("'", '"')
            status_id = 3
            update_insert_log_control(self.header_id, self.source_file_path, batch_id, log_entry_type, log_entry_des, error_des, status_id, self.log_table, self.control_table)
            raise Exception(e)
    
    def get_corrupt_data_count(self,df,batch_id) -> int:
        """
        Description:
            Get the corrupt data count
        """
        # Logging
        log_entry_type = 'AUTO_LOADER'
        log_entry_des = 'Bad data checking.'
        error_des = ''
        status_id = 1
        update_insert_log_control(self.header_id,self.source_file_path,batch_id,log_entry_type,log_entry_des,error_des,status_id,self.log_table,self.control_table)

        currupt_count = df.distinct().filter(col('_rescued_data').isNotNull()).count()
        return currupt_count

    def corrupt_data(self,df,batch_id) -> None:
        """
        Description:
            Corrupt data insert into the delta locaton and move the source file in to error location
        """
        (df
        .write
        .format('delta')
        .mode('overwrite')
        .save(f'{self.corrupt_location}'))

        rescued_data = df.distinct().filter(col('_rescued_data').isNotNull()).collect()

        # Logging
        spark.sql(f"UPDATE {self.control_table} SET StatusID = 1, PreviousBatchID = {self.batch_id}, "
                          f"LatestBatchID = {batch_id + 1}, LastUpdateTime = CURRENT_TIMESTAMP() "
                          f"WHERE HeaderID = {self.header_id}")

        SendGoogleChat = SendGChat(self.webhook_url)

        for rescued_data_ in rescued_data:
            dbutils.fs.mv(rescued_data_.source_file_path,f'{self.error_location}{self.error_file_name}')
        
            raise Exception('Bad records')
    
    def generate_error_file_name(self):
        c_datetime = datetime.now()
        current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
        error_file_name = f"error_file_{current_time}.{self.file_format}"
        return error_file_name
    
    def load_data(self,df,overwrite_flag,batch_id) -> None:
        """
        Load data into tables.

        Args:
            input_dataframe (DataFrame): The input data.
            overwrite_flag (int): Flag indicating whether to overwrite existing data (1) or append (0).
            batch_id (int): Batch ID for logging purposes.
        """
        # Logging
        log_entry_type = 'AUTO_LOADER'
        log_entry_des = 'Data Load Start.'
        error_des = ''
        status_id = 1
        update_insert_log_control(self.header_id,self.source_file_path,batch_id,log_entry_type,log_entry_des,error_des,status_id,self.log_table,self.control_table)
        
        try:
            df = df.select(*self.get_selected_columns())
            
            for column_info in self.column_info():
                df = df.withColumnRenamed(column_info['SourceColumnName'],column_info['DeltaColumnName'])

            if self.is_pii == 1:
                get_pii_columns_query = f"""
                        SELECT
                            DeltaColumnName
                        FROM
                            {self.column_details_table}
                        WHERE
                            HeaderID = {self.header_id}
                            AND IsCurrent = 1
                            AND IsPII = 1
                        ORDER BY
                            ColumnOrder ASC
                        """
                
                pii_confs = spark.sql(get_pii_columns_query).collect()

                df_pii = df

                for pii_col in pii_confs:
                    df_pii = df_pii.withColumn(pii_col.DeltaColumnName,lit('xxxxxxx'))
                
                if overwrite_flag == 1:
                    df.write.format('delta').mode("overwrite").saveAsTable(self.table_name)
                    df_pii.write.format("delta").mode("overwrite").saveAsTable(self.pii_target_table)
                else:
                    df.write.format('delta').mode("append").saveAsTable(self.table_name)
                    df_pii.write.format("delta").mode("append").saveAsTable(self.pii_target_table)

                # Optimize
                self.optimize_and_z_order(self.table_name)
                self.optimize_and_z_order(self.pii_target_table)

            else:
                if overwrite_flag == 1:
                    df.write.format('delta').mode("overwrite").saveAsTable(self.table_name)
                else:
                    df.write.format('delta').mode("append").saveAsTable(self.table_name)
                
                # Optimize
                self.optimize_and_z_order(self.table_name)
            
            row_count=df.count()

            # Logging
            log_entry_type = 'ROW_COUNT'
            log_entry_des = f'{row_count}'
            error_des = ''
            status_id = 1
            update_insert_log_control(self.header_id,self.source_file_path,batch_id,log_entry_type,log_entry_des,error_des,status_id,self.log_table,self.control_table)

            # Update control table
            control_update_q = f"""
            UPDATE {self.control_table} SET StatusID = 1, LastUpdateTime = CURRENT_TIMESTAMP(), LatestBatchID = {batch_id} WHERE HeaderID = {self.header_id}
            """
            
            spark.sql(control_update_q)

        except Exception as e:
            # Logging
            log_entry_type = 'AUTO_LOADER'
            log_entry_des = 'Data Load Error.'
            error_des = str(e).replace("'",'"')
            status_id = 3
            update_insert_log_control(self.header_id,self.source_file_path,batch_id + 1,log_entry_type,log_entry_des,error_des,status_id,self.log_table,self.control_table)
            raise Exception(e)    

    def microbatch_process(self, input_batch_dataframe, batch_id, last_batch_id, overwrite_flag) -> None:
        """
        Write data to the delta table (Micro batch process).

        Args:
            input_batch_dataframe (DataFrame): The input batch data.
            batch_id (int): Current batch ID.
            last_batch_id (int): ID of the last processed batch.
            overwrite_flag (int): Flag indicating whether to overwrite existing data (1) or append (0).
        """
        try:
            # Add batch-related columns
            input_batch_dataframe = input_batch_dataframe.withColumn('BatchId', lit(batch_id + 1))
            input_batch_dataframe = input_batch_dataframe.withColumn('InsertDate', current_date())
            input_batch_dataframe = input_batch_dataframe.withColumn('ModifiedDateTime', current_timestamp())

            # Check for extra columns and update delta config tables
            self.check_extra_columns_and_add_to_delta_config_tables(input_batch_dataframe, batch_id)

            # Handle corrupt data
            if self.get_corrupt_data_count(input_batch_dataframe, batch_id) > 0:
                self.corrupt_data(input_batch_dataframe, batch_id)
            else:
                self.load_data(input_batch_dataframe, overwrite_flag, batch_id)
            
            # Update control table
            control_update_q = f"""
            UPDATE {self.control_table} SET StatusID = 1, LastUpdateTime = CURRENT_TIMESTAMP(), LatestBatchID = {batch_id} WHERE HeaderID = {self.header_id}
            """
            
            spark.sql(control_update_q)

        except Exception as e:
            SendGoogleChat = SendGChat(self.webhook_url)
            if 'Bad records' in str(e):
                # Logging
                log_entry_type = 'AUTO_LOADER'
                log_entry_des = 'Bad records found.'
                error_des = f'Please check error location: {self.error_location}{self.error_file_name} and corrupt data location (delta): {self.corrupt_location}'
                status_id = 3
                update_insert_log_control(self.header_id,self.source_file_path,batch_id + 1,log_entry_type,log_entry_des,error_des,status_id,self.log_table,self.control_table)
                
                # Send message about table creation
                error_message = f"<b>CORRUPT_DATA</b> - Corrupt data has been found in {self.table_name}.\n<b>Error File Location:</b> {self.error_location}{self.error_file_name}\n<b>Corrupt Records (Delta format):</b> {self.corrupt_location}"
                job_id = spark.sql(f"SELECT JobID FROM {self.control_table} WHERE HeaderID = {self.header_id}").collect()[0][0]
                job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
                c_datetime = datetime.now()
                current_time = c_datetime.strftime('%Y-%m-%d %H:%m:%S')
                SendGoogleChat.failed_message(self.table_name,error_message,job_url,current_time)

                raise Exception(e)
            else:
                # Handle streaming error
                log_entry_type = 'AUTO_LOADER'
                log_entry_des = 'Streaming Error.'
                error_des = str(e).replace("'", '"')
                status_id = 3
                update_insert_log_control(self.header_id, self.source_file_path, batch_id + 1,
                                        log_entry_type, log_entry_des, error_des, status_id,
                                        self.log_table, self.control_table)

                # Send notification about failure
                error_message = f"<b>AUTO_LOADER</b> - Streaming Failed {self.table_name}"
                job_id = spark.sql(f"SELECT JobID FROM {self.control_table} WHERE HeaderID = {self.header_id}").collect()[0][0]
                job_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{job_id}"
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                SendGoogleChat.failed_message(self.table_name, error_message, job_url, current_time)

                raise Exception(e)

    def optimize_and_z_order(self,table_name) -> None:
        """
        Optimize the specified table using Z-Order.

        Args:
            table_name (str): Name of the table to optimize.
        """
        z_order_columns = ''

        for z_order_col in self.column_info():
            if z_order_col["ZOrder"] == 1:
                col_name = z_order_col["DeltaColumnName"]
                z_order_columns += f"{col_name} ,"

        if z_order_columns == '':
            spark.sql(f"OPTIMIZE {table_name}")
        else:
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_columns[:-1]})")
    
    def stop_batch(self,query):
        """
        Stop the data load process.

        Args:
            query (StreamingQuery): The active streaming query.
        """
        print('Stop query Started..')
        while query.isActive :
            lastet_batch_id = spark.sql(f"SELECT LatestBatchID FROM {self.control_table} WHERE HeaderID = {self.header_id}").collect()[0][0]
            if (query.lastProgress != None and lastet_batch_id > (self.batch_id + self.file_count -1)):
                while not query.status['isDataAvailable']:
                    query.stop()
                    query.awaitTermination()
                    print(f"\nlast active process: {query.lastProgress}")
                    batch_id_latest = query.lastProgress['batchId']
                    spark.sql(f"UPDATE {self.control_table} SET StatusID = 2, PreviousBatchID = {self.batch_id}, "
                          f"LatestBatchID = {batch_id_latest}, LastUpdateTime = CURRENT_TIMESTAMP() "
                          f"WHERE HeaderID = {self.header_id}")

                    # Logging
                    log_entry_type = 'END'
                    log_entry_des = 'Process is over.'
                    error_des = ""
                    status_id = 2
                    update_insert_log_control(self.header_id,self.source_file_path,query.lastProgress['batchId'],log_entry_type,log_entry_des,error_des,status_id,self.log_table,self.control_table)

                    print('Autoloader process is over.')
                    break