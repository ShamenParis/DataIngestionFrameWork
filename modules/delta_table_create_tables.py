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


class DeltaTableOperations:
    """
    Creator:
    --------
    Created by: Shamen Paris
    Email: shamen_paris@next.co.uk
    
    Manages Delta tables for data operations.

    Args:
        target_table (str): Name of the target Delta table.
        external_location (str): External location for the table.
        column_details_table (str): Name of the table containing column details.
        header_id (int): Header ID for reference.
    """
    def __init__(self,target_table,external_location,column_details_table,header_id) -> None:
        self.target_table = target_table
        self.external_location = external_location
        self.column_details_table = column_details_table
        self.header_id = header_id
        
    def column_object(self,col_config) -> list:
        """
        Get column information related to each process.

        Args:
            col_config (list): Column configurations from the column details table.

        Returns:
            list: List of dictionaries containing column information.
        """
        col_list = list()
        try:
            for conf in col_config:
                col_list.append(
                    dict(
                        ColumnOrder     = conf["ColumnOrder"],
                        DeltaColumnName = conf["DeltaColumnName"],
                        DeltaDataType   = conf["DeltaDataType"],
                        ZOrder          = conf["ZOrder"]
                    )
                )
            return col_list
        except Exception as e:
            raise Exception(e)
    
    def get_column_info(self) -> list:
        """
        Get column objects containing ColumnOrder, DeltaColumnName, DeltaDataType, and ZOrder.

        Returns:
            list: List of dictionaries representing column information.
        """
        col_query = f"""
                    SELECT
                        ColumnOrder,
                        DeltaColumnName,
                        DeltaDataType,
                        ZOrder
                    FROM
                        {self.column_details_table}
                    WHERE
                        HeaderID = {self.header_id}
                        AND IsCurrent = 1
                    ORDER BY
                        ColumnOrder ASC
                    """
        col_configurations = spark.sql(col_query).collect()

        columnobject = self.column_object(col_configurations)
        return columnobject
     
    def create_table(self) -> None:
        """
        Create a new Delta table using configuration details if the table does not exist.
        """
        try:
            external_location = f"LOCATION '{self.external_location}'"
            create_table_q_1 = f"CREATE OR REPLACE TABLE {self.target_table} ("
            create_table_q_2 = ""

            for cdetails in self.get_column_info():
                col_name = cdetails["DeltaColumnName"]
                data_type = cdetails["DeltaDataType"]
                create_table_q_2 += f"{col_name} {data_type} ,"
            
            create_table_q_2 = create_table_q_2[:-1]

            create_table_q = (create_table_q_1 + create_table_q_2 + ",BatchId INT,InsertDate DATE,ModifiedDateTime TIMESTAMP) USING DELTA " 
                            + external_location 
                            + " TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true)")
            print(f"CREATE SCRIPT: \n{create_table_q}")
            spark.sql(create_table_q)
            print(f'{self.target_table} table has been created.')
        except Exception as e:
            raise Exception(e)