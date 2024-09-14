import json
import os
import glob
import pandas as pd
import cron_descriptor
from jsonschema import validate, ValidationError, SchemaError
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('availability_check').getOrCreate()
dbutils = DBUtils(spark)

class ValidateConfig:
    """
    Creator:
    --------
    Created by: Shamen Paris
    Email: shamen_paris@next.co.uk
    
    Description:
        Get all the configurations from json files and validate the records
    Parameter:
        files_path
    """
    def __init__(self,files_path):
        self.files_path = files_path
    
    def validate_json_file(self):
        try:
            json_files = glob.glob(f'{self.files_path}/*.json')
            with open('./config/config_template.json','r') as schema_file:
                schema = json.load(schema_file)
            
            for file_path in json_files:
                with open(file_path,'r') as file:
                    file_name = os.path.basename(file_path)
                    data = json.load(file)
                    validate(instance=data, schema=schema)
                    print(f"JSON data is valid ({file_name}).")
        except ValidationError as e:
            print(f"JSON data is invalid. Validation error ({file_name}):", e.message)
            raise
        except SchemaError as e:
            print(f"Invalid JSON schema. Schema error ({file_name}):", e.message)
            raise
    
    def get_cron_description(self,cron_string):
        try:
            return cron_descriptor.get_description(cron_string)
        except Exception as e:
            return str(e)
    
    def get_header_config(self) -> DataFrame:
        json_files = glob.glob(os.path.join(self.files_path, '*.json'))
        dataframes = []
        for json_file in json_files:
            with open(json_file) as file:
                file_name = os.path.basename(json_file)
                data = json.load(file)
                data.pop('JobConfig', None)
                data.pop('Columns', None)
                df = pd.DataFrame([data])
                df['ConfigFile'] = file_name
                dataframes.append(df)

        all_data_df = pd.concat(dataframes, ignore_index=True)
        spark_df = spark.createDataFrame(all_data_df)
        return spark_df
    
    def get_job_config(self):
        json_files = glob.glob(os.path.join(self.files_path, '*.json'))
        get_cron_description_udf = udf(self.get_cron_description, StringType())
        dataframes = []
        for json_file in json_files:
            with open(json_file) as file:
                data = json.load(file)
                header_id = data.get('HeaderID')
                job_config_data = data.get('JobConfig', {})
                df = pd.DataFrame([job_config_data])
                df['HeaderID'] = header_id
                df['CronSyntaxDescription'] = df['CronSyntax'].apply(self.get_cron_description)
                cols = ['HeaderID'] + [col for col in df.columns if col not in ('HeaderID', 'CronSyntaxDescription', 'CronSyntax')] + ['CronSyntax', 'CronSyntaxDescription']
                df = df[cols]
                dataframes.append(df)

        all_data_df = pd.concat(dataframes, ignore_index=True)
        spark_df = spark.createDataFrame(all_data_df)
        return spark_df
    
    def get_column_config(self) -> DataFrame:
        json_files = glob.glob(os.path.join(self.files_path, '*.json'))
        dataframes = []
        for json_file in json_files:
            with open(json_file) as file:
                data = json.load(file)
                header_id = data.get('HeaderID')
                columns_data = data.get('Columns', [])
                column_properties = []
                for column in columns_data:
                    column_properties.append({
                        'HeaderID': header_id,
                        'SourceColumnName': column['SourceColumnName'],
                        'DeltaColumnName': column['DeltaColumnName'],
                        'DeltaDataType': column['DeltaDataType'],
                        'ColumnOrder': column['ColumnOrder'],
                        'ZOrder': column['ZOrder'],
                        'IsPII': column['IsPII']
                    })
                df = pd.DataFrame(column_properties)
                dataframes.append(df)

        all_data_df = pd.concat(dataframes, ignore_index=True)
        spark_df = spark.createDataFrame(all_data_df)
        spark_df = spark_df.withColumn("DeltaDataType", lower(spark_df["DeltaDataType"]))
        return spark_df