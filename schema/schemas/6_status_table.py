# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Status Table
# MAGIC Create the status cofiguration table and alter the column if needed
# MAGIC
# MAGIC #### Note
# MAGIC If you add a new column, please ensure that all previous rows are updated manually.
# MAGIC  
# MAGIC #### History
# MAGIC  
# MAGIC | Date       | By  | Reason           |
# MAGIC |------------|-----|------------------|
# MAGIC | 2024-03-18 | Shamen Paris | Status Table Creation. |

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os
import sys
import json

# COMMAND ----------

# DBTITLE 1,Get external locations
# MAGIC %run /root/BusinessIntelligenceSystems/bissparkshared/Scripts/Global.Configs

# COMMAND ----------

# DBTITLE 1,Setup the path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f"/Workspace/{repo_root.replace('/notebooks','').replace('schema','')}")
%pwd

# COMMAND ----------

# DBTITLE 1,Table Names
conf_json = open('./config/schemas_tables.json',"r")
conf_sc_tbl = json.load(conf_json)

# Status Table Info
status_schema                       = f"{conf_sc_tbl['status']['schema']}"
status                              = f"{conf_sc_tbl['status']['table_name']}"

print(f'{status_schema}.{status}')

conf_json.close()

# COMMAND ----------

# DBTITLE 1,External Locations
dbktable = spark.conf.get('conf.dbktables')

# COMMAND ----------

# DBTITLE 1,Libraries
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Schema definition
"""
Add new fileds if needed
"""
# Define the schema based on the provided information
schema = StructType([
    StructField('StatusID', LongType(), False),
    StructField('StatusDescription', StringType(), False)
])

# Create an empty DataFrame with the defined schema
df = spark.createDataFrame([], schema)

# COMMAND ----------

# DBTITLE 1,Create main table
# Path to the Delta table
status_table_external_location = f"{dbktable}/{status_schema}/{status}"

# Table name
status_table_name = f'{status_schema}.{status}'

# Check if the Delta table already exists
is_delta_table = DeltaTable.isDeltaTable(spark,status_table_external_location)

if is_delta_table:
    # Load the existing Delta table
    deltaTable = DeltaTable.forPath(spark, status_table_external_location)

    # Get the current schema of the Delta table
    current_schema = deltaTable.toDF().schema

    # Get the names of the fields in the current schema
    current_field_names = [field.name for field in current_schema.fields]

    # Filter the schema to only include fields that are not already in the current schema
    new_fields = [field for field in schema.fields if field.name not in current_field_names]

    # If there are new fields, add them to the current schema
    if new_fields:
        # Create a new schema that includes the new fields
        updated_schema = StructType(current_schema.fields + new_fields)

        # Register the Delta table in the Databricks Unity Catalog
        for field in new_fields:
            # Generate ALTER TABLE command
            alter_cmd = f"ALTER TABLE {status_table_name} ADD COLUMN {field.name} {field.dataType.simpleString()}"
            
            # Execute ALTER TABLE command
            spark.sql(alter_cmd)

        print(f'Added new column(s) to {status_table_name}')
else:
    # Create a new Delta table with the schema
    spark.createDataFrame([], schema).write.format("delta").option("mergeSchema", "true").option("delta.enableChangeDataFeed", "true").save(status_table_external_location)

    # Register the Delta table in the Databricks Unity Catalog
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {status_table_name}
        USING DELTA
        LOCATION '{status_table_external_location}'
    """)

    print(f'{status_table_name} table has been created.')

# Arrange columns
columns = []
for col in schema:
    columns.append(col.name)

for i in range(len(columns) - 1):
    change_column_position_q = f"ALTER TABLE {status_table_name} CHANGE COLUMN {columns[i + 1]} AFTER {columns[i]};"
    spark.sql(change_column_position_q)
