# Introduction 
This process will be used to insert raw data into the delta tables using the customised Databricks Auto Loader process. This is a configurable process.

# Getting Started
Follow the below steps to use this tool:
1. Add configuration table information inside the `config` folder.
2. Add all the python libraries inside the `requirements.txt` file.
3. Add the main configurations into main header configuration table. (Ex: Source info, Target info, PII info etc.)
4. Add the column configurations into the column configuration table.

# Power BI Monitoring Report for active Auto Loader Headers

This report was pulled together to monitor any autoloader jobs added for the Unity project. It reads data from the autoloader catalog in databricks and collects its data from 

Catalog : autoloader

1. column_config
2. header_config
3. logs
4. status
