import json
import cron_descriptor
from modules.validate_configurations import *
from pyspark.sql.functions import col
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *
from databricks.sdk.service import iam
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create_workflow_jobs').getOrCreate()
dbutils = DBUtils(spark)

# Prepare Configurations to create jobs
class CreateWorkFlowJobs(ValidateConfig):
    """
    Creator:
    --------
    Created by: Shamen Paris
    Email: shamen_paris@next.co.uk

    Description:
    A class to create, reset, and manage workflow jobs in Databricks.
    """
    def __init__(self,environment,files_path) -> None:
        """
        Initialize the CreateWorkFlowJobs class with the given environment.

        Parameters:
        environment (str): The environment in which the jobs are to be created.
        """
        super().__init__(files_path)
        self.environment = environment

    def get_tables(self) -> None:
        conf_json = open('./config/schemas_tables.json',"r")
        conf_sc_tbl = json.load(conf_json)

        # Header Table Info
        self.header_table                = f"{conf_sc_tbl['header_table']['schema']}.{conf_sc_tbl['header_table']['table_name']}"
        # Control Table Info
        self.control_table               = f"{conf_sc_tbl['control_table']['schema']}.{conf_sc_tbl['control_table']['table_name']}"
        # Job Config Table Info
        self.job_config_table            = f"{conf_sc_tbl['job_table']['schema']}.{conf_sc_tbl['job_table']['table_name']}"

        conf_json.close()
    
    def get_main_job_configurations(self) -> None:
        """
        Load main job configurations from the JSON file and set instance variables for job settings.
        """
        conf_json = open(f'./config/{self.environment}/job_config/job_configurations.json',"r")
        conf_job = json.load(conf_json)

        # Main Job Configurations

        self.source                         = conf_job['source']
        self.notebook_path                  = conf_job['notebook_path']
        self.pool_name                      = conf_job['pool_name']
        self.catalog_name                   = conf_job['catalog_name']
        self.cluster_prefix_name            = conf_job['cluster_name']
        self.can_manage_user_group_name     = conf_job['can_manage_user_group_name']
        self.can_manage_user_name           = conf_job['can_manage_user_name']
        self.can_manage_spn                 = conf_job['can_manage_spn']
        self.can_manage_run_user_group_name = conf_job['can_manage_run_user_group_name']
        self.can_manage_run_user_name       = conf_job['can_manage_run_user_name']
        self.can_manage_run_spn             = conf_job['can_manage_run_spn']

        conf_json.close()

    def get_configurations(self,header_id) -> list:
        """
        Retrieve job configurations for a given header ID from the database.

        Parameters:
        header_id (int): The header ID for which the configurations are to be retrieved.

        Returns:
        dict: A dictionary containing job configurations.
        """
        self.get_tables()
        config = f"""
        SELECT 
            H.HeaderID,
            H.SourceContainer,
            H.SourceFilePath,
            H.SourceFileFormat,
            H.SourceFileHeader,
            H.SourceFileDelimiter,
            H.DeltaTableSchema,
            H.DeltaTableName,
            H.IsPII,
            H.PIISchema,
            H.PIITableName,
            H.OverWriteFlag,
            H.BatchFileCount,
            H.ContinuousRunFlag,
            H.IsCurrent,
            C.JobID,
            J.Alert,
            split(J.Emails,',') as Emails,
            J.WarningDuration,
            J.TimeOut,
            J.Retries,
            J.ClusterMaxWorkers,
            J.SparkConf,
            J.CronSyntax
        FROM 
            {self.header_table} H
        INNER JOIN
            {self.job_config_table} J ON H.HeaderID = J.HeaderID
        INNER JOIN
            {self.control_table} C ON H.HeaderID = C.HeaderID
        WHERE
            H.HeaderID = {header_id}
        """

        list_of_rows = spark.sql(config).collect()
        list_of_dicts = [row.asDict() for row in list_of_rows]
        return list_of_dicts[0]

    def get_parameters(self,header_id) -> list:
        """
        Retrieve parameters for a given header ID to pass to the AL Notebook.

        Parameters:
        header_id (int): The header ID for which the parameters are to be retrieved.

        Returns:
        list: A list containing a dictionary of parameters for the AL Notebook.
        """
        parameters = list()
        config = self.get_configurations(header_id)
        header_id = config['HeaderID']
        file_format = config['SourceFileFormat']
        source_file_header = config['SourceFileHeader']
        source_file_delimiter = config['SourceFileDelimiter']
        source_file_path = spark.conf.get(config['SourceContainer']) + "/" + config['SourceFilePath']
        schema_location = spark.conf.get('conf.dbktables') + "/al_schemalocation/" + config['DeltaTableSchema'] + "/" + config['DeltaTableName'] + "/"
        checkpoint_path = spark.conf.get('conf.dbktables') + "/al_checkpoint/" + config['DeltaTableSchema'] + "/" + config['DeltaTableName'] + "/"
        staging_path = spark.conf.get('conf.dbktables') + "/al_staging/" + config['DeltaTableSchema'] + "/" + config['DeltaTableName'] + "/"
        target_table_schema = config['DeltaTableSchema']
        target_table_name = config['DeltaTableName']
        is_pii = config['IsPII']
        pii_table_schema = config['PIISchema']
        pii_table_name = config['PIITableName']
        overwrite_flag = config['OverWriteFlag']
        file_count = config['BatchFileCount']
        archive_path = spark.conf.get('conf.dbktables') + "/al_archive/" + config['DeltaTableSchema'] + "/" + config['DeltaTableName'] + "/"
        corrupt_delta_location = spark.conf.get('conf.dbktables') + "/al_corrupt/" + config['DeltaTableSchema'] + "/" + config['DeltaTableName'] + "/"
        error_file_location = spark.conf.get('conf.dbktables') + "/al_error_file/" + config['DeltaTableSchema'] + "/" + config['DeltaTableName'] + "/"
        continue_run_flag = config['ContinuousRunFlag']
        
        parameters.append(dict(
            job_id = "{{job.id}}",
            header_id = header_id,
            file_format = file_format,
            source_file_header = source_file_header,
            source_file_delimiter = source_file_delimiter,
            source_file_path = source_file_path,
            schema_location = schema_location,
            checkpoint_path = checkpoint_path,
            staging_path = staging_path,
            target_table_schema = target_table_schema,
            target_table_name = target_table_name,
            is_pii = is_pii,
            pii_table_schema = pii_table_schema,
            pii_table_name = pii_table_name,
            overwrite_flag = overwrite_flag,
            file_count = file_count,
            archive_path = archive_path,
            corrupt_delta_location = corrupt_delta_location,
            error_file_location = error_file_location,
            continue_run_flag = continue_run_flag
            ))
        return parameters[0]
    

    def create_and_reset_jobs(self) -> None:
        """
        Create and reset jobs based on the configurations and manage job permissions.
        """
        self.get_main_job_configurations()
        self.get_tables()

        header_ids = spark.sql(f'SELECT HeaderID FROM {self.header_table} ORDER BY HeaderID ASC').collect()

        def add_permissions(names, key, level):
            for name in names.split(','):
                if name:
                    permissions_settings.append({key: name, 'permission_level': level})

        permissions_settings = []

        if any([self.can_manage_user_group_name, self.can_manage_user_name, self.can_manage_spn]):
            add_permissions(self.can_manage_user_group_name, 'group_name', iam.PermissionLevel.CAN_MANAGE)
            add_permissions(self.can_manage_user_name, 'user_name', iam.PermissionLevel.CAN_MANAGE)
            add_permissions(self.can_manage_spn, 'service_principal_name', iam.PermissionLevel.CAN_MANAGE)

        if any([self.can_manage_run_user_group_name, self.can_manage_run_user_name, self.can_manage_run_spn]):
            add_permissions(self.can_manage_run_user_group_name, 'group_name', iam.PermissionLevel.CAN_MANAGE_RUN)
            add_permissions(self.can_manage_run_user_name, 'user_name', iam.PermissionLevel.CAN_MANAGE_RUN)
            add_permissions(self.can_manage_run_spn, 'service_principal_name', iam.PermissionLevel.CAN_MANAGE_RUN)

        if not permissions_settings:
            raise AssertionError('Permissions are not defined.')

        for header_id_ in header_ids:
            header_id = header_id_.HeaderID

            w = WorkspaceClient()

            config = self.get_configurations(header_id)

            job_id = config['JobID']
            job_name  = f"{config['DeltaTableSchema']}.{config['DeltaTableName']}"
            job_name_ = f"{config['DeltaTableSchema']}_{config['DeltaTableName']}"
            cluster_name = f'AL-{job_name_}-{time.time_ns()}'
            description = f"{config['DeltaTableSchema']}.{config['DeltaTableName']} data ingestion using the Databricks Auto Loader"
            task_key = f"{config['DeltaTableSchema']}_{config['DeltaTableName']}"
            retries  = config['Retries']
            warning_duration  = config['WarningDuration']
            time_out = config['TimeOut']
            emails  = config['Emails']
            max_workers = config['ClusterMaxWorkers']
            spark_conf = json.loads(config['SparkConf'])
            spark_conf['spark.databricks.sql.initial.catalog.name'] = self.catalog_name
            schedule_corn_syntax = config['CronSyntax']
            is_current = config['IsCurrent']
            latest = w.clusters.select_spark_version(latest=True, long_term_support=True)

            pool_id = next((pool.instance_pool_id for pool in w.instance_pools.list() if pool.instance_pool_name == self.pool_name), '')

            # Reset jobs if they are current
            if job_id:
                j = w.jobs.reset(
                    job_id=job_id,
                    new_settings=JobSettings(
                        name=job_name,
                        timeout_seconds=time_out,
                        health = JobsHealthRules(
                        rules=[
                            JobsHealthRule(
                            metric= JobsHealthMetric.RUN_DURATION_SECONDS,
                            op=JobsHealthOperator.GREATER_THAN,
                            value= warning_duration
                            )
                            ]
                        ),
                        schedule=CronSchedule(
                            quartz_cron_expression= schedule_corn_syntax,
                            timezone_id= "Europe/London",
                            pause_status= PauseStatus.PAUSED if is_current == 0 else PauseStatus.UNPAUSED
                        ),
                        email_notifications=JobEmailNotifications(
                            on_failure = emails,
                            on_duration_warning_threshold_exceeded=emails,
                            no_alert_for_skipped_runs = True
                        ),
                        job_clusters = [
                        JobCluster(
                            job_cluster_key = cluster_name,
                            new_cluster = compute.ClusterSpec(
                            spark_version = latest,
                            instance_pool_id =pool_id,
                            autoscale=compute.AutoScale(
                                min_workers=1,
                                max_workers=max_workers
                            ),
                            spark_conf = spark_conf
                            )
                        )
                        ],
                        tasks = [
                        Task(
                            description = description,
                            job_cluster_key = cluster_name,
                            max_retries = retries,
                            min_retry_interval_millis=5000,
                            retry_on_timeout=True,
                            timeout_seconds=0,
                            notebook_task = NotebookTask(
                                base_parameters = self.get_parameters(header_id),
                                notebook_path = self.notebook_path,
                                source = Source(self.source)
                            ),
                            task_key = task_key
                        )
                        ]
                    )
                )

                for permissions_setting in permissions_settings:
                    w.jobs.update_permissions(job_id=job_id,access_control_list=[
                        iam.AccessControlRequest(
                            **permissions_setting
                        )
                        ])

                print(f"Job ID: {job_id} reset process completed\n")
            
            # Create new jobs if job ID does not exist and pool ID is available
            elif not job_id:
                j = w.jobs.create(
                    name = job_name,
                    timeout_seconds=time_out,
                    access_control_list = [iam.AccessControlRequest(**permissions_setting) for permissions_setting in permissions_settings],
                    job_clusters = [
                    JobCluster(
                        job_cluster_key = cluster_name,
                        new_cluster = compute.ClusterSpec(
                        spark_version= latest,
                        instance_pool_id=pool_id,
                        autoscale=compute.AutoScale(
                            min_workers=1,
                            max_workers=max_workers
                        ),
                        spark_conf = spark_conf
                        )
                    )
                    ],
                    tasks = [
                        Task(
                        description = description,
                        job_cluster_key = cluster_name,
                        max_retries = retries,
                        min_retry_interval_millis=5000,
                        retry_on_timeout=True,
                        timeout_seconds=0,
                        notebook_task = NotebookTask(
                            base_parameters = self.get_parameters(header_id),
                            notebook_path = self.notebook_path,
                            source = Source(self.source)
                        ),
                        task_key = task_key
                        )
                    ],
                    schedule= CronSchedule(
                        quartz_cron_expression= schedule_corn_syntax,
                        timezone_id= "Europe/London",
                        pause_status= PauseStatus.PAUSED if is_current == 0 else PauseStatus.UNPAUSED
                    ),
                    email_notifications=JobEmailNotifications(
                        on_failure = emails,
                        on_duration_warning_threshold_exceeded=emails,
                        no_alert_for_skipped_runs = True
                    ),
                    health = JobsHealthRules(
                    rules=[
                        JobsHealthRule(
                        metric= JobsHealthMetric.RUN_DURATION_SECONDS,
                        op=JobsHealthOperator.GREATER_THAN,
                        value= warning_duration
                        )
                    ]
                    )
                )

                spark.sql(f"UPDATE {self.control_table} SET JobID = {j.job_id} WHERE HeaderID = {header_id}")
                print(f"View the job at https://{spark.conf.get('spark.databricks.workspaceUrl')}/#job/{j.job_id}\n")

            elif not pool_id:
                raise AssertionError(f'{self.pool_name} is not available')

            else:
                raise AssertionError('Something went wrong')