import os
import datetime
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


DB_CONN = "db"
S3_CONN = "s3"
S3_BUCKET = Variable.get("s3_bucket")
TAXI_COLOR = Variable.get("taxi_color")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
TABLE_COLS = Variable.get("taxi_columns", deserialize_json=True)
DB_URL = Variable.get("db_con")

@dag(
    start_date=datetime.datetime(2019, 1, 1), 
    schedule="0 0 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["backfill"]
)
def ny_taxi_el_backfill():
    
    file_name = f"{TAXI_COLOR}_tripdata_{{{{ logical_date.strftime(\'%Y-%m\') }}}}.csv"
    file_url = f"{BASE_URL}/{TAXI_COLOR}/{file_name}.gz"
    downloaded_file_path = f"{AIRFLOW_HOME}/{file_name}.gz"
    uncompressed_file_path = f"{AIRFLOW_HOME}/{file_name}"
    insert_table_name = f"{TAXI_COLOR}_tripdata_staging"
    insert_table_cols = TABLE_COLS[TAXI_COLOR]

    
    @task.bash
    def download_dataset(file_url, output_path):
        return f"wget -O {output_path} {file_url}"
    

    @task.bash
    def unzip_file(file_path):
        return f"gunzip -f {file_path}"

    
    @task
    def update_filename_env(key, val):
        Variable.set(key=key, value=val)
    

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        aws_conn_id=S3_CONN,
        filename=uncompressed_file_path,
        dest_key=f"{TAXI_COLOR}/{file_name}",
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    @task.branch
    def check_taxi_color(taxi_color):
        return taxi_color
    

    @task_group
    def green():
        
        create_green_tripdata = SQLExecuteQueryOperator(
            task_id="create_green_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/green_main_create.sql"
        )

        create_green_staging_tripdata = SQLExecuteQueryOperator(
            task_id="create_green_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/green_stg_create.sql"
        )

        truncate_green_staging_tripdata = SQLExecuteQueryOperator(
            task_id="truncate_green_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/green_stg_truncate.sql"
        )

        @task.bash(env={"db_con": DB_URL, "table_columns": insert_table_cols, "table_name": insert_table_name, "file_path": uncompressed_file_path})
        def insert_green_stg_tripdata():
            return "helpers/scripts/local_to_postgres.sh"
        
        update_green_staging_tripdata = SQLExecuteQueryOperator(
            task_id="update_green_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/green_stg_update.sql"
        )

        merge_green_staging_tripdata = SQLExecuteQueryOperator(
            task_id="merge_green_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/green_main_stg_merge.sql"
        )

        create_green_tripdata >> create_green_staging_tripdata >> truncate_green_staging_tripdata >> insert_green_stg_tripdata() >> update_green_staging_tripdata >> merge_green_staging_tripdata
    

    @task_group
    def yellow():
        
        create_yellow_tripdata = SQLExecuteQueryOperator(
            task_id="create_yellow_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/yellow_main_create.sql"
        )

        create_yellow_staging_tripdata = SQLExecuteQueryOperator(
            task_id="create_yellow_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/yellow_stg_create.sql"
        )

        truncate_yellow_staging_tripdata = SQLExecuteQueryOperator(
            task_id="truncate_yellow_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/yellow_stg_truncate.sql"
        )

        @task.bash(env={"db_con": DB_URL, "table_columns": insert_table_cols, "table_name": insert_table_name, "file_path": uncompressed_file_path})
        def insert_yellow_stg_tripdata():
            return "helpers/scripts/local_to_postgres.sh"
        
        update_yellow_staging_tripdata = SQLExecuteQueryOperator(
            task_id="update_yellow_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/yellow_stg_update.sql"
        )

        merge_yellow_staging_tripdata = SQLExecuteQueryOperator(
            task_id="merge_yellow_staging_tripdata",
            conn_id=DB_CONN,
            sql = "helpers/sql/yellow_main_stg_merge.sql"
        )

        create_yellow_tripdata >> create_yellow_staging_tripdata >> truncate_yellow_staging_tripdata >> insert_yellow_stg_tripdata() >> update_yellow_staging_tripdata >> merge_yellow_staging_tripdata

    @task.bash(trigger_rule="none_failed_min_one_success")
    def clean_up(file_name):
        return f"rm {file_name}"

    download_dataset(file_url, downloaded_file_path) >> unzip_file(downloaded_file_path) >> update_filename_env("file_name", file_name) >> upload_to_s3 >> check_taxi_color(TAXI_COLOR) >> [green(), yellow()] >> clean_up(uncompressed_file_path)

    # @task.bash(env={"db_conn": DB_URL})
    # def test_pg_bash():
    #     # return "echo $db_conn"
    #     return "scripts/test.sh"
    
    # test_pg_bash()

    # @task.bash(env={"db_con": DB_URL, "table_columns": insert_table_cols, "table_name": insert_table_name, "file_path": uncompressed_file_path})
    # def test_print():
    #     return "scripts/local_to_postgres.sh"
    
    # test_print()

ny_taxi_el_backfill()