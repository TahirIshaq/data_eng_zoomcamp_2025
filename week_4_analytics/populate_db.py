import os
import time
import json
import fastjsonschema


base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{taxi_year}-{taxi_month}.csv.gz"


def get_args():
    """Returns db url"""
    username = os.getenv("PG_USERNAME")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    db_name = os.getenv("PG_DB_NAME")
    schema = os.getenv("PG_SCHEMA", "public")

    db_url = f"postgresql://{username}:{password}@{host}:{port}/{db_name}?options=-csearch_path%3D{schema}"
    return db_url


def download_file(taxi_type, taxi_year, taxi_month, file_path="/app"):
    """Download the file and return the file name"""
    url = base_url.format(taxi_type=taxi_type, taxi_year=taxi_year, taxi_month=str(taxi_month).zfill(2))
    file_name = url.split("/")[-1]
    file_path = os.path.join(file_path, file_name)
    os.system(f"wget -O {file_path} {url}")
    return file_path


def unzip_file(zip_file_path):
    """Returns uncompressed file path"""
    os.system(f"gunzip -f {zip_file_path}")
    unzip_file_path = zip_file_path[:zip_file_path.find(".gz")]
    return unzip_file_path


def remove_file(file_path):
    """Remove the file"""
    os.system(f"rm -rf {file_path}*")


def update_db(db_url, file_name, table_name, ):
    """Populate table"""
    os.system(f"psql {db_url} -c \"COPY {table_name}_taxi FROM STDIN (FORMAT CSV, DELIMITER ',', HEADER)\" < {file_name}")


def main():
    taxi_data = os.getenv("TAXI_DATA")
    taxi_data_schema = os.getenv("TAXI_DATA_SCHEMA")
    try:
        taxi_data = json.loads(taxi_data)
        with open(taxi_data_schema) as f:
            taxi_schema = json.load(f)
            point_validator = fastjsonschema.compile(taxi_schema)
            point_validator(taxi_data)
    except Exception as err:
        print("Invalid taxi json schema")
        raise err
    
    db_url = get_args()
    
    all_tables_start_time = time.time()
    
    for taxi_details in taxi_data:
        taxi_type = taxi_details["taxi_type"]
        taxi_year = taxi_details["taxi_year"]
        for taxi_month in taxi_details["taxi_months"]:
            print(f"Downloading file {taxi_type}, {taxi_year}, {taxi_month}")
            zip_file_path = download_file(taxi_type=taxi_type, taxi_year=taxi_year, taxi_month=taxi_month)
            print(f"Download finished {taxi_type}, {taxi_year}, {taxi_month}")
            file_path = unzip_file(zip_file_path)
            table_start_time = time.time()
            update_db(db_url=db_url, file_name=file_path, table_name=taxi_type)
            table_end_time = time.time() - table_start_time
            file_name = file_path.split("/")[-1]
            print(f"File {file_name} inserted in {taxi_type} in {table_end_time:.2f} seconds")
            remove_file(file_path=file_path)

    all_tables_end_time = time.time() - all_tables_start_time
    print(f"All data inserted in {all_tables_end_time:.2f} seconds")

                
if __name__ == "__main__":
    main()
