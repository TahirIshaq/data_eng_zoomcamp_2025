import os
import time
import pandas as pd
import df_col_dtypes
import sqlalchemy


def get_args():
    """
    Returns the database server configuration
    """
    db_args = dict()
    db_args["username"] = os.getenv("PG_USERNAME")
    db_args["password"] = os.getenv("PG_PASSWORD")
    db_args["host"] = os.getenv("PG_HOST")
    db_args["port"] = os.getenv("PG_PORT")
    db_args["db_name"] = os.getenv("PG_DB_NAME")
    db_args["urls"] = os.getenv("URL")

    return db_args


def get_file_name(url):
    """Returns the file name of the URL"""
    file_full_name = os.path.basename(url)
    file_name = file_full_name.split(".")[0]
    
    return file_full_name, file_name


def get_df_dtypes(table_name):
    """Return df col dtypes"""
    datetime_col, other_col = None, None
    if "green_tripdata" in table_name:
        datetime_col = df_col_dtypes.green_taxi_datetime
        other_col = df_col_dtypes.green_taxi_data_types
    elif "yellow_tripdata" in table_name:
        datetime_col = df_col_dtypes.yellow_taxi_datetime
        other_col = df_col_dtypes.yellow_taxi_data_types
    elif "taxi_zone" in table_name:
        other_col = df_col_dtypes.taxi_zone

    return datetime_col, other_col


def main():
    """The main function"""
    args = get_args()
    username = args["username"]
    password = args["password"]
    host = args["host"]
    port = args["port"]
    db_name = args["db_name"]
    urls = args["urls"].split(",")

    db_url = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
    print(db_url)
    
    for url in urls:
        os.system(f"curl -LO {url}")
        file_name, table_name = get_file_name(url)
        datetime_col, other_col = get_df_dtypes(table_name)
        df = pd.read_csv(file_name, parse_dates=datetime_col, dtype=other_col, iterator=True, chunksize=100000)
        create_table = True
        with sqlalchemy.create_engine(db_url).connect() as engine:
            for data in df:
                start_time = time.time()
                if create_table:
                    create_table = False
                    data.head(n=0).to_sql(con=engine, name=table_name, if_exists="replace")
                data.to_sql(con=engine, name=table_name, if_exists="append")
                end_time = time.time() - start_time
                print(f"{len(data)} rows inserted in Table {table_name} in {end_time:.4f} seconds")
        print(f"Loading of Table {table_name} completed in {end_time:.4f} seconds")


if __name__ == "__main__":
    main()