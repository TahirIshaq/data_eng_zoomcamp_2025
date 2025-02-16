import os
import dlt
import psycopg2
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

username = os.getenv("PG_USERNAME")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
db_name = os.getenv("PG_DB_NAME")
schema = os.getenv("PG_DB_SCHEMA")

db_url = f"postgresql://{username}:{password}@{host}:{port}/{db_name}?options=-csearch_path%3D{schema}"

# Source setup
@dlt.resource(name="rides", write_disposition="replace")
def ny_taxi():
    client = RESTClient(
        base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator = PageNumberPaginator(
            base_page = 1,
            total_path = None
        )
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


# Pipeline with destination
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination=dlt.destinations.postgres(db_url),
    dataset_name=schema
)

load_info = pipeline.run(ny_taxi)

print(f"DLT version: {dlt.__version__}")

query1 = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'ny_taxi_data';"
query2 = "SELECT COUNT(*) AS total_records FROM rides;"
query3 = """
    SELECT AVG(EXTRACT(EPOCH FROM (trip_dropoff_date_time - trip_pickup_date_time))) / 60 AS minute_difference
    FROM rides;
   """
queries = [query1, query2, query3]
with psycopg2.connect(db_url).cursor() as curs:
    for idx, query in enumerate(queries):
        curs.execute(query)
        res = curs.fetchall()
        print(f"query {idx+1}: {res}")