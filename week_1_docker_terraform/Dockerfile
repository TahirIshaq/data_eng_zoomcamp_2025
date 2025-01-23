FROM python:3.12.8-slim-bullseye

RUN apt update && apt upgrade && apt install -y curl

ENV PG_USERNAME="postgres" \
PG_PASSWORD="" \
PG_HOST="db" \
PG_PORT="5432" \
PG_DB_NAME="ny_taxi" \
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv,https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

WORKDIR /app

COPY ["etl.py", "df_col_dtypes.py", "requirements.txt", "."]

RUN pip install  --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "-u", "etl.py"]