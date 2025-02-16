FROM python:3.12.8-slim-bullseye

WORKDIR /app

COPY ["requirements.txt", "el_dlt.py", "."]

RUN pip install  --no-cache-dir -r requirements.txt

ENV PG_USERNAME="loader" \
PG_PASSWORD="" \
PG_HOST="db" \
PG_PORT="5432" \
PG_DB_NAME="ny_taxi" \
PG_DB_SCHEMA="ny_taxi_data"

ENTRYPOINT ["python", "-u", "el_dlt.py"]