FROM python:3.12.8-slim-bullseye

RUN apt update && apt install -y postgresql-client wget

ENV PG_USERNAME="" \
    PG_PASSWORD="" \
    PG_HOST="" \
    PG_PORT="5432" \
    PG_DB_NAME="" \
    PG_SCHEMA="" \
    TAXI_DATA_SCHEMA="/app/taxi_data_schema.json" \
    TAXI_DATA=''

WORKDIR /app

COPY ["populate_db.py", "taxi_data_schema.json", "requirements.txt", "."]

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "-u", "populate_db.py"]