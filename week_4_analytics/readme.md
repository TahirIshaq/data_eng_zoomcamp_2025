# Week 4: Analytics Engineering
`docker-compose.yml` contains the following services:

1. Postgres (db)
2. Python (update_db)
3. DBT-Postgres(dbt_etl)

The servies run in the following order: `db -> update_db -> dbt_etl`

## Postgres (db)
A postgres server is [initialized](init_db/init.sql) with `green`, `yellow` and `fhv` tables.

## Python (update_db)
A python program that updates the tables. By default green and yellow tables are updated with 2019 and 2020 data whereas fhv is only with 2019. `psql` was used to insert the data as postgres supports loading data directly from the `csv` files. Updating the tables with the defailt input took approximately 8 minutes. The [schema](taxi_data_schema.json) and [sample](taxi_data_sample.json) describe the acceptable values for `TAXI_DATA`.

## DBT (etl_dbt)
The loaded data is transformed using `dbt-postgres` connector. Data is transformed in 2 stages: `staging` and `core`. By default only 100 rows of `green` and `yellow` taxi tables. Once the desired results are obtained, the variable can be overwritten using `IS_TEST_RUN` to `true` and `DBT_TARGET` to `prod`. TO generate lineage graph: `dbt docs generate` and then `dbt docs serve` will start a server at port `8080` be default [reference](https://docs.getdbt.com/reference/commands/cmd-docs). At the bottom right, the image the connection graph can be generated as show in [here](dbt-dag.png)

## Homework

Start the services by running: `docker compose up -d`. Once `update_db` is completed, login to the postgres container: 

```
docker exec -it postgres /bin/bash
psql "postgresql://postgres:postgres@localhost:5432/ny_taxi?options=-csearch_path%3Draw_data"

SELECT 
(SELECT COUNT(*) FROM green_taxi) AS green_records,
(SELECT COUNT(*) FROM yellow_taxi) AS yellow_records,
(SELECT COUNT(*) FROM fhv_taxi) AS fhv_records;

ny_taxi-# (SELECT COUNT(*) FROM fhv_taxi) AS fhv_records;
 green_records | yellow_records | fhv_records 
---------------+----------------+-------------
       7778101 |      109047518 |    43244696
(1 row)
```

Loading all the tables to the database took approximately 8 minutes and dbt with test run takes approximately 6 minutes.

### Question 1: What does this .sql model compile to?

A new source was added to [schema.yml](ny_taxi/models/staging/schema.yml).

```
- name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

Two environment variable were initialized/updated:

```
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

Run `dbt compile --inline select * from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}`. `dbt compile` is dependent on [dbt debug](https://docs.getdbt.com/faqs/Warehouse/db-connection-dbt-compile). The output can be found in `ny_taxi/target/`

**Answer**: select * from "myproject"."raw_nyc_tripdata"."ext_green_taxi"

### Question 2: which takes precedence over DEFAULT value?
[dbt variables precedence](https://docs.getdbt.com/docs/build/project-variables#defining-variables-on-the-command-line)

Set the environment varialbe: `export DAYS_BACK=20` and 

```
echo "SELECT (CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY)" > ny_taxi/models/staging/q21.sql
echo "SELECT (CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY)" > ny_taxi/models/staging/q22.sql
echo "SELECT (CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30"))}}' DAY)" > ny_taxi/models/staging/q23.sql
echo "SELECT (CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY)" > ny_taxi/models/staging/q24.sql
```

```
dbt compile --select "q21" --vars 'days_back: 7'
dbt compile --select "q22" --vars 'days_back: 7'
dbt compile --select "q23" --vars 'days_back: 7'
dbt compile --select "q24" --vars 'days_back: 7'
```

The output compiled SQL command can be found in `ny_taxi/target`

**Answer:** `select * from {{ ref('fact_taxi_trips') }} where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

### Question 3: Select the option that does NOT apply for materializing fct_taxi_monthly_zone_revenue:
[dbt plus operator](https://docs.getdbt.com/reference/node-selection/graph-operators)

From the available information, `fct_taxi_monthly_zone_revenue.sql` is located in `dbt_project/core`

***Answer:** `dbt run --select models/staging/+`

### Question 4: Select all statements that are true to the models using it:
[dbt variables precedence](https://docs.getdbt.com/docs/build/project-variables#defining-variables-on-the-command-line)

According to the variable precedence in dbt

**Answer:** All statements are `True` except `Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile`.