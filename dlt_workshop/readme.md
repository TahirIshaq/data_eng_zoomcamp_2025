# Usage

`docker compose up -d` will first start postgres and then data will be loaded into it using DLT.

After a minute `docker logs el_dlt` will return:

`docker compose down -v --rmi all` will stop and remove the running containers and their images.

```
DLT version: 1.6.1
query 1: [('_dlt_pipeline_state',), ('_dlt_loads',), ('_dlt_version',), ('rides',)]
query 2: [(10000,)]
query 3: [(Decimal('12.3049183333333333'),)]
```

## Homework

### Question 1: dlt Version

```
print(f"DLT version: {dlt.__version__}")
```

Answer: **DLT version: 1.6.1**

### Question 2: Define & Run the Pipeline (NYC Taxi API)
How many tables were created?

```
SELECT table_name FROM information_schema.tables WHERE table_schema = 'ny_taxi_data';
[('_dlt_pipeline_state',), ('_dlt_loads',), ('_dlt_version',), ('rides',)]
```

Answer: **4**

### Question 3: Explore the loaded data
What is the total number of records extracted?

```
SELECT COUNT(*) AS total_records FROM rides;
```

Answer: **10000**

### Question 4: Trip Duration Analysis
What is the average trip duration?

```
SELECT AVG(EXTRACT(EPOCH FROM (trip_dropoff_date_time - trip_pickup_date_time))) / 60 AS minute_difference FROM rides;
```

Answer: **12.3049**


## To do
- [ ] The output of `el_dlt.py` should be written in a file.