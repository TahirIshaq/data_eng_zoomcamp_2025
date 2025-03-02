# Spark
A data processing platform

## Running Spark
Docker was used to run `spark`. To start: `docker compose up -d`

Open `localhost:8888` or the forwarded host port to access jupyter notebook. If prompted for the token, run: `docker logs pyspark_jupyter 2>&1 | grep -o "http://127.0.0.1:8888/lab?token=.*"` and copy then token from the output. Once Spark cluster has been created, its web UI can be accessed from `localhost:4040` or the forwarded host port.

To stop: `docker compose down` or for complete cleanup: `docker compose down -v --rmi all`

## Homework

### Question 1: Install Spark and PySpark
```
import pyspark
pyspark.__version__
'3.5.0'
```

**Answer:** `3.5.0`

### Question 2: Yellow October 2024
```
df_yellow = spark.read.option("header", "true").parquet("yellow_tripdata_2024-10.parquet")
df_yellow.repartition(4).write.parquet(path="yellow_taxi", mode="overwrite")

!ls -lh yellow_taxi

total 97M
-rw-r--r-- 1 jovyan users 25M Mar  2 15:36 part-00000-189129b0-8b51-4efc-b6c8-c85d6dbcf9df-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 25M Mar  2 15:36 part-00001-189129b0-8b51-4efc-b6c8-c85d6dbcf9df-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 25M Mar  2 15:36 part-00002-189129b0-8b51-4efc-b6c8-c85d6dbcf9df-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 25M Mar  2 15:36 part-00003-189129b0-8b51-4efc-b6c8-c85d6dbcf9df-c000.snappy.parquet
-rw-r--r-- 1 jovyan users   0 Mar  2 15:36 _SUCCESS
```

**Answer:** `25MB`

### Question 3: Count records
The dateformat is: **YYY-MM--DD**

```
query = """SELECT COUNT(*) AS total_trips FROM trips_data WHERE DATE(tpep_pickup_datetime)='2024-10-15';"""
df_result = spark.sql(query)
df_result.show()

+-----------+
|total_trips|
+-----------+
|     128893|
+-----------+
```

**Answer:** `125,567`

### Question 4: Longest trip
```
query = """SELECT (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 3600 AS diff FROM trips_data ORDER BY diff DESC LIMIT 5;"""
df_result = spark.sql(query)
df_result.show(truncate=False)

+------------------+
|diff              |
+------------------+
|162.61777777777777|
|143.325           |
|137.76055555555556|
|114.83472222222223|
|89.89833333333333 |
+------------------+
```

**Answer:** `162`

### Question 5: User Interface
**Answer:** `4040`

### Question 6: Least frequent pickup location zone
```
query = """
WITH lest_freq_pu_id AS
(
    SELECT PULocationID as pu_id, COUNT(*) AS total_count
    FROM trips_data
    GROUP BY pu_id
    ORDER BY total_count ASC
    LIMIT 1
)
SELECT Zone, LocationID FROM trips_zone WHERE LocationID = (SELECT pu_id FROM lest_freq_pu_id);
"""
df_result = spark.sql(query)
df_result.show(truncate=False)

+---------------------------------------------+----------+
|Zone                                         |LocationID|
+---------------------------------------------+----------+
|Governor's Island/Ellis Island/Liberty Island|105       |
+---------------------------------------------+----------+
```

**Answer:** `Governor's Island/Ellis Island/Liberty Island`

## Helpful Links
- [Pyspark jupyter notebook docker](https://www.youtube.com/watch?v=ISD1RrOdn28)