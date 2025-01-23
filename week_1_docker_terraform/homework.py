import psycopg2
import os

QUERY3 ="""
WITH oct_trips AS (
    SELECT
        trip_distance
    FROM
        "green_tripdata_2019-10"
    WHERE
        DATE(lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31' AND
        DATE(lpep_dropoff_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
    )
SELECT
    (SELECT COUNT(*) FROM oct_trips WHERE trip_distance <= 1) AS a,
    (SELECT COUNT(*) FROM oct_trips WHERE trip_distance > 1 AND trip_distance <= 3) AS b,
    (SELECT COUNT(*) FROM oct_trips WHERE trip_distance > 3 AND trip_distance <= 7) AS c,
    (SELECT COUNT(*) FROM oct_trips WHERE trip_distance > 7 AND trip_distance <= 10) AS d,
    (SELECT COUNT(*) FROM oct_trips WHERE trip_distance > 10) AS e;
"""
QUERY4 ="""
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date
FROM
    "green_tripdata_2019-10"
WHERE
    trip_distance = (SELECT MAX(trip_distance) FROM "green_tripdata_2019-10");
"""
QUERY5 = """
WITH famous_pickup AS (
    SELECT
        "PULocationID", SUM(total_amount) as total
    FROM
        "green_tripdata_2019-10"
    WHERE
        DATE(lpep_pickup_datetime) = '2019-10-18'
    GROUP BY
        "PULocationID"
    ORDER BY
        total desc)
SELECT
    "Zone"
FROM
    taxi_zone_lookup
WHERE
    "LocationID" IN (SELECT "PULocationID" FROM famous_pickup WHERE total > 13000 LIMIT 3);
"""
QUERY6 = """
WITH table_1 AS(
    SELECT
        "DOLocationID"
    FROM
        "green_tripdata_2019-10"
    WHERE
        (DATE(lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31')
        AND "PULocationID" = (SELECT 
                                "LocationID" 
                            FROM 
                                taxi_zone_lookup 
                            WHERE 
                                "Zone" = 'East Harlem North')
    ORDER BY
        tip_amount DESC
    LIMIT 1
    )
SELECT
    "Zone"
FROM
    taxi_zone_lookup
WHERE
    "LocationID" = (SELECT "DOLocationID" FROM table_1);
"""

QUERIES = [QUERY3, QUERY4, QUERY5, QUERY6]

def get_args():
    """
    Returns the database server configuration
    """
    db_args = dict()
    db_args["username"] = os.getenv("PG_USERNAME")
    db_args["password"] = os.getenv("PG_PASSWORD")
    db_args["host"] = os.getenv("PG_HOST",)
    db_args["port"] = os.getenv("PG_PORT")
    db_args["db_name"] = os.getenv("PG_DB_NAME")
    
    return db_args


def main():
    """The main function"""
    args = get_args()
    username = args["username"]
    password = args["password"]
    host = args["host"]
    port = args["port"]
    db_name = args["db_name"]

    db_url = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"

    with psycopg2.connect(db_url).cursor() as cur:
        for idx, query in enumerate(QUERIES):
            cur.execute(query)
            resp = cur.fetchall()
            print(f"Question {idx+3}: {resp}")
            with open("/app/results/results.txt", "a") as f:
                f.write(f"Question {idx+3}: {resp}\n")
    

if __name__ == "__main__":
    main()