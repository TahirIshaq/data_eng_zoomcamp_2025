UPDATE {{ var.value.taxi_color }}_tripdata_staging
SET 
  "unique_row_id" = md5(
    COALESCE(CAST("VendorID" AS text), '') ||
    COALESCE(CAST("lpep_pickup_datetime" AS text), '') || 
    COALESCE(CAST("lpep_dropoff_datetime" AS text), '') || 
    COALESCE("PULocationID", '') || 
    COALESCE("DOLocationID", '') || 
    COALESCE(CAST("fare_amount" AS text), '') || 
    COALESCE(CAST("trip_distance" AS text), '')      
  ),
  "filename" = '{{ var.value.file_name }}';