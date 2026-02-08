# Module 3 Homework: Data Warehousing and BigQuery

## Question 1 - Counting Records
20332093

```sql
SELECT COUNT(*) FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`;
```

## Question 2 - Data Read Estimation
0 MB for the External Table and 155.12 MB for the Materialized Table

```sql
SELECT COUNT(DISTINCT PULocationID) FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_external`;
SELECT COUNT(DISTINCT PULocationID) FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`;
```

## Question 3 - Understanding Columnar Storage
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

```sql
-- one column
SELECT PULocationID FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`;

-- two columns
SELECT PULocationID, DOLocationID FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`;
```

## Question 4 - Counting Zero Fare Trips
8333

```sql
SELECT COUNT(*) FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`
WHERE fare_amount = 0;
```

## Question 5 - Partitioning and Clustering
Partition by tpep_dropoff_datetime and Cluster on VendorID

```sql
CREATE OR REPLACE TABLE `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`;
```

## Question 6 - Partition Benefits
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```sql
-- non-partitioned table
SELECT DISTINCT VendorID
FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- partitioned table
SELECT DISTINCT VendorID
FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

## Question 7 - External Table Storage
GCP Bucket

## Question 8 - Clustering Best Practices
False

## Question 9 - Understanding Table Scans
0 bytes because BigQuery can use metadata to answer the query without scanning the entire table.

```sql
SELECT COUNT(*) FROM `dtc-de-course-484819.dezoomcamp_hw3.yellow_taxi_materialized`;
```