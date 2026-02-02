# Module 2 Homework: Workflow Orchestration

## Question 1
134.5 MB

## Question 2
green_tripdata_2020-04.csv

## Question 3
24,648,663

```sql
SELECT COUNT(*) as total_rows
FROM `dtc-de-course-484819.zoomcamp.yellow_tripdata`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;
```

## Question 4
1,734,039

```sql
SELECT COUNT(*) as total_rows
FROM `dtc-de-course-484819.zoomcamp.green_tripdata`
WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = 2020;
```

## Question 5
1,925,130

```sql
SELECT COUNT(*) as total_rows
FROM `dtc-de-course-484819.zoomcamp.yellow_tripdata`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2021
  AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 3;
```

## Question 6
Add a timezone property set to America/New_York in the Schedule trigger configuration