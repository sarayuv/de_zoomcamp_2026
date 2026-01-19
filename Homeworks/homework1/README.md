# Module 1 Homework: Docker & SQL

## Question 1. Understanding Docker images
```
>> docker run -it --entrypoint bash python:3.13
>> pip list
```

The version of ```pip``` in the image is 25.3.

## Question 2. Understanding Docker networking and docker-compose
The ```hostname``` and ```port``` that pgadmin should use is db:5432.

## Question 3. Counting short trips
```
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
    AND trip_distance <= 1;
```

8,007

## Question 4. Longest trip for each day
```
SELECT DATE(lpep_pickup_datetime) as pickup_day, MAX(trip_distance) as max_distance
FROM green_taxi_trips
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```

2025-11-14

## Question 5. Biggest pickup zone
```
SELECT zpu."Zone", SUM(gt.total_amount) as total_sum
FROM green_taxi_trips gt
JOIN zones zpu ON gt."PULocationID" = zpu."LocationID"
WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
GROUP BY zpu."Zone"
ORDER BY total_sum DESC
LIMIT 1;
```

East Harlem North

## Question 6. Largest tip
```
SELECT zdo."Zone" as dropoff_zone, MAX(gt.tip_amount) as max_tip
FROM green_taxi_trips gt
JOIN zones zpu ON gt."PULocationID" = zpu."LocationID"
JOIN zones zdo ON gt."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
    AND gt.lpep_pickup_datetime >= '2025-11-01'
    AND gt.lpep_pickup_datetime < '2025-12-01'
GROUP BY zdo."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

Yorkville West

## Question 7. Terraform Workflow
terraform init, terraform apply -auto-approve, terraform destroy