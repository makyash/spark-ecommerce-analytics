# Spark E-commerce Analytics pipeline

## Overview
This project implements a batch data processing pipeline using Apache Spark to analyze e-commerce event data.

Raw CSV → Data Cleaning → Transformations → Aggregations → Parquet Output

This pipeline reads raw CSV data, performs, transformations, and generates key business KPIs

## Tech Stack
- Python 
- Pyspark 
- Pandas
- Parquet 

## Sample output for below 2 KPI

- Event counts: 
                                                       
| event_type|event_count|
|-----------|-----------|
|   purchase|      39762|
|     search|      70258|


- Purchase totals by user:   

|user_id|total_purchase_amount|
|-------|---------------------|
|   1941|              3383.29|
|  15030|              3372.52|
|  38191|               3298.3|


