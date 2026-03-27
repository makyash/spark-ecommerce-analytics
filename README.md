# 🚀 Spark E-commerce Analytics pipeline

## 📌 Overview
This project implements an end-to-end batch data pipeline using Apache Spark to analyze e-commerce event data.

The pipeline processes raw event logs, enriches them using dimension tables (users and products), and generates business-critical KPIs such as revenue trends, user behavior insights, and product performance metrics.

Raw CSV → Data Cleaning → Transformations → Aggregations → Parquet Output.


## 🧱Tech Stack
- Python 
- Pyspark 
- Pandas
- Parquet 


## 📊 Data Model
The project follows a star schema design:

Fact Table
- events → user activity (views, purchases)

Dimension Tables
- users → user attributes
- products → product metadata

## 🔄 Data Pipeline Flow
Raw Events CSV 

   → Data Cleaning & Transformation.

   → Join with Users & Products.

   → KPI Aggregations.

   → Partitioned Parquet Output.


## ⚙️ Features Implemented
###  🔹 Data Processing
* Spark DataFrame transformations
* Timestamp parsing and enrichment
* Data cleaning and normalization
### 🔹 Joins
* Fact-to-dimension joins (users, products)
* Handling duplicate keys and join correctness
* Broadcast joins for performance optimization
### 🔹 Ranking Functions
* First purchase per user
* Running total of user spend
* User-level ranking
### 🔹 KPI Generation
* Revenue by category
* Revenue by country
* Top users by spend

### 🔹 Data Storage
* Parquet format for efficient storage
* Partitioned data by event date

## ▶️ How to Run
### 1. Setup environment
- `python3 -m venv venv`
- `source venv/bin/activate`
- `pip install -r requirements.txt`

### 2. Run the pipeline
- `python jobs/batch_job.py`


## 💡 Business Impact

This pipeline enables:

1. Better understanding of user behavior.
2. Revenue trend analysis.
3. Product performance tracking.
4. Data-driven decision making.