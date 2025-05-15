# SparkFlow-ETL
**Overview**
-This project demonstrates how to build modular and scalable ETL (Extract, Transform, Load) pipelines using Databricks Community Edition with PySpark, the Python API for Apache Spark.
-It covers data ingestion from multiple formats, transformation using Spark SQL and the DataFrame API, and loading results into storage layers like DataLakes and Delta tables.

**1. Extractor Module**
Uses a Factory Pattern to load data from:
i.CSV
ii.Parquet
iii.Delta Tables

**2. Transformer Module**
Implements business logic using:
PySpark DataFrame API
Spark SQL

Sample use cases:
Customers who bought AirPods after iPhones
Customers who only bought iPhones and AirPods

**3. Loader Module**
Persists output to:
DBFS File Paths (/FileStore/tables/...)
Delta Lake Tables (managed tables)

**How to Run This Project**
1. Import notebooks into Databricks Community Edition.
2. Attach all notebooks to a cluster.
3. Open apple_analysis.ipynb.
4. Set workflow type:
  name = "OnlyIphoneAndAirpodWorkFlow"  # or "AirpodsAfterIphoneWorkFlow"
  WorkflowRunner(name).runner()
5. Run the notebook to extract, transform, and save results.
6. Check output:
  Tables: via Data > Tables
  Files: under Data > DBFS > FileStore > tables > Output

**🛠 Tech Stack**
Apache Spark (PySpark)
Databricks Community Edition
Delta Lake
Spark SQL
Factory Design Pattern

**📦Workflows**

1. AirpodsAfterIphoneWorkFlow
Identifies customers who bought AirPods after iPhones
Output is written to: /FileStore/tables/Output/AirpodsAfterIphone

2. OnlyIphoneAndAirpodWorkFlow
Identifies customers who only bought iPhones and AirPods
Output is written to: /FileStore/tables/Output/OnlyIphoneAndAirpods
