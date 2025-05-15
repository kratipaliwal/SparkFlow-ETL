
# SparkFlow-ETL

## 📌 Overview

This project demonstrates how to build **modular and scalable ETL (Extract, Transform, Load) pipelines** using **Databricks Community Edition** with **PySpark**, the Python API for Apache Spark.

It covers:
- Ingesting data from multiple formats
- Transforming it using **Spark SQL** and **DataFrame API**
- Loading the results into storage layers like **Data Lakes** and **Delta tables**

---

## 🧱 1. Extractor Module

Uses a **Factory Pattern** to load data from:
- CSV  
- Parquet  
- Delta Tables

---

## 🔧 2. Transformer Module

Implements business logic using:
- PySpark DataFrame API  
- Spark SQL  

### Sample Use Cases:
- Customers who bought AirPods after buying iPhones  
- Customers who only bought iPhones and AirPods

---

## 💾 3. Loader Module

Persists output to:
- DBFS File Paths (`/FileStore/tables/...`)  
- Delta Lake Tables (managed tables)

---

## ▶️ How to Run This Project

1. Import notebooks into **Databricks Community Edition**
2. Attach all notebooks to a running cluster
3. Open `workflow_runner.ipynb`
4. Set the workflow type:
   ```python
   name = "OnlyIphoneAndAirpodWorkFlow"  # or "AirpodsAfterIphoneWorkFlow"
   WorkflowRunner(name).runner()
   ```
5. Run the notebook to extract, transform, and load results
6. Check output:
   - **Tables**: via `Data > Tables`  
   - **Files**: under `Data > DBFS > FileStore > tables > Output`

---

## 🛠 Tech Stack

- Apache Spark (PySpark)  
- Databricks Community Edition  
- Spark SQL  
- Factory Design Pattern

---

## 📦 Workflows

### 1. `AirpodsAfterIphoneWorkFlow`
- Identifies customers who bought **AirPods after iPhones**
- Output path:  
  `/FileStore/tables/Output/AirpodsAfterIphone`

### 2. `OnlyIphoneAndAirpodWorkFlow`
- Identifies customers who **only bought iPhones and AirPods**
- Output path:  
  `/FileStore/tables/Output/OnlyIphoneAndAirpods`
