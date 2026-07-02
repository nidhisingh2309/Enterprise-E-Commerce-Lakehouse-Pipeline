![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Spark SQL](https://img.shields.io/badge/Spark_SQL-FF9900?style=for-the-badge&logo=apache-spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
# 🚀 Enterprise E-Commerce Lakehouse Pipeline

An end-to-end **Data Engineering** project built using **Databricks, PySpark, Spark SQL, Delta Lake, and Power BI**. The project implements the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw e-commerce data into analytics-ready datasets for Business Intelligence and Machine Learning.

---

## 🛠 Tech Stack

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Power BI
- PySpark MLlib
- Git & GitHub

---

## 📂 Project Architecture

```
Olist E-Commerce Dataset
          │
          ▼
 Bronze (Raw Layer)
          │
          ▼
 Silver (Clean Layer)
          │
          ▼
 Gold (Star Schema)
          │
 ┌────────┼─────────┐
 ▼        ▼         ▼
SQL     Power BI   ML Features
Analytics            │
                     ▼
          Customer Segmentation
```

---

## 📁 Project Workflow

- **Bronze Layer** – Raw data ingestion into Delta tables
- **Silver Layer** – Data cleaning, validation, and standardization
- **Gold Layer** – Star Schema with fact and dimension tables
- **Business Analytics** – KPI generation using Spark SQL
- **Feature Engineering** – Customer, Product, and Seller feature creation
- **Machine Learning** – Customer segmentation using K-Means
- **Power BI** – Interactive business dashboards

---

## ⭐ Key Features

- End-to-end ETL pipeline using PySpark
- Medallion Architecture implementation
- Star Schema dimensional modeling
- Data quality validation framework
- Business KPI generation
- Feature engineering for analytics
- Customer segmentation with K-Means
- Interactive Power BI dashboards


## 🚀 Future Improvements

- Incremental ETL pipelines
- Apache Airflow orchestration
- Real-time data processing with Structured Streaming
- CI/CD integration

---

## 👩‍💻 Author

**Nidhi Singh**

⭐ If you found this project useful, consider giving it a star!
