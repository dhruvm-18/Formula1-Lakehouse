# 🏎️ Formula 1 Lakehouse Analytics Platform

An end-to-end scalable Formula 1 Lakehouse project built using Microsoft Azure and Azure Databricks to simulate a modern enterprise-grade data engineering solution. The project focuses on automated ingestion, transformation, orchestration, incremental processing, and analytics workflows using the Medallion Architecture.

---

# 🚀 Project Overview

This project was designed to replicate how real-world enterprise data platforms process and analyze large-scale data efficiently using cloud-native technologies and Lakehouse architecture principles.

The platform processes historical Formula 1 datasets updated till the **2025 season** and supports automated batch ingestion workflows where new race datasets can be uploaded directly into cloud storage and processed automatically without manually rerunning notebooks.

The project follows the **Bronze → Silver → Gold** Medallion Architecture using Delta Lake for scalable and reliable data engineering workflows.

---

# 🏗️ Architecture

## Medallion Architecture

### 🥉 Bronze Layer
- Raw data ingestion from Formula 1 datasets
- Schema validation and metadata handling
- Storage of unprocessed/raw data

### 🥈 Silver Layer
- Data cleansing and transformation
- Standardization and normalization
- Incremental processing logic
- Derived columns and business transformations

### 🥇 Gold Layer
- Analytical and reporting-ready tables
- Aggregated metrics and KPIs
- Dashboard consumption layer

---

# 🖼️ Project Architecture & Workflow Screenshots

## 📂 Project Directory Structure

![Project Directory](images/Directory.png)

---

## ⚙️ Batch Processing Directory Workflow

![Batch Processing Directory](images/BatchProcessDirectory.png)

---

## ☁️ Azure Databricks Cluster Setup

![Cluster Setup](images/ClusterSetup.png)

---

## 🔄 Main Orchestration Workflow

![Main Job Workflow](images/MainJob.png)

---

## 🏗️ Lakehouse Processing Pipeline

![Lakehouse Job](images/LakehouseJob.png)

---

# ⚙️ Features

- End-to-end Lakehouse implementation
- Automated ETL pipelines using PySpark and SQL
- Full refresh and incremental batch processing
- Workflow orchestration using Databricks Jobs & Pipelines
- Dynamic batch detection and execution
- Delta Lake-based scalable storage architecture
- Unity Catalog integration
- Interactive analytics dashboards
- Enterprise-style data engineering workflows

---

# 🔄 Incremental Processing Workflow

One of the core highlights of this project is the automated incremental orchestration workflow.

Whenever a new Formula 1 race dataset arrives:

1. The new batch file is uploaded into the Azure storage container
2. The orchestration workflow detects the new batch
3. Bronze, Silver, and Gold layer pipelines are triggered automatically
4. Only newly arrived data is processed
5. Analytical dashboards are refreshed automatically

This eliminates manual notebook execution and simulates real-world enterprise data pipeline automation.

---

# 📊 Dashboards Created

## 🏁 Driver Championship Standings

![Dashboard 1](images/Dashboard%201.png)

---

## 🏎️ Constructor Championship Standings

![Dashboard 2](images/Dashboard%202.png)

---

## 👑 Dominant Drivers of All Time

![Dashboard 3](images/Dashboard%203.png)

---

## 🚀 Dominant Teams of All Time

![Dashboard 4](images/Dashboard%204.png)

---

# 🧠 Greatness Score System

To identify dominant drivers and teams across different Formula 1 eras, a custom **Greatness Score** metric system was designed.

## 📌 Scoring Logic

- Championship = 100 Points
- Race Win = 10 Points
- Podium Finish = 3 Points

This helped create a balanced performance comparison model across multiple generations of Formula 1.

## 📈 Greatness Score Visualization

![Greatness Score](images/GreatnessScore.png)

---

# 🛠️ Tech Stack

## ☁️ Cloud & Data Platform
- Azure Databricks
- Azure Data Lake Storage
- Unity Catalog

## ⚡ Data Engineering
- PySpark
- SQL
- Delta Lake
- ETL Pipelines
- Incremental Processing
- Workflow Orchestration

## 📊 Analytics & Visualization
- Power BI

---

# 📂 Project Workflow

```text
Raw Dataset
     ↓
Bronze Layer (Raw Ingestion)
     ↓
Silver Layer (Transformations & Cleaning)
     ↓
Gold Layer (Analytics & KPIs)
     ↓
Power BI Dashboards
```

---

# 📈 Key Learnings

Through this project, I gained hands-on experience with:

- Enterprise-scale Lakehouse Architecture
- Azure Databricks ecosystem
- Delta Lake implementation
- Incremental ETL pipeline design
- Workflow orchestration
- Scalable data processing
- Data modeling and analytics
- Cloud-based data engineering workflows

---

# 🔥 Future Improvements

- Real-time streaming pipelines using Kafka
- CI/CD integration for Databricks workflows
- Data quality monitoring framework
- ML-based race outcome predictions
- Automated testing for ETL pipelines

---

# 👨‍💻 Author

## Dhruv Mendiratta

- GitHub: https://github.com/dhruvm-18
- LinkedIn: https://www.linkedin.com/in/dhruvmendiratta18/
- Portfolio: https://dhruv-portfolio-bay.vercel.app/

---
