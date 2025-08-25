# 🏢 Enterprise Lakehouse with Machine Learning

This repository contains an **end-to-end enterprise data platform** that simulates how modern companies build **data lakehouses** for analytics, machine learning, and BI.

---

## 🚀 Tech Stack

- **Apache Spark** – Distributed ETL & transformations  
- **Apache Airflow** – Workflow orchestration  
- **MLflow** – ML experiment tracking  
- **PostgreSQL** – Metadata storage  
- **MinIO (S3-like)** – Data Lake (Raw → Bronze → Silver → Gold layers)  
- **Apache Superset** – BI & dashboards  
- **FastAPI** – ML model serving  
- **Docker & Docker Compose** – Containerized environment  

---

## 📂 Project Architecture

```mermaid
flowchart TD
    A[Raw Data in MinIO] --> B[Bronze Layer]
    B --> C[Silver Layer - Cleaned Data]
    C --> D[Gold Layer - Aggregated Data]
    D --> E[ML Models tracked in MLflow]
    D --> F[Superset Dashboards]
    E --> G[FastAPI ML Inference API]
    H[Airflow] -->|Schedules & Orchestrates| B
    H --> C
    H --> D
