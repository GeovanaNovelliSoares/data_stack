# Modern Data Stack Local: End-to-End Pipeline (OLTP to OLAP)

This project demonstrates the implementation of a modern and complete data infrastructure, operating 100% locally and free of charge. The goal is to simulate the data lifecycle in a corporate scenario, from transactional generation to advanced analytical visualization.

## 🏗️ Solution Architecture

The architecture follows the "Modern Data Stack in a Box" concept, using high-performance tools for columnar processing and dimensional modeling.

1. **Source (OLTP):** A **SQLite** database simulating the production system of an e-commerce platform, structured in Third Normal Form (3NF).

2. **Ingestion & Storage (Data Lake):** Raw data extraction process to **Apache Parquet** files, optimizing storage and preparing the ground for columnar reads.

3. **Data Warehouse (OLAP):** Use of **DuckDB** for transformation and modeling. The data is structured in a **Star Schema**, facilitating low-latency analytical queries.

4. **Presentation (BI):** Interactive dashboard developed in **Streamlit** with dynamic and animated visualizations via **Plotly**.

---

## 🛠️ Technologies Used

* **Language:** Python 3.10+
* **Transactional Database:** SQLite
* **Analytic Engine/DWH:** DuckDB
* **Storage Format:** Apache Parquet
* **Main Libraries:** Pandas, Plotly, Streamlit, Faker
* **Data Modeling:** Star Schema (Facts and Dimensions)

---

## 📈 Dimensional Modeling (Star Schema)

To optimize analytical performance, the data warehouse was modeled following Business Intelligence best practices:

* **Fact Table (`fact_vendas`):** Contains quantitative metrics and foreign keys.

* **Dimensions (`dim_cliente`, `dim_produto`, `dim_tempo`):** Contain descriptive attributes for filtering and grouping (slicing and dicing).

---

## 🚀 How to Run the Project

### 1. Requirements
Make sure you have Python installed. Using a virtual environment (`venv`) is recommended.

``bash
pip install -r requirements.txt
