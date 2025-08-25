# 📊 Airflow ETL Pipeline for Stock Market Data  

This project is an **ETL pipeline built with Apache Airflow** that fetches real-time and historical stock market data from the **Alpha Vantage API**, transforms it, and loads it into a **PostgreSQL database** for analysis and reporting.  

---

## 🚀 Pipeline Architecture  

The workflow follows the standard **Extract → Transform → Load (ETL)** process:  

1. **Extract** – Data is fetched from the Alpha Vantage API.  
2. **Transform** – Data is cleaned, normalized, and structured for database insertion.  
3. **Load** – Final data is stored in PostgreSQL tables.  

![ETL Pipeline](docs/etl_pipeline.png)  

---

## ⚙️ Features  
- Automated stock data ingestion from Alpha Vantage.  
- Task scheduling and orchestration with **Apache Airflow**.  
- PostgreSQL integration for structured storage.  
- Modular DAG design for scalability.  
- Easy visualization and monitoring via Airflow UI.  

---

## 🚀 Pipeline Overview  

Below is a visualization of the DAG as seen in the **Airflow UI pipeline graph**:

![Airflow Pipeline Graph](docs/AirflowGraph.png)

## 🗄️ Database Tables  

The pipeline creates a `time_series` table in PostgreSQL to store historical stock data.  

Example schema and sample rows:  

![Database Tables](docs/pipeline_tables.png)  

---

## 📂 Project Structure  
airflow/
│── dags/ # Airflow DAGs
│ └── etl_pipeline.py
│── docs/ # Documentation and diagrams
│ ├── etl_pipeline.png
│ └── pipeline_tables.png
│── modules/ # Custom ETL modules
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ ├── fetch.py
│ ├── utils.py
│ └── init.py
│── requirements.txt # Python dependencies
│── README.md # Project documentation
│── .gitignore # Git ignore rules
