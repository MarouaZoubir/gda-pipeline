 📦 Data Pipeline for MySQL to Postgres + Kafka Integration with Debezium, Dagster, Metabase & Data Quality

This project implements a full data pipeline that:

- **Migrates data** from a **MySQL** source to **PostgreSQL** using **pgloader**
- **Streams changes** from MySQL to Kafka using **Debezium**
- **Visualizes** data in **Metabase**
- Orchestrates and **validates data quality** using **Dagster** and **Great Expectations**

---

## � Architecture Overview
MySQL (source) → pgloader → PostgreSQL (target)
|
(Debezium CDC)
↓
Kafka (broker)
↓
Dagster (orchestration & DQ)
↓
Metabase (BI & dashboarding)

text

## 🐳 Docker Services

| Service          | Description                                      |
|------------------|--------------------------------------------------|
| source-mysql     | MySQL 5.7 instance with initialized data         |
| target-postgres  | PostgreSQL 15 for storing migrated data          |
| pgloader         | One-time migration from MySQL to PostgreSQL      |
| zookeeper        | Zookeeper instance required for Kafka            |
| kafka            | Apache Kafka message broker                      |
| debezium         | Debezium CDC connector for MySQL to Kafka        |
| metabase         | Visualization and BI dashboard                   |

## 📁 Project Structure
.
├── docker-compose.yml
├── 1_mysql_initialization/
│ └── sql/ # Initial SQL scripts for MySQL
├── 2_postgres_migration/
│ ├── pgloader-config/
│ │ └── migration.load # pgloader configuration
│ └── debezium-config/
│ └── mysql-connector.json # Debezium config
└── data_pipeline/
├── data_pipeline/ # Dagster project (C:\Users\HA\Desktop\stage-gda-real\uatrs-data-pipeline\data_pipeline\data_pipeline)
└── dbt/ # dbt project

text

## 🛠️ Requirements

### Python (for Dagster in data_pipeline/data_pipeline/)
```bash
pip install great-expectations==0.15.50 pandas==1.5.3 numpy==1.23.5 SQLAlchemy==1.4.5
pip install dagster==1.11.2 dagster-webserver==1.11.2 dagster-graphql==1.11.2 \
            dagster-dbt==0.27.2 dagster-postgres==0.27.2 dagster-ge==0.27.2
🚀 Getting Started
Clone the repository:

bash
git clone <repo-url>
cd <repo-folder>
Start the stack:

bash
docker-compose up --build
⚠️ Ensure ports 3307, 5433, 29092, 8083, and 3001 are available.

Access services:

Metabase: http://localhost:3001

Dagster: http://localhost:3000 (after starting with dagster dev)

🔐 Database Credentials
Component	Host	Port	DB Name	User	Password
MySQL	localhost	3307	uatrs_db	uatrs_user	uatrs_pass
PostgreSQL	localhost	5433	uatrs_db	uatrs_user	uatrs_pass
🧪 Running Data Quality Checks
Navigate to Dagster project:

bash
cd data_pipeline/data_pipeline/
Start Dagster:

bash
dagster dev
Access UI at http://localhost:3000 to:

Run pipelines

Validate data with Great Expectations

Monitor data quality

📊 Metabase Setup
Access http://localhost:3001

Connect to PostgreSQL using credentials above

Create dashboards and visualizations

📌 Notes
Debezium config: ./2_postgres_migration/debezium-config/mysql-connector.json

MySQL init scripts: ./1_mysql_initialization/sql/

Data persists in Docker volumes:

mysql_data

postgres_data