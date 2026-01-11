# Bitcoin Airflow Pipeline

## Purpose

This project is primarily a **learning exercise for Apache Airflow**.  
The dbt models and ingestion logic are intentionally kept simple so the focus remains on:

- Airflow DAG design
- Task dependencies and scheduling
- Integrating dbt with Airflow using **Astronomer Cosmos**
- Running workflows in a **Dockerized Airflow (Astro)** environment

This is **not** intended to be a production-grade ETL or analytics system.

---

## Architecture

Snowflake BRONZE (Airflow ingestion)
↓
dbt SILVER / GOLD
↓
Airflow orchestration (Astro + Cosmos)

---

## Tech Stack

- Apache Airflow (Astro)
- Snowflake
- dbt Core
- Astronomer Cosmos
- Docker

---

## DAG Design

**Single DAG:** `bitcoin_pipeline`

1. **Ingestion**
   - Runs Snowflake SQL via Airflow
2. **Transformations**
   - dbt models executed using `DbtTaskGroup`
   - dbt tests defined in `schema.yml` are automatically executed

Ingestion always runs **before** dbt.

---

## Scheduling

```python
schedule="@daily"
catchup=False
```

## Key Takeaway

This project demonstrates Airflow orchestration patterns rather than advanced ETL or data modeling.

## Run

Start Astro

Configure Snowflake connection in Airflow

Trigger bitcoin_pipeline
