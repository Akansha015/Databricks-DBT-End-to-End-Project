# Databricks <> DBT <> End-to-End <> Project

An end-to-end analytics pipeline that coordinates CDC ingestion in Databricks
with layered dbt transformations and quality checks orchestrated by Apache
Airflow.

## Pipeline overview

The pipeline loads changes into a Databricks bronze layer, then builds
incremental technical silver tables, a business-oriented silver model, and
gold analytics models. Airflow coordinates the steps and stops downstream
work when ingestion or a dbt test fails.

## Flow architecture

```text
Databricks CDC job
        |
        v
Databricks bronze tables
        |
        v
dbt source freshness check
        |
        v
Silver technical models (incremental) -> dbt tests
        |
        v
Silver business model -> dbt tests
        |
        v
Gold ephemeral models
        |
        v
dbt snapshots (dimension history)
        |
        v
Gold fact model
```

The `orchestrate` Airflow DAG starts the configured Databricks job and waits for
it to finish successfully. It then cleans dbt's generated artifacts, checks
source freshness, and runs the dbt stages in dependency order. The dbt models
read the `walmart.bronze` source tables for orders, customers, products,
order items, stores, and employees.

## Technology stack

- **Databricks** and the Databricks SDK for CDC ingestion job execution
- **dbt Core** with the **dbt-databricks** adapter for SQL transformations,
  snapshots, and tests
- **Apache Airflow** for workflow orchestration
- **Docker Compose**, with PostgreSQL and Redis supporting the local Airflow
  development environment
- **Python** for orchestration and project tooling; **SQL/Jinja** for dbt models
- **uv** for Python dependency and environment management

## Repository layout

- `airflow/dags/` — Airflow DAGs, including the end-to-end orchestrator
- `airflow/` — local Airflow Docker setup and the dbt project mounted into the
  Airflow containers
- `airflow/wm_project/models/` — dbt source definitions and silver/gold models
- `airflow/wm_project/snapshots/` — dbt snapshots for dimension history
- `wm_project/` — standalone copy of the dbt project for local development

## Configuration and credentials

Do not commit credentials or generated local configuration. Supply
`DATABRICKS_HOST` and `DATABRICKS_TOKEN` through the Airflow environment (for
local Compose use, add them to an untracked `airflow/.env` file). The Databricks
SDK reads these standard environment variables when `WorkspaceClient()` is
created.

Configure a local dbt `profiles.yml` for the Databricks workspace and warehouse
before running dbt commands. The profile is intentionally excluded from Git.
The Airflow project also requires its local Airflow configuration; the
generated `airflow/config/airflow.cfg` is excluded.

## Running locally

From `airflow/`, configure the local environment and dbt profile, then start the
Airflow services with Docker Compose:

```sh
docker compose up --build
```

Open the Airflow UI at `http://localhost:8080` and trigger the `orchestrate`
DAG. Ensure the referenced Databricks job and required bronze tables are
available before triggering it.