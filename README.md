# Attendance Lark Data Pipeline

This project orchestrates a standard **Medallion Architecture** workflow using Apache Airflow. It extracts HR data from the Lark API, processes it through Google Cloud Storage (GCS) and Databricks Delta Lake, and finally lands the analyzed data into Google BigQuery.

## Architecture & Flow (Bronze -> Silver -> Gold)
1. **Source -> Local CSV** (`fetch_and_transform_to_csv`)
   - Retrieves employee directory, attendance records, and approval records via Lark Open API.
   - Flattens the JSON response and stores it temporarily in local CSV files (`/tmp/lark_data/`).
2. **Local CSV -> GCS Bronze Layer** (`upload_to_gcs_bronze`)
   - Airflow uploads the CSV files into a Google Cloud Storage bucket (`GCS_BUCKET_NAME`).
   - *This acts as the raw/historical landing zone.*
3. **GCS -> Databricks Silver Layer** (`databricks_load_silver`)
   - Airflow triggers a Databricks SQL command (`COPY INTO ... USING DELTA`).
   - Databricks reads the CSVs from GCS and copies them into Silver Delta Tables (`silver_lark_employees`, `silver_lark_attendance`, etc.) holding clean, typed data.
4. **Databricks Silver -> Gold Layer** (`databricks_transform_gold`)
   - Databricks runs aggregations on the Silver tables to create Business-Level KPI tables (Gold Layer).
   - Example: `gold_lark_attendance_summary`.
5. **Databricks -> BigQuery** (`databricks_to_bigquery`)
   - Extracts the Silver & Gold tables from Databricks Data Intelligence Platform.
   - Uploads and overwrites the data in Google BigQuery for final dashboarding and reporting.

The entire process is orchestrated by a single master DAG: `dags/master_pipeline_dag.py`.

## Running the Pipeline Locally with Docker

This project is fully dockerized to make running Airflow locally simple.

1. Create a `.env` file based on `.env.example` and fill in all the necessary credentials.
2. Initialize the Airflow database:
   ```bash
   docker-compose up airflow-init
   ```
3. Start the services:
   ```bash
   docker-compose up -d
   ```
4. Access the Airflow Web UI at `http://localhost:8080` (default login is `airflow` / `airflow`).
5. Ensure your Google Cloud Connection (`google_cloud_default`) in Airflow is properly configured, or ensure Airflow has access to `BIGQUERY_CREDENTIALS_PATH` (which is used as `GOOGLE_APPLICATION_CREDENTIALS`).
6. Enable and trigger the `master_lark_databricks_bigquery_pipeline` DAG.

## CI/CD (GitHub Actions)
A `.github/workflows/deploy.yml` file is provided to automatically run basic linting and build the Docker image when pushing to the `main` branch. 

## Required Environment Variables
Ensure the following variables are present in your `.env` or CI/CD secrets:
- **Lark API:** `LARK_APP_ID`, `LARK_APP_SECRET`
- **Databricks:** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`
- **GCP & BigQuery:** `BIGQUERY_PROJECT_ID`, `BIGQUERY_DATASET`, `GCS_BUCKET_NAME`, `BIGQUERY_CREDENTIALS_PATH`
