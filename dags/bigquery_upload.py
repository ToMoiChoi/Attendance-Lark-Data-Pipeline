"""
BigQuery Upload Module.

Uploads data to BigQuery via pandas or direct load jobs.
"""

import logging
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

logger = logging.getLogger(__name__)

class BigQueryUploader:
    """Upload data to Google BigQuery."""

    def __init__(self):
        """Initialize BigQuery client with credentials."""
        self.project_id = os.getenv("BIGQUERY_PROJECT_ID")
        self.dataset = os.getenv("BIGQUERY_DATASET")
        
        # We will attempt to connect either with GOOGLE_APPLICATION_CREDENTIALS
        # or a specific BIGQUERY_CREDENTIALS_PATH
        creds_path = os.getenv("BIGQUERY_CREDENTIALS_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        if not self.project_id or not self.dataset:
            raise ValueError("BIGQUERY_PROJECT_ID and BIGQUERY_DATASET must be set.")
            
        if creds_path and os.path.exists(creds_path):
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            self.client = bigquery.Client(
                project=self.project_id,
                credentials=credentials,
            )
        else:
            # Fallback to default credentials (e.g. Workload Identity) if no path is given
            self.client = bigquery.Client(project=self.project_id)
            
        logger.info(f"BigQuery uploader initialized for project: {self.project_id}, dataset: {self.dataset}")

    def upload_dataframe(self, df: pd.DataFrame, table_name: str, mode: str = "WRITE_TRUNCATE"):
        """
        Upload DataFrame to a BigQuery table.
        
        mode: "WRITE_TRUNCATE" to overwrite, "WRITE_APPEND" to append.
        """
        if df is None or df.empty:
            logger.info(f"DataFrame is empty, skipping upload for {table_name}.")
            return

        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        logger.info(f"Uploading {len(df)} rows to BigQuery table: {table_id} (mode={mode})")
        
        # We try to let BigQuery infer the schema
        job_config = bigquery.LoadJobConfig(
            write_disposition=mode,
            # Let BigQuery detect schema. If we strictly needed to specify string types for all, we could map them here.
            autodetect=True 
        )

        try:
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
            
            table = self.client.get_table(table_id)  # Make an API request
            logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
            
        except Exception as e:
            logger.error(f"Failed to load data into BigQuery: {e}")
            if hasattr(job, 'errors') and job.errors:
                 logger.error(f"Job errors: {job.errors}")
            raise
