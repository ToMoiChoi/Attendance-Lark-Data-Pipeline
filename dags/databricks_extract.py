"""
Databricks Extract Module.

Extracts data from Databricks tables into pandas DataFrames.
"""

import logging
import pandas as pd
from databricks import sql as databricks_sql
import os

logger = logging.getLogger(__name__)

class DatabricksExtractor:
    """Extract data from Databricks."""

    def __init__(self):
        """Initialize Databricks connection settings."""
        self.host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.catalog = os.getenv("DATABRICKS_CATALOG", "main")
        self.schema = os.getenv("DATABRICKS_SCHEMA", "default")
        
        if not self.host or not self.token or not self.http_path:
            raise ValueError("Missing required Databricks credentials in environment variables.")

        logger.info(f"Databricks extractor initialized for catalog.schema: {self.catalog}.{self.schema}")

    def get_connection(self):
        return databricks_sql.connect(
            server_hostname=self.host.replace("https://", ""),
            http_path=self.http_path,
            access_token=self.token,
        )

    def extract_table(self, table_name: str) -> pd.DataFrame:
        """
        Extract full table data as pandas DataFrame from Databricks.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        logger.info(f"Extracting data from Databricks table: {full_table_name}")
        
        query = f"SELECT * FROM {full_table_name}"
        
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(query)
                    
                    # Optimization: Use Arrow for much faster and memory-efficient extraction
                    # compared to cursor.fetchall()
                    arrow_table = cursor.fetchall_arrow()
                    
                    if arrow_table is None or arrow_table.num_rows == 0:
                        logger.info(f"Table {full_table_name} is empty.")
                        # Get columns just in case to return empty df with schema
                        columns = [desc[0] for desc in cursor.description]
                        return pd.DataFrame(columns=columns)
                    
                    df = arrow_table.to_pandas()
                    logger.info(f"Successfully extracted {len(df)} rows from {full_table_name}")
                    return df
                except Exception as e:
                    logger.error(f"Error fetching from Databricks: {e}")
                    raise
