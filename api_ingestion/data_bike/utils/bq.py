from google.cloud import bigquery
from google.auth import load_credentials_from_file
from api_ingestion.data_bike.utils.log_config import logger
import pandas as pd
import os


class BQConnector:
    """Simple BigQuery connector class for querying tables."""

    def __init__(self):
        """
        Initialize BigQuery client.
        """
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        project = os.getenv("PROJECT_ID")

        if not credentials_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
        if not project:
            raise ValueError("PROJECT_ID environment variable not set")

        # Load credentials from the OIDC configuration file
        credentials = load_credentials_from_file(credentials_path)[0]
        
        # --- CODE CHANGE HERE: Added location="EU" ---
        self.client = bigquery.Client(credentials=credentials, project=project, location="EU")

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        """
        try:
            df = self.client.query(query).to_dataframe()
            return df
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def get_table_schema(self, dataset_id: str, table_id: str) -> list:
        """
        Get table schema information.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)

        schema_info = []
        for field in table.schema:
            schema_info.append({
                'name': field.name,
                'field_type': field.field_type,
                'mode': field.mode,
                'description': field.description
            })

        return schema_info

    def execute_query(self, query: str) -> bigquery.QueryJob:
        """
        Execute a query and return the job (for non-SELECT operations).
        """
        try:
            logger.info(f"Executing query: {query}")
            job = self.client.query(query)
            job.result()  # Wait for completion
            return job
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    def create_table(self, dataset_name: str, table_name: str, column_dict: dict, exist_ok: bool = True) -> None:
        """
        Create a new table in BigQuery.
        """
        try:
            schema = [bigquery.SchemaField(name, field_type) for name, field_type in column_dict.items()]
            table_ref = self.client.dataset(dataset_name).table(table_name)
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table=table, exists_ok=exist_ok)
            logger.info(f"Table {dataset_name}.{table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise

    def load_dataframe_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_name: str,
        write_disposition: str = "WRITE_TRUNCATE",
        create_disposition: str = "CREATE_IF_NEEDED",
        null_marker: str = "NULL",
        max_bad_records: int = 0
    ) -> bigquery.LoadJob:
        """
        Advanced version with more control options.
        """
        job = None
        table_id = f"{dataset_name}.{table_name}"

        # Convert all columns to string with custom null handling
        df_string = df.copy()
        for column in df_string.columns:
            # Replace NaN/None with custom marker, then convert to string
            df_string[column] = df_string[column].fillna(null_marker).astype(str)

        # Create schema
        schema = []
        for column in df_string.columns:
            schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.STRING))

        if df_string.index.name is not None:
            schema.append(bigquery.SchemaField(df_string.index.name, bigquery.enums.SqlTypeNames.STRING))

        # Configure job with advanced options
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            max_bad_records=max_bad_records,
            ignore_unknown_values=True,
        )

        # Load data
        try:
            logger.info(f"Loading DataFrame to {table_id}")
            job = self.client.load_table_from_dataframe(df_string, table_id, job_config=job_config)
            job.result()
            table = self.client.get_table(table_id)
            logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

            return job

        except Exception as e:
            raise Exception(f"BigQuery load error: {e}")

    def merge_delta_data(
        self,
        dataset_name: str,
        table_name: str,
        join_keys: list,
        updated_date_col: str = None,
    ) -> bigquery.QueryJob:
        """
        Perform a MERGE operation between two tables.
        """
        main_table = f"{dataset_name}.{table_name}"
        delta_table = f"{dataset_name}.{table_name}_delta"
        table_column_details = self.get_table_schema(dataset_id=dataset_name, table_id=table_name)
        table_columns = [col["name"] for col in table_column_details]
        join_condition = " and ".join([f"main.{key} = delta.{key}" for key in join_keys])
        if updated_date_col:
            date_filter = f"main.{updated_date_col} < delta.{updated_date_col}"
        else:
            date_filter = "true"

        query = f"""
            merge `{main_table}` main
            using `{delta_table}` delta
            on {join_condition}
            when matched and {date_filter} then
                update set {', '.join([f"{col} = delta.{col}" for col in table_columns])}
            when not matched then
                insert ({', '.join([f"{col}" for col in table_columns])})
                values ({', '.join([f"delta.{col}" for col in table_columns])})
        """

        return self.execute_query(query)

    def create_table_from_existing(
        self,
        dataset_name: str,
        source_table: str,
        target_table: str,
        replace_if_exists: bool = False,
    ) -> bigquery.QueryJob:
        """
        Create a new table from an existing table.
        """
        if replace_if_exists:
            query = "create or replace table"

        else:
            query = "create table if not exists"

        query += f""" `{dataset_name}.{target_table}` as
            select * from `{dataset_name}.{source_table}`
            where false
        """

        return self.execute_query(query)

    def drop_table(self, dataset_name: str, table_name: str) -> None:
        """
        Drop a table in BigQuery.
        """
        try:
            table_ref = self.client.dataset(dataset_name).table(table_name)
            self.client.delete_table(table_ref, not_found_ok=True)
            logger.info(f"Table {dataset_name}.{table_name} dropped successfully.")

        except Exception as e:
            logger.error(f"Error dropping table {dataset_name}.{table_name}: {e}")
            raise