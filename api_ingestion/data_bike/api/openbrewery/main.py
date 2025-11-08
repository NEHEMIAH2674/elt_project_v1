import json
import os
import pandas as pd
from api_ingestion.data_bike.api.openbrewery.openbrewery_operator import OpenBreweryOperator
from ...utils.bq import BQConnector
from datetime import datetime, UTC 

OUTPUT_FILE = os.path.join(os.getcwd(), 'raw_breweries.json')

DATASET = "operations"  # The BigQuery Dataset ID
TABLE = "raw_breweries" # The BigQuery Table ID (Must match your dbt source YAML)


def run_ingestion():  # <-- REMOVED 'state' parameter
    operator = OpenBreweryOperator()
    
    # CALL THE OPERATOR WITHOUT THE 'state' FILTER
    # The operator now handles fetching all pages/states internally.
    breweries_data = operator.fetch_breweries(per_page=50) # Use a lower per_page for better API behavior

    if not breweries_data:
        print("❌ No data fetched or API returned an error.")
        return

    # Update log message to reflect fetching ALL data
    print(f"✅ Fetched a total of {len(breweries_data)} breweries.") 

    # Convert to DataFrame and add timestamp
    df = pd.DataFrame(breweries_data)
    df["fetched_at"] = datetime.now(UTC) 

    # Save locally (optional)
    df.to_json(OUTPUT_FILE, orient="records", indent=4)
    print(f"💾 Saved locally to {OUTPUT_FILE}")

    # Initialize BQ Connector
    bq = BQConnector()

    # Load DataFrame
    # IMPORTANT: Use WRITE_TRUNCATE here if you want a fresh snapshot of ALL breweries 
    # every time you run the script, which is the standard ELT practice.
    bq.load_dataframe_to_table(
        df=df,
        table_name=TABLE,
        dataset_name=DATASET,
        write_disposition="WRITE_TRUNCATE" # <-- CHANGED to replace the entire table
    )

    print(f"Data successfully loaded into {DATASET}.{TABLE}")


if __name__ == "__main__":
    # Call the function without arguments
    run_ingestion()