# test_brewery.py
import json
from data_bike.api.openbrewery.openbrewery_hook import OpenBreweryApiHook 
from typing import List, Dict

# Initialize the hook
brewery_hook = OpenBreweryApiHook()
print("🔄 Sending request to Open Brewery DB API...")

try:
    # We hit the /v1/breweries endpoint
    endpoint = "/v1/breweries"
    
    # Define parameters: fetch a few California breweries
    params = {
        "by_state": "california", 
        "per_page": 5             
    }

    # Perform a GET request. The hook returns the list of data directly on success.
    # If the request fails (e.g., 404, 500, connection error), the hook will raise an exception.
    data: List[Dict] = brewery_hook.run(endpoint, method="GET", params=params)

    # Check for success (data is a non-empty list)
    if data and isinstance(data, list):
        print("Connection successful!")
        print(f"Fetched {len(data)} breweries.")
        print("--- Sample Brewery Data ---")
        
        # Ensure the list is not empty before attempting to access the first element
        if len(data) > 0:
            print(json.dumps(data[0], indent=4))
        else:
            print("Fetched 0 breweries (Check filters or API availability).")
            
    else:
        # This block catches cases where the hook returned None or something unexpected 
        # without raising a proper exception.
        print("API Request Failed or returned empty/malformed data.")

except Exception as e:
    # This catches all connection errors and HTTP status code errors (>= 400) 
    # raised by the hook's internal logic.
    print(f"Connection failed: {e}")