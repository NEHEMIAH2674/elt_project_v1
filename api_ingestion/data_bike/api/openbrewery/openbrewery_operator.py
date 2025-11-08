# openbrewery_operator.py
from .openbrewery_hook import OpenBreweryApiHook
import time # Import time for rate limit management (good practice)

class OpenBreweryOperator:
    """
    Operator responsible for fetching data from the Open Brewery DB.
    """
    def __init__(self):
        # Instantiate the hook
        self.hook = OpenBreweryApiHook()
        # Ensure the endpoint is correct for the API version you are using
        self.endpoint = "/v1/breweries" 
        
    def fetch_breweries(self, per_page: int = 50) -> list: # Removed 'state' parameter
        """
        Fetches ALL breweries across all available pages.
        
        Args:
            per_page (int): Number of results per page (max 200). Using 50 is safer.
            
        Returns:
            list: A list of all brewery dictionaries.
        """
        all_breweries = []
        page = 1
        
        print("tarting paginated fetch for ALL breweries...")
        
        while True:
            # 1. Build request parameters for the current page
            params = {
                "page": page,
                "per_page": per_page,
            }
            
            # 2. Execute the request using the hook
            try:
                # Use the hook to run the request. data_keypath=None because the response is a list.
                # Assumes your hook.run() or hook.get() handles the request.
                page_data = self.hook.run(self.endpoint, method="GET", params=params) 
                
            except Exception as e:
                print(f" Error on page {page}. Stopping fetch: {e}")
                break
            
            # 3. Check for stop condition: No data means end of results
            if not page_data:
                print(f"Reached end of data at page {page}. Stopping.")
                break
                
            # 4. Accumulate data and prepare for the next loop
            all_breweries.extend(page_data)
            print(f"   -> Fetched {len(page_data)} records on page {page}. Total so far: {len(all_breweries)}")
            page += 1
            
            # 5. Respect Rate Limiting (Optional but Recommended)
            # time.sleep(0.5) 
            
        return all_breweries