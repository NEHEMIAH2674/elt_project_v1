# openbrewery_hook.py
from api_ingestion.data_bike.utils.api_hook import BaseApiHook


class OpenBreweryApiHook(BaseApiHook):
    """
    Hook for connecting to the Open Brewery DB API (public and unauthenticated).
    """

    def __init__(self, **kwargs):
        # 1. Define the base URL (Host).
        host = "https://api.openbrewerydb.org" 
        
        # 2. Set basic headers for JSON response.
        headers = {
            "Accept": "application/json",
        }

        # 3. Initialize the BaseApiHook.
        super().__init__(host=host, headers=headers, **kwargs)