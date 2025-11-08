from datetime import datetime
from api_ingestion.data_bike.utils.general_utils import resolve_nested_key
from api_ingestion.data_bike.utils.log_config import logger
from requests.auth import AuthBase
from requests.exceptions import RequestException, HTTPError
from dotenv import load_dotenv
import inspect
import json
import os
import requests
import time
from typing import Any, Mapping, Optional

class BaseApiHook:
    """Reusable API base class for CoreInsights ETL."""

    def __init__(
        self,
        host: str,
        auth: AuthBase | tuple | None = None,
        headers: dict | None = None,
        max_retries: int = 3,
        backoff_factor: int = 2,
        wait_time: int | None = None,
        **kwargs
    ):
        self.host = host.rstrip("/")
        self.auth = auth
        self.headers = headers or {}
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.wait_time = wait_time
        self.session = requests.Session()
        # Add headers to the session for persistence
        self.session.headers.update(self.headers)
        self.kwargs = kwargs

    # ---------------------------
    # Core request handling logic (Kept as is - it's very robust!)
    # ---------------------------
    def _request_with_retries(
        self,
        method: str,
        url: str,
        data_keypath: str | None = None,
        output_type: str = "json",
        **kwargs
    ):
        retries = 0
        backoff = 1

        while retries < self.max_retries:
            try:
                # Use request_kwargs from the kwargs passed to run/get/post
                response = self.session.request(
                    method, url, auth=self.auth, **kwargs
                )
                
                # Check for rate limiting errors (often handled with 429 status code)
                if response.status_code == 429 and retries < self.max_retries:
                    raise RequestException(f"Rate limited by API: {url}")
                
                response.raise_for_status()

                # Handle output formats
                if output_type == "json":
                    try:
                        json_response = response.json()
                        data = resolve_nested_key(json_response, data_keypath)
                    except json.decoder.JSONDecodeError:
                        logger.error(f"JSON decode failed for URL {url}")
                        data = None
                elif output_type == "text":
                    data = response.text
                elif output_type == "response": # New option to return the raw response object
                    return response
                else:
                    raise AttributeError(f"Unsupported output type '{output_type}'")

                return data

            except RequestException as e:
                retries += 1
                if retries >= self.max_retries:
                    logger.error(f"Request failed after {retries} attempts: {e}")
                    raise
                logger.warning(f"Retrying {method.upper()} {url} in {backoff} sec...")
                time.sleep(self.wait_time or backoff)
                backoff *= self.backoff_factor

    # ---------------------------
    # Public Methods for HTTP Verbs (Refined and Added)
    # ---------------------------
    
    def _get_url(self, endpoint: str) -> str:
        """Helper to combine host and endpoint safely."""
        return f"{self.host}/{endpoint.strip('/')}"
    
    def get(self, endpoint: str, data_keypath: str | None = None, output_type: str="json", **kwargs) -> Any:
        url = self._get_url(endpoint)
        return self._request_with_retries("GET", url, data_keypath, output_type, **kwargs)

    def post(self, endpoint: str, data: dict | None = None, json: dict | None = None, data_keypath: str | None = None, output_type: str="json", **kwargs) -> Any:
        url = self._get_url(endpoint)
        return self._request_with_retries("POST", url, data_keypath, output_type, data=data, json=json, **kwargs)

    def put(self, endpoint: str, data: dict | None = None, json: dict | None = None, data_keypath: str | None = None, output_type: str="json", **kwargs) -> Any:
        url = self._get_url(endpoint)
        return self._request_with_retries("PUT", url, data_keypath, output_type, data=data, json=json, **kwargs)

    def delete(self, endpoint: str, data_keypath: str | None = None, output_type: str="json", **kwargs) -> Any:
        url = self._get_url(endpoint)
        return self._request_with_retries("DELETE", url, data_keypath, output_type, **kwargs)
        
    # ---------------------------
    # Public Convenience Method (The 'run' method you originally wanted)
    # ---------------------------
    
    def run(self, endpoint: str, method: str = "GET", data_keypath: str | None = None, output_type: str="json", **kwargs) -> Any:
        """
        Generic method to run an API request by delegating to the appropriate verb method.
        """
        method = method.upper()
        
        if method == "GET":
            return self.get(endpoint, data_keypath, output_type, **kwargs)
        elif method == "POST":
            return self.post(endpoint, data_keypath=data_keypath, output_type=output_type, **kwargs)
        elif method == "PUT":
            return self.put(endpoint, data_keypath=data_keypath, output_type=output_type, **kwargs)
        elif method == "DELETE":
            return self.delete(endpoint, data_keypath=data_keypath, output_type=output_type, **kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")