import requests
import dagster as dg
from dagster import load_assets_from_modules
from dagster_aws.s3 import S3Resource
import os
from .assets import (
    breweries
)
from .jobs.breweries_job import breweries_job


class LakehousePythonResource(dg.ConfigurableResource):
    python_path: str = os.environ["LAKEHOUSE_PYTHON_PATH"]
    
class BreweriesResource(dg.ConfigurableResource):
    country: str = "united_states"
    items_per_page: int = 20

    def query_string(self, country: str, items_per_page: int = 20) -> str:
        return f"https://api.openbrewerydb.org/v1/breweries?by_country={country}&per_page={items_per_page}"

    def breweries(self, country: str | None = None,
                  items_per_page: int | None = None) -> list[dict]:
        country = country or self.country
        items_per_page = items_per_page or self.items_per_page
        data = requests.get(
            self.query_string(country, items_per_page),
            timeout=5
        ).json()
        return data


ozone_s3_resource = S3Resource(   # region and aws params arent needed for localhost for Ozone, but boto3 requires *something*
    endpoint_url="http://localhost:9878",
    aws_access_key_id="anything",
    aws_secret_access_key="anything",
    region_name="us-east-1",
)

# Load all assets from the asset module
all_assets = load_assets_from_modules([breweries])

# Load all jobs from the jobs module
all_jobs = [breweries_job]


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        assets=all_assets,
        jobs=all_jobs,
        resources={
            "resource": BreweriesResource(),
            "lakehouse_python": LakehousePythonResource(),
            "ozone_s3": ozone_s3_resource
        }
    )