import dagster as dg
from dagster_aws.s3 import S3Resource
import subprocess
import json
from ..resources import BreweriesResource, LakehousePythonResource
from ...utils.lakehouse_loader import BronzeAsset, upload_bronze, get_current_calmonth


# ==== Bronze ====
@dg.asset(
    group_name="bronze",
    kinds={"python"},
    tags={"business_unit": "usa", "domain": "breweries"}
)
def staging_usa_breweries(context: dg.AssetExecutionContext, resource: BreweriesResource) -> list[dict]:
    breweries_data = resource.breweries(country="united_states")
    context.add_output_metadata({"num_breweries": len(breweries_data), "country": "united_states"})
    return breweries_data


@dg.asset(
    group_name="bronze",
    kinds={"python"},
    tags={"business_unit": "germany", "domain": "breweries"}
)
def staging_ger_breweries(context: dg.AssetExecutionContext, resource: BreweriesResource) -> list[dict]:
    breweries_data = resource.breweries(country="germany")
    context.add_output_metadata({"num_breweries": len(breweries_data), "country": "germany"})
    return breweries_data


def breweries_bronze_loading(country: str, upstream_asset_name: str):
    @dg.asset(
        name=f"bronze_{country}_breweries",
        ins={"breweries_data": dg.AssetIn(upstream_asset_name)},
        group_name="bronze",
        kinds={"python", "s3"},
        tags={"business_unit": country, "domain": "breweries"}
    )
    def _upload(context: dg.AssetExecutionContext, ozone_s3: S3Resource,
                                                    breweries_data: list[dict]) -> dg.MaterializeResult:
        bucket = "bronze"

        _, year, month, day = get_current_calmonth()

        key = f"breweries/{country}/{year}/{month}/{day}/breweries.json"        
        file_bytes = json.dumps(breweries_data).encode("utf-8")
        
        asset = BronzeAsset(bucket=bucket, key=key, file=file_bytes)
        return upload_bronze(context=context, ozone_s3=ozone_s3, asset=asset)

    return _upload


bronze_usa_breweries = breweries_bronze_loading("usa", "staging_usa_breweries")
bronze_ger_breweries = breweries_bronze_loading("germany", "staging_ger_breweries")    


#  ==== Silver ====
SILVER_SCRIPT = dg.file_relative_path(
    __file__,
    "../../../../../lakehouse/transforms/silver/glb/breweries/silver_breweries.py",
)

@dg.asset(
    deps=[bronze_usa_breweries, bronze_ger_breweries],
    group_name="silver",
    kinds={"python"},
    tags={"business_unit": "global", "domain": "breweries"}
)
def silver_breweries(context: dg.AssetExecutionContext, lakehouse_python: LakehousePythonResource) -> dg.MaterializeResult:
    result = subprocess.run(
        [lakehouse_python.python_path, SILVER_SCRIPT],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"silver_breweries script failed: {result.stderr}")

    return dg.MaterializeResult(metadata={"stdout_tail": result.stdout[-2000:]})