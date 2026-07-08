import dagster as dg

breweries_job = dg.define_asset_job(
    name="breweries_job",
    selection='tag:"domain"="breweries"',
    tags={"domain": "breweries"}
)