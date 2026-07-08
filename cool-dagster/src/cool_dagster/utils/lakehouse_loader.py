import dagster as dg
from dagster_aws.s3 import S3Resource
from dataclasses import dataclass
from typing import Any
from datetime import datetime

@dataclass
class BronzeAsset:
    bucket: str
    key: str
    file: bytes

def upload_bronze(context: dg.AssetExecutionContext, ozone_s3: S3Resource, asset: BronzeAsset) -> dg.MaterializeResult:    

        s3_client = ozone_s3.get_client()
        s3_client.put_object(Bucket=asset.bucket, Key=asset.key, Body=asset.file)

        return dg.MaterializeResult(
            metadata={
                "bucket": asset.bucket,
                "key": asset.key,
                "path": f"ofs://om/lakehouse/{asset.bucket}/{asset.key}",
                "size_bytes": len(asset.file),
            }
        )

def get_current_calmonth() -> tuple[str, str, str, str]:
    today = datetime.now().date()
    calmonth = today.strftime("%Y%m%d")
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    return calmonth, year, month, day