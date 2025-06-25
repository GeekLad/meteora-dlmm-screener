import dagster as dg
import pandas as pd
from meteora_dlmm_screener_data_pipeline.assets.apis.meteora_dlmm_api import meteora_dlmm_api

@dg.asset(
    description="Archive the DLMM API data in a parquet file.",
    group_name="archive_data",
    deps=[meteora_dlmm_api],
    io_manager_key="archive_parquet_io_manager",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
)
async def archive_dlmm_api_data(meteora_dlmm_api: pd.DataFrame) -> pd.DataFrame:
    return meteora_dlmm_api