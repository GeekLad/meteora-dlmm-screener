import dagster as dg
from typing import List

@dg.asset(
    description="Archive the returned API data in a jsonl.gz file.",
    group_name="archive_data",
    io_manager_key="archive_json_io_manager",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
    automation_condition=dg.AutomationCondition.eager(),
)
async def archive_dlmm_api_data(meteora_dlmm_api_raw: List[str]) -> List[str]:
    return meteora_dlmm_api_raw