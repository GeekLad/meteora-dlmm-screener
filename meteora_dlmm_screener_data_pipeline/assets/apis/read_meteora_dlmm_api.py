from io import StringIO
import json
from typing import List, Optional, Tuple
import pandas as pd
from pydantic import BaseModel
import dagster as dg
from utils.fetch_utils import fetch_pages
import time

class TimeWindowStats(BaseModel):
    hour_1: float
    hour_2: float
    hour_4: float
    hour_12: float
    hour_24: float
    min_30: float

class DlmmPair(BaseModel):
    address: str
    apr: float
    apy: float
    base_fee_percentage: str
    bin_step: int
    cumulative_fee_volume: str
    cumulative_trade_volume: str
    current_price: float
    farm_apr: float
    farm_apy: float
    fee_tvl_ratio: TimeWindowStats
    fees: TimeWindowStats
    fees_24h: float
    hide: bool
    is_blacklisted: bool
    launchpad: Optional[str]
    liquidity: str
    max_fee_percentage: str
    mint_x: str
    mint_y: str
    name: str
    protocol_fee_percentage: str
    reserve_x: str
    reserve_x_amount: int
    reserve_y: str
    reserve_y_amount: int
    reward_mint_x: str
    reward_mint_y: str
    tags: List[str]
    today_fees: float
    trade_volume_24h: float
    volume: TimeWindowStats

class MeteoraDlmmApiResponse(BaseModel):
    pairs: List[DlmmPair]
    total: int

@dg.multi_asset(
    group_name="api_ingestion",
    outs={
        "meteora_dlmm_api_raw": dg.AssetOut(
            description="Raw JSON string from the Meteora DLMM API.",
            freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
        ),
        "meteora_dlmm_api": dg.AssetOut(
            description="Dataframe with data from the Meteora DLMM API.",
            freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
        ),
    }
)
async def read_meteora_dlmm_api(context: dg.AssetExecutionContext) -> Tuple[List[str], pd.DataFrame]:
    url = "https://dlmm-api.meteora.ag/pair/all_with_pagination"
    concurrency = 30
    retries = 10
    backoff_factor = 0.5
    df = None
    pages = []
    page = 0
    while True:
        # Prepare requests for concurrent fetching
        requests = [
            {
                "url": url, 
                "params": {"page": page + i, "limit": 100}, 
                "model": MeteoraDlmmApiResponse
            }
            for i in range(concurrency)
        ]
        # Fetch pages concurrently
        responses = await fetch_pages(
            requests, 
            read_json=False, 
            concurrency=concurrency, 
            retries=retries, 
            backoff_factor=backoff_factor
        )

        # Combine the pairs from all responses
        for resp in responses:
            json_data = json.loads(resp)
            new_df = pd.read_json(StringIO(json.dumps(json_data["pairs"])))
            df = pd.concat([df, new_df], ignore_index=True) if df is not None else new_df
            pages.append(resp)
        
        # Check if the last pair in the df has volume.hour_24 <= 0
        last_row_volume_hour_24 = df.iloc[-1]["volume"]["hour_24"]
        if last_row_volume_hour_24 == 0:
            break
        
        page += concurrency
    
    # Filter pairs with non-zero volume.hour_24
    df = df[df["volume"].apply(lambda x: x["hour_24"] > 0)]
    
    # Add created_at column with current unix timestamp
    df["created_at"] = int(time.time())

    context.add_output_metadata(
        {
            "num_pages_read": len(pages),
        },
        output_name="meteora_dlmm_api_raw"
    )
    context.add_output_metadata(
        {
            "num_pairs_with_volume": len(df),
        },
        output_name="meteora_dlmm_api_raw"
    )

    return pages, df