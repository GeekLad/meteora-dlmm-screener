from io import StringIO
import json
from typing import List, Optional
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

@dg.asset(
    description="Download DLMM pairs from Meteora API as pandas dataframe.",
    group_name="api_ingestion",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
)
async def meteora_dlmm_api(context: dg.AssetExecutionContext) -> pd.DataFrame:
    url = "https://dlmm-api.meteora.ag/pair/all_with_pagination"
    concurrency = 30
    retries = 10
    backoff_factor = 0.5
    all_pairs = []
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
            read_json=True, 
            concurrency=concurrency, 
            retries=retries, 
            backoff_factor=backoff_factor
        )

        # Combine the pairs from all responses
        page_pairs = []
        for resp in responses:
            if hasattr(resp, 'pairs'):
                page_pairs.extend(resp.pairs)
        if not page_pairs:
            break
        all_pairs.extend(page_pairs)
        
        # Check if the last pair in the last response has volume.min_30 <= 0
        last_pair = page_pairs[-1]
       
        # If the last pair's volume.min_30 is 0, stop fetching more pages
        if getattr(getattr(last_pair, 'volume', None), 'min_30', 1) == 0:
            break
        page += concurrency
    
    # Create a DataFrame from the list of DlmmPair objects
    pairs_list = [pair.model_dump() for pair in all_pairs]
    json_string = json.dumps(pairs_list)
    pairs_df = pd.read_json(StringIO(json_string))

    # Filter pairs with non-zero volume.min_30 and convert to DataFrame
    pairs_df = pairs_df[pairs_df["volume"].apply(lambda x: x["min_30"] > 0)]
    
    # Drop duplicates based on address
    pairs_df = pairs_df.drop_duplicates(subset=["address"])
    
    # Add created_at column with current unix timestamp
    pairs_df["created_at"] = int(time.time())

    context.add_output_metadata(
        {
            "num_pairs_with_volume": len(pairs_df),
        }
    )

    return pairs_df