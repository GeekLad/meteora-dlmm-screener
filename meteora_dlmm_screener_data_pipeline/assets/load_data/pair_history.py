import pandas as pd
import dagster as dg

@dg.asset(
    description="History of liquidity, volume, fees, and prices for each DLMM pair.",
    group_name="load_data",
    io_manager_key="duckdb_pair_history_io_manager",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
)
def pair_history(
    meteora_dlmm_api: pd.DataFrame,
    pairs: pd.DataFrame,
) -> pd.DataFrame:
    # Map pair IDs to their addresses
    pair_id_to_address = dict(zip(pairs['pair_address'], pairs['id']))
    
    # Convert 'created_at' from Unix timestamp to pandas timestamp
    created_at_ts = pd.to_datetime(meteora_dlmm_api['created_at'], unit='s')

    # Create a DataFrame for pair history
    return pd.DataFrame({
        'created_at': created_at_ts,
        'pair_id': meteora_dlmm_api['address'].map(pair_id_to_address),
        'price': meteora_dlmm_api['current_price'],
        'liquidity': meteora_dlmm_api['liquidity'],
        'cumulative_trade_volume': meteora_dlmm_api['cumulative_trade_volume'],
        'cumulative_fee_volume': meteora_dlmm_api['cumulative_fee_volume'],
    })