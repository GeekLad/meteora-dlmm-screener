import pandas as pd
import dagster as dg

@dg.asset(
    description="All DLMM pairs.",
    group_name="load_data",
    io_manager_key="duckdb_pairs_io_manager",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
)
def pairs(
    meteora_dlmm_api: pd.DataFrame,
    tokens: pd.DataFrame,
) -> pd.DataFrame:
    # Map mint_x and mint_y to their token IDs
    result = meteora_dlmm_api.copy()
    mint_to_id = dict(zip(tokens['mint'], tokens['id']))
    result['mint_x_id'] = result['mint_x'].map(mint_to_id)
    result['mint_y_id'] = result['mint_y'].map(mint_to_id)

    return pd.DataFrame({
        'pair_address': result['address'],
        'name': result['name'],
        'mint_x_id': result['mint_x_id'],
        'mint_y_id': result['mint_y_id'],
        'bin_step': result['bin_step'],
        'base_fee_percentage': result['base_fee_percentage'].astype(float),
        'hide': result['hide'],
        'is_blacklisted': result['is_blacklisted'],
    })
