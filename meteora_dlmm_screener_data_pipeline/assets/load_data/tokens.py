import dagster as dg
import pandas as pd

@dg.asset(
    description="All tokens that are in DLMM pairs.",
    group_name="load_data",
    io_manager_key="duckdb_tokens_io_manager",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
)
def tokens(
    meteora_dlmm_api: pd.DataFrame,
) -> pd.DataFrame:
    # Parse names for each mint
    def parse_names(row):
        name_x, name_y = row["name"].rsplit("-", 1)
        return pd.Series({"name_x": name_x, "name_y": name_y})

    names = meteora_dlmm_api.apply(parse_names, axis=1)
    tokens_x = pd.DataFrame({
        "mint": meteora_dlmm_api["mint_x"],
        "symbol": names["name_x"]
    })
    tokens_y = pd.DataFrame({
        "mint": meteora_dlmm_api["mint_y"],
        "symbol": names["name_y"]
    })
    tokens_df = pd.concat([tokens_x, tokens_y], ignore_index=True).drop_duplicates().reset_index(drop=True)
    return tokens_df
