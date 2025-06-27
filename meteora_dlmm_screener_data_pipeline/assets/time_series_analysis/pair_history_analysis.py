import pandas as pd
import dagster as dg

@dg.asset(
    description="Pair history time series analysis",
    group_name="times_series_analysis",
    io_manager_key="duckdb_pandas_io_manager",
    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=1),
    automation_condition=dg.AutomationCondition.eager(),
)
def pair_history_analysis(pair_history: pd.DataFrame) -> pd.DataFrame:
    # Ensure 'created_at' is a datetime
    df = pair_history.copy()
    df['created_at'] = pd.to_datetime(df['created_at'])
    # Set index to pair_address and 1-minute rounded created_at
    df['created_at_minute'] = df['created_at'].dt.floor('T')
    # Set multi-index
    df = df.set_index(['pair_id', 'created_at_minute'])

    # Forward fill missing values
    df = df.groupby('pair_id').ffill()

    # Reset index to have pair_id and created_at_minute as columns
    df = df.reset_index()
    # Drop the original 'created_at' column
    df = df.drop(columns=['created_at'])
    # Rename 'created_at_minute' to 'time'
    df = df.rename(columns={'created_at_minute': 'time'})

    # Take the last 24 hours of data
    df = df[df['time'] >= (df['time'].max() - pd.Timedelta(days=1))]
    
    # Calculate the trade volume and fee volume by taking the difference of the prior value
    df['trade_volume'] = df.groupby('pair_id')['cumulative_trade_volume'].diff()
    df['fee_volume'] = df.groupby('pair_id')['cumulative_fee_volume'].diff()
    
    # Drop the cumulative columns as they are no longer needed
    df = df.drop(columns=['cumulative_trade_volume', 'cumulative_fee_volume'])

    # Function for the display time for a window in minutes
    def display_time_for_window(window):
        if window < 60:
            return f"{window}m"
        if window < 1440:
            return f"{window // 60}h"
        return f"{window // 1440}d"

    # Calculate the rolling metrics for different time windows
    for window in [5, 15, 30, 60, 60*6, 60*12, 60*24]:
        # Column names
        pct_minutes_with_volume = f'pct_minutes_with_volume_{display_time_for_window(window)}'
        liquidity = f'liquidity_{display_time_for_window(window)}_avg'
        fees = f'fees_{display_time_for_window(window)}_avg'
        fee_to_tvl = f'24h_fee_to_tvl_{display_time_for_window(window)}_avg'
        geek_24h_fee_to_tvl = f'geek_24h_fee_to_tvl_{display_time_for_window(window)}_avg'

        # Calculate the percentage of minutes with volume
        df[pct_minutes_with_volume] = df.groupby('pair_id')['trade_volume'].transform(
            lambda x: (x.rolling(window=window, min_periods=window)
                .apply(lambda w: (w > 0).sum(), raw=True) / window) * 100
        )
        
        # Liquidity rolling average
        df[liquidity] = df.groupby('pair_id')['liquidity'].transform(
            lambda x: x.rolling(window=window, min_periods=window).mean()
        )

        # Total fees rolling average
        df[fees] = df.groupby('pair_id')['fee_volume'].transform(
            lambda x: x.rolling(window=window, min_periods=window).sum()
        )

        # Projected 24h fees
        df[fee_to_tvl] = df[fees] * 100 * 1440 / window / df[liquidity]

        # Calculate the standard deviation of liquidity over the same window
        liquidity_std_dev = df.groupby('pair_id')['liquidity'].transform(
            lambda x: x.rolling(window=window, min_periods=window).std()
        )

        # Geek fee to TVL ratio by adding in the liquidity standard deviation
        geek_fee_to_tvl = df[fees] / (df[liquidity] + liquidity_std_dev)
    
        # Geek 24h fee to TVL ratio
        df[geek_24h_fee_to_tvl] = geek_fee_to_tvl * 100 * 1440 / window

    df = df.dropna(subset=['trade_volume', 'fee_volume'])
    return df