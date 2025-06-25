from dotenv import load_dotenv
import os
import dagster as dg
from dagster_duckdb import DuckDBResource

# API import
from meteora_dlmm_screener_data_pipeline.assets.apis.meteora_dlmm_api import meteora_dlmm_api

# API data archiving
from meteora_dlmm_screener_data_pipeline.assets.archive_data.archive_dlmm_api_data import archive_dlmm_api_data

# Build database assets
from meteora_dlmm_screener_data_pipeline.assets.build_database.create_tables_and_views import create_tables_and_views

# Load data assets
from meteora_dlmm_screener_data_pipeline.assets.load_data.pairs import pairs
from meteora_dlmm_screener_data_pipeline.assets.load_data.tokens import tokens
from meteora_dlmm_screener_data_pipeline.assets.load_data.pair_history import pair_history

# IO Managers
from meteora_dlmm_screener_data_pipeline.resources.archive_data.archive_parquet_io_manager import archive_parquet_io_manager
from meteora_dlmm_screener_data_pipeline.resources.load_data.duckdb_pairs_io_manager import duckdb_pairs_io_manager
from meteora_dlmm_screener_data_pipeline.resources.load_data.duckdb_tokens_io_manager import duckdb_tokens_io_manager
from meteora_dlmm_screener_data_pipeline.resources.load_data.duckdb_pair_history_io_manager import duckdb_pair_history_io_manager

# Load environment variables from .env file
load_dotenv()

# Set a global data path for IO managers, defaulting to './data' if not set in environment
DATA_PATH = os.getenv("DATA_PATH", "./data").rstrip("/")

# Ensure the data directory exists
os.makedirs(DATA_PATH, exist_ok=True)

# Define a job to refresh all assets
refresh_job = dg.define_asset_job(
    "refresh_job",
    selection=[
        "meteora_dlmm_api",
        "archive_dlmm_api_data",
        "tokens",
        "pairs",
        "pair_history",
    ],
)

# Schedule the job to run every minute
refresh_schedule = dg.ScheduleDefinition(
    job=refresh_job,
    cron_schedule="* * * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    execution_timezone="America/New_York",
)

# Sensor to create tables on startup if they do not exist
create_table_sensor = dg.AutomationConditionSensorDefinition(
    name="create_table_startup_sensor",
    target=dg.AssetSelection.keys(
        "create_tables_and_views",
    ),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        meteora_dlmm_api,
        archive_dlmm_api_data,
        create_tables_and_views,
        tokens,
        pairs,
        pairs,
        pair_history,
    ],
    resources={
        "duckdb": DuckDBResource(
            database=DATA_PATH+"/meteora_dlmm_screener.duckdb",
        ),
        "duckdb_tokens_io_manager": duckdb_tokens_io_manager,
        "duckdb_pairs_io_manager": duckdb_pairs_io_manager,
        "duckdb_pair_history_io_manager": duckdb_pair_history_io_manager,
        "archive_parquet_io_manager": archive_parquet_io_manager.configured({
            "data_path": DATA_PATH,
        })
    },
    jobs=[refresh_job],
    schedules=[refresh_schedule],
    sensors=[create_table_sensor],
)