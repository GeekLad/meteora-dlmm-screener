from dotenv import load_dotenv
import os
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

# API import
from meteora_dlmm_screener_data_pipeline.assets.apis.read_meteora_dlmm_api import read_meteora_dlmm_api

# API data archiving
from meteora_dlmm_screener_data_pipeline.assets.archive_data.archive_dlmm_api_data import archive_dlmm_api_data

# Build database assets
from meteora_dlmm_screener_data_pipeline.assets.build_database.create_tables_and_views import create_tables_and_views

# Load data assets
from meteora_dlmm_screener_data_pipeline.assets.load_data.pairs import pairs
from meteora_dlmm_screener_data_pipeline.assets.load_data.tokens import tokens
from meteora_dlmm_screener_data_pipeline.assets.load_data.pair_history import pair_history

# Time series analysis
from meteora_dlmm_screener_data_pipeline.assets.time_series_analysis.pair_history_analysis import pair_history_analysis

# IO Managers
from meteora_dlmm_screener_data_pipeline.resources.archive_data.json_archive_io_manager import archive_json_io_manager
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
        "meteora_dlmm_api_raw",
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

# Sensor to update the pair history analysis asset
pair_history_sensor = dg.AutomationConditionSensorDefinition(
    name="pair_history_sensor",
    target=dg.AssetSelection.keys(
        "pair_history_analysis",
    ),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

# Sensor to archive data
archive_dlmm_api_data_sensor = dg.AutomationConditionSensorDefinition(
    name="archive_dlmm_api_data_sensor",
    target=dg.AssetSelection.keys(
        "archive_dlmm_api_data",
    ),
    default_status=dg.DefaultSensorStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        create_tables_and_views,
        read_meteora_dlmm_api,
        tokens,
        pairs,
        pairs,
        pair_history,
        archive_dlmm_api_data,
        pair_history_analysis,
    ],
    resources={
        "duckdb": DuckDBResource(
            database=os.path.join(DATA_PATH, "meteora_dlmm_screener.duckdb"),
        ),
        "duckdb_tokens_io_manager": duckdb_tokens_io_manager,
        "duckdb_pairs_io_manager": duckdb_pairs_io_manager,
        "duckdb_pair_history_io_manager": duckdb_pair_history_io_manager,
        "duckdb_pandas_io_manager": DuckDBPandasIOManager(
            database=os.path.join(DATA_PATH, "meteora_dlmm_screener.duckdb"),
            schema="main",
        ),
        "archive_json_io_manager": archive_json_io_manager.configured({
            "data_path": os.path.join(DATA_PATH, "archive"),
        })
    },
    jobs=[refresh_job],
    schedules=[refresh_schedule],
    sensors=[create_table_sensor, archive_dlmm_api_data_sensor, pair_history_sensor],
)