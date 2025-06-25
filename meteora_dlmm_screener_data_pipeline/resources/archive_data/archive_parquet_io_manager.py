import os
from datetime import date
import pandas as pd
import dagster as dg

class ArchiveParquetIOManager(dg.IOManager):
    def __init__(self, data_path: str = "."):
        self.data_path = data_path

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame):
        file_name = f"archive_{date.today().strftime('%Y_%m_%d')}.parquet"
        file_path = os.path.join(self.data_path, file_name)
        with context.resources.duckdb.get_connection() as con:
            if os.path.exists(file_path):
                # Read existing data
                existing_df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
                combined_df = pd.concat([existing_df, obj], ignore_index=True)
            else:
                combined_df = obj
            # Ensure all columns are supported types and handle missing/nulls
            con.register("df", combined_df)
            con.execute(f"COPY df TO '{file_path}' (FORMAT 'parquet', OVERWRITE 1);")
            con.unregister("df")

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        file_name = context.resource_config.get("file_name", "archive.parquet") if context.resource_config else "archive.parquet"
        file_path = os.path.join(self.data_path, file_name)
        with self.duckdb.get_connection() as con:
            df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
        return df

@dg.io_manager(config_schema={"data_path": str}, required_resource_keys={"duckdb"})
def archive_parquet_io_manager(init_context: dg.InputContext) -> ArchiveParquetIOManager:
    data_path = init_context.resource_config.get("data_path", "./data").rstrip("/")
    return ArchiveParquetIOManager(data_path=data_path)