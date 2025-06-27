import os
import gzip
import json
from typing import List
import dagster as dg
from datetime import datetime

class ArchiveJsonIOManager(dg.IOManager):
    def __init__(self, data_path: str = "."):
        self.data_path = data_path

    def handle_output(self, context: dg.OutputContext, obj: List[str]):
        dt_str = datetime.now().strftime('%Y_%m_%d')
        time_str = datetime.now().strftime('%H_%M')
        jsonl_filename = f"archive_{dt_str}.jsonl.gz"
        jsonl_path = os.path.join(self.data_path, jsonl_filename)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

        # Write each JSON string as a line with timestamp
        with gzip.open(jsonl_path, "at", encoding="utf-8") as f:
            for json_str in obj:
                f.write(f'{{"timestamp": "{dt_str}T{time_str}:00", "data": {json_str}}}\n')

    def load_input(self, context: dg.InputContext) -> list:
        dt_str = datetime.now().strftime('%Y_%m_%d')
        jsonl_filename = context.resource_config.get("file_name", f"archive_{dt_str}.jsonl.gz") if context.resource_config else f"archive_{dt_str}.jsonl.gz"
        jsonl_path = os.path.join(self.data_path, jsonl_filename)
        json_objects = []
        with gzip.open(jsonl_path, "rt", encoding="utf-8") as f:
            for line in f:
                json_objects.append(json.loads(line))
        return json_objects

@dg.io_manager(config_schema={"data_path": str}, required_resource_keys={"duckdb"})
def archive_json_io_manager(init_context: dg.InputContext) -> ArchiveJsonIOManager:
    data_path = init_context.resource_config.get("data_path", "./data").rstrip("/")
    return ArchiveJsonIOManager(data_path=data_path)