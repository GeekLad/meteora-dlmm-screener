import dagster as dg

class DuckDBPairHistoryIOManager(dg.IOManager):
    def handle_output(self, context, obj):
        with context.resources.duckdb.get_connection() as con:
            con.register("pair_history_df", obj)
            con.execute("""
                INSERT INTO pair_history (
                    created_at,
                    pair_id,
                    price,
                    liquidity,
                    cumulative_trade_volume,
                    cumulative_fee_volume
                )
                SELECT
                    created_at,
                    pair_id,
                    price,
                    liquidity,
                    cumulative_trade_volume,
                    cumulative_fee_volume
                FROM pair_history_df
            """)
            total_pair_history = con.execute("SELECT COUNT(*) FROM pair_history").fetchone()[0]
            sample_df = con.execute("SELECT * FROM pair_history_df LIMIT 5").fetchdf()
            context.add_output_metadata({
                "sample_rows": dg.MetadataValue.md(sample_df.head().to_markdown(index=False)),
                "new_pair_history_added": len(obj) if not obj.empty else 0,
                "total_pair_history": total_pair_history,
            })

    def load_input(self, context):
        with context.resources.duckdb.get_connection() as con:
            df = con.execute("SELECT * FROM pair_history").fetchdf()
            return df

@dg.io_manager(required_resource_keys={"duckdb"})
def duckdb_pair_history_io_manager():
    return DuckDBPairHistoryIOManager()

