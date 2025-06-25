import dagster as dg

class DuckDBPairsIOManager(dg.IOManager):
    def handle_output(self, context, obj):
        with context.resources.duckdb.get_connection() as con:
            con.register("pairs_df", obj)
            new_pairs_added = con.execute("""
                SELECT count(*) FROM pairs_df
                WHERE pair_address NOT IN (SELECT pair_address FROM pairs)
            """).fetchone()[0]
            con.execute("""
                INSERT INTO pairs (
                    pair_address,
                    name,
                    mint_x_id,
                    mint_y_id,
                    bin_step,
                    base_fee_percentage,
                    hide,
                    is_blacklisted
                )
                SELECT
                    pair_address,
                    name,
                    mint_x_id,
                    mint_y_id,
                    bin_step,
                    base_fee_percentage,
                    hide,
                    is_blacklisted
                FROM pairs_df
                WHERE pair_address NOT IN (SELECT pair_address FROM pairs)
            """)
            total_pairs = con.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]
            sample_df = con.execute("SELECT * FROM pairs LIMIT 5").fetchdf()
            context.add_output_metadata({
                "sample_rows": dg.MetadataValue.md(sample_df.head().to_markdown(index=False)),
                "new_pairs_added": new_pairs_added,
                "total_pairs": total_pairs
            })


    def load_input(self, context):
        with context.resources.duckdb.get_connection() as con:
            df = con.execute("SELECT * FROM pairs").fetchdf()
            return df

@dg.io_manager(required_resource_keys={"duckdb"})
def duckdb_pairs_io_manager():
    return DuckDBPairsIOManager()

