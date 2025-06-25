import dagster as dg

class DuckDBTokensIOManager(dg.IOManager):
    def handle_output(self, context, obj):
        with context.resources.duckdb.get_connection() as con:
            con.register("tokens_df", obj)
            new_tokens_added = con.execute("""
                SELECT count(*) FROM tokens_df
                WHERE mint NOT IN (SELECT mint FROM tokens)
            """).fetchone()[0]
            con.execute("""
                INSERT INTO tokens (mint, symbol)
                SELECT mint, symbol FROM tokens_df
                WHERE mint NOT IN (SELECT mint FROM tokens)
            """)
            total_tokens = con.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
            sample_df = con.execute("SELECT * FROM tokens LIMIT 5").fetchdf()
            context.add_output_metadata({
                "sample_rows": dg.MetadataValue.md(sample_df.head().to_markdown(index=False)),
                "new_tokens_added": new_tokens_added,
                "total_tokens": total_tokens,
            })

    def load_input(self, context):
        with context.resources.duckdb.get_connection() as con:
            df = con.execute("SELECT * FROM tokens").fetchdf()
            return df

@dg.io_manager(required_resource_keys={"duckdb"})
def duckdb_tokens_io_manager():
    return DuckDBTokensIOManager()

