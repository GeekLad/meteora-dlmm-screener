import dagster as dg

@dg.asset(
    description="Creates all tables and views in DuckDB.",
    group_name="build_database",
    required_resource_keys={"duckdb"},
    automation_condition=dg.AutomationCondition.newly_missing().with_label("run_once_on_startup"),
)
def create_tables_and_views(context: dg.AssetExecutionContext) -> None:
    with context.resources.duckdb.get_connection() as con:
        con.sql("""
            -- Tokens Table
            CREATE SEQUENCE IF NOT EXISTS tokens_id_seq;
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER DEFAULT nextval('tokens_id_seq') PRIMARY KEY,
                mint VARCHAR(44) NOT NULL UNIQUE,
                symbol VARCHAR(44)
            );
            CREATE INDEX IF NOT EXISTS tokens_mint_IDX ON tokens (mint);
                
            -- Pairs Table
            CREATE SEQUENCE IF NOT EXISTS pairs_id_seq;
            CREATE TABLE IF NOT EXISTS pairs (
                id INTEGER DEFAULT nextval('pairs_id_seq') PRIMARY KEY,
                pair_address VARCHAR(44) NOT NULL UNIQUE,
                name VARCHAR NOT NULL,
                mint_x_id INTEGER NOT NULL REFERENCES tokens(id),
                mint_y_id INTEGER NOT NULL REFERENCES tokens(id),
                bin_step INTEGER NOT NULL,
                base_fee_percentage FLOAT NOT NULL,
                hide BOOLEAN DEFAULT FALSE NOT NULL,
                is_blacklisted BOOLEAN DEFAULT FALSE NOT NULL
            );
            CREATE INDEX IF NOT EXISTS pairs_pair_address_IDX ON pairs (pair_address);
            
            -- Pair History Table
            CREATE TABLE IF NOT EXISTS pair_history (
            created_at TIMESTAMP NOT NULL,
            pair_id INTEGER NOT NULL REFERENCES pairs(id),
            price FLOAT NOT NULL,
            liquidity FLOAT NOT NULL,
            cumulative_trade_volume FLOAT NOT NULL,
            cumulative_fee_volume FLOAT
            );
            CREATE INDEX IF NOT EXISTS pair_history_update_id_IDX ON pair_history (created_at);
            
            -- Pair History View
            CREATE VIEW IF NOT EXISTS v_pair_history AS
            SELECT
                ph.created_at,
                p.pair_address,
                p.name,
                p.bin_step,
                p.base_fee_percentage,
                p.hide,
                p.is_blacklisted,
                t_x.mint AS mint_x,
                t_x.symbol AS symbol_x,
                t_y.mint AS mint_y,
                t_y.symbol AS symbol_y,
                ph.price,
                ph.liquidity,
                ph.cumulative_trade_volume,
                ph.cumulative_fee_volume
            FROM pair_history ph
            JOIN pairs p ON ph.pair_id = p.id
            JOIN tokens t_x ON p.mint_x_id = t_x.id
            JOIN tokens t_y ON p.mint_y_id = t_y.id;
    """)