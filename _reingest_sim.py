"""
Re-ingest raw.fact_sim with added columns: sexo, idade.
Drops the existing table and reloads from BigQuery.
"""
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import create_engine, text
from ingestion.config import config
from ingestion.sources.bigquery import fetch_sim_obitos

engine = create_engine(config.database_url)

print("Fetching SIM data from BigQuery (this may take a few minutes)...")
df = fetch_sim_obitos()
print(f"  Fetched {len(df):,} rows | columns: {list(df.columns)}")

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS raw.fact_sim CASCADE"))
    conn.commit()
    print("  Dropped raw.fact_sim")

df.to_sql(
    "fact_sim",
    engine,
    schema="raw",
    if_exists="replace",
    index=False,
    method="multi",
    chunksize=5000,
)
print(f"  Loaded {len(df):,} rows into raw.fact_sim")

# Verify
with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM raw.fact_sim")).scalar()
    cols  = conn.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='raw' AND table_name='fact_sim' ORDER BY ordinal_position"
    )).fetchall()
    print(f"  Verified: {count:,} rows | columns: {[c[0] for c in cols]}")

print("Done.")
