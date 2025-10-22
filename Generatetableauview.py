import os
import re
import sys
import pandas as pd
import yaml
from sqlalchemy import create_engine, text, Numeric
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.types import String, Integer, Numeric
import urllib.parse
from string import Template

#  Load environment variables from .env file
load_dotenv()

def load_config():
    with open('aur_config.yml', 'r') as f:
        config = yaml.safe_load(f)
    return config

# Load Configuration
CONFIG = load_config()

host = os.getenv("PG_Host", "localhost")
port = os.getenv("PG_Port", "5432")
user = os.getenv("PG_User", "")
password = os.getenv("PG_Password", "")
db = os.getenv("PG_DB", "")
# schema = CONFIG.get('schema','wur')

user_enc = urllib.parse.quote_plus(user)
password_enc = urllib.parse.quote_plus(password)

DB_URL = f"postgresql+psycopg2://{user_enc}:{password_enc}@{host}:{port}/{db}"


engine = create_engine(DB_URL)

try:
    with engine.connect() as connection:
        results = connection.execute(text("SELECT 1"))
        print("Database connection established.")
except Exception as e:
    print(f"Database connection failed: {e}")
    sys.exit(1) 

def _kvp_rows_fixed():
    # Hard-coded 
    return [
        ("rank display" , "s.overall_rank_display::text" , "str"), 
        ("score display" , "s.overall_score_display::text" , "str"),
        ("rank number" , "s.overall_rank::text" , "float"),
        ("score number" , "s.overall_score::text" , "float"), 
        ("teaching score" , "s.teaching_score::text" , "float"), 
        ("teaching score display" , "s.teaching_score_display::text" , "float"), 
        ("research environment score" , "s.research_environment_score::text" , "float"), 
        ("research environment score display" , "s.research_environment_score_display::text" , "float"),
        ("research quality score" , "s.research_quality_score::text" , "float"),
        ("research quality score display" , "s.research_quality_score_display" , "float"), 
        ("industry score" , "s.industry_score::text", "float"),
        ("industry score display" , "s.industry_score_display" , "float"), 
        ("international score" , "s.international_score::text" , "float"),
        ("international score display" , "s.international_score_display" , "float"), 
        ("t1" , "s.t1::text" , "float"), 
        ("t2" , "s.t2::text" , "float"), 
        ("t3" , "s.t3::text" , "float"), 
        ("t4" , "s.t4::text" , "float"), 
        ("t5" , "s.t5::text" , "float"), 
        ("r1" , "s.r1::text" , "float"), 
        ("r2" , "s.r2::text" , "float"), 
        ("r3" , "s.r3::text" , "float"), 
        ("c1" , "s.c1::text" , "float"), 
        ("c2" , "s.c2::text" , "float"),
        ("c3" , "s.c3::text" , "float"), 
        ("c4" , "s.c4::text" , "float"), 
        ("e1" , "s.e1::text" , "float"), 
        ("e2" , "s.e2::text" , "float"), 
        ("i1" , "s.i1::text" , "float"), 
        ("i2" , "s.i2::text" , "float"), 
        ("i3" , "s.i3::text" , "float"), 
        ("i4" , "s.i4::text" , "float"), 
    ]

def _values_sql(rows):
    return ",\n              ".join(
        f"('f{label}'::text , {expr}, '{type}'::text)" for (label, expr, type) in rows
    )

def create_tableau_view(
    engine,
    *,
    source_schema: str,
    source_view: str,
    target_schema: str,
    target_view: str,
    ranking: str,
    year: int,
    ranking_detail: str,
):
    """Create or refresh the target view from the source view using fixed KVP unpivot logic."""
    
    values_sql = _values_sql(_kvp_rows_fixed())

    # --- Build the main SELECT for your view ---
    body_sql = f"""
        SELECT 
            s.id,
            '{ranking}'::text AS ranking,
            {year} AS year,
            '{ranking_detail}'::text AS ranking_detail,
            v.field,
            v.value,
            v.type
        FROM {source_schema}.{source_view} s
        CROSS JOIN LATERAL (
            VALUES
                {values_sql}
        ) AS v(field, value, type)
    """

    # --- Drop and recreate view directly (no DO $$) ---
    ddl = f"""
        DROP VIEW IF EXISTS {target_schema}.{target_view} CASCADE;
        CREATE VIEW {target_schema}.{target_view} AS
        {body_sql};
    """

    # --- Execute with proper transaction control ---
    with engine.begin() as conn:
        conn.execute(text(ddl))
        print(f"View {target_schema}.{target_view} created/refreshed successfully.")



from sqlalchemy import text

def append_year_to_union_view(engine, target_schema: str, union_view: str, yearly_prefix: str, year: int):
    """
    Append the given year's view (e.g. kvp_arab_2026_vw) to the existing union view if:
    1. It exists in the schema, and
    2. It's not already included in the union view definition.
    """
    new_view = f"{yearly_prefix}_{year}_vw"

    with engine.begin() as conn:
        # Step 1: Check if the new yearly view exists
        check_new_view = text("""
            SELECT EXISTS (
                SELECT 1 FROM pg_views
                WHERE schemaname = :schema
                AND viewname = :viewname
            );
        """)
        exists = conn.execute(check_new_view, {"schema": target_schema, "viewname": new_view}).scalar()

        if not exists:
            print(f"View {target_schema}.{new_view} not found — skipping update.")
            return

        # Step 2: Get existing union view definition
        get_definition = text("""
            SELECT definition 
            FROM pg_views
            WHERE schemaname = :schema
            AND viewname = :viewname;
        """)
        result = conn.execute(get_definition, {"schema": target_schema, "viewname": union_view}).fetchone()

        if not result:
            print(f"Union view {target_schema}.{union_view} not found.")
            return

        view_def = result[0]

        # Step 3: Check if the new year's view is already in the union
        if new_view in view_def:
            print(f" {new_view} is already included in {union_view}. No update needed.")
            return

        # Step 4: Append the new view SQL
        updated_sql = view_def.strip().rstrip(';') + f"\nUNION ALL\nSELECT * FROM {target_schema}.{new_view}"

        ddl = f"""
            DROP VIEW IF EXISTS {target_schema}.{union_view} CASCADE;
            CREATE VIEW {target_schema}.{union_view} AS
            {updated_sql};
        """

        conn.execute(text(ddl))
        print(f"Added {new_view} to {target_schema}.{union_view}.")


job = CONFIG["job"]

create_tableau_view(
    engine,
    source_schema=job["source"]["schema"],
    source_view=job["source"]["view"],
    target_schema=job["target"]["schema"],
    target_view=job["target"]["view"],
    ranking=job["constants"]["ranking"],
    year=job["year"],
    ranking_detail=job["constants"]["ranking_detail"],
)

append_year_to_union_view(
    engine,
    target_schema=job["target"]["schema"],      
    union_view="kvp_arab_rankings_vw_test",
    yearly_prefix="kvp_arab",
    year=job["year"]                           
)
