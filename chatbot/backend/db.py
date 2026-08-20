import os
import re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# Load environment variables relative to the script location
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=env_path, override=True)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "data_warehouse")
PG_USER = os.getenv("PG_USER", "chatbot_readonly")
PG_PASSWORD = os.getenv("PG_PASSWORD", "chatbot_readonly_pass")
PG_SCHEMA = os.getenv("PG_SCHEMA", "gold")

# Check for single database connection URL (e.g., Neon or Render DATABASE_URL)
db_url_env = os.getenv("DATABASE_URL")
if db_url_env:
    if db_url_env.startswith("postgres://"):
        DATABASE_URL = db_url_env.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url_env.startswith("postgresql://") and not db_url_env.startswith("postgresql+psycopg2://"):
        DATABASE_URL = db_url_env.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        DATABASE_URL = db_url_env
else:
    DATABASE_URL = URL.create(
        "postgresql+psycopg2",
        username=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=int(PG_PORT),
        database=PG_DB,
    )

MAX_ROWS_RETURNED = 500
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)

# Setup SQLAlchemy connection engine
# Note: We include gold, silver, and metadata in the search_path to support queries across these layers.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"options": f"-c search_path={PG_SCHEMA},silver,metadata,public"}
)

def is_safe_select(sql: str) -> bool:
    """Reject anything that isn't a plain read-only SELECT or WITH statement."""
    stripped = sql.strip().rstrip(";")
    upper_sql = stripped.upper()
    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH")):
        return False
    if _FORBIDDEN.search(stripped):
        return False
    return True

def run_query(sql: str) -> dict:
    """Validate and execute a SELECT statement."""
    # Clean SQL formatting
    sql_clean = sql.strip().strip("`").replace("sql\n", "", 1).strip()
    
    if not is_safe_select(sql_clean):
        raise ValueError("Rejected unsafe SQL command: Only read-only SELECT statements are allowed.")

    # Ensure LIMIT exists to prevent database overloading and token exhaustion
    if "limit" not in sql_clean.lower():
        sql_clean = sql_clean.rstrip(";") + f" LIMIT {MAX_ROWS_RETURNED}"

    with engine.connect() as conn:
        result = conn.execute(text(sql_clean))
        columns = result.keys()
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

    return {"row_count": len(rows), "rows": rows}
