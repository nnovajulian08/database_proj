import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "postgres")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")

try:
    print("🔌 Trying to connect to database...")
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
    print("✅ Connection established successfully!")
    print(f"Connected to: {PGDATABASE} on {PGHOST}")
    conn.close()
except Exception as e:
    print("❌ Connection failed:")
    print(e)
