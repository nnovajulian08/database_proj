# Configuration 

PGHOST = "dbm.fe.up.pt"
PGPORT = 5433
PGDATABASE = "fced01"
PGUSER = "fced01"
PGPASSWORD = "GROUP1"
PGSCHEMA = "electrogrid"

CSV_DIR = "./raw_data" 

#Establishing database access

import psycopg2

conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
cursor = conn.cursor()

TABLES = [
    "service_orders",
    "bills",
    "connections",
    "technician_skill",
    "skills",
    "technician",
    "client",
    "person",
    "service_type",
    "status",
    "connection_type",
    "region"
]

try:
    # Connect to the database
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
    cursor = conn.cursor()
    print("Connected successfully!")

    # Delete from each table
    for table in TABLES:
        cursor.execute(f"DELETE FROM {PGSCHEMA}.{table};")
        print(f" Cleared table: {table}")

    # Commit changes
    conn.commit()
    print(" All tables cleared successfully.")

except Exception as e:
    print("Error clearing tables:", e)
    conn.rollback()


#Read, clean, and transform the data

#
