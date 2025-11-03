import psycopg2
from psycopg2 import sql


DB_CONFIG = {
    "host": "dbm.fe.up.pt",
    "dbname": "fced01",
    "user": "fced01",
    "password": "GROUP1",
    "port": 5433
}

DATA_FOLDER = "/"  # folder containing all CSVs

# ===============================
# CONNECT TO POSTGRESQL
# ===============================
conn = psycopg2.connect(
    database = DB_CONFIG["dbname"],
    user = DB_CONFIG["user"],
    password = DB_CONFIG["password"],
    host = DB_CONFIG["host"],
    port = DB_CONFIG["port"]
)


cursor = conn.cursor()

def fn():
    cursor.execute(f"select * from electrogrid.client")
    employee = cursor.fetchall()
    print(employee)
    conn.close()
fn()