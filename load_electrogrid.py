import os
import sys
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# ===============================================================
# CONFIGURATION
# ===============================================================
load_dotenv()  # Load from .env

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "postgres")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")
PGSCHEMA = os.getenv("PGSCHEMA", "public")
CSV_DIR = Path(os.getenv("CSV_DIR", "."))

FILES = {
    "clients": CSV_DIR / "clients_raw.csv",
    "technicians": CSV_DIR / "technicians_raw.csv",
    "connections": CSV_DIR / "connections_raw.csv",
    "bills": CSV_DIR / "bills_raw.csv",
    "service_orders": CSV_DIR / "service_orders_raw.csv",
}

# ===============================================================
# DB CONNECTION
# ===============================================================
def get_conn():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD
    )

def tq(name):  # qualify with schema
    return f'{PGSCHEMA}."{name}"'

# ===============================================================
# CLEANING HELPERS
# ===============================================================
def clean_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s != "" else None

def to_date(s):
    if pd.isna(s) or s == "":
        return None
    dt = pd.to_datetime(s, errors="coerce")
    return None if pd.isna(dt) else dt.date()

def load_csv(path):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for c in df.columns:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
        df[c] = df[c].replace({"": None})
    return df

# ===============================================================
# DELETE EXISTING DATA
# ===============================================================
def delete_all(cur):
    tables = [
        "Technician_Skill",
        "Bills",
        "Service_Orders",
        "Connections",
        "Technician",
        "Client",
        "Person",
        "Skills",
        "Region",
        "Connection_Type",
        "Status",
        "Service_Type",
    ]
    for t in tables:
        cur.execute(f"DELETE FROM {tq(t)};")

# ===============================================================
# MAIN LOAD LOGIC
# ===============================================================
def main():
    print("📂 Reading CSVs...")
    clients = load_csv(FILES["clients"])
    technicians = load_csv(FILES["technicians"])
    connections = load_csv(FILES["connections"])
    bills = load_csv(FILES["bills"])
    service_orders = load_csv(FILES["service_orders"])

    # ===========================================================
    # 🧾 RENAME BLOCKS (adjust column names to match your CSVs)
    # ===========================================================

    # --- CLIENTS ---
    print("🧩 Normalizing clients...")
    clients = clients.rename(columns={
        "client_id": "person_id",
        "client_name": "name",
        "email": "email",
        "phone": "phone",
        "address": "address",
        # if you have a different column name for address, e.g. "property_address"
        # "property_address": "address"
    })

    # --- TECHNICIANS ---
    print("🧩 Normalizing technicians...")
    technicians = technicians.rename(columns={
        "technician_id": "person_id",
        "technician_name": "name",
        "email": "email",
        "phone": "phone",
        "region": "region_name",  
        "skills": "skills"
    })

    # --- CONNECTIONS ---
    print("🧩 Normalizing connections...")
    connections = connections.rename(columns={
        "connection_id": "connection_id",
        "client_id": "client_id",
        "technician_id": "technician_id",
        "property_address": "property_address",
        "install_date": "install_date",
        "meter_serial": "meter_serial",
        "connection_type": "connection_type",
        "status": "status",
        # if you have something like "address" or "city"
        # "address": "property_address",
        # "city": "city"
    })

    # --- BILLS ---
    print("🧩 Normalizing bills...")
    bills = bills.rename(columns={
        "bill_id": "bills_id",
        "period_start": "period_starts",
        "period_end": "period_ends",
        "kwh_used": "kwh_used",
        "amount": "amount",
        "issue_date": "issue_date",
        "payment_date": "payment_date",
        "client_id": "client_id",
        "connection_id": "connection_id"
    })

    # --- SERVICE ORDERS ---
    print("🧩 Normalizing service orders...")
    service_orders = service_orders.rename(columns={
        "service_order_id": "service_order_id",
        "service_type": "service_type",
        "start_date": "start_date",
        "end_date": "end_date",
        "notes": "notes",
        "client_id": "client_id",
        "technician_id": "technician_id",
        "connection_id": "connection_id"
    })

    # ===========================================================
    # 🧠 CLEANING AND LOADING LOGIC (same as before)
    # ===========================================================
    technicians["skills_list"] = technicians["skills"].fillna("").map(
        lambda s: [clean_str(x) for x in s.split(",") if clean_str(x)]
    )
    all_skills = sorted({s for lst in technicians["skills_list"] for s in lst})

    # Collect lookups
    regions = sorted({clean_str(x) for x in technicians.get("region_name", pd.Series()).dropna()})
    conn_types = sorted({clean_str(x) for x in connections.get("connection_type", pd.Series()).dropna()})
    statuses = sorted({clean_str(x) for x in connections.get("status", pd.Series()).dropna()})
    serv_types = sorted({clean_str(x) for x in service_orders.get("service_type", pd.Series()).dropna()})

    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            print("🧹 Deleting old data...")
            delete_all(cur)

            print("📥 Inserting lookups...")
            for table, col, values in [
                ("Region", "region_name", regions),
                ("Connection_Type", "connection_type", conn_types),
                ("Status", "status", statuses),
                ("Service_Type", "service_type", serv_types),
                ("Skills", "skill_name", all_skills),
            ]:
                if values:
                    q = f'INSERT INTO {tq(table)} ({col}) VALUES (%s) ON CONFLICT DO NOTHING;'
                    execute_batch(cur, q, [(v,) for v in values])

            print("👥 Loading Person / Client / Technician...")
            persons = pd.concat([clients[["person_id", "name", "email", "phone"]],
                                 technicians[["person_id", "name", "email", "phone"]]]).drop_duplicates()
            execute_batch(cur,
                f'INSERT INTO {tq("Person")}(person_id,name,email,phone) VALUES (%s,%s,%s,%s) '
                'ON CONFLICT (person_id) DO NOTHING;',
                persons.itertuples(index=False, name=None),
                page_size=500,
            )

            execute_batch(cur,
                f'INSERT INTO {tq("Client")}(person_id,address) VALUES (%s,%s) '
                'ON CONFLICT (person_id) DO NOTHING;',
                clients[["person_id", "address"]].itertuples(index=False, name=None),
                page_size=500,
            )

            execute_batch(cur,
                f'INSERT INTO {tq("Technician")}(person_id,region_name) VALUES (%s,%s) '
                'ON CONFLICT (person_id) DO NOTHING;',
                technicians[["person_id", "region_name"]].itertuples(index=False, name=None),
                page_size=500,
            )

            print("🧠 Loading Technician_Skill...")
            tech_skill_rows = [
                (row.person_id, skill)
                for _, row in technicians.iterrows()
                for skill in row.skills_list
            ]
            execute_batch(cur,
                f'INSERT INTO {tq("Technician_Skill")}(technician_id,skill_name) VALUES (%s,%s) '
                'ON CONFLICT (technician_id,skill_name) DO NOTHING;',
                tech_skill_rows, page_size=1000
            )

            print("🔌 Loading Connections...")
            connections["install_date"] = connections["install_date"].map(to_date)
            execute_batch(cur,
                f'''INSERT INTO {tq("Connections")}
                    (connection_id,property_address,install_date,meter_serial,
                     connection_type,status,client_id,technician_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (connection_id) DO NOTHING;''',
                connections[["connection_id","property_address","install_date","meter_serial",
                             "connection_type","status","client_id","technician_id"]].itertuples(index=False, name=None),
                page_size=1000
            )

            print("💡 Loading Bills...")
            for col in ["period_starts","period_ends","issue_date","payment_date"]:
                bills[col] = bills[col].map(to_date)
            bills["kwh_used"] = pd.to_numeric(bills["kwh_used"], errors="coerce")
            bills["amount"] = pd.to_numeric(bills["amount"], errors="coerce")
            execute_batch(cur,
                f'''INSERT INTO {tq("Bills")}
                    (bills_id,period_starts,period_ends,kwh_used,amount,
                     issue_date,payment_date,client_id,connection_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (bills_id) DO NOTHING;''',
                bills[["bills_id","period_starts","period_ends","kwh_used","amount",
                       "issue_date","payment_date","client_id","connection_id"]].itertuples(index=False, name=None),
                page_size=1000
            )

            print("🧾 Loading Service Orders...")
            for col in ["start_date","end_date"]:
                service_orders[col] = service_orders[col].map(to_date)
            execute_batch(cur,
                f'''INSERT INTO {tq("Service_Orders")}
                    (service_order_id,service_type,start_date,end_date,notes,
                     client_id,technician_id,connection_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (service_order_id) DO NOTHING;''',
                service_orders[["service_order_id","service_type","start_date","end_date","notes",
                                "client_id","technician_id","connection_id"]].itertuples(index=False, name=None),
                page_size=1000
            )

        conn.commit()
        print("✅ Load complete!")
    except Exception as e:
        conn.rollback()
        print("❌ Error:", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
