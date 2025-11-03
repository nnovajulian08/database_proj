import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ===============================
# CONFIGURATION
# ===============================
DB_CONFIG = {
    "host": "localhost",
    "dbname": "energy_services",
    "user": "postgres",
    "password": "your_password",
    "port": 5432
}

DATA_FOLDER = "data/"  # folder containing all CSVs

# ===============================
# CONNECT TO POSTGRESQL
# ===============================
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

def truncate_tables():
    """Delete all data from tables in dependency-safe order."""
    tables = [
        "Technician_Skill", "Service_Order", "Bill", "Connection",
        "Technician", "Client", "Person",
        "Skill", "Service_Type", "Connection_Type", "Region", "Status"
    ]
    for t in tables:
        cursor.execute(f"DELETE FROM {t};")
    conn.commit()
    print("✅ Existing data cleared.")

# ===============================
# STEP 1: LOAD RAW DATASETS
# ===============================
clients = pd.read_csv(DATA_FOLDER + "Client_raw.csv")
connections = pd.read_csv(DATA_FOLDER + "Connections_raw.csv")
bills = pd.read_csv(DATA_FOLDER + "Bills_raw.csv")
technicians = pd.read_csv(DATA_FOLDER + "Technicians_raw.csv")
service_orders = pd.read_csv(DATA_FOLDER + "Service_orders_raw.csv")

# ===============================
# STEP 2: DATA CLEANING / NORMALIZATION
# ===============================

# --- Normalize technician skills ---
technicians["skills"] = technicians["skills"].fillna("").apply(lambda x: [s.strip() for s in x.split(",") if s.strip()])
all_skills = sorted({skill for sub in technicians["skills"] for skill in sub})
skill_df = pd.DataFrame({"skill_name": all_skills})
skill_df["skill_id"] = range(1, len(skill_df) + 1)

# --- Lookup tables ---
status_df = pd.DataFrame({"status_name": ["Active", "Suspended", "Disconnected"]})
status_df["status_id"] = range(1, len(status_df) + 1)

region_df = pd.DataFrame({"region_name": technicians["region"].unique()})
region_df["region_id"] = range(1, len(region_df) + 1)

conn_type_df = pd.DataFrame({"type_name": connections["connection_type"].unique()})
conn_type_df["connection_type_id"] = range(1, len(conn_type_df) + 1)

service_type_df = pd.DataFrame({"type_name": service_orders["service_type"].unique()})
service_type_df["service_type_id"] = range(1, len(service_type_df) + 1)

# ===============================
# STEP 3: INSERT DATA HELPERS
# ===============================
def insert_df(df, table):
    cols = ",".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    execute_values(cursor, f"INSERT INTO {table} ({cols}) VALUES %s", values)

# ===============================
# STEP 4: POPULATE LOOKUPS
# ===============================
truncate_tables()

insert_df(status_df, "Status")
insert_df(region_df, "Region")
insert_df(conn_type_df, "Connection_Type")
insert_df(service_type_df, "Service_Type")
insert_df(skill_df[["skill_id", "skill_name"]], "Skill")
conn.commit()
print("✅ Lookup tables populated.")

# ===============================
# STEP 5: PERSON, CLIENT, TECHNICIAN
# ===============================
# -- Clients (insert as Person + Client)
person_clients = clients[["client_id", "client_name", "email", "phone"]].rename(
    columns={"client_id": "person_id", "client_name": "name"}
)
insert_df(person_clients, "Person")

clients_clean = clients.merge(status_df, left_on="status", right_on="status_name")[
    ["client_id", "address", "status_id"]
]
insert_df(clients_clean, "Client")

# -- Technicians (insert as Person + Technician)
person_tech = technicians[["technician_id", "technician_name", "email", "phone"]].rename(
    columns={"technician_id": "person_id", "technician_name": "name"}
)
insert_df(person_tech, "Person")

technicians_clean = technicians.merge(region_df, left_on="region", right_on="region_name")[
    ["technician_id", "region_id"]
]
insert_df(technicians_clean, "Technician")
conn.commit()
print("✅ Person, Client, and Technician tables populated.")

# ===============================
# STEP 6: CONNECTIONS
# ===============================
connections_clean = (
    connections.merge(conn_type_df, left_on="connection_type", right_on="type_name")
    .merge(status_df, left_on="status", right_on="status_name")
)[["connection_id", "client_id", "property_address", "connection_type_id", "install_date", "meter_serial", "status_id"]]

insert_df(connections_clean, "Connection")
conn.commit()
print("✅ Connections populated.")

# ===============================
# STEP 7: BILLS
# ===============================
bills_clean = bills[[
    "bill_id", "connection_id", "period_start", "period_end",
    "kwh_used", "amount", "issue_date", "payment_date"
]]
insert_df(bills_clean, "Bill")
conn.commit()
print("✅ Bills populated.")

# ===============================
# STEP 8: SERVICE ORDERS
# ===============================
service_orders_clean = (
    service_orders.merge(service_type_df, left_on="service_type", right_on="type_name")
)[["service_order_id", "connection_id", "technician_id", "service_type_id", "start_date", "end_date", "notes"]]

insert_df(service_orders_clean, "Service_Order")
conn.commit()
print("✅ Service Orders populated.")

# ===============================
# STEP 9: TECHNICIAN SKILLS (M:N)
# ===============================
tech_skill_pairs = []
for _, row in technicians.iterrows():
    for s in row["skills"]:
        skill_id = skill_df.loc[skill_df["skill_name"] == s, "skill_id"].values[0]
        tech_skill_pairs.append((row["technician_id"], skill_id))

execute_values(cursor, "INSERT INTO Technician_Skill (technician_id, skill_id) VALUES %s", tech_skill_pairs)
conn.commit()

# ===============================
# DONE
# ===============================
cursor.close()
conn.close()
print("🎉 Database fully populated and normalized successfully in PostgreSQL!")
