import pandas as pd
from pathlib import Path

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
CSV_DIR = Path("./raw_data")

# Service Orders

# 1. Read CSV
df_raw = pd.read_csv(CSV_DIR/"service_orders_raw.csv")

print("🔹 Raw data preview:")
print(df_raw.head())

# 2. Keep only the columns that match the database table
columns_to_keep_service_orders = [
    "service_order_id",
    "start_date",
    "end_date",
    "notes",
    "client_id",
    "technician_id",
    "connection_id",
    "service_type"
]
df_service_orders = df_raw[columns_to_keep_service_orders].copy()

# 3. Clean data types and formats

# Convert dates to proper datetime objects
df_service_orders["start_date"] = pd.to_datetime(df_service_orders["start_date"], errors="coerce")
df_service_orders["end_date"] = pd.to_datetime(df_service_orders["end_date"], errors="coerce")

# Strip text fields
df_service_orders["notes"] = df_service_orders["notes"].astype(str).str.strip()
df_service_orders["service_type"] = df_service_orders["service_type"].astype(str).str.strip()

# Convert ID fields to strings and strip spaces
df_service_orders["client_id"] = df_service_orders["client_id"].astype(str).str.strip()
df_service_orders["technician_id"] = df_service_orders["technician_id"].astype(str).str.strip()
df_service_orders["connection_id"] = df_service_orders["connection_id"].astype(str).str.strip()
df_service_orders["service_type"] = df_service_orders["service_type"].astype(str).str.strip()


# 4. Drop rows with missing critical foreign keys or primary key
df_service_orders = df_service_orders.dropna(subset=["service_order_id", "client_id", "technician_id", "connection_id", "service_type"])

# 5. Remove duplicates
df_service_orders = df_service_orders.drop_duplicates(subset=["service_order_id"])

# 6. Reorder columns to match SQL schema definition
df_service_orders = df_service_orders[
    [
        "service_order_id",
        "start_date",
        "end_date",
        "notes",
        "client_id",
        "technician_id",
        "connection_id",
        "service_type"
    ]
]

# 7. Final checks
print("\n✅ Cleaned DataFrame preview:")
print(df_service_orders.head())
print("\nData types:")
print(df_service_orders.dtypes)
print(f"\nTotal valid service orders ready for insertion: {len(df_service_orders)}")

#Bills
# 1. Read CSV
df_raw = pd.read_csv(CSV_DIR/"bills_raw.csv")

print("🔹 Raw data preview:")
print(df_raw.head())


# 2. Keep only the columns that match the database schema
columns_to_keep_bills = [
    "bill_id",
    "connection_id",
    "client_id",
    "period_start",
    "period_end",
    "kwh_used",
    "amount",
    "issue_date",
    "payment_date"
]
df_bills = df_raw[columns_to_keep_bills].copy()

# 3. Rename columns to match SQL table structure
df_bills = df_bills.rename(columns={
    "period_start": "period_starts",
    "period_end": "period_ends",
    "bill_id":"bills_id"
})

# 4. Clean and standardize data

# IDs: treat as text (VARCHAR), strip spaces
for col in ["bills_id", "client_id", "connection_id"]:
    df_bills[col] = df_bills[col].astype(str).str.strip()

# Convert numeric columns
df_bills["kwh_used"] = pd.to_numeric(df_bills["kwh_used"], errors="coerce")
df_bills["amount"] = pd.to_numeric(df_bills["amount"], errors="coerce").round(2)

# Convert date columns
date_cols = ["period_starts", "period_ends", "issue_date", "payment_date"]
for col in date_cols:
    df_bills[col] = pd.to_datetime(df_bills[col], errors="coerce")

# 5. Drop rows with missing critical fields
df_bills = df_bills.dropna(subset=["bills_id", "client_id", "connection_id"])

# 6. Remove duplicates
df_bills = df_bills.drop_duplicates(subset=["bills_id"])

# 7. Reorder columns to match SQL table definition
df_bills = df_bills[
    [
        "bills_id",
        "period_starts",
        "period_ends",
        "kwh_used",
        "amount",
        "issue_date",
        "payment_date",
        "client_id",
        "connection_id"
    ]
]

# 8. Final verification
print("\n✅ Cleaned DataFrame preview:")
print(df_bills.head())
print("\nData types:")
print(df_bills.dtypes)
print(f"\nTotal valid bills ready for insertion: {len(df_bills)}")


#Connections

# 1. Read CSV
df_raw = pd.read_csv(CSV_DIR/"connections_raw.csv")

print("🔹 Raw data preview:")
print(df_raw.head())

# 2. Keep only relevant columns
columns_to_keep_connections = [
    "connection_id",
    "client_id",
    "property_address",
    "connection_type",
    "install_date",
    "meter_serial",
    "status",
    "technician_id"
]
df_connnections = df_raw[columns_to_keep_connections].copy()

# 3. Clean and standardize

# Text fields: strip spaces
text_cols = [
    "connection_id",
    "client_id",
    "property_address",
    "meter_serial",
    "connection_type",
    "status",
    "technician_id"
]
for col in text_cols:
    df_connnections[col] = df_connnections[col].astype(str).str.strip()

# Standardize case for connection_type and status
df_connnections["connection_type"] = df_connnections["connection_type"].str.title()
df_connnections["status"] = df_connnections["status"].str.title()

# Convert date column
df_connnections["install_date"] = pd.to_datetime(df_connnections["install_date"], errors="coerce")

# Drop rows missing critical foreign keys or primary key
df_connnections = df_connnections.dropna(subset=["connection_id", "client_id", "technician_id"])

# Remove duplicates
df_connnections = df_connnections.drop_duplicates(subset=["connection_id"])

# Reorder columns to match SQL schema
df_connnections = df_connnections[
    [
        "connection_id",
        "property_address",
        "install_date",
        "meter_serial",
        "connection_type",
        "status",
        "client_id",
        "technician_id"
    ]
]

# 4. Final verification
print("\n✅ Cleaned DataFrame preview:")
print(df_connnections.head())
print("\nData types:")
print(df_connnections.dtypes)
print(f"\nTotal valid connections ready for insertion: {len(df_connnections)}")

#Technician_Skills and Skills

df_raw = pd.read_csv(CSV_DIR/"technicians_raw.csv")

print("🔹 Raw data preview:")
print(df_raw.head())

# 2. Keep only the relevant columns
df = df_raw[["technician_id", "skills"]].copy()

# 3. Clean IDs and split skills
df["technician_id"] = df["technician_id"].astype(str).str.strip()

# Split the skills by comma and explode into multiple rows
df["skills"] = df["skills"].astype(str).str.split(",")

df_exploded = df.explode("skills", ignore_index=True)

# 4. Clean up skill names
df_exploded["skills"] = df_exploded["skills"].astype(str).str.strip().str.title()

# 5. Drop empty skill rows
df_exploded = df_exploded[df_exploded["skills"] != ""]

# 6. Rename to match SQL schema
df_technician_skill = df_exploded.rename(columns={"skills": "skill_name"})

# 7. Remove duplicates (same technician-skill combination)
df_technician_skill = df_technician_skill.drop_duplicates(subset=["technician_id", "skill_name"])

# 8. Create separate df for unique skills
df_skills = pd.DataFrame(df_technician_skill["skill_name"].unique(), columns=["skill_name"])

# ===============================================================
# VERIFY RESULTS
# ===============================================================
print("\n✅ Technician-Skill pairs:")
print(df_technician_skill.head())

print("\n✅ Unique Skills:")
print(df_skills.head())

print(f"\nTotal skills listed: {len(df_skills)}")
print(f"Total technician-skill pairs: {len(df_technician_skill)}")

# Technicians
# 1. Read CSV
df_raw = pd.read_csv(CSV_DIR/"technicians_raw.csv")

print("🔹 Raw data preview:")
print(df_raw.head())

# 2. Keep only relevant columns
df_technicians = df_raw[["technician_id", "region"]].copy()

# 3. Rename columns to match SQL schema
df_technicians = df_technicians.rename(columns={
    "technician_id": "person_id",
    "region": "region_name"
})

# 4. Clean and standardize
df_technicians["person_id"] = df_technicians["person_id"].astype(str).str.strip()
df_technicians["region_name"] = df_technicians["region_name"].astype(str).str.strip().str.title()  # Normalize capitalization

# 5. Drop rows with missing values
df_technicians = df_technicians.dropna(subset=["person_id", "region_name"])

# 6. Remove duplicates
df_technicians = df_technicians.drop_duplicates(subset=["person_id"])

# 7. Reorder columns to match SQL table
df_technicians = df_technicians[["person_id", "region_name"]]

# 8. Verify result
print("\n✅ Cleaned Technician DataFrame:")
print(df_technicians.head())
print("\nData types:")
print(df_technicians.dtypes)
print(f"\nTotal valid technicians ready for insertion: {len(df_technicians)}")