import pandas as pd
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values


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

#---------------------------------- KIRILL's DATA CLEANING --------------------------------------#

#Read, clean, and transform the data

raw_cndf = pd.read_csv('raw_data/connections_raw.csv')
raw_tdf = pd.read_csv('raw_data/technicians_raw.csv')
raw_sdf = pd.read_csv('raw_data/service_orders_raw.csv')
raw_cdf = pd.read_csv('raw_data/clients_raw.csv')

# region df

region_df = (raw_tdf[['region']]).rename(columns={'region':'region_name'}).copy()
region_df['region_name'] = region_df['region_name'].astype(str).str.strip()
region_df = region_df.drop_duplicates()

# connection_type df

connection_type_df = raw_cndf[['connection_type']].copy()
connection_type_df['connection_type'] = connection_type_df['connection_type'].astype(str).str.strip()
connection_type_df = connection_type_df.drop_duplicates()

# status df

status_df = raw_cndf[['status']].copy()
status_df['status'] = status_df['status'].astype(str).str.strip()
status_df = status_df.drop_duplicates()

# service_type df

service_type_df = raw_sdf[['service_type']].copy()
service_type_df['service_type'] = service_type_df['service_type'].astype(str).str.strip()
service_type_df = service_type_df.drop_duplicates()

# person df

client_df = (raw_cdf[['client_id', 'client_name', 'email', 'phone']]
             .rename( columns={'client_id': 'person_id', 'client_name': 'name'})).copy()
client_df['person_id'] = client_df['person_id'].astype(str).str.strip()
client_df['name'] = client_df['name'].astype(str).str.strip()
client_df['email'] = client_df['email'].astype(str).str.strip()
client_df['phone'] = client_df['phone'].astype(str).str.strip()
client_df = client_df.drop_duplicates(subset=['person_id'], keep='first')

tech_df = (raw_tdf[['technician_id', 'technician_name', 'email', 'phone']]
             .rename( columns={'technician_id': 'person_id', 'technician_name': 'name'})).copy()
tech_df['person_id'] = tech_df['person_id'].astype(str).str.strip()
tech_df['name'] = tech_df['name'].astype(str).str.strip()
tech_df['email'] = tech_df['email'].astype(str).str.strip()
tech_df['phone'] = tech_df['phone'].astype(str).str.strip()
tech_df = tech_df.drop_duplicates(subset=['person_id'], keep='first')

person_df = (pd.concat([client_df, tech_df],
                       ignore_index=True))
person_df['phone'] = person_df['phone'].astype(str).str.replace(r'[^\d]', '', regex=True).str[-9:]
person_df  = person_df.drop_duplicates(subset=['person_id'], keep='first')

# client df

client_df = ((raw_cdf[['client_id', 'address']])
             .rename( columns={'client_id': 'person_id', 'client_name': 'name'})).copy()
client_df['person_id'] = client_df['person_id'].astype(str).str.strip()
client_df['address'] = client_df['address'].astype(str).str.strip()
client_df = client_df.drop_duplicates(subset=['person_id'], keep='first')


# Convert DataFrame to list of tuples

datamart_region = [tuple(x) for x in region_df.to_numpy()]
columns_region = ', '.join(region_df.columns)

datamart_connection_type = [tuple(x) for x in connection_type_df.to_numpy()]
columns_connection_type = ', '.join(connection_type_df.columns)

datamart_status = [tuple(x) for x in status_df.to_numpy()]
columns_status = ', '.join(status_df.columns)

datamart_connection_type = [tuple(x) for x in connection_type_df.to_numpy()]
columns_connection_type = ', '.join(connection_type_df.columns)

datamart_service_type = [tuple(x) for x in service_type_df.to_numpy()]
columns_service_type = ', '.join(service_type_df.columns)

datamart_person = [tuple(x) for x in person_df.to_numpy()]
columns_person = ', '.join(person_df.columns)

datamart_client = [tuple(x) for x in client_df.to_numpy()]
columns_client = ', '.join(client_df.columns)


#-------------------------- JULIAN'S DATA CLEANING ---------------------------------------#

#Read, clean, and transform the data
CSV_DIR = Path("./raw_data")

# -------------------------- SERVICE ORDERS ----------------------------------------------#
df_raw = pd.read_csv(CSV_DIR/"service_orders_raw.csv")

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


# Drop rows with missing critical foreign keys or primary key
df_service_orders = df_service_orders.dropna(subset=["service_order_id", "client_id", "technician_id", "connection_id", "service_type"])

# Remove duplicates
df_service_orders = df_service_orders.drop_duplicates(subset=["service_order_id"])

# Reorder columns to match SQL schema definition
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

#Convert data to tuples & Extract column names 
datamart_service_orders = [tuple(x) for x in df_service_orders.to_numpy()]
columns_service_orders = ', '.join(df_service_orders.columns)




#-----------------------------------Bills--------------------------------------------------------#

df_raw = pd.read_csv(CSV_DIR/"bills_raw.csv")

# Keep only the columns that match the database schema
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

# Rename columns to match SQL table structure
df_bills = df_bills.rename(columns={
    "period_start": "period_starts",
    "period_end": "period_ends",
    "bill_id":"bills_id"
})


# strip spaces form text
for col in ["bills_id", "client_id", "connection_id"]:
    df_bills[col] = df_bills[col].astype(str).str.strip()

# Convert numeric columns
df_bills["kwh_used"] = pd.to_numeric(df_bills["kwh_used"], errors="coerce")
df_bills["amount"] = pd.to_numeric(df_bills["amount"], errors="coerce").round(2)

# Convert date columns
date_cols = ["period_starts", "period_ends", "issue_date", "payment_date"]
for col in date_cols:
    df_bills[col] = pd.to_datetime(df_bills[col], errors="coerce")

# Drop rows with missing critical fields
df_bills = df_bills.dropna(subset=["bills_id", "client_id", "connection_id"])

# Remove duplicates
df_bills = df_bills.drop_duplicates(subset=["bills_id"])

# Reorder columns to match SQL table definition
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

#Convert data to tuples & Extract column names 
datamart_bills = [tuple(x) for x in df_bills.to_numpy()]
columns_bills = ', '.join(df_bills.columns)


#-------------------------------------Connections---------------------------------------------#

# Read CSV
df_raw = pd.read_csv(CSV_DIR/"connections_raw.csv")


# Keep only relevant columns
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


# strip spaces
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



#Convert data to tuples & Extract column names 
datamart_connections = [tuple(x) for x in df_connnections.to_numpy()]
columns_connections = ', '.join(df_connnections.columns)

# ------------------------------------Technician_Skills and Skills------------------#

#Read CSV file
df_raw = pd.read_csv(CSV_DIR/"technicians_raw.csv")


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


#Convert data to tuples & Extract column names 
datamart_technician_skill = [tuple(x) for x in df_technician_skill.to_numpy()]
columns_technician_skill = ', '.join(df_technician_skill.columns)

datamart_skills = [tuple(x) for x in df_skills.to_numpy()]
columns_skills = ', '.join(df_skills.columns)




# ------------------------------------Technicians-----------------------------------------#
# Read CSV
df_raw = pd.read_csv(CSV_DIR/"technicians_raw.csv")

# Keep only relevant columns
df_technicians = df_raw[["technician_id", "region"]].copy()

#Rename columns to match SQL schema
df_technicians = df_technicians.rename(columns={
    "technician_id": "person_id",
    "region": "region_name"
})

# Clean and standardize
df_technicians["person_id"] = df_technicians["person_id"].astype(str).str.strip()
df_technicians["region_name"] = df_technicians["region_name"].astype(str).str.strip().str.title()  # Normalize capitalization

# Drop rows with missing values
df_technicians = df_technicians.dropna(subset=["person_id", "region_name"])

# Remove duplicates
df_technicians = df_technicians.drop_duplicates(subset=["person_id"])

# Reorder columns to match SQL table
df_technicians = df_technicians[["person_id", "region_name"]]

#Convert data to tuples & Extract column names 

datamart_technicians = [tuple(x) for x in df_technicians.to_numpy()]
columns_technicians = ', '.join(df_technicians.columns)


#----------------------- LOAD DATA INTO DATABASE ----------------------------------------#

# SQL query


try:
    with conn.cursor() as cur:
        # Insert into region
        execute_values(
            cur,
            f"INSERT INTO electrogrid.region ({columns_region}) VALUES %s",
            datamart_region
        )

        # Insert into connection_type
        execute_values(
            cur,
            f"INSERT INTO electrogrid.connection_type ({columns_connection_type}) VALUES %s",
            datamart_connection_type
        )

        # Insert into status
        execute_values(
            cur,
            f"INSERT INTO electrogrid.status ({columns_status}) VALUES %s",
            datamart_status
        )

        # Insert into service_type
        execute_values(
            cur,
            f"INSERT INTO electrogrid.service_type ({columns_service_type}) VALUES %s",
            datamart_service_type
        )

        # Insert into person
        execute_values(
            cur,
            f"INSERT INTO electrogrid.person ({columns_person}) VALUES %s",
            datamart_person
        )

        # Insert into client
        execute_values(
            cur,
            f"INSERT INTO electrogrid.client ({columns_client}) VALUES %s",
            datamart_client
        )

        # Insert into technician
        execute_values(
            cur,
            f"INSERT INTO electrogrid.technician ({columns_technicians}) VALUES %s",
            datamart_technicians
        )

        # Insert into skills
        execute_values(
            cur,
            f"INSERT INTO electrogrid.skills ({columns_skills}) VALUES %s",
            datamart_skills
        )

        # Insert into technician_skill
        execute_values(
            cur,
            f"INSERT INTO electrogrid.technician_skill ({columns_technician_skill}) VALUES %s",
            datamart_technician_skill
        )

        # Insert into connections
        execute_values(
            cur,
            f"INSERT INTO electrogrid.connections ({columns_connections}) VALUES %s",
            datamart_connections
        )

        # Insert into bills
        execute_values(
            cur,
            f"INSERT INTO electrogrid.bills ({columns_bills}) VALUES %s",
            datamart_bills
        )

    conn.commit()
    print("All data inserted successfully!")

except Exception as e:
    conn.rollback()
    print(f"Error: {e}")

conn.commit()
cursor.close()
conn.close()


