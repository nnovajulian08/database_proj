import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
# Configuration


PGHOST = "dbm.fe.up.pt"
PGPORT = 5433
PGDATABASE = "fced01"
PGUSER = "fced01"
PGPASSWORD = "GROUP1"
PGSCHEMA = "electrogrid"

CSV_DIR = "./raw_data"

#Establishing database access


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

# SQL query
# insert_query = (f"INSERT INTO electrogrid.region ({columns_region}) VALUES %s;"
#                 f"INSERT INTO electrogrid.connection_type ({columns_connection_type}) VALUES %s;"
#                 f"INSERT INTO electrogrid.status ({columns_status}) VALUES %s;"
#                 f"INSERT INTO electrogrid.service_type ({columns_service_type}) VALUES %s;"
#                 f"INSERT INTO electrogrid.person ({columns_person}) VALUES %s;"
#                 f"INSERT INTO electrogrid.client ({columns_client}) VALUES %s;")

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

    conn.commit()
    print("All data inserted successfully!")

except Exception as e:
    conn.rollback()
    print(f"Error: {e}")

conn.commit()
cursor.close()
conn.close()



