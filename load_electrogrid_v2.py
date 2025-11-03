import psycopg2
import pandas as pd
# Configuration
#
# PGHOST = "dbm.fe.up.pt"
# PGPORT = 5433
# PGDATABASE = "fced01"
# PGUSER = "fced01"
# PGPASSWORD = "GROUP1"
# PGSCHEMA = "electrogrid"
#
# CSV_DIR = "./raw_data"
#
# #Establishing database access
#
#
# conn = psycopg2.connect(
#         host=PGHOST,
#         port=PGPORT,
#         dbname=PGDATABASE,
#         user=PGUSER,
#         password=PGPASSWORD
#     )
# cursor = conn.cursor()
#
# TABLES = [
#     "service_orders",
#     "bills",
#     "connections",
#     "technician_skill",
#     "skills",
#     "technician",
#     "client",
#     "person",
#     "service_type",
#     "status",
#     "connection_type",
#     "region"
# ]
#
# try:
#     # Connect to the database
#     conn = psycopg2.connect(
#         host=PGHOST,
#         port=PGPORT,
#         dbname=PGDATABASE,
#         user=PGUSER,
#         password=PGPASSWORD
#     )
#     cursor = conn.cursor()
#     print("Connected successfully!")
#
#     # Delete from each table
#     for table in TABLES:
#         cursor.execute(f"DELETE FROM {PGSCHEMA}.{table};")
#         print(f" Cleared table: {table}")
#
#     # Commit changes
#     conn.commit()
#     print(" All tables cleared successfully.")
#
# except Exception as e:
#     print("Error clearing tables:", e)
#     conn.rollback()


#Read, clean, and transform the data

raw_cndf = pd.read_csv('raw_data/connections_raw.csv')
raw_tdf = pd.read_csv('raw_data/technicians_raw.csv')
raw_sdf = pd.read_csv('raw_data/service_orders_raw.csv')
raw_cdf = pd.read_csv('raw_data/clients_raw.csv')


region_df = raw_tdf[['region']].drop_duplicates()
connection_type_df = raw_cndf[['connection_type']].drop_duplicates()
status_df = raw_cndf[['status']].drop_duplicates()
service_type_df = raw_sdf[['service_type']].drop_duplicates()
person_df = (pd.concat([raw_cdf[['client_id', 'client_name', 'email', 'phone']].drop_duplicates()
                       .rename( columns={'client_id': 'person_id', 'client_name': 'name'}),
                       raw_tdf[['technician_id', 'technician_name', 'email', 'phone']].drop_duplicates()
                       .rename( columns={'technician_id': 'person_id', 'technician_name': 'name'})],
                       ignore_index=True))
person_df['phone'] = person_df['phone'].astype(str).str.replace(r'[^\d]', '', regex=True).str[-9:]
