import psycopg2
from psycopg2 import sql
from getpass import getpass
from tabulate import tabulate

#Establish the connection 


PGHOST = "dbm.fe.up.pt"
PGPORT = 5433
PGDATABASE = "fced01"
PGUSER = "fced01"
PGPASSWORD = "GROUP1"
PGSCHEMA = "electrogrid"


# Connect to the database

conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
cursor = conn.cursor()

#---------------------Insert new client to database-------------------------------------#
def insert_client(conn):
    print("\n== Insert New Client ==")
    name = input("Client name: ").strip()
    email = input("Email: ").strip() 
    phone = input("Phone: ").strip() 
    address = input("Address: ").strip()

    cur = conn.cursor()
    try:
        # Get the last client ID
        cur.execute("""
            SELECT person_id
            FROM electrogrid.person
            WHERE person_id LIKE 'C%'
            ORDER BY CAST(SUBSTRING(person_id FROM 2) AS INTEGER) DESC
            LIMIT 1;
        """)
        last_id = cur.fetchone()
        if last_id:
            last_num = int(last_id[0][1:])
        else:
            last_num = 0
        new_id = f"C{last_num + 1}"

        # Insert into person
        cur.execute("""
            INSERT INTO electrogrid.person (person_id, name, email, phone)
            VALUES (%s, %s, %s, %s);
        """, (new_id, name, email, phone))

        # Insert into client
        cur.execute("""
            INSERT INTO electrogrid.client (person_id, address)
            VALUES (%s, %s);
        """, (new_id, address))

        # Commit
        conn.commit()
        print(f"Client inserted successfully with ID {new_id}")

    except Exception as e:
        conn.rollback()
        print(" Error inserting client:", e)

    finally:
        cur.close()

#--------------------------- SEARCH FOR A CLIENT ---------------------------------------#


def search_client(conn):
    print("\n== Search for a client's information ==")
    phone = input("Phone: ").strip()

    cur = conn.cursor()

    try:
        # Get client personal information
        cur.execute("""
            SELECT p.person_id, p.name, p.phone, p.email, c.address
            FROM electrogrid.person p
            JOIN electrogrid.client c ON p.person_id = c.person_id
            WHERE CAST(p.phone AS TEXT) = %s;
        """, (phone,))   

        client = cur.fetchone()

        if not client:
            print("No client found with that phone number.")
            return
        
        # Print tabulated client personal information
        headers = ["person_id", "name", "phone", "email", "address"]
        print("\n-- Client Info --")
        print(tabulate([client], headers=headers, tablefmt="grid"))

        # Get connection information for that client
        cur.execute("""
            SELECT connection_id, property_address, install_date, meter_serial,
                   connection_type, status
            FROM electrogrid.connections
            WHERE client_id = %s AND status = 'Active';
        """, (client[0],))
        
        connections = cur.fetchall()

        # Print active connections for that client
        print("\n-- Active Connections --")
        if not connections:
            print("(none)")
        else:
            headers = ["connection_id", "property_address", "install_date", "meter_serial", "connection_type", "status"]
            print(tabulate(connections, headers=headers, tablefmt="grid"))

    # Get service orders related to this client
        cur.execute("""
            SELECT so.service_order_id, so.connection_id, so.service_type,
           so.start_date, so.end_date, so.notes, so.technician_id
            FROM electrogrid.service_orders AS so
            JOIN electrogrid.connections AS con
            ON so.connection_id = con.connection_id
            WHERE con.client_id = %s
            AND con.status = 'Active'
            ORDER BY so.start_date DESC;
            """, (client[0],))
        
        service_orders = cur.fetchall()

        print("\n-- Service Orders for this Client's Connections --")
        if not service_orders:
            print("(none)")
        else:
            headers = ["service_order_id", "connection_id", "service_type", 
                       "start_date", "end_date", "notes", "technician_id"]
            print(tabulate(service_orders, headers=headers, tablefmt="grid"))
    
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()

#----------------------------SEARCH FOR TECHNICIAN INFO BASED ON REGION--------------------#
def search_technician(conn):
    print("\n== Search for a technician by region ==")
    print("Select region:")
    print(" 1) Coimbra")
    print(" 2) Faro")
    print(" 3) Lisboa")
    print(" 4) Porto")

    choice = input("Enter your choice: ").strip()

    region_map = {
        "1": "Coimbra",
        "2": "Faro",
        "3": "Lisboa",
        "4": "Porto"
    }

    region_name = region_map.get(choice)
    if not region_name:
        print("Invalid choice.")
        return

    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT t.person_id, p.name, p.email, p.phone, t.region_name
            FROM electrogrid.technician AS t
            JOIN electrogrid.person AS p
              ON t.person_id = p.person_id
            WHERE t.region_name = %s
            ORDER BY p.name;
        """, (region_name,))

        technicians = cur.fetchall()

        print(f"\n-- Technicians in region: {region_name} --")
        if not technicians:
            print("(none)")
        else:
            headers = ["person_id", "name", "email", "phone", "region"]
            print(tabulate(technicians, headers=headers, tablefmt="grid"))
            print(f"\nTotal technicians in {region_name}: {len(technicians)}")


    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()


#--------------------------- Show meter cheks by client phone number -------------------#
def list_meter_checks(conn):
    print("\n== List of all meter checks (most recent first) ==")

    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 
                c.person_id AS client_id,
                mc.meter_serial,
                mc.check_date,
                mc.meter_read,
                con.property_address
            FROM electrogrid.meter_check AS mc
            JOIN electrogrid.connections AS con 
                ON mc.meter_serial = con.meter_serial
            JOIN electrogrid.client AS c 
                ON con.client_id = c.person_id
            JOIN electrogrid.person AS p_client 
                ON c.person_id = p_client.person_id
            ORDER BY mc.check_date DESC;
        """)

        rows = cur.fetchall()

        print("\n-- Meter Checks (sorted by most recent date) --")
        if not rows:
            print("(none found)")
        else:
            headers = ["client_id", "meter_serial", 
                       "check_date", "meter_read", "property_address"]
            print(tabulate(rows, headers=headers, tablefmt="grid"))

    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()

#------------------------------MENU ----------------------------------------------------#
print("\n========= ELECTROGRID MENU =========")
print("1) Insert new client")
print("2) Search for a client's information")
print("3) Search for a technician based on region")
print("4) List all meter checks (most recent on top)")
print("5) Exit")
choice = input("Enter your choice: ").strip()

if choice == "1":
        insert_client(conn)

elif choice == "2":
        search_client(conn)

elif choice == "3":
        search_technician(conn)

elif choice == "4":
    list_meter_checks(conn)

elif choice == "5":
    print("Exiting program.")
    conn.close()
    
else:
    print("Invalid option. Try again.")

