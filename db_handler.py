"""
db_handler.py — SQL Server Database Module
===========================================
This module handles ALL database operations:
  - Connecting to SQL Server
  - Creating the database
  - Creating the table
  - Inserting rows
  - Verifying/querying data

What is pyodbc?
  - pyodbc is a Python library that talks to SQL Server via ODBC drivers
  - ODBC = Open Database Connectivity (a standard way to talk to databases)
  - It's like C#'s SqlConnection / SqlCommand but for Python
"""

import sys
import pyodbc


def connect(server, driver, database="master"):
    """
    Connects to SQL Server using Windows Authentication.

    Args:
        server (str):   Server name, e.g. "localhost"
        driver (str):   ODBC driver name, e.g. "{ODBC Driver 17 for SQL Server}"
        database (str): Database to connect to (default: "master")

    Returns:
        pyodbc.Connection: An active database connection

    How it works:
        - Builds a connection string with server, database, and driver info
        - "Trusted_Connection=yes" means it uses your Windows login
          (no username/password needed)
        - This is equivalent to C#'s:
            new SqlConnection("Server=localhost;Database=master;Trusted_Connection=True;")
    """
    conn_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
    try:
        conn = pyodbc.connect(conn_string)
        print(f"✅ Connected to SQL Server [{server}] → database [{database}]")
        return conn
    except pyodbc.Error as e:
        print(f"❌ ERROR connecting to SQL Server: {e}")
        print("\n💡 Make sure:")
        print("   1. SQL Server service is running")
        print("   2. ODBC Driver 17 for SQL Server is installed")
        print("   3. SQL Server allows Windows Authentication")
        sys.exit(1)


def create_database(conn, db_name):
    """
    Creates the database if it doesn't already exist.

    Args:
        conn (pyodbc.Connection): Connection to the 'master' database
        db_name (str): Name of the database to create

    How it works:
        - autocommit=True is required for CREATE DATABASE
        - Checks sys.databases to avoid creating duplicates
        - This is equivalent to C#'s:
            SqlCommand("CREATE DATABASE ase_config", connection).ExecuteNonQuery()
    """
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"""
        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}')
        BEGIN
            CREATE DATABASE [{db_name}]
        END
    """)

    print(f"✅ Database '{db_name}' is ready")
    cursor.close()
    conn.close()


def create_table(conn):
    """
    Creates the 'tags' table if it doesn't already exist.

    Table Structure:
        id           - Auto-increment primary key (IDENTITY)
        channel_name - The communication channel (e.g., "echt", "venray")
        device_name  - The device under the channel (e.g., "Jazinta_Meter_89")
        tag_name     - The tag/sensor name (e.g., "443000_AC_POWER")
        address      - Modbus register address (e.g., "443000")
        data_type    - Data type code (e.g., 8 = Word/Integer)
        scan_rate    - How often to read in milliseconds (e.g., 100, 60000)
    """
    cursor = conn.cursor()

    # Drop and recreate table to add new columns
    cursor.execute("IF OBJECT_ID('tags', 'U') IS NOT NULL DROP TABLE tags")
    cursor.execute("""
        CREATE TABLE tags (
            id                INT IDENTITY(1,1) PRIMARY KEY,
            channel_name      VARCHAR(100),
            device_name       VARCHAR(100),
            device_id_string  VARCHAR(100),
            tag_name          VARCHAR(150),
            address           VARCHAR(50),
            data_type         INT,
            scan_rate         INT
        )
    """)
    conn.commit()
    print("✅ Table 'tags' is ready")
    cursor.close()


def insert_rows(conn, rows):
    """
    Inserts all rows into the 'tags' table using batch insert.

    Args:
        conn (pyodbc.Connection): Connection to the ase_config database
        rows (list): List of tuples to insert

    How it works:
        1. First DELETEs all existing rows (to avoid duplicates on re-run)
        2. Uses executemany() for efficient batch insert
           - This is like C#'s SqlBulkCopy.WriteToServer(dataTable)
        3. Inserts in batches of 1000 for large datasets
        4. Commits after each batch
    """
    cursor = conn.cursor()

    # Clear existing data to avoid duplicates on re-run
    cursor.execute("DELETE FROM tags")
    conn.commit()
    print("🧹 Cleared existing data from 'tags' table")

    insert_query = """
        INSERT INTO tags (channel_name, device_name, device_id_string, tag_name, address, data_type, scan_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    # Insert in batches of 1000 for large datasets
    batch_size = 1000
    total = len(rows)

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()
        inserted = min(i + batch_size, total)
        print(f"   ⬆️  Inserted {inserted}/{total} rows...")

    print(f"✅ All {total} rows inserted successfully!")
    cursor.close()


def verify_data(conn):
    """
    Verifies the inserted data by printing summary statistics.

    Shows:
        - Total row count
        - First 10 rows as sample
        - Tag count per channel
    """
    cursor = conn.cursor()

    # Total count
    cursor.execute("SELECT COUNT(*) FROM tags")
    total = cursor.fetchone()[0]
    print(f"\n📊 Total rows in 'tags' table: {total}")

    # Show sample data
    print("\n📋 Sample data (first 10 rows):")
    print("-" * 100)
    print(f"{'ID':<6} {'Channel':<12} {'Device':<22} {'ID String':<22} {'Tag Name':<30} {'Address':<10} {'Type':<6} {'Rate'}")
    print("-" * 115)

    cursor.execute("SELECT TOP 10 * FROM tags")
    for row in cursor.fetchall():
        print(f"{row.id:<6} {str(row.channel_name):<12} {str(row.device_name):<22} "
              f"{str(row.device_id_string):<22} {str(row.tag_name):<30} "
              f"{str(row.address):<10} {str(row.data_type):<6} {row.scan_rate}")

    print("-" * 100)

    # Count by channel
    print("\n📊 Rows per channel:")
    cursor.execute("""
        SELECT channel_name, COUNT(*) as tag_count
        FROM tags
        GROUP BY channel_name
        ORDER BY tag_count DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row.channel_name}: {row.tag_count} tags")

    cursor.close()
