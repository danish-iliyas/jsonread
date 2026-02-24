import json
import pyodbc
import sys
import os
import time

# =============================================================================
# ASE DataCollector JSON → SQL Server Import Script
# =============================================================================
# This script:
#   1. Reads (deserializes) the ASE DataCollector JSON file
#   2. Extracts all tag data into a flat list (like a C# DataTable)
#   3. Creates the database and table in SQL Server (if not exists)
#   4. Inserts all rows into the 'tags' table
# =============================================================================

# ---- CONFIGURATION ----
JSON_FILE = r"C:\Users\delhisafri87\Downloads\20260218_ASE DataCollector.json"
SERVER = "localhost"
DATABASE = "ase_config"
DRIVER = "{ODBC Driver 17 for SQL Server}"


def deserialize_json(file_path):
    """Step 1: Read and deserialize the JSON file into a Python dictionary."""
    if not os.path.exists(file_path):
        print(f"❌ ERROR: JSON file not found at: {file_path}")
        sys.exit(1)

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"📖 Reading JSON file ({file_size_mb:.1f} MB)...")

    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    print("✅ JSON deserialized successfully!")
    return data


def build_datatable(data):
    """Step 2: Extract tag data into a flat list of tuples (like C# DataTable)."""
    rows = []

    channels = data.get("project", {}).get("channels", [])
    print(f"📊 Found {len(channels)} channels in JSON")

    for channel in channels:
        channel_name = channel.get("common.ALLTYPES_NAME", "")

        devices = channel.get("devices", [])
        for device in devices:
            device_name = device.get("common.ALLTYPES_NAME", "")

            tags = device.get("tags", [])
            for tag in tags:
                rows.append((
                    channel_name,
                    device_name,
                    tag.get("common.ALLTYPES_NAME"),
                    tag.get("servermain.TAG_ADDRESS"),
                    tag.get("servermain.TAG_DATA_TYPE"),
                    tag.get("servermain.TAG_SCAN_RATE_MILLISECONDS")
                ))

    print(f"✅ Extracted {len(rows)} tag rows from JSON")
    return rows


def connect_to_sql_server(server, driver, database="master"):
    """Step 3: Connect to SQL Server using Windows Authentication."""
    conn_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
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
    """Step 4: Create the database if it doesn't exist."""
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
    """Step 5: Create the tags table if it doesn't exist."""
    cursor = conn.cursor()

    cursor.execute("""
        IF OBJECT_ID('tags', 'U') IS NULL
        BEGIN
            CREATE TABLE tags (
                id              INT IDENTITY(1,1) PRIMARY KEY,
                channel_name    VARCHAR(100),
                device_name     VARCHAR(100),
                tag_name        VARCHAR(150),
                address         VARCHAR(50),
                data_type       INT,
                scan_rate       INT
            )
        END
    """)
    conn.commit()
    print("✅ Table 'tags' is ready")
    cursor.close()


def insert_rows(conn, rows):
    """Step 6: Insert all rows into the tags table using batch insert."""
    cursor = conn.cursor()

    # Clear existing data to avoid duplicates on re-run
    cursor.execute("DELETE FROM tags")
    conn.commit()
    print("🧹 Cleared existing data from 'tags' table")

    insert_query = """
        INSERT INTO tags (channel_name, device_name, tag_name, address, data_type, scan_rate)
        VALUES (?, ?, ?, ?, ?, ?)
    """

    # Insert in batches of 1000 for large datasets
    batch_size = 1000
    total = len(rows)

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()
        inserted = min(i + batch_size, total)
        print(f"   ⬆️  Inserted {inserted}/{total} rows...", end="\r")

    print(f"\n✅ All {total} rows inserted successfully!")
    cursor.close()


def verify_data(conn):
    """Step 7: Verify the inserted data."""
    cursor = conn.cursor()

    # Total count
    cursor.execute("SELECT COUNT(*) FROM tags")
    total = cursor.fetchone()[0]
    print(f"\n📊 Total rows in 'tags' table: {total}")

    # Show sample data
    print("\n📋 Sample data (first 10 rows):")
    print("-" * 100)
    print(f"{'ID':<6} {'Channel':<15} {'Device':<25} {'Tag Name':<35} {'Address':<10} {'Type':<6} {'Rate'}")
    print("-" * 100)

    cursor.execute("SELECT TOP 10 * FROM tags")
    for row in cursor.fetchall():
        print(f"{row.id:<6} {str(row.channel_name):<15} {str(row.device_name):<25} {str(row.tag_name):<35} {str(row.address):<10} {str(row.data_type):<6} {row.scan_rate}")

    print("-" * 100)

    # Count by channel
    print("\n📊 Rows per channel:")
    cursor.execute("SELECT channel_name, COUNT(*) as tag_count FROM tags GROUP BY channel_name ORDER BY tag_count DESC")
    for row in cursor.fetchall():
        print(f"   {row.channel_name}: {row.tag_count} tags")

    cursor.close()


# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    start_time = time.time()
    print("=" * 60)
    print("  ASE DataCollector → SQL Server Import Tool")
    print("=" * 60)

    # Step 1: Deserialize JSON
    data = deserialize_json(JSON_FILE)

    # Step 2: Build DataTable (flat list of rows)
    rows = build_datatable(data)

    if len(rows) == 0:
        print("⚠️  No tags found in JSON file. Nothing to insert.")
        sys.exit(0)

    # Step 3: Connect to master database first
    print("\n🔌 Connecting to SQL Server...")
    conn = connect_to_sql_server(SERVER, DRIVER, "master")

    # Step 4: Create database
    create_database(conn, DATABASE)

    # Step 5: Reconnect to new database and create table
    conn = connect_to_sql_server(SERVER, DRIVER, DATABASE)
    create_table(conn)

    # Step 6: Insert all rows
    print(f"\n⬆️  Inserting {len(rows)} rows into SQL Server...")
    insert_rows(conn, rows)

    # Step 7: Verify
    verify_data(conn)

    # Done!
    conn.close()
    elapsed = time.time() - start_time
    print(f"\n🎉 ALL DONE in {elapsed:.1f} seconds!")
    print("=" * 60)
