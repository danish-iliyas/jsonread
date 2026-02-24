"""
main.py — Entry Point
======================
This is the main script that ties everything together.
It imports the two modules and runs the full pipeline:

  1. json_reader.py  → Reads JSON file & extracts tag data
  2. db_handler.py   → Connects to SQL Server & inserts data

How to run:
  py main.py
"""

import time
import sys

# Import our custom modules
from json_reader import read_json_file, extract_tags
from db_handler import connect, create_database, create_table, insert_rows, verify_data


# ---- CONFIGURATION ----
JSON_FILE = r"C:\Users\delhisafri87\Downloads\20260218_ASE DataCollector.json"
SERVER = "localhost"
DATABASE = "ase_config"
DRIVER = "{ODBC Driver 17 for SQL Server}"


def main():
    """Main function that orchestrates the entire import process."""
    start_time = time.time()

    print("=" * 60)
    print("  ASE DataCollector → SQL Server Import Tool")
    print("=" * 60)

    # ──────────────────────────────────────────────
    # PHASE 1: READ JSON (using json_reader module)
    # ──────────────────────────────────────────────
    print("\n📂 PHASE 1: Reading JSON file...")
    data = read_json_file(JSON_FILE)       # Deserialize JSON → Python dict
    rows = extract_tags(data)              # Extract tags → list of tuples

    if len(rows) == 0:
        print("⚠️  No tags found in JSON file. Nothing to insert.")
        sys.exit(0)

    # ──────────────────────────────────────────────
    # PHASE 2: DATABASE SETUP (using db_handler module)
    # ──────────────────────────────────────────────
    print("\n🗄️  PHASE 2: Setting up database...")
    conn = connect(SERVER, DRIVER, "master")     # Connect to master first
    create_database(conn, DATABASE)               # Create ase_config DB

    conn = connect(SERVER, DRIVER, DATABASE)      # Reconnect to ase_config
    create_table(conn)                            # Create tags table

    # ──────────────────────────────────────────────
    # PHASE 3: INSERT DATA (using db_handler module)
    # ──────────────────────────────────────────────
    print(f"\n⬆️  PHASE 3: Inserting {len(rows)} rows...")
    insert_rows(conn, rows)

    # ──────────────────────────────────────────────
    # PHASE 4: VERIFY (using db_handler module)
    # ──────────────────────────────────────────────
    print("\n🔍 PHASE 4: Verifying data...")
    verify_data(conn)

    # Done!
    conn.close()
    elapsed = time.time() - start_time
    print(f"\n🎉 ALL DONE in {elapsed:.1f} seconds!")
    print("=" * 60)


# This ensures main() only runs when you execute this file directly
# (not when it's imported as a module)
if __name__ == "__main__":
    main()
