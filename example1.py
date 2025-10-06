# main.py

import json
import sqlite3
import os

# Import the simulator class from your package
from src.simulator.sqlitesimulator import SQLiteSimulator


def run_simulation():
    """
    Main function to run the SQLite database simulation.
    """
    CONFIG_FILE = "src/simulator/resources/db_config.json"
    SCHEMA_FILE = "src/simulator/resources/enterprise_schema.json"

    print("🚀 Starting SQLite Database Simulation...")

    # --- Step 1: Load Configuration and Schema ---
    print(f"🔵 Loading configuration from '{CONFIG_FILE}'...")
    try:
        with open(CONFIG_FILE, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: Configuration file '{CONFIG_FILE}' not found. Make sure paths are correct.")
        return

    print(f"🔵 Loading schema from '{SCHEMA_FILE}'...")
    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: Schema file '{SCHEMA_FILE}' not found. Make sure paths are correct.")
        return

    # --- Step 2: Initialize and Run the Simulator ---
    # Select the 'sqlite' configuration from the config file
    sqlite_config = config_data.get('sqlite')
    if not sqlite_config:
        print("❌ Error: 'sqlite' configuration not found in db_config.json.")
        return

    # Initialize the simulator with the correct config and schema
    simulator = SQLiteSimulator(config=sqlite_config, schema=schema_data)

    # Create the database
    db_file = simulator.create_database()

    # --- Step 3: Verify the Created Database ---
    print("\n--- Verifying Created Database ---")
    if not os.path.exists(db_file):
        print(f"❌ Verification failed. Database file '{db_file}' was not created.")
        return

    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()

            # --- CORRECTED QUERY BLOCK ---
            # The query is updated to use the correct table 'employee_skills'
            # and count skills instead of projects.
            query = """
            SELECT
                e.first_name || ' ' || e.last_name AS employee,
                p.title AS position,
                d.name AS department,
                COUNT(es.skill_id) AS skill_count
            FROM employees e
            JOIN positions p ON e.position_id = p.id
            JOIN departments d ON e.department_id = d.id
            LEFT JOIN employee_skills es ON e.id = es.employee_id
            GROUP BY e.id
            ORDER BY RANDOM()
            LIMIT 5;
            """

            print("Query: Fetching 5 random employees with their skill counts...")
            results = cursor.execute(query).fetchall()

            if results:
                for row in results:
                    # Updated print statement to match the query
                    print(f"  - Employee: {row[0]}, Position: {row[1]}, Department: {row[2]}, Skills: {row[3]}")
            else:
                print("  - Verification query returned no results.")
            # --- END OF CORRECTION ---

    except sqlite3.Error as e:
        print(f"❌ Verification failed with a database error: {e}")

    print("\n✅ Simulation complete.")


if __name__ == '__main__':
    run_simulation()