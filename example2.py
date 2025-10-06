# main.py

import os
import json

# Import the simulator and the simplified describer
from src.simulator.sqlitesimulator import SQLiteSimulator
from src.describer.sqlitedescriber import SQLiteDescriber


def main():
    """
    Main function that simulates a database and then saves its
    JSON description to a file.
    """
    # --- 1. Configuration ---
    CONFIG_FILE = "src/simulator/resources/db_config.json"
    SCHEMA_FILE = "src/simulator/resources/enterprise_schema.json"

    # --- 2. Simulate the Database ---
    print("--- Step 1: Simulating a complex database... ---")
    with open(CONFIG_FILE, 'r') as f:
        config_data = json.load(f)
    with open(SCHEMA_FILE, 'r') as f:
        schema_data = json.load(f)

    sqlite_config = config_data['sqlite']
    simulator = SQLiteSimulator(config=sqlite_config, schema=schema_data)
    db_path = simulator.create_database()
    print("-" * 60)

    # --- 3. Describe the Database ---
    print("\n--- Step 2: Describing the database schema... ---")

    describer = SQLiteDescriber(db_path=db_path)
    json_description = describer.describe()

    # --- 4. Save the Output to a JSON File ---
    # This new section handles saving the file.

    output_dir = "output"
    # Get the base name of the database file (e.g., "company_from_config")
    base_name = os.path.splitext(os.path.basename(db_path))[0]
    output_filename = f"{base_name}_description.json"
    output_filepath = os.path.join(output_dir, output_filename)

    # Ensure the 'output' directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"\n--- Step 3: Saving description to file... ---")
    with open(output_filepath, 'w') as f:
        f.write(json_description)

    print(f"✔️ Successfully saved description to: {output_filepath}")

    print("\n✅ Process complete.")


if __name__ == '__main__':
    # Make sure you have installed the required packages:
    # pip install faker requests
    main()