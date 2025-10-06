# simulator/sqlitesimulator.py

import os
import json
import sqlite3
import random
from faker import Faker


class SQLiteSimulator:
    """
    Creates and populates a SQLite database from a JSON schema and config.
    """

    def __init__(self, config: dict, schema: dict):
        """
        Initializes the simulator.

        Args:
            config (dict): The database configuration dictionary.
            schema (dict): The database schema dictionary.
        """
        if config['type'].lower() != 'sqlite':
            raise ValueError("Configuration is not for a SQLite database.")

        self.db_path = config['db_path']
        self.schema = schema
        self.fake = Faker()
        self.generated_ids = {}

        # Ensure the directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        print(f"✅ SQLite Simulator initialized for database at '{self.db_path}'")

    def _execute_query(self, query: str, params=None, fetch=None):
        """A helper method to execute SQL queries."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute(query, params or [])
            conn.commit()
            if fetch == "one":
                return cursor.fetchone()
            if fetch == "all":
                return cursor.fetchall()

    def _generate_fake_data(self, rule):
        """Generates a single piece of fake data based on a JSON rule."""
        provider_name = rule["faker_provider"]
        provider = self.fake
        for part in provider_name.split('.'):
            provider = getattr(provider, part)
        args = rule.get("args", {})
        return provider(**args)

    def _create_and_populate(self):
        """Creates tables and populates them according to the schema."""
        for table in self.schema["tables"]:
            table_name = table["name"]
            print(f"🟡 Processing table: {table_name}")

            # 1. Create Table
            columns_def = [
                f"{col['name']} {col['type']}" + (f" REFERENCES {col['foreign_key']}" if 'foreign_key' in col else "")
                for col in table["columns"]]
            if 'primary_key' in table:
                columns_def.append(f"PRIMARY KEY ({', '.join(table['primary_key'])})")
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)});"
            self._execute_query(create_sql)

            # 2. Populate Table
            if "populate" not in table: continue
            self.generated_ids[table_name] = []

            if "count" in table["populate"]:
                for _ in range(table["populate"]["count"]):
                    columns, placeholders, values = [], [], []
                    for rule in table["populate"]["data"]:
                        columns.append(rule["column"])
                        placeholders.append("?")
                        if "faker_provider" in rule and rule["faker_provider"] == "random_element":
                            values.append(random.choice(self.generated_ids[rule["source_table"]]))
                        elif "faker_provider" in rule:
                            values.append(self._generate_fake_data(rule))

                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                    self._execute_query(insert_sql, values)
                    pk_col_name = table["columns"][0]["name"]
                    last_id = \
                    self._execute_query(f"SELECT {pk_col_name} FROM {table_name} WHERE rowid=last_insert_rowid()",
                                        fetch="one")[0]
                    self.generated_ids[table_name].append(last_id)

            elif "count_per_source" in table["populate"]:
                pps = table["populate"]["count_per_source"]
                for source_id in self.generated_ids[pps["source_table"]]:
                    num_assignments = random.randint(pps["min"], pps["max"])
                    for _ in range(num_assignments):
                        columns, placeholders, values = [], [], []
                        for rule in table["populate"]["data"]:
                            columns.append(rule["column"])
                            placeholders.append("?")
                            if rule.get("is_source_id"):
                                values.append(source_id)
                            elif rule["faker_provider"] == "random_element":
                                values.append(random.choice(self.generated_ids[rule["source_table"]]))

                        insert_sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                        self._execute_query(insert_sql, values)

            print(f"✔️ Table '{table_name}' created and populated.")

    def create_database(self):
        """Creates and populates the database based on the loaded JSON."""
        if os.path.exists(self.db_path) and self.schema.get("overwrite", True):
            print(f"🗑️ Deleting existing database at {self.db_path}.")
            os.remove(self.db_path)
        self._create_and_populate()
        print(f"🎉 Database '{self.db_path}' created successfully!")
        return self.db_path


# Example of how to use the simulator
if __name__ == '__main__':
    CONFIG_FILE = "db_config.json"
    SCHEMA_FILE = "example_schema.json"

    # 1. Load config and schema from JSON files
    with open(CONFIG_FILE, 'r') as f:
        config_data = json.load(f)
    with open(SCHEMA_FILE, 'r') as f:
        schema_data = json.load(f)

    # 2. Select the 'sqlite' configuration
    sqlite_config = config_data['sqlite']

    # 3. Initialize the simulator with the correct config and schema
    simulator = SQLiteSimulator(config=sqlite_config, schema=schema_data)

    # 4. Create the database
    db_file = simulator.create_database()

    # 5. Connect and run a query to verify
    print("\n--- Verifying Database ---")
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        query = "SELECT e.first_name, p.name as project_name FROM employees e JOIN employee_projects ep ON e.id = ep.employee_id JOIN projects p ON ep.project_id = p.id LIMIT 5;"
        print("Query: Find 5 employee-project assignments...")
        for row in cursor.execute(query).fetchall():
            print(f"  - Employee '{row[0]}' is assigned to project '{row[1]}'")