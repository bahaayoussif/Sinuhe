# simulator/sqlitesimulator.py

import os
import json
import sqlite3
import random
from faker import Faker
from decimal import Decimal


class SQLiteSimulator:
    """
    Creates and populates a SQLite database from a JSON schema and config.
    """

    def __init__(self, config: dict, schema: dict):
        if config['type'].lower() != 'sqlite':
            raise ValueError("Configuration is not for a SQLite database.")

        self.db_path = config['db_path']
        self.schema = schema
        self.fake = Faker()
        self.generated_ids = {}

        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        print(f"✅ SQLite Simulator initialized for database at '{self.db_path}'")

    def _generate_fake_data(self, rule):
        provider_name = rule["faker_provider"]
        provider = self.fake
        for part in provider_name.split('.'):
            provider = getattr(provider, part)
        args = rule.get("args", {})
        return provider(**args)

    def _create_and_populate(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")

            # First pass: Create and populate all tables
            for table in self.schema["tables"]:
                table_name = table["name"]
                print(f"🟡 Processing table: {table_name}")

                columns_def = [f"{col['name']} {col['type']}" + (
                    f" REFERENCES {col['foreign_key']}" if 'foreign_key' in col else "") for col in table["columns"]]
                if 'primary_key' in table:
                    columns_def.append(f"PRIMARY KEY ({', '.join(table['primary_key'])})")
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)});"
                cursor.execute(create_sql)

                if "populate" not in table: continue
                self.generated_ids[table_name] = []

                if "source_data" in table["populate"]:
                    for row in table["populate"]["source_data"]:
                        columns = list(row.keys())
                        placeholders = ['?'] * len(columns)
                        values = list(row.values())

                        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                        cursor.execute(insert_sql, values)
                        self.generated_ids[table_name].append(cursor.lastrowid)

                elif "count" in table["populate"]:
                    for _ in range(table["populate"]["count"]):
                        columns, placeholders, values = [], [], []
                        for rule in table["populate"]["data"]:
                            columns.append(rule["column"])
                            placeholders.append("?")

                            is_fk_link = ("faker_provider" in rule and
                                          rule["faker_provider"] == "random_element" and
                                          "source_table" in rule)

                            if is_fk_link:
                                source_list = self.generated_ids.get(rule["source_table"], [])
                                if "allow_null" in rule and random.random() < rule["allow_null"]:
                                    values.append(None)
                                elif source_list:
                                    values.append(random.choice(source_list))
                                else:
                                    values.append(None)
                            elif "faker_provider" in rule:
                                values.append(self._generate_fake_data(rule))

                        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                        values = [float(v) if isinstance(v, Decimal) else v for v in values]
                        cursor.execute(insert_sql, values)
                        self.generated_ids[table_name].append(cursor.lastrowid)

                elif "map_source" in table["populate"]:  # <--- ADD THIS NEW BLOCK
                    ms = table["populate"]["map_source"]
                    source_ids = self.generated_ids[ms["source_table"]][:]  # Create a copy
                    random.shuffle(source_ids)  # Shuffle to make it random

                    for source_id in source_ids:
                        columns, placeholders, values = [], [], []
                        for rule in table["populate"]["data"]:
                            columns.append(rule["column"])
                            placeholders.append("?")
                            if rule.get("is_source_id"):
                                values.append(source_id)
                            else:
                                values.append(self._generate_fake_data(rule))

                        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                        values = [float(v) if isinstance(v, Decimal) else v for v in values]
                        cursor.execute(insert_sql, values)
                        self.generated_ids[table_name].append(cursor.lastrowid)

                elif "count_per_source" in table["populate"]:
                    pps = table["populate"]["count_per_source"]
                    for source_id in self.generated_ids[pps["source_table"]]:
                        num_assignments = random.randint(pps["min"], pps["max"])

                        target_rule = next((r for r in table["populate"]["data"] if not r.get("is_source_id")), None)
                        if not target_rule:
                            print(f"⚠️ Warning: No target rule found for many-to-many in {table_name}. Skipping.")
                            continue

                        if "source_table" in target_rule:
                            target_ids = self.generated_ids[target_rule["source_table"]]
                            if not target_ids: continue

                            for target_id in random.sample(target_ids, k=min(num_assignments, len(target_ids))):
                                columns, placeholders, values = [], [], []
                                for rule in table["populate"]["data"]:
                                    columns.append(rule["column"])
                                    placeholders.append("?")
                                    if rule.get("is_source_id"):
                                        values.append(source_id)
                                    else:
                                        values.append(target_id)

                                insert_sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                                cursor.execute(insert_sql, values)
                        else:
                            for _ in range(num_assignments):
                                columns, placeholders, values = [], [], []
                                for rule in table["populate"]["data"]:
                                    columns.append(rule["column"])
                                    placeholders.append("?")
                                    if rule.get("is_source_id"):
                                        values.append(source_id)
                                    else:
                                        values.append(self._generate_fake_data(rule))
                                insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
                                values = [float(v) if isinstance(v, Decimal) else v for v in values]
                                cursor.execute(insert_sql, values)

                print(f"✔️ Table '{table_name}' created and populated.")

            # Second pass: Handle post_populate for self-referencing keys
            print("\n🟡 Running post-population updates...")
            for table in self.schema["tables"]:
                if "post_populate" in table.get("populate", {}):
                    table_name = table["name"]
                    all_ids = self.generated_ids[table_name]
                    for rule in table["populate"]["post_populate"]:
                        column_to_update = rule["column"]
                        for record_id in all_ids:
                            value = None
                            if "allow_null" in rule and random.random() < rule["allow_null"]:
                                value = None
                            else:
                                source_list = self.generated_ids.get(rule["source_table"], [])
                                possible_values = [i for i in source_list if i != record_id]
                                if possible_values:
                                    value = random.choice(possible_values)

                            if value is not None:
                                update_sql = f"UPDATE {table_name} SET {column_to_update} = ? WHERE id = ?;"
                                cursor.execute(update_sql, (value, record_id))
                    print(f"✔️ Post-population finished for '{table_name}'.")

    def create_database(self):
        if os.path.exists(self.db_path) and self.schema.get("overwrite", True):
            print(f"🗑️ Deleting existing database at {self.db_path}.")
            os.remove(self.db_path)
        self._create_and_populate()
        print(f"🎉 Database '{self.db_path}' created successfully!")
        return self.db_path