# describer/sqlitedescriber.py

import json
import sqlite3


class SQLiteDescriber:
    """
    Analyzes a SQLite database using Python's built-in sqlite3 library
    and returns its schema as a JSON object.
    """

    def __init__(self, db_path: str):
        """
        Initializes the describer with the database path.

        Args:
            db_path (str): The file path to the SQLite database.
        """
        self.db_path = db_path
        print(f"✅ SQLite Describer (using sqlite3) initialized for '{db_path}'")

    def _run_query(self, query: str) -> list:
        """Helper to run a SQL query and return results as a list of dicts."""
        with sqlite3.connect(self.db_path) as conn:
            # This makes the cursor return dictionary-like rows
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            # Convert row objects to plain dictionaries
            return [dict(row) for row in cursor.fetchall()]

    def _get_table_names(self) -> list:
        """Retrieves a list of all user-defined tables in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        return [row['name'] for row in self._run_query(query)]

    def _analyze_relationships(self, schema_dict: dict) -> list:
        """Analyzes foreign keys to determine relationship types."""
        relationships = []
        all_pks = {tbl: details['primary_key'] for tbl, details in schema_dict.items()}
        for table_name, details in schema_dict.items():
            for fk in details['foreign_keys']:
                from_cols = fk['from_columns']
                rel_type = "one-to-one" if from_cols == all_pks.get(table_name) else "one-to-many"
                relationships.append({
                    "from_table": table_name, "to_table": fk['to_table'], "type": rel_type,
                    "on": f"{', '.join(from_cols)} -> {', '.join(fk['to_columns'])}"
                })
        return relationships

    def describe(self) -> str:
        """Inspects the database and returns a JSON string of its structure."""
        print("🟡 Describing database schema...")
        table_names = self._get_table_names()
        db_schema = {}
        for table_name in table_names:
            db_schema[table_name] = {"columns": [], "primary_key": [], "foreign_keys": []}
            pk_cols = []
            for col_row in self._run_query(f"PRAGMA table_info('{table_name}')"):
                db_schema[table_name]["columns"].append(
                    {"name": col_row['name'], "type": col_row['type'], "nullable": col_row['notnull'] == 0})
                if col_row['pk'] > 0:
                    pk_cols.append((col_row['pk'], col_row['name']))
            db_schema[table_name]['primary_key'] = [name for _, name in sorted(pk_cols)]
            fks = {}
            for fk_row in self._run_query(f"PRAGMA foreign_key_list('{table_name}')"):
                fk_id = fk_row['id']
                if fk_id not in fks:
                    fks[fk_id] = {'to_table': fk_row['table'], 'from_columns': [], 'to_columns': []}
                fks[fk_id]['from_columns'].append(fk_row['from'])
                fks[fk_id]['to_columns'].append(fk_row['to'])
            db_schema[table_name]['foreign_keys'] = list(fks.values())

        final_structure = {
            "database_type": "sqlite",
            "tables": db_schema,
            "relationships": self._analyze_relationships(db_schema)
        }
        print("✔️ Database description complete.")
        return json.dumps(final_structure, indent=4)