# connectors/sqlite_connector.py

import os
from pyspark.sql import SparkSession, DataFrame

class SqlLiteConnector:
    """Connects to a local SQLite database file."""
    def __init__(self, db_path: str, spark_session: SparkSession):
        self.db_path = db_path
        self.spark = spark_session
        self.jdbc_url = f"jdbc:sqlite:{self.db_path}"
        self.jdbc_driver = "org.sqlite.JDBC"
        self._is_connected = False
        print(f"✅ [SQLite] Connector initialized for database: {self.db_path}")

    def check_connection(self) -> bool:
        """
        Verifies database connectivity by running a simple query.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            version = self.query("SELECT sqlite_version() as v", is_test=True).first()['v']
            print(f"✔️ [SQLite] Connection successful. Version: {version}")
            return True
        except Exception as e:
            print(f"❌ [SQLite] Connection check failed: {e}")
            return False

    def connect(self) -> bool:
        """
        Establishes the connection to the database.

        Returns:
            bool: True if the connection is established, False otherwise.
        """
        print("🟡 [SQLite] Attempting to connect...")
        self._is_connected = self.check_connection()
        return self._is_connected

    def query(self, sql_query: str, is_test: bool = False) -> DataFrame:
        if not is_test:
            print(f"Executing query: \"{sql_query}\"")
        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("driver", self.jdbc_driver) \
            .option("dbtable", f"({sql_query}) as t") \
            .load()

    def stop_connection(self):
        print("🔴 [SQLite] Stopping the SparkSession...")
        self.spark.stop()

# Example of direct usage (will only run if this file is executed directly)
if __name__ == '__main__':
    print("--- Running SQLite Connector Direct Example ---")
    spark = SparkSession.builder.appName("SQLite Direct Test").master("local[*]").getOrCreate()
    connector = SqlLiteConnector("standalone_test.sqlite", spark)
    if connector.connect():
        connector.query("SELECT 1 as test_col").show()
    connector.stop_connection()
    if os.path.exists("standalone_test.sqlite"):
        os.remove("standalone_test.sqlite")