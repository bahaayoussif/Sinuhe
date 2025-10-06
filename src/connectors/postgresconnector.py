# connectors/postgres_connector.py

import os
from pyspark.sql import SparkSession, DataFrame

class PostgresConnector:
    """Connects to a PostgreSQL database."""
    def __init__(self, host, port, database, user, password, spark_session):
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.spark = spark_session
        self.connection_properties = {"user": user, "password": password, "driver": "org.postgresql.Driver"}
        self._is_connected = False
        print(f"✅ [PostgreSQL] Connector initialized for DB '{database}' at: {host}")

    def check_connection(self) -> bool:
        """
        Verifies database connectivity by running a simple query.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            version_info = self.query("SELECT version()", is_test=True).first()[0]
            print(f"✔️ [PostgreSQL] Connection successful. {version_info}")
            return True
        except Exception as e:
            print(f"❌ [PostgreSQL] Connection check failed: {e}")
            return False

    def connect(self) -> bool:
        """
        Establishes the connection to the database.

        Returns:
            bool: True if the connection is established, False otherwise.
        """
        print("🟡 [PostgreSQL] Attempting to connect...")
        self._is_connected = self.check_connection()
        return self._is_connected

    def query(self, sql_query: str, is_test: bool = False) -> DataFrame:
        if not is_test:
            print(f"Executing query: \"{sql_query}\"")
        return self.spark.read.jdbc(url=self.jdbc_url, table=f"({sql_query}) as t", properties=self.connection_properties)

    def stop_connection(self):
        print("🔴 [PostgreSQL] Stopping the SparkSession...")
        self.spark.stop()

# Example of direct usage
if __name__ == '__main__':
    print("--- Running PostgreSQL Connector Direct Example ---")
    spark = SparkSession.builder.appName("PostgreSQL Direct Test").master("local[*]").getOrCreate()
    config = {
        "host": "localhost", "port": 5432, "database": "dvdrental",
        "user": os.environ.get("PG_USER", "postgres"),
        "password": os.environ.get("PG_PASS", "your_password")
    }
    connector = PostgresConnector(**config, spark_session=spark)
    if connector.connect():
        connector.query("SELECT film_id, title FROM film LIMIT 2").show()
    connector.stop_connection()