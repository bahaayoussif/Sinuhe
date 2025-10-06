# connectors/mysql_connector.py

import os
from pyspark.sql import SparkSession, DataFrame

class MySqlConnector:
    """Connects to a MySQL database."""
    def __init__(self, host, port, database, user, password, spark_session):
        self.jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
        self.spark = spark_session
        self.connection_properties = {"user": user, "password": password, "driver": "com.mysql.cj.jdbc.Driver"}
        self._is_connected = False
        print(f"✅ [MySQL] Connector initialized for DB '{database}' at: {host}")

    def check_connection(self) -> bool:
        """
        Verifies database connectivity by running a simple query.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            version_info = self.query("SELECT VERSION() as v", is_test=True).first()['v']
            print(f"✔️ [MySQL] Connection successful. Version: {version_info}")
            return True
        except Exception as e:
            print(f"❌ [MySQL] Connection check failed: {e}")
            return False

    def connect(self) -> bool:
        """
        Establishes the connection to the database.

        Returns:
            bool: True if the connection is established, False otherwise.
        """
        print("🟡 [MySQL] Attempting to connect...")
        self._is_connected = self.check_connection()
        return self._is_connected

    def query(self, sql_query: str, is_test: bool = False) -> DataFrame:
        if not is_test:
            print(f"Executing query: \"{sql_query}\"")
        return self.spark.read.jdbc(url=self.jdbc_url, table=f"({sql_query}) as t", properties=self.connection_properties)

    def stop_connection(self):
        print("🔴 [MySQL] Stopping the SparkSession...")
        self.spark.stop()

# Example of direct usage
if __name__ == '__main__':
    print("--- Running MySQL Connector Direct Example ---")
    spark = SparkSession.builder.appName("MySQL Direct Test").master("local[*]").getOrCreate()
    config = {
        "host": "localhost", "port": 3306, "database": "sakila",
        "user": os.environ.get("MYSQL_USER", "root"),
        "password": os.environ.get("MYSQL_PASS", "your_password")
    }
    connector = MySqlConnector(**config, spark_session=spark)
    if connector.connect():
        connector.query("SELECT actor_id, first_name FROM actor LIMIT 2").show()
    connector.stop_connection()