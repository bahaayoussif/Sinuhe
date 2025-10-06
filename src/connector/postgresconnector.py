#
# This single file contains the PostgresConnector class and two usage examples.
#
# REMINDER: You MUST provide the PostgreSQL JDBC driver to Spark for this code to run.
# For example, using: spark-submit --jars /path/to/postgresql-42.7.3.jar this_script.py
#

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


################################################################################
##
## The PostgresConnector Class Definition
##
################################################################################

class PostgresConnector:
    """
    A connector class to interact with a PostgreSQL database using PySpark's JDBC interface.

    This class provides methods to connect, check the connection, execute queries,
    and stop the underlying SparkSession.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str, spark_session: SparkSession):
        """
        Initializes the connector with PostgreSQL database connection details.

        This method sets up the configuration for the connection but does not
        establish it. Call the .connect() method to verify connectivity.

        Args:
            host (str): The hostname or IP address of the PostgreSQL server.
            port (int): The port number for PostgreSQL (e.g., 5432).
            database (str): The name of the database to connect to.
            user (str): The username for authentication.
            password (str): The password for the user.
            spark_session (SparkSession): The pre-configured SparkSession object.
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.spark = spark_session

        # PostgreSQL JDBC URL format
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        self.jdbc_driver = "org.postgresql.Driver"

        # JDBC properties
        self.connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": self.jdbc_driver
        }

        self._is_connected = False
        print(f"✅ Connector initialized for PostgreSQL DB '{self.database}' at: {self.host}")

    def connect(self) -> bool:
        """
        Establishes and verifies the connection to the PostgreSQL database.

        Returns:
            bool: True if the connection is successfully verified, False otherwise.
        """
        print("🟡 Attempting to connect to the PostgreSQL database...")
        if self.check_connection():
            self._is_connected = True
            return True
        else:
            self._is_connected = False
            return False

    def check_connection(self) -> bool:
        """
        Checks if a valid connection to the database can be made.

        This is done by executing a simple query to get the current version of PostgreSQL.

        Returns:
            bool: True if the test query succeeds, False otherwise.
        """
        try:
            # A simple query to test connectivity and get the DB version
            test_df = self.query("SELECT version()", is_test=True)
            version_info = test_df.first()[0]
            print(f"✔️ Connection successful. {version_info}")
            return True
        except (Py4JJavaError, AnalysisException, AttributeError) as e:
            print(f"❌ Connection failed. Check credentials, network, and JDBC driver. Error: {e}")
            return False

    def query(self, sql_query: str, is_test: bool = False) -> DataFrame:
        """
        Executes a SQL SELECT query and returns the result as a Spark DataFrame.

        Args:
            sql_query (str): The SQL query to execute.
            is_test (bool): Internal flag to suppress print statements during checks.

        Returns:
            DataFrame: A Spark DataFrame with the query results, or an empty DataFrame on failure.
        """
        if not is_test:
            print(f"Executing query: \"{sql_query}\"")

        try:
            query_as_table = f"({sql_query}) as compatibility_alias"

            reader = self.spark.read \
                .jdbc(url=self.jdbc_url,
                      table=query_as_table,
                      properties=self.connection_properties)

            return reader
        except (Py4JJavaError, AnalysisException) as e:
            if not is_test:
                print(f"❌ Query failed: {e}")
            return self.spark.createDataFrame([], schema=self.spark.sparkContext.emptyRDD().schema)

    def stop_connection(self):
        """
        Stops the underlying SparkSession. ⚠️ This is a terminal operation.
        """
        if self.spark:
            print("🔴 Stopping the SparkSession...")
            self.spark.stop()
            self._is_connected = False
            print("✅ SparkSession stopped.")


################################################################################
##
## Main Execution Block with Examples
##
################################################################################

if __name__ == "__main__":

    # --------------------------------------------------------------------------
    ## Example 1: Connecting to a Local or On-Premise PostgreSQL Database
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 1: ON-PREMISE POSTGRESQL DATABASE")
    print("==========================================================")

    # Setup Spark Session for the local example
    spark_onprem = SparkSession.builder \
        .appName("OnPremise Postgres Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT: Replace with your actual on-premise PostgreSQL details ---
    ONPREM_HOST = "localhost"
    ONPREM_PORT = 5432
    ONPREM_DB = "dvdrental"
    ONPREM_USER = os.environ.get("PG_USER", "postgres")  # Best practice: use env vars
    ONPREM_PASSWORD = os.environ.get("PG_PASS", "your_password")  # Never hardcode passwords

    # Instantiate the connector
    onprem_connector = PostgresConnector(
        host=ONPREM_HOST,
        port=ONPREM_PORT,
        database=ONPREM_DB,
        user=ONPREM_USER,
        password=ONPREM_PASSWORD,
        spark_session=spark_onprem
    )

    # Connect and verify
    if onprem_connector.connect():
        print("\nQuerying film data from the public schema:")
        film_df = onprem_connector.query("SELECT film_id, title, release_year FROM film LIMIT 5")
        film_df.show()

    # Stop the SparkSession
    onprem_connector.stop_connection()

    print("\n\n")

    # --------------------------------------------------------------------------
    ## Example 2: Connecting to a Remote or Cloud PostgreSQL Database (e.g., AWS RDS)
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 2: REMOTE/CLOUD POSTGRESQL DATABASE")
    print("==========================================================")

    # Setup a new Spark Session for the remote example
    spark_cloud = SparkSession.builder \
        .appName("Cloud Postgres Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT: Replace with your actual Cloud/Remote PostgreSQL details ---
    CLOUD_HOST = "your-rds-instance.random-chars.region.rds.amazonaws.com"
    CLOUD_PORT = 5432
    CLOUD_DB = "production"
    CLOUD_USER = os.environ.get("PG_CLOUD_USER", "admin")
    CLOUD_PASSWORD = os.environ.get("PG_CLOUD_PASS", "your_cloud_password")

    # Instantiate the connector
    cloud_connector = PostgresConnector(
        host=CLOUD_HOST,
        port=CLOUD_PORT,
        database=CLOUD_DB,
        user=CLOUD_USER,
        password=CLOUD_PASSWORD,
        spark_session=spark_cloud
    )

    # Connect and verify
    if cloud_connector.connect():
        print("\nQuerying customer data from the remote database:")
        customers_df = cloud_connector.query("SELECT customer_id, first_name, last_name, email FROM customers LIMIT 5")
        customers_df.show()

    # Stop the SparkSession
    cloud_connector.stop_connection()

    print("\n==========================================================")
    print("✅ BOTH EXAMPLES FINISHED.")
    print("==========================================================")