#
# This single file contains the MySqlConnector class and two usage examples.
#
# REMINDER: You MUST provide the MySQL JDBC driver (Connector/J) to Spark for this code to run.
# For example, using: spark-submit --jars /path/to/mysql-connector-j-8.4.0.jar this_script.py
#

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


################################################################################
##
## The MySqlConnector Class Definition
##
################################################################################

class MySqlConnector:
    """
    A connector class to interact with a MySQL database using PySpark's JDBC interface.

    This class provides methods to connect, check the connection, execute queries,
    and stop the underlying SparkSession.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str, spark_session: SparkSession):
        """
        Initializes the connector with MySQL database connection details.

        This method sets up the configuration for the connection but does not
        establish it. Call the .connect() method to verify connectivity.

        Args:
            host (str): The hostname or IP address of the MySQL server.
            port (int): The port number for MySQL (e.g., 3306).
            database (str): The name of the database schema to connect to.
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

        # MySQL JDBC URL format
        self.jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
        self.jdbc_driver = "com.mysql.cj.jdbc.Driver"

        # JDBC properties
        self.connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": self.jdbc_driver
        }

        self._is_connected = False
        print(f"✅ Connector initialized for MySQL DB '{self.database}' at: {self.host}")

    def connect(self) -> bool:
        """
        Establishes and verifies the connection to the MySQL database.

        Returns:
            bool: True if the connection is successfully verified, False otherwise.
        """
        print("🟡 Attempting to connect to the MySQL database...")
        if self.check_connection():
            self._is_connected = True
            return True
        else:
            self._is_connected = False
            return False

    def check_connection(self) -> bool:
        """
        Checks if a valid connection to the database can be made.

        This is done by executing a simple query to get the current version of MySQL.

        Returns:
            bool: True if the test query succeeds, False otherwise.
        """
        try:
            # A simple query to test connectivity and get the DB version
            test_df = self.query("SELECT VERSION()", is_test=True)
            version_info = test_df.first()[0]
            print(f"✔️ Connection successful. MySQL Version: {version_info}")
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
    ## Example 1: Connecting to a Local or On-Premise MySQL Database
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 1: ON-PREMISE MYSQL DATABASE")
    print("==========================================================")

    # Setup Spark Session for the local example
    spark_onprem = SparkSession.builder \
        .appName("OnPremise MySql Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT: Replace with your actual on-premise MySQL details ---
    ONPREM_HOST = "127.0.0.1"
    ONPREM_PORT = 3306
    ONPREM_DB = "sakila"  # A common sample database for MySQL
    ONPREM_USER = os.environ.get("MYSQL_USER", "root")  # Best practice: use env vars
    ONPREM_PASSWORD = os.environ.get("MYSQL_PASS", "your_password")  # Never hardcode passwords

    # Instantiate the connector
    onprem_connector = MySqlConnector(
        host=ONPREM_HOST,
        port=ONPREM_PORT,
        database=ONPREM_DB,
        user=ONPREM_USER,
        password=ONPREM_PASSWORD,
        spark_session=spark_onprem
    )

    # Connect and verify
    if onprem_connector.connect():
        print("\nQuerying actor data from the sakila database:")
        actor_df = onprem_connector.query("SELECT actor_id, first_name, last_name FROM actor LIMIT 5")
        actor_df.show()

    # Stop the SparkSession
    onprem_connector.stop_connection()

    print("\n\n")

    # --------------------------------------------------------------------------
    ## Example 2: Connecting to a Remote or Cloud MySQL Database (e.g., AWS RDS)
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 2: REMOTE/CLOUD MYSQL DATABASE")
    print("==========================================================")

    # Setup a new Spark Session for the remote example
    spark_cloud = SparkSession.builder \
        .appName("Cloud MySql Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT: Replace with your actual Cloud/Remote MySQL details ---
    CLOUD_HOST = "your-rds-mysql-instance.random-chars.region.rds.amazonaws.com"
    CLOUD_PORT = 3306
    CLOUD_DB = "webappdb"
    CLOUD_USER = os.environ.get("MYSQL_CLOUD_USER", "mainuser")
    CLOUD_PASSWORD = os.environ.get("MYSQL_CLOUD_PASS", "your_cloud_password")

    # Instantiate the connector
    cloud_connector = MySqlConnector(
        host=CLOUD_HOST,
        port=CLOUD_PORT,
        database=CLOUD_DB,
        user=CLOUD_USER,
        password=CLOUD_PASSWORD,
        spark_session=spark_cloud
    )

    # Connect and verify
    if cloud_connector.connect():
        print("\nQuerying user profiles from the remote webapp database:")
        users_df = cloud_connector.query("SELECT user_id, username, created_at FROM user_profiles LIMIT 5")
        users_df.show()

    # Stop the SparkSession
    cloud_connector.stop_connection()

    print("\n==========================================================")
    print("✅ BOTH EXAMPLES FINISHED.")
    print("==========================================================")