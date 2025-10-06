#
# This single file contains the OracleConnector class and two usage examples.
#
# REMINDER: You MUST provide the Oracle JDBC (OJDBC) driver to Spark for this to run.
# For example, using: spark-submit --jars /path/to/ojdbc8.jar this_script.py
#

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


################################################################################
##
## The OracleConnector Class Definition
##
################################################################################

class OracleConnector:
    """
    A connector class to interact with an Oracle database using PySpark's JDBC interface.

    This class provides methods to connect, check the connection, execute queries,
    and stop the underlying SparkSession.
    """

    def __init__(self, host: str, port: int, service_name: str, user: str, password: str, spark_session: SparkSession):
        """
        Initializes the connector with Oracle database connection details.

        This method sets up the configuration for the connection but does not
        establish it. Call the .connect() method to verify connectivity.

        Args:
            host (str): The hostname or IP address of the Oracle database server.
            port (int): The port number for the Oracle listener (e.g., 1521).
            service_name (str): The service name of the Oracle database.
            user (str): The username for authentication.
            password (str): The password for the user.
            spark_session (SparkSession): The pre-configured SparkSession object.
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
        self.spark = spark_session

        # Oracle JDBC URL format
        self.jdbc_url = f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}"
        self.jdbc_driver = "oracle.jdbc.driver.OracleDriver"

        # JDBC properties
        self.connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": self.jdbc_driver
        }

        self._is_connected = False
        print(f"✅ Connector initialized for Oracle DB at: {self.host}")

    def connect(self) -> bool:
        """
        Establishes and verifies the connection to the Oracle database.

        Returns:
            bool: True if the connection is successfully verified, False otherwise.
        """
        print("🟡 Attempting to connect to the Oracle database...")
        if self.check_connection():
            self._is_connected = True
            return True
        else:
            self._is_connected = False
            return False

    def check_connection(self) -> bool:
        """
        Checks if a valid connection to the database can be made.

        This is done by executing a simple query against Oracle's DUAL table.

        Returns:
            bool: True if the test query succeeds, False otherwise.
        """
        try:
            # A standard, non-intrusive query in Oracle to test connectivity
            test_df = self.query("SELECT 1 FROM DUAL", is_test=True)
            if test_df.count() == 1:
                print("✔️ Connection successful.")
                return True
            else:
                return False
        except (Py4JJavaError, AnalysisException, AttributeError) as e:
            print(f"❌ Connection failed. Check credentials, network, and OJDBC driver. Error: {e}")
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
            query_as_table = f"({sql_query}) compatibility_alias"

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
    ## Example 1: Connecting to a Local or On-Premise Oracle Database
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 1: ON-PREMISE ORACLE DATABASE")
    print("==========================================================")

    # Setup Spark Session for the local example
    spark_onprem = SparkSession.builder \
        .appName("OnPremise Oracle Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT: Replace with your actual on-premise Oracle DB details ---
    ONPREM_HOST = "oracle-server.company.local"
    ONPREM_PORT = 1521
    ONPREM_SERVICE = "ORCLPDB1"
    ONPREM_USER = os.environ.get("ORACLE_USER", "hr")  # Best practice: use env vars
    ONPREM_PASSWORD = os.environ.get("ORACLE_PASS", "your_password")  # Never hardcode passwords

    # Instantiate the connector
    onprem_connector = OracleConnector(
        host=ONPREM_HOST,
        port=ONPREM_PORT,
        service_name=ONPREM_SERVICE,
        user=ONPREM_USER,
        password=ONPREM_PASSWORD,
        spark_session=spark_onprem
    )

    # Connect and verify
    if onprem_connector.connect():
        print("\nQuerying employees from the HR schema:")
        employees_df = onprem_connector.query("SELECT * FROM employees WHERE ROWNUM <= 5")
        employees_df.show()

    # Stop the SparkSession
    onprem_connector.stop_connection()

    print("\n\n")

    # --------------------------------------------------------------------------
    ## Example 2: Connecting to a Remote or Cloud Oracle Database
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 2: REMOTE/CLOUD ORACLE DATABASE")
    print("==========================================================")

    # Setup a new Spark Session for the remote example
    spark_cloud = SparkSession.builder \
        .appName("Cloud Oracle Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT: Replace with your actual Cloud/Remote Oracle DB details ---
    CLOUD_HOST = "xyz.oraclecloud.com"  # Public endpoint of your cloud DB
    CLOUD_PORT = 1521
    CLOUD_SERVICE = "salespdb.atp.oraclecloud.com"
    CLOUD_USER = os.environ.get("ORACLE_CLOUD_USER", "admin")
    CLOUD_PASSWORD = os.environ.get("ORACLE_CLOUD_PASS", "your_cloud_password")

    # Instantiate the connector
    cloud_connector = OracleConnector(
        host=CLOUD_HOST,
        port=CLOUD_PORT,
        service_name=CLOUD_SERVICE,
        user=CLOUD_USER,
        password=CLOUD_PASSWORD,
        spark_session=spark_cloud
    )

    # Connect and verify
    if cloud_connector.connect():
        print("\nQuerying sales data from the remote database:")
        sales_df = cloud_connector.query(
            "SELECT product_id, sale_amount FROM sales WHERE sale_date > SYSDATE - 30 AND ROWNUM <= 5")
        sales_df.show()

    # Stop the SparkSession
    cloud_connector.stop_connection()

    print("\n==========================================================")
    print("✅ BOTH EXAMPLES FINISHED.")
    print("==========================================================")
