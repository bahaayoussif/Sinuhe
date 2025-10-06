#
# This single file contains the SqlLiteConnector class and two usage examples.
#
# REMINDER: You MUST provide the SQLite JDBC driver to Spark for this code to run.
# For example, using: spark-submit --jars /path/to/driver.jar this_script.py
#

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


################################################################################
##
## The SqlLiteConnector Class Definition
##
################################################################################

class SqlLiteConnector:
    """
    A connector class to interact with a SQLite database using PySpark's JDBC interface.

    This class provides methods to connect, check the connection, execute queries,
    and stop the underlying SparkSession.
    """

    def __init__(self, db_path: str, spark_session: SparkSession):
        """
        Initializes the connector with database path and an active SparkSession.

        This method sets up the configuration for the connection but does not
        establish it. Call the .connect() method to verify connectivity.

        Args:
            db_path (str): The file path to the SQLite database.
                           Example: '/path/to/my_data.sqlite'
            spark_session (SparkSession): The pre-configured SparkSession object that has
                                          access to the SQLite JDBC driver.
        """
        self.db_path = db_path
        self.spark = spark_session
        self.jdbc_url = f"jdbc:sqlite:{self.db_path}"
        self.jdbc_driver = "org.sqlite.JDBC"
        self._is_connected = False
        print(f"✅ Connector initialized for database: {self.db_path}")

    def connect(self) -> bool:
        """
        Establishes and verifies the connection to the SQLite database.

        This method runs a simple test query to ensure that the database is
        accessible and the JDBC driver is correctly configured.

        Returns:
            bool: True if the connection is successfully verified, False otherwise.
        """
        print("🟡 Attempting to connect to the database...")
        if self.check_connection():
            self._is_connected = True
            return True
        else:
            self._is_connected = False
            return False

    def check_connection(self) -> bool:
        """
        Checks if a valid connection to the database can be made.

        This is done by executing a query to fetch the SQLite database version.
        It is used internally by the .connect() method but can also be called
        directly to check the connection status at any time.

        Returns:
            bool: True if the test query succeeds, False otherwise.
        """
        try:
            # Perform a simple, non-intrusive query to test connectivity.
            version_df = self.query("SELECT sqlite_version() as version", is_test=True)
            version = version_df.first()["version"]
            print(f"✔️ Connection successful. SQLite Version: {version}")
            return True
        except (Py4JJavaError, AnalysisException, AttributeError):
            print("❌ Connection failed. Check the database file path and ensure the JDBC driver is provided.")
            return False

    def query(self, sql_query: str, is_test: bool = False) -> DataFrame:
        """
        Executes a SQL SELECT query and returns the result as a Spark DataFrame.

        The provided SQL query is executed against the SQLite database. The results
        are loaded into a distributed Spark DataFrame for further processing.

        Args:
            sql_query (str): The SQL query to execute.
            is_test (bool): Internal flag to suppress print statements during connection checks.

        Returns:
            DataFrame: A Spark DataFrame containing the results of the query.
                       If the query fails, an empty DataFrame is returned.
        """
        if not is_test:
            print(f"Executing query: \"{sql_query}\"")

        try:
            # For PySpark's JDBC source, a full query must be wrapped
            # in parentheses and given an alias to be treated like a table.
            query_as_table = f"({sql_query}) as temp_table"

            reader = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("driver", self.jdbc_driver) \
                .option("dbtable", query_as_table)

            return reader.load()
        except (Py4JJavaError, AnalysisException) as e:
            if not is_test:
                print(f"❌ Query failed: {e}")
            # On failure, return an empty DataFrame to prevent crashes.
            return self.spark.createDataFrame([], schema=self.spark.sparkContext.emptyRDD().schema)

    def stop_connection(self):
        """
        Stops the underlying SparkSession.

        ⚠️ Important: This is a terminal operation. It stops the entire Spark
        context, releasing all its resources. This should typically be the
        last operation in your application. It does not just close a single
        database connection but shuts down Spark itself.
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
    ## Example 1: Connecting to a Local SQLite Database
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 1: LOCAL DATABASE")
    print("==========================================================")

    # Setup Spark Session for the local example
    spark_local = SparkSession.builder \
        .appName("Local SQLite Example") \
        .master("local[*]") \
        .getOrCreate()

    # The path to your local database file.
    # SQLite will create this file if it doesn't exist upon the first write.
    local_db_file = "my_local_database.sqlite"

    # Instantiate the connector
    local_connector = SqlLiteConnector(db_path=local_db_file, spark_session=spark_local)

    # Connect and verify
    if local_connector.connect():
        # A simple query to prove connection. This shows internal system tables.
        # Your database will be empty until you write data to it.
        print("\nQuerying system tables to confirm connection:")
        tables_df = local_connector.query("SELECT name FROM sqlite_master WHERE type='table'")
        tables_df.show()

    # Stop the SparkSession
    local_connector.stop_connection()

    # Clean up the created file for the next run
    if os.path.exists(local_db_file):
        os.remove(local_db_file)

    print("\n\n")

    # --------------------------------------------------------------------------
    ## Example 2: Connecting to a "Remote" SQLite Database (Network Share)
    # --------------------------------------------------------------------------
    print("==========================================================")
    print("🚀 STARTING EXAMPLE 2: REMOTE DATABASE (NETWORK SHARE)")
    print("==========================================================")

    # Setup a new Spark Session for the remote example
    spark_remote = SparkSession.builder \
        .appName("Remote SQLite Example") \
        .master("local[*]") \
        .getOrCreate()

    # --- IMPORTANT ---
    # Replace this with the actual UNC path to a database file on a network share.
    # The machine running this script must have permissions to access this path.
    #
    # Windows Example: r"\\fileserver\shared_data\production.sqlite"
    # Linux/macOS Example (with mounted share): "/mnt/shared_data/production.sqlite"
    remote_db_file = r"//network-server/shared-folder/remote_db.sqlite"

    # Instantiate the connector
    remote_connector = SqlLiteConnector(db_path=remote_db_file, spark_session=spark_remote)

    # Connect and verify
    if remote_connector.connect():
        # Query an existing table you expect to be in the remote database.
        # This query will fail if the table 'products' does not exist.
        print("\nQuerying 'products' table from the remote database:")
        products_df = remote_connector.query("SELECT product_id, name, price FROM products LIMIT 10")

        if products_df:
            products_df.show()

    # Stop the SparkSession
    remote_connector.stop_connection()

    print("\n==========================================================")
    print("✅ BOTH EXAMPLES FINISHED.")
    print("==========================================================")