# connectors/connector.py

from pyspark.sql import SparkSession

# Import the specific connector classes from their files within the same package
from .sqlliteconnector import SqlLiteConnector
from .postgresconnector import PostgresConnector
from .mysqlconnector import MySqlConnector
from .oracleconnector import OracleConnector


class Connector:
    """
    A factory class that creates and returns a specific database connector.
    This acts as the single entry point for creating any database connection.
    """

    @staticmethod
    def create(db_type: str, spark_session: SparkSession, **db_params):
        """
        Creates a specific database connector based on the db_type.

        Args:
            db_type (str): The type of database. Supported: 'sqlite', 'postgresql', 'mysql', 'oracle'.
            spark_session (SparkSession): The active SparkSession.
            **db_params: A dictionary of connection parameters specific to the database.

        Returns:
            An instance of a specific connector class (e.g., PostgresConnector) or raises ValueError.
        """
        db_type = db_type.lower()

        if db_type == 'sqlite':
            if "db_path" not in db_params:
                raise ValueError("Missing required parameter for SQLite: 'db_path'")
            return SqlLiteConnector(db_params["db_path"], spark_session)

        elif db_type == 'postgresql':
            required_keys = ['host', 'port', 'database', 'user', 'password']
            if not all(key in db_params for key in required_keys):
                raise ValueError(f"Missing one or more required parameters for PostgreSQL: {required_keys}")
            return PostgresConnector(**db_params, spark_session=spark_session)

        elif db_type == 'mysql':
            required_keys = ['host', 'port', 'database', 'user', 'password']
            if not all(key in db_params for key in required_keys):
                raise ValueError(f"Missing one or more required parameters for MySQL: {required_keys}")
            return MySqlConnector(**db_params, spark_session=spark_session)

        elif db_type == 'oracle':
            required_keys = ['host', 'port', 'service_name', 'user', 'password']
            if not all(key in db_params for key in required_keys):
                raise ValueError(f"Missing one or more required parameters for Oracle: {required_keys}")
            return OracleConnector(**db_params, spark_session=spark_session)

        else:
            raise ValueError(
                f"Unsupported database type: '{db_type}'. Supported types are 'sqlite', 'postgresql', 'mysql', 'oracle'.")
