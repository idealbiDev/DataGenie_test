
import os
import json
from pathlib import Path

def create_connection_string(config):
    """
    Generates a database connection string based on the configuration.
    
    Args:
        config (dict): A dictionary containing database configuration.
        
    Returns:
        str: The formatted connection string, or None if the db_type is unsupported.
    """
    db_type = config.get('db_type', '').lower()
    username = config.get('username')
    password = config.get('password')
    host = config.get('host')
    port = config.get('port')
    database = config.get('database')
    
    connection_string = None

    if db_type == 'mysql':
        # Example format for SQLAlchemy with MySQL-connector:
        # 'mysql+mysqlconnector://user:password@host:port/database'
        # Note: You need to install the appropriate driver (e.g., 'pip install mysql-connector-python')
        connection_string = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
    elif db_type == 'mssql':
        # Example format for SQLAlchemy with pyodbc:
        # 'mssql+pyodbc://user:password@host:port/database?driver=ODBC Driver 17 for SQL Server'
        # Note: You'll need the ODBC driver and pyodbc ('pip install pyodbc')
        connection_string = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC Driver 17 for SQL Server"
    elif db_type == 'redshift':
        # Example format for SQLAlchemy with psycopg2:
        # 'postgresql+psycopg2://user:password@host:port/database'
        # Note: Redshift uses the PostgreSQL protocol, so you use the psycopg2 driver
        connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    else:
        print(f"Error: Unsupported database type '{db_type}'")
        return None

    return connection_string
