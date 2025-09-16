import pyodbc
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging

# Load .env variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DBManager:
    """Manage database connections for SQL Server and Redshift."""

    @staticmethod
    def get_connection(db_type='local'):
        """
        Get a database connection for SQL Server or Redshift.
        
        Parameters:
        - db_type (str): 'local' for SQL Server, 'redshift' for Redshift.
        
        Returns:
        - Connection object (pyodbc or psycopg2).
        """
        try:
            if db_type == 'local':
                conn_str = (
                    f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
                    f"SERVER={os.getenv('DB_SERVER')};"
                    f"DATABASE={os.getenv('DB_NAME')};"
                    f"Trusted_Connection=yes;"
                )
                conn = pyodbc.connect(conn_str)
                logging.info(f"Connected to SQL Server: {os.getenv('DB_NAME')}")
                return conn
            else:  # Redshift
                conn = psycopg2.connect(
                    host=os.getenv('REDSHIFT_HOST'),
                    port=os.getenv('REDSHIFT_PORT'),
                    database=os.getenv('REDSHIFT_DATABASE'),
                    user=os.getenv('REDSHIFT_USER'),
                    password=os.getenv('REDSHIFT_PASSWORD')
                )
                logging.info(f"Connected to Redshift: {os.getenv('REDSHIFT_DATABASE')}")
                return conn
        except Exception as e:
            logging.error(f"Error connecting to {db_type} database: {str(e)}")
            raise

    @staticmethod
    def get_dictionary_storage(db_type='local'):
        """
        Get SQLAlchemy engine for dictionary storage.
        
        Parameters:
        - db_type (str): 'local' for SQL Server, 'redshift' for Redshift.
        
        Returns:
        - SQLAlchemy engine.
        """
        try:
            if db_type == 'local':
                conn_str = (
                    f"mssql+pyodbc://{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}"
                    f"?driver={os.getenv('DB_DRIVER').replace(' ', '+')}&Trusted_Connection=yes"
                )
                engine = create_engine(conn_str)
                logging.info(f"Created SQLAlchemy engine for SQL Server: {os.getenv('DB_NAME')}")
                return engine
            else:  # Redshift
                conn_str = (
                    f"postgresql+psycopg2://{os.getenv('REDSHIFT_USER')}:{os.getenv('REDSHIFT_PASSWORD')}"
                    f"@{os.getenv('REDSHIFT_HOST')}:{os.getenv('REDSHIFT_PORT')}/{os.getenv('REDSHIFT_DATABASE')}"
                )
                engine = create_engine(conn_str)
                logging.info(f"Created SQLAlchemy engine for Redshift: {os.getenv('REDSHIFT_DATABASE')}")
                return engine
        except Exception as e:
            logging.error(f"Error creating storage engine for {db_type}: {str(e)}")
            raise