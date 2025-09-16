import pandas as pd
import os
from dotenv import load_dotenv
from .db_manager import DBManager
import logging
from sqlalchemy import text

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_schemas(db_type='local'):
    """
    Get list of schemas in the database.
    
    Parameters:
    - db_type: Database type ('local' for SQL Server, else Redshift).
    
    Returns:
    - list: List of schema names.
    """
    try:
        engine = DBManager.get_dictionary_storage(db_type)
        with engine.connect() as conn:
            if db_type == 'local':
                query = text("""
                SELECT SCHEMA_NAME 
                FROM INFORMATION_SCHEMA.SCHEMATA 
                WHERE CATALOG_NAME = :db_name
                """)
                params = {'db_name': os.getenv('DB_NAME')}
            else:  # Redshift
                query = text("""
                SELECT nspname 
                FROM pg_namespace 
                WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'
                """)
                params = {}
            
            df = pd.read_sql(query, conn, params=params)
            return df['SCHEMA_NAME'].tolist() if db_type == 'local' else df['nspname'].tolist()
    except Exception as e:
        logging.error(f"Failed to fetch schemas: {str(e)}")
        raise

def get_tables(db_type='local', schema=None):
    """
    Get list of tables in the specified schema.
    
    Parameters:
    - db_type: Database type ('local' for SQL Server, else Redshift).
    - schema (str): Schema name (default 'IDIMDM' for SQL Server, 'public' for Redshift).
    
    Returns:
    - list: List of table names.
    """
    try:
        engine = DBManager.get_dictionary_storage(db_type)
        schema = schema or ('IDIMDM' if db_type == 'local' else 'public')
        logging.info(f"Fetching tables for db_type={db_type}, schema={schema}")
        print(f"Fetching tables for schema: {schema}")
        
        with engine.connect() as conn:
            if db_type == 'local':
                query = text("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_CATALOG = :db_name AND TABLE_SCHEMA = :schema
                """)
                params = {'db_name': os.getenv('DB_NAME'), 'schema': schema}
            else:  # Redshift
                query = text("""
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = :schema
                """)
                params = {'schema': schema}
            
            logging.info(f"Executing query: {str(query)} with params={params}")
            df = pd.read_sql(query, conn, params=params)
            tables = df['TABLE_NAME'].tolist() if db_type == 'local' else df['tablename'].tolist()
            logging.info(f"Retrieved tables: {tables}")
            print(f"Retrieved tables: {tables}")
            return tables
    except Exception as e:
        logging.error(f"Failed to fetch tables for schema {schema}: {str(e)}")
        print(f"Error fetching tables: {str(e)}")
        return []
def get_table_sample(table_name, db_type='local', limit=5, schema=None):
    """
    Get sample rows from a table.
    
    Parameters:
    - table_name (str): Name of the table.
    - db_type (str): Database type ('local' for SQL Server, else Redshift).
    - limit (int): Number of rows to return (default 5).
    - schema (str): Schema name (optional).
    
    Returns:
    - pd.DataFrame: Sample data.
    """
    try:
        engine = DBManager.get_dictionary_storage(db_type)
        with engine.connect() as conn:
            schema = schema or ('IDIMDM' if db_type == 'local' else 'public')
            if db_type == 'local':
                query = text(f"SELECT TOP {limit} * FROM [{schema}].[{table_name}]")
                params = {}
            else:
                query = text(f"SELECT * FROM {schema}.{table_name} LIMIT :limit")
                params = {'limit': limit}
            
            df = pd.read_sql(query, conn, params=params)
            return df
    except Exception as e:
        logging.error(f"Error fetching sample for {table_name}: {str(e)}")
        raise

def get_column_descriptions(table_name, db_type='local', schema=None):
    """
    Retrieve column descriptions from data_dictionary table.
    
    Parameters:
    - table_name (str): Name of the table.
    - db_type (str): Database type ('local' for SQL Server, else Redshift).
    - schema (str): Schema name (optional).
    
    Returns:
    - dict: Mapping of column names to descriptions.
    """
    try:
        engine = DBManager.get_dictionary_storage(db_type)
        with engine.connect() as conn:
            schema = schema or ('IDIMDM' if db_type == 'local' else 'public')
            if db_type == 'local':
                query = text("""
                SELECT ColumnName, ColumnDescription
                FROM IDIMDM.data_dictionary
                WHERE TableName = :table_name
                """)
                params = {'table_name': table_name}
            else:  # Redshift
                query = text("""
                SELECT column_name, column_description
                FROM public.data_dictionary
                WHERE table_name = :table_name
                """)
                params = {'table_name': table_name}
            
            df = pd.read_sql(query, conn, params=params)
            return dict(zip(df['ColumnName' if db_type == 'local' else 'column_name'], 
                            df['ColumnDescription' if db_type == 'local' else 'column_description']))
    except Exception as e:
        logging.error(f"Error fetching descriptions for {table_name}: {str(e)}")
        # Fallback: Get column names from table
        try:
            with engine.connect() as conn:
                if db_type == 'local':
                    query = text(f"SELECT TOP 1 * FROM [{schema}].[{table_name}]")
                else:
                    query = text(f"SELECT * FROM {schema}.{table_name} LIMIT 1")
                df = pd.read_sql(query, conn)
                return {col: 'No description available' for col in df.columns}
        except Exception as fallback_e:
            logging.error(f"Fallback failed for {table_name}: {str(fallback_e)}")
            return {}

def get_table_exists(table_name, db_type='local', schema=None):
    """
    Check if a table exists in the database.
    
    Parameters:
    - table_name (str): Name of the table.
    - db_type (str): Database type ('local' for SQL Server, else Redshift).
    - schema (str): Schema name (optional).
    
    Returns:
    - bool: True if table exists, False otherwise.
    """
    try:
        engine = DBManager.get_dictionary_storage(db_type)
        with engine.connect() as conn:
            schema = schema or ('IDIMDM' if db_type == 'local' else 'public')
            if db_type == 'local':
                query = text("""
                SELECT 1 FROM sys.objects WHERE name = :table_name AND type = 'U' AND SCHEMA_NAME(schema_id) = :schema
                """)
                params = {'table_name': table_name, 'schema': schema}
            else:  # Redshift
                query = text("""
                SELECT 1 FROM pg_tables WHERE tablename = :table_name AND schemaname = :schema
                """)
                params = {'table_name': table_name, 'schema': schema}
            
            result = conn.execute(query, params).fetchone()
            return result is not None
    except Exception as e:
        logging.error(f"Error checking table existence for {table_name}: {str(e)}")

        return False
    

def getDQRules():
    dq_rules = [
        {'rule': 'NotNull', 'description': 'Ensures the column value is not null'},
        {'rule': 'ValidFormat', 'description': 'Checks if the value matches the expected format'},
        {'rule': 'Unique', 'description': 'Ensures the column value is unique'},
        {'rule': 'RangeCheck', 'description': 'Verifies the value is within an acceptable range'}
    ]

    return dq_rules