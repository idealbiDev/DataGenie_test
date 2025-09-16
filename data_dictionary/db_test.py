import pyodbc
from pyodbc import Error as PyodbcError

# SQL Server configuration
sql_server_config = {
  'db_type': 'mssql_local',
  'server': 'localhost\\MSSQLSERVER01',
  'port': '1433',
  'database': 'IBIDemoDB',
  'username': 'ibidemo_user',
  'password': 'IbiDemo@2025!',
  'driver': 'ODBC Driver 18 for SQL Server',
  'timeout': '30'
}


def get_sql_server_connection(config):
    """Establish a connection to SQL Server"""
    try:
        conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']},{config['port']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']}"
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except PyodbcError as e:
        print(f"Error connecting to SQL Server: {e}")
        return None

def test_sql_server_connection(config):
    """Test the SQL Server connection"""
    conn = get_sql_server_connection(config)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION, DB_NAME(), SYSTEM_USER, SCHEMA_NAME()")
            version, db, user, schema = cursor.fetchone()
            
            print("SQL Server Connection successful!")
            print(f"Version: {version}")
            print(f"Database: {db}")
            print(f"User: {user}")
            print(f"Current schema: {schema}")
            
            cursor.close()
        except Exception as e:
            print(f"Error executing test query: {e}")
        finally:
            conn.close()

def get_sql_server_schemas(config):
    """Get all schema names in SQL Server"""
    schemas = []
    conn = get_sql_server_connection(config)
    if conn:
        try:
            cursor = conn.cursor()
            query = """
            SELECT name 
            FROM sys.schemas
            WHERE principal_id = 1  -- Only user-created schemas
            ORDER BY name
            """
            cursor.execute(query)
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
        except Exception as e:
            print(f"Error fetching schemas: {e}")
        finally:
            conn.close()
    return schemas

def get_sql_server_tables(config, schema_name):
    """Get all tables in a specific schema"""
    tables = []
    conn = get_sql_server_connection(config)
    if conn:
        try:
            cursor = conn.cursor()
            query = """
            SELECT t.name 
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ?
            ORDER BY t.name
            """
            cursor.execute(query, schema_name)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
        except Exception as e:
            print(f"Error fetching tables: {e}")
        finally:
            conn.close()
    return tables

def get_sql_server_table_columns(config, schema_name, table_name):
    """Get column information for a specific table"""
    columns = []
    conn = get_sql_server_connection(config)
    if conn:
        try:
            cursor = conn.cursor()
            query = """
            SELECT c.name, t.name AS type, c.is_nullable
            FROM sys.columns c
            INNER JOIN sys.tables tab ON c.object_id = tab.object_id
            INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE s.name = ? AND tab.name = ?
            ORDER BY c.column_id
            """
            cursor.execute(query, (schema_name, table_name))
            columns = cursor.fetchall()
            cursor.close()
        except Exception as e:
            print(f"Error fetching columns: {e}")
        finally:
            conn.close()
    return columns

if __name__ == "__main__":
    # Test connection
    print("Testing SQL Server connection...")
    test_sql_server_connection(sql_server_config)
    
    # Get all schemas
    print("\nFetching all schemas...")
    schemas = get_sql_server_schemas(sql_server_config)
    print("Available schemas:")
    for schema in schemas:
        print(f" - {schema}")
    
    # Get tables in the first schema (if any exist)
    if schemas:
        target_schema = schemas[0]
        print(f"\nFetching tables in schema '{target_schema}'...")
        tables = get_sql_server_tables(sql_server_config, target_schema)
        print(f"Tables in schema '{target_schema}':")
        for table in tables:
            print(f" - {table}")
        
        # Example: Get columns for the first table (if any exist)
        if tables:
            example_table = tables[0]
            print(f"\nFetching columns for table '{target_schema}.{example_table}'...")
            columns = get_sql_server_table_columns(sql_server_config, target_schema, example_table)
            print(f"Columns in '{target_schema}.{example_table}':")
            for col in columns:
                print(f" - {col[0]} ({col[1]}, nullable: {'YES' if col[2] else 'NO'})")