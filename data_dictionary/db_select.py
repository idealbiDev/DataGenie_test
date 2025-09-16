from sqlalchemy import create_engine
import pandas as pd
import os
from pathlib import Path
import logging
from urllib.parse import quote_plus, unquote_plus
import pyodbc
import re
import json
from .db_conns import redshift_connection,get_tables_in_schema,get_tables_in_schema_test

from sqlalchemy.exc import DatabaseError


# Enable SQLAlchemy logging for debugging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Global database_configs dictionary
database_configs = {
    'redshift': {
        'display_name': 'Amazon Redshift',
        'icon': 'fab fa-aws',
        'color': '#FF4500',
        'driver': 'redshift+psycopg2',
        'default_port': '5439',
        'connection_string': '{driver}://{username}:{password}@{hostname}:{port}/{database}',
        'fields': [
            {'name': 'hostname', 'label': 'Cluster Endpoint', 'type': 'text', 'required': True, 'placeholder': 'your-cluster-name.xxxxxx.us-west-2.redshift.amazonaws.com', 'help_text': 'Your Redshift cluster endpoint without the port number'},
            {'name': 'port', 'label': 'Port', 'type': 'number', 'default': '5439', 'required': True, 'min': 1024, 'max': 65535, 'help_text': 'Typically 5439 for Redshift'},
            {'name': 'database', 'label': 'Database Name', 'type': 'text', 'required': True, 'placeholder': 'dev', 'help_text': 'Initial database to connect to'},
            {'name': 'username', 'label': 'Username', 'type': 'text', 'required': True, 'placeholder': 'awsuser', 'help_text': 'Master username for the cluster'},
            {'name': 'password', 'label': 'Password', 'type': 'password', 'required': True, 'help_text': 'Password for the master user'},
            {'name': 'timeout', 'label': 'Connection Timeout (seconds)', 'type': 'number', 'default': '30', 'required': False, 'help_text': 'Optional connection timeout setting'},
            {'name': 'sslmode', 'label': 'SSL Mode', 'type': 'select', 'options': [{'value': 'require', 'label': 'Require'}, {'value': 'verify-ca', 'label': 'Verify CA'}, {'value': 'verify-full', 'label': 'Verify Full'}, {'value': 'disable', 'label': 'Disable'}], 'default': 'require', 'required': False, 'help_text': 'SSL encryption setting'}
        ]
    },
    'mssql_local': {
        'display_name': 'Microsoft SQL Server (Local)',
        'icon': 'fab fa-windows',
        'color': '#005BAC',
        'driver': 'mssql+pyodbc',
        'default_port': '1433',
        'connection_string': '{driver}://{username}:{password}@{hostname}:{port}/{database}?driver={odbc_driver}&TrustServerCertificate=yes',
        'fields': [
            {'name': 'hostname', 'label': 'Server Name', 'type': 'text', 'required': True, 'placeholder': 'localhost or server_name\\instance_name', 'help_text': 'SQL Server instance name, e.g., localhost or SERVER\\SQLEXPRESS'},
            {'name': 'port', 'label': 'Port', 'type': 'number', 'default': '1433', 'required': True, 'min': 1024, 'max': 65535, 'help_text': 'Typically 1433 for SQL Server'},
            {'name': 'database', 'label': 'Database Name', 'type': 'text', 'required': True, 'placeholder': 'mydb', 'help_text': 'Name of the database to connect to'},
            {'name': 'username', 'label': 'Username', 'type': 'text', 'required': False, 'placeholder': 'sa', 'help_text': 'SQL Server username (leave blank for Windows Authentication)'},
            {'name': 'password', 'label': 'Password', 'type': 'password', 'required': False, 'help_text': 'Password for SQL Server user (leave blank for Windows Authentication)'},
            {'name': 'odbc_driver', 'label': 'ODBC Driver', 'type': 'select', 'options': [{'value': 'ODBC+Driver+17+for+SQL+Server', 'label': 'ODBC Driver 17 for SQL Server'}, {'value': 'ODBC%20Driver%2018%20for%20SQL%20Server', 'label': 'ODBC Driver 18 for SQL Server'}, {'value': 'SQL+Server', 'label': 'SQL Server'}], 'default': 'ODBC+Driver+17+for+SQL+Server', 'required': True, 'help_text': 'ODBC driver installed on your system'},
            {'name': 'trusted_connection', 'label': 'Use Windows Authentication', 'type': 'checkbox', 'default': 'yes', 'required': False, 'help_text': 'Use Windows credentials for authentication'},
            {'name': 'timeout', 'label': 'Connection Timeout (seconds)', 'type': 'number', 'default': '30', 'required': False, 'help_text': 'Optional connection timeout setting'},
            {'name': 'encrypt', 'label': 'Encrypt Connection', 'type': 'checkbox', 'default': 'yes', 'required': False, 'help_text': 'Enable encryption for the connection'}
        ]
    },
    'azure_sql': {
        'display_name': 'Azure SQL Database',
        'icon': 'fab fa-microsoft',
        'color': '#00BCF2',
        'driver': 'mssql+pyodbc',
        'default_port': '1433',
        'connection_string': '{driver}://{username}:{password}@{hostname}:{port}/{database}?driver={odbc_driver}&authentication={authentication}',
        'fields': [
            {'name': 'hostname', 'label': 'Server Name', 'type': 'text', 'required': True, 'placeholder': 'yourserver.database.windows.net', 'help_text': 'Fully qualified Azure SQL server name'},
            {'name': 'port', 'label': 'Port', 'type': 'number', 'default': '1433', 'required': True, 'min': 1024, 'max': 65535, 'help_text': 'Typically 1433 for Azure SQL Database'},
            {'name': 'database', 'label': 'Database Name', 'type': 'text', 'required': True, 'placeholder': 'mydb', 'help_text': 'Name of the Azure SQL database'},
            {'name': 'username', 'label': 'Username', 'type': 'text', 'required': False, 'placeholder': 'adminuser', 'help_text': 'SQL authentication username or Microsoft Entra ID user (leave blank for managed identity)'},
            {'name': 'password', 'label': 'Password', 'type': 'password', 'required': False, 'help_text': 'Password for SQL authentication (leave blank for Microsoft Entra ID or managed identity)'},
            {'name': 'odbc_driver', 'label': 'ODBC Driver', 'type': 'select', 'options': [{'value': 'ODBC+Driver+17+for+SQL+Server', 'label': 'ODBC Driver 17 for SQL Server'}, {'value': 'ODBC+Driver+18+for+SQL+Server', 'label': 'ODBC Driver 18 for SQL Server'}], 'default': 'ODBC+Driver+18+for+SQL+Server', 'required': True, 'help_text': 'ODBC driver installed on your system'},
            {'name': 'authentication', 'label': 'Authentication Type', 'type': 'select', 'options': [{'value': 'SqlPassword', 'label': 'SQL Authentication'}, {'value': 'ActiveDirectoryPassword', 'label': 'Microsoft Entra ID Password'}, {'value': 'ActiveDirectoryMSI', 'label': 'Microsoft Entra ID Managed Identity'}, {'value': 'ActiveDirectoryInteractive', 'label': 'Microsoft Entra ID Interactive'}], 'default': 'SqlPassword', 'required': True, 'help_text': 'Authentication method for Azure SQL Database'},
            {'name': 'timeout', 'label': 'Connection Timeout (seconds)', 'type': 'number', 'default': '30', 'required': False, 'help_text': 'Optional connection timeout setting'},
            {'name': 'encrypt', 'label': 'Encrypt Connection', 'type': 'checkbox', 'default': 'yes', 'required': False, 'help_text': 'Encryption is required for Azure SQL Database'}
        ]
    },
    'file_system': {
        'display_name': 'File System',
        'icon': 'fas fa-folder-open',
        'color': "#AF4C7D",
        'file_extensions': ['txt', 'csv', 'parquet'],
        'connection_string': 'file://{directory_path}',
        'fields': [
            {'name': 'directory_path', 'label': 'Directory Path', 'type': 'text', 'required': True, 'placeholder': './Uploads/', 'help_text': 'Path to the directory containing TXT, CSV, or Parquet files'},
            {'name': 'file_type', 'label': 'File Type', 'type': 'select', 'options': [{'value': 'txt', 'label': 'Text (TXT)'}, {'value': 'csv', 'label': 'CSV'}, {'value': 'parquet', 'label': 'Parquet'}], 'default': 'csv', 'required': True, 'help_text': 'Select the type of files to process'},
            {'name': 'delimiter', 'label': 'Delimiter (for CSV/TXT)', 'type': 'text', 'required': False, 'default': ',', 'placeholder': ',', 'help_text': 'Delimiter for CSV or TXT files (e.g., comma, tab, semicolon)'},
            {'name': 'header', 'label': 'Has Header (for CSV/TXT)', 'type': 'checkbox', 'default': 'yes', 'required': False, 'help_text': 'Check if the CSV/TXT file has a header row'},
            {'name': 'infer_schema', 'label': 'Infer Schema (for Parquet)', 'type': 'checkbox', 'default': 'yes', 'required': False, 'help_text': 'Check to automatically infer schema for Parquet files'}
        ]
    }
}

# Schema queries dictionary
schema_queries = {
    'redshift': """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1')
        ORDER BY schema_name;
    """,
    'mssql_local': """
        SELECT name AS schema_name
        FROM sys.schemas
        WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'db_owner', 'db_accessadmin', 
                           'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 
                           'db_datareader', 'db_datawriter', 'db_denydatareader', 
                           'db_denydatawriter')
        ORDER BY name;
    """,
    'azure_sql': """
        SELECT name AS schema_name
        FROM sys.schemas
        WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 
                           'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
        
        ORDER BY name;
        
    """

}

# Table queries dictionary
table_queries = {
    'redshift': """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """,
    'mssql_local': """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """,
    'azure_sql': """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
}
columns_query = {
    'redshift': """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position;
    """,
    'mssql_local': """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position;
    """,
    'azure_sql': """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position;
    """
}
top_records_query = {
    'redshift': """
        SELECT *
        FROM {schema_name}.{table_name}
        LIMIT 10;
    """,
    'mssql_local': """
        SELECT TOP 10 *
        FROM [{schema_name}].[{table_name}];
    """,
    'azure_sql': """
        SELECT TOP 10 *
        FROM [{schema_name}].[{table_name}];
    """
}

def get_db_types():
    """Return a list of database types with their display_name, icon, and color."""
    return [
        {
            'db_type': db_type,
            'display_name': config['display_name'],
            'icon': config['icon'],
            'color': config['color']
        }
        for db_type, config in database_configs.items()
    ]

def get_db_properties(db_type):
    """Return the properties for the specified database type."""
    selected_db_type = database_configs.get(db_type)
    if selected_db_type is None:
        raise ValueError(f"Unknown database type: {db_type}")
    return selected_db_type



def build_connection_string_2(db_type, form_data):
    """
    Build a connection string based on db_type and form_data.

    - For 'mssql_local', it returns a raw pyodbc connection string (for pyodbc.connect).
    - For other DBs, it returns a SQLAlchemy-compatible URI.
    """
    DB_TYPES = ['redshift', 'mssql_local', 'azure_sql', 'file_system']

    print(f"Building connection string for {db_type} with form_data: {form_data}")
    
    if db_type not in DB_TYPES:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    db_config = get_db_properties(db_type)
    base_template = db_config['connection_string']
    
    variables = {'driver': db_config.get('driver', '')}

    for field in db_config['fields']:
        field_name = field['name']
        value = form_data.get(field_name, field.get('default'))
        
        if value is None and field.get('required', False):
            raise ValueError(f"Missing required field: {field_name}")
        
        if field_name == 'odbc_driver' and value:
            value = unquote_plus(value)
        
        variables[field_name] = value

    # Handle file_system path
    if db_type == 'file_system':
        if 'directory_path' not in variables:
            raise ValueError("directory_path is required for file_system")
        directory_path = os.path.normpath(variables['directory_path'])
        return f"file://{directory_path}"

    # Encode username/password for SQLAlchemy URIs
    for key in ['username', 'password']:
        if key in variables and variables[key]:
            variables[key] = quote_plus(variables[key])

    # Common query parameters
    query_params = []

    if 'odbc_driver' in variables:
        query_params.append(f"driver={quote_plus(variables['odbc_driver'])}")

    if variables.get('encrypt', '').lower() in ['yes', 'true', '1']:
        query_params.append("Encrypt=yes")
    elif variables.get('encrypt', '').lower() in ['no', 'false', '0']:
        query_params.append("Encrypt=no")

    if variables.get('trusted_connection', '').lower() in ['yes', 'true', '1']:
        query_params.append("Trusted_Connection=yes")

    query_params.append("TrustServerCertificate=yes")

    if 'timeout' in variables and variables['timeout']:
        query_params.append(f"timeout={variables['timeout']}")

    # === Special handling for mssql_local using raw pyodbc ===
    if db_type == 'mssql_local':
        raw_conn_str = (
            f"DRIVER={{{variables['odbc_driver']}}};"
            f"SERVER={variables['hostname']},{variables['port']};"
            f"DATABASE={variables['database']};"
        )
        if variables.get('trusted_connection', '').lower() in ['yes', 'true', '1']:
            raw_conn_str += "Trusted_Connection=yes;"
        else:
            raw_conn_str += f"UID={unquote_plus(variables['username'])};PWD={unquote_plus(variables['password'])};"
        
        raw_conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
        if 'timeout' in variables and variables['timeout']:
            raw_conn_str += f"Connection Timeout={variables['timeout']};"
        
        return raw_conn_str

    # === Default: return SQLAlchemy URI ===
    query_string = '&'.join(query_params)
    return (
        f"{variables['driver']}://{variables['username']}:{variables['password']}"
        f"@{variables['hostname']}:{variables['port']}/{variables['database']}?{query_string}"
    )



def build_connection_string(db_type, form_data, return_pyodbc_for_local=True):
    """Build a connection string based on db_type and form_data test.
    
    For 'mssql_local', if return_pyodbc_for_local=True, returns raw pyodbc connection string
    with proper UID/PWD or Trusted_Connection.
    For others, returns SQLAlchemy style connection string.
    """
    DB_TYPES = ['redshift', 'mssql_local', 'azure_sql', 'file_system']
    
    print(f"Building connection string for {db_type} with form_data: {form_data}")
    
    if db_type not in DB_TYPES:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    db_config = get_db_properties(db_type)
    base_template = db_config['connection_string']
    
    variables = {
        'driver': db_config.get('driver', '')
    }

    # Collect form fields into variables
    for field in db_config['fields']:
        field_name = field['name']
        value = form_data.get(field_name, field.get('default'))
        
        if value is None and field.get('required', False):
            raise ValueError(f"Missing required field: {field_name}")
        
        # Decode URL-encoded ODBC driver string if any
        if field_name == 'odbc_driver' and value:
            value = unquote_plus(value)
        
        variables[field_name] = value

    # Handle file_system
    if db_type == 'file_system':
        if 'directory_path' not in variables:
            raise ValueError("directory_path is required for file_system")
        directory_path = os.path.normpath(variables['directory_path'])
        return f"file://{directory_path}"

    # Build connection string for mssql_local using pyodbc format if requested
    if db_type == 'mssql_local' and return_pyodbc_for_local:
        conn_parts = [
            f"DRIVER={{{variables['odbc_driver']}}}",
            f"SERVER={variables['hostname']},{variables['port']}",
            f"DATABASE={variables['database']}"
        ]

        # Authentication logic
        trusted = str(variables.get('trusted_connection', '')).lower() in ['yes', 'true', '1']
        if trusted:
            conn_parts.append("Trusted_Connection=yes")
        else:
            # Ensure username and password present for SQL auth
            if not variables.get('username') or not variables.get('password'):
                raise ValueError("Username and password required for SQL Server Authentication")
            conn_parts.append(f"UID={variables['username']}")
            conn_parts.append(f"PWD={variables['password']}")

        # Encryption param
        encrypt = str(variables.get('encrypt', '')).lower()
        if encrypt in ['yes', 'true', '1']:
            conn_parts.append("Encrypt=yes")
        else:
            conn_parts.append("Encrypt=no")

        # Always trust server certificate
        conn_parts.append("TrustServerCertificate=yes")

        # Timeout
        if variables.get('timeout'):
            conn_parts.append(f"Connection Timeout={variables['timeout']}")

        return ";".join(conn_parts) + ";"

    # For other db_types or mssql_local when not returning pyodbc string,
    # build SQLAlchemy style connection string
    
    # URL encode username and password
    for key in ['username', 'password']:
        if key in variables and variables[key]:
            variables[key] = quote_plus(variables[key])

    # Build query parameters for SQL Server or similar
    query_params = []

    if 'odbc_driver' in variables:
        query_params.append(f"driver={quote_plus(variables['odbc_driver'])}")

    encrypt = str(variables.get('encrypt', '')).lower()
    if encrypt in ['yes', 'true', '1']:
        query_params.append("Encrypt=yes")
    elif encrypt in ['no', 'false', '0']:
        query_params.append("Encrypt=no")

    trusted = str(variables.get('trusted_connection', '')).lower()
    if trusted in ['yes', 'true', '1']:
        query_params.append("Trusted_Connection=yes")

    query_params.append("TrustServerCertificate=yes")

    if variables.get('timeout'):
        query_params.append(f"timeout={variables['timeout']}")

    query_string = '&'.join(query_params)
    connection_string = f"{variables['driver']}://{variables['username']}:{variables['password']}@{variables['hostname']}:{variables['port']}/{variables['database']}?{query_string}"
    print('connection_string :',connection_string)

    return connection_string




def build_connection_string_1(db_type, form_data):
    """Build a connection string based on db_type and form_data."""
    DB_TYPES = ['redshift', 'mssql_local', 'azure_sql', 'file_system']
    
    print(f"Building connection string for {db_type} with form_data: {form_data}")
    
    if db_type not in DB_TYPES:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    db_config = get_db_properties(db_type)
    base_template = db_config['connection_string']
    
    variables = {
        'driver': db_config.get('driver', '')
    }

    # Collect form fields into variables
    for field in db_config['fields']:
        field_name = field['name']
        value = form_data.get(field_name, field.get('default'))
        
        if value is None and field.get('required', False):
            raise ValueError(f"Missing required field: {field_name}")
        
        # Decode URL-encoded ODBC driver string
        if field_name == 'odbc_driver' and value:
            value = unquote_plus(value)
        
        variables[field_name] = value

    # Special handling for file_system
    if db_type == 'file_system':
        if 'directory_path' not in variables:
            raise ValueError("directory_path is required for file_system")
        directory_path = os.path.normpath(variables['directory_path'])
        variables['directory_path'] = directory_path
        return f"file://{directory_path}"

    # URL encode username and password
    for key in ['username', 'password']:
        if key in variables and variables[key]:
            variables[key] = quote_plus(variables[key])

    # Build query string for SQL Server (or similar)
    query_params = []

    if 'odbc_driver' in variables:
        query_params.append(f"driver={quote_plus(variables['odbc_driver'])}")

    if variables.get('encrypt', '').lower() in ['yes', 'true', '1']:
        query_params.append("Encrypt=yes")
    elif variables.get('encrypt', '').lower() in ['no', 'false', '0']:
        query_params.append("Encrypt=no")

    if variables.get('trusted_connection', '').lower() in ['yes', 'true', '1']:
        query_params.append("Trusted_Connection=yes")

    # Trust the server certificate by default
    query_params.append("TrustServerCertificate=yes")

    if 'timeout' in variables and variables['timeout']:
        query_params.append(f"timeout={variables['timeout']}")

    # Final connection string
    query_string = '&'.join(query_params)
    connection_string = f"{variables['driver']}://{variables['username']}:{variables['password']}@{variables['hostname']}:{variables['port']}/{variables['database']}?{query_string}"

    return connection_string




def build_connection_string_old(db_type, form_data):
    """Build a connection string based on db_type and form_data."""
    DB_TYPES = ['redshift', 'mssql_local', 'azure_sql', 'file_system']
    
    print(f"Building connection string for {db_type} with form_data: {form_data}")
    if db_type not in DB_TYPES:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    db_config = get_db_properties(db_type)
    connection_template = db_config['connection_string']
    
    variables = {'driver': db_config.get('driver', '')}
    
    for field in db_config['fields']:
        field_name = field['name']
        if field_name in form_data:
            variables[field_name] = form_data[field_name]
        elif 'default' in field and field.get('required', False) is False:
            variables[field_name] = field['default']
    
    # URL encode sensitive values for database types
    if db_type != 'file_system':
        for key in ['username', 'password']:
            if key in variables:
                variables[key] = quote_plus(variables[key])
    
    # Special handling for file_system
    if db_type == 'file_system':
        # Ensure directory_path is provided
        if 'directory_path' not in variables:
            raise ValueError("directory_path is required for file_system")
        # Normalize path and add file:// prefix
        directory_path = os.path.normpath(variables['directory_path'])
        variables['directory_path'] = directory_path
    
    try:
        return connection_template.format(**variables)
    except KeyError as e:
        raise ValueError(f"Missing required field in form_data: {e}")


def get_schema_names(connection_string, db_type):
    """Retrieve schema names from the database or return directory path for file_system."""
    #print("DB_TYPE get_schema_names : ",db_type)
    try:
        if db_type == 'file_system':
            directory_path = connection_string.replace('file://', '')
            return directory_path
        
        query = schema_queries.get(db_type)
     
        if query is None:
            raise ValueError(f"No schema query defined for database type: {db_type}")
        
        if db_type == 'mssql_local':
            print("TESTING  Use pyodbc directly for mssql_local")
            server="localhost"
            database="POCDataMesh"
            username="chatbot_user"
            password="chatbot_user_password"
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "TrustServerCertificate=yes;"
            )
            
            with pyodbc.connect(conn_str, timeout=5) as conn:

                print("Connection successful!")
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION;")
                row = cursor.fetchone()
                print("SQL Server version:", row[0])

            print("SQL Server version:NEXT")
            with pyodbc.connect(conn_str) as conn:
                result = pd.read_sql(query, conn)
        elif db_type == 'redshift':
            print("TRYING schema names  conn_str:",connection_string)
            print("########################")
            redshift_config = {
            'host': 'tesserai-poc-public-redshift-nbl-e1012ded7050baf0.elb.eu-west-1.amazonaws.com',
            #'host':'tesserai-poc-workgroup.854988700158.eu-west-1.redshift-serverless.amazonaws.com',
            'port': 5439,
            'database': 'dev',
            'user': 'data_virtuality',
            'password': 'R@d35Mv}N+J2urp0'
            #'schema': 'dwh_raw'
                            }
            result = redshift_connection(redshift_config)

            


        else:
            # Use SQLAlchemy engine for other DBs
            #print("conn_str :  ",conn_str)
            
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                result = pd.read_sql(query, conn)
        
        return result#['schema_name'].tolist()
    
    except Exception as e:
        print(f"Error retrieving schema names: {e}")
        return []


def get_schema_names_1(connection_string, db_type):
    """Retrieve schema names from the database or return empty list for file_system."""
    try:
        if db_type == 'file_system':
            directory_path = connection_string.replace('file://', '')
            return directory_path

        query = schema_queries.get(db_type)
        if query is None:
            raise ValueError(f"No schema query defined for database type: {db_type}")

        print("get_schema_names : ",query)
        if db_type == 'mssql_local':
            print("mssql_local :connection_string: ",connection_string)
            # Assume raw pyodbc connection string for local MSSQL
            with pyodbc.connect(connection_string) as conn:
                df = pd.read_sql(query, conn)
                return df['schema_name'].tolist()
        else:
            # For other DBs, use SQLAlchemy
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
                return df['schema_name'].tolist()

    except Exception as e:
        print(f"Error retrieving schema names: {e}")
        return []

def get_schema_names_old(connection_string, db_type):
    """Retrieve schema names from the database or return empty list for file_system."""
    try:
        if db_type == 'file_system':
            directory_path = connection_string.replace('file://', '')
            return directory_path
        query = schema_queries.get(db_type)
        if query is None:
            raise ValueError(f"No schema query defined for database type: {db_type}")
        
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = pd.read_sql(query, conn)
        return result['schema_name'].tolist()
    except Exception as e:
        print(f"Error retrieving schema names: {e}")
        return []

def get_table_names(connection_string, db_type, schema_name=None):
    """Retrieve table names from the specified schema or files for file_system."""
    try:
        if db_type == 'file_system':
            directory = connection_string.replace('file://', '')
            if not os.path.isdir(directory):
                raise ValueError(f"Directory does not exist: {directory}")
            
            valid_extensions = tuple(f'.{ext}' for ext in database_configs['file_system']['file_extensions'])
            files = [f.name for f in Path(directory).glob('*') if f.suffix.lower() in valid_extensions]
            return sorted(files)
        print("get_table_names :","db_type : ",db_type,table_queries.keys())
        if db_type in table_queries.keys():
            print("AVAILABLE :","db_type : ",db_type ,"  :  " , table_queries.get(db_type))
        else:
            print("NOT AVAILABLE :","db_type : ",db_type)
        query = table_queries.get(db_type)
       
        if not query:
            raise ValueError(f"No table query defined for database type: {db_type}")
        
        if schema_name is None:
            print(f"No schema provided for {db_type}, skipping table query")
            return []
        
        print(f"Executing table query for {db_type} with schema: {schema_name}")

        try:
            if db_type == 'file_system':
                print('file_system')


            elif db_type == 'redshift':
                print('redshift')
        
        except Exception as e:

            print("errror")
        
        
        engine = create_engine(connection_string)
        with engine.connect() as conn:
             my_tuple = (schema_name,)
             
             result = pd.read_sql(query, conn, params=[my_tuple])
             tables = result['table_name'].tolist()
             
             

            
        
        #print(f"Tables retrieved for schema {schema_name}: {tables}")
        return tables
    except Exception as e:
        #print(f"Error retrieving table names for {'file_system' if db_type == 'file_system' else f'schema {schema_name}'}: {e}")
        return []

def get_columns(connection_string, db_type, schema_name=None, tables=None):
    """Retrieve columns for a list of tables, returning a dictionary with table names as keys and column lists as values."""
  
    if not tables:
        print(f"No tables provided for {db_type}, returning empty dict")
        return {}
    
    columns_dict = {}
    try:
        if db_type == 'file_system':
            directory = connection_string.replace('file://', '')
            if not os.path.isdir(directory):
                raise ValueError(f"Directory does not exist: {directory}")
            
           
            table=re.sub(r"[\[\]']", "", tables)
            
            file_path = os.path.join(directory, table)
            column_info_list=[]
            if table.lower().endswith('.csv'):
                    df = pd.read_csv(file_path, nrows=1)  # Read only headers
                    columns_dict[table] = df.columns.tolist()
                    first_row = df.iloc[0].to_dict() if not df.empty else {}
                    column_info_list.append(first_row)
                    columns_dict[table] = column_info_list
            elif table.lower().endswith('.parquet'):
                df = pd.read_parquet(file_path, columns=[])
                columns_dict[table] = df.columns.tolist()
                first_row = df.iloc[0].to_dict() if not df.empty else {}
                column_info_list.append(first_row)
                columns_dict[table] = column_info_list
            else:
                print(f"Unsupported file type for {table}, skipping")
            #continue
            
                print(f"Error reading columns for file {table}: {e}")
                #columns_dict[table] = []
                columns_dict[table] = column_info_list
            print('columns_dict',columns_dict)    
            return columns_dict
        
        query = columns_query.get(db_type)
        if not query:
            raise ValueError(f"No column query defined for database type: {db_type}")
        
        if schema_name is None:
            print(f"No schema provided for {db_type}, skipping column query")
            return {}
        
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            for table in tables:
                print(f"Executing column query for {db_type}, schema: {schema_name}, table: {table}")
                try:
                    result = pd.read_sql(query, conn, params=[schema_name, table])
                    columns = result['column_name'].tolist()
                    columns_dict[table] = columns
                    print(f"Columns retrieved for table {table}: {columns}")
                except Exception as e:
                    print(f"Error retrieving columns for table {table}: {e}")
                    columns_dict[table] = []
        return columns_dict
    except Exception as e:
        print(f"Error in get_columns for {db_type}: {e}")
        return columns_dict

# Top records query dictionary

# Test form_data for each db_type
redshift_form_data = {
    'hostname': 'tesserai-poc-public-redshift-nbl-e1012ded7050baf0.elb.eu-west-1.amazonaws.com',
    'port': '5439',
    'database': 'dev',
    'username': 'data_virtuality',
    'password': 'R@d35Mv}N+J2urp0',
    'sslmode': 'require'
}

mssql_form_data = {
    'hostname': 'localhost',
    'port': '1433',
    'database': 'LegalChatbotDB',
    'username': 'chatbot_user',
    'password': 'chatbot_user_password',
    'odbc_driver': 'ODBC+Driver+17+for+SQL+Server'
}

mssql_form_data_alt = {
    'hostname': 'localhost\\SQLEXPRESS',
    'port': '',  # Port is ignored for named instances
    'database': 'LegalChatbotDB',
    'username': 'chatbot_user',
    'password': 'chatbot_user_password',
    'odbc_driver': 'ODBC+Driver+17+for+SQL+Server'
}

file_system_form_data = {
    'directory_path': r'C:\Users\xds\Documents\pee\ibidemos',
    'file_type': 'csv',
    'delimiter': ',',
    'header': 'yes'
}


def get_top_records(doservice_list):
    """Retrieve top 10 records for a list of tables, returning a dictionary with table names as keys and DataFrames as values."""
    
    if isinstance(doservice_list, str):
        try:
            doservice_list = json.loads(doservice_list)
            print("Parsed doservice_list from string to dict:", doservice_list)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON string: {e}")
            return {}
    elif not isinstance(doservice_list, dict):
        print(f"Invalid input type for doservice_list: {type(doservice_list)}. Expected dict or str.")
        return {}
    
   
    
    # Extract values with defaults
    tables = doservice_list.get('dict_tables', [])
    tables_dq = doservice_list.get('dq_tables', [])  # Unused but kept for compatibility
    db_type = doservice_list.get('db_type')
    connection_string = doservice_list.get('conn_str')
    schema_name = doservice_list.get('db_schema_name')
    conn_params = doservice_list.get('conn_params')
    if not tables:
        print(f"No tables provided for {db_type}, returning empty dict")
        return {}
    
    records_dict = {}
    
    try:
        if db_type == 'file_system':
            directory = connection_string.replace('file://', '')
            if not os.path.isdir(directory):
                raise ValueError(f"Directory does not exist: {directory}")
            
            for table in tables:
                file_path = os.path.join(directory, table)
                if not os.path.isfile(file_path):
                    print(f"File {table} not found in {directory}, skipping")
                    continue
                
                try:
                    if table.lower().endswith('.csv'):
                        df = pd.read_csv(file_path, nrows=10)
                    elif table.lower().endswith('.parquet'):
                        df = pd.read_parquet(file_path).head(10)  # nrows not supported in current pandas
                    else:
                        print(f"Unsupported file type for {table}, skipping")
                        continue
                    records_dict[table] = df
                except FileNotFoundError as e:
                    print(f"File not found for {table}: {e}")
                    records_dict[table] = pd.DataFrame()
                except Exception as e:
                    print(f"Error reading file {table}: {e}")
                    records_dict[table] = pd.DataFrame()
            return records_dict
        
        query_template = top_records_query.get(db_type)
        if not query_template:
            raise ValueError(f"No top records query defined for database type: {db_type}")
        
        if schema_name is None:
            print(f"No schema provided for {db_type}, skipping records query")
            return {}
        
        
        if db_type=='redshift':
            
            records_dict= get_tables_in_schema_test(conn_params,query_template,tables,schema_name)
            return records_dict
            
        else:
            engine = create_engine(connection_string)
            try:
                
                with engine.connect() as conn:
                    for table in tables:
                        query = query_template.format(
                            schema_name=schema_name,
                            table_name=table
                        )
                        print(f"Executing query for {db_type}, schema: {schema_name}, table: {table}")
                        try:
                            df = pd.read_sql(query, conn)
                            records_dict[table] = df
                            print(f"Records retrieved for table {table}: {len(df)} rows")
                        except DatabaseError as e:
                            print(f"Database error for table {table}: {e}")
                            records_dict[table] = pd.DataFrame()
            finally:
                engine.dispose()  # Clean up engine
            return records_dict
        
    except Exception as e:
        print(f"Error in get_top_records for {db_type}: {e}")
        return records_dict

def get_top_records_2(doservice_list):
    """Retrieve top 10 records for a list of tables, returning a dictionary with table names as keys and DataFrames as values."""
    print('doservice: doservice_list : ',doservice_list)
    print(f"Type of doservice_list: {type(doservice_list)}")
    print(f"Value of doservice_list: {doservice_list}")
    tables=['products']#doservice_list['dict_tables']
    tables_dq = doservice_list['dq_tables']
    db_type= doservice_list['db_type']
    connection_string= doservice_list['conn_str']
    schema_name= doservice_list['db_schema_name']
    
    if not tables:
        print(f"No tables provided for {db_type}, returning empty dict")
        return {}
    
    records_dict = {}
    try:
        if db_type == 'file_system':
            directory = connection_string.replace('file://', '')
            if not os.path.isdir(directory):
                raise ValueError(f"Directory does not exist: {directory}")
            
            for table in tables:
                file_path = os.path.join(directory, table)
                if not os.path.isfile(file_path):
                    print(f"File {table} not found in {directory}, skipping")
                    continue
                
                try:
                    if table.lower().endswith('.csv'):
                        df = pd.read_csv(file_path, nrows=10)
                        records_dict[table] = df
                    elif table.lower().endswith('.parquet'):
                        df = pd.read_parquet(file_path).head(10)
                        records_dict[table] = df
                    else:
                        print(f"Unsupported file type for {table}, skipping")
                        continue
                except Exception as e:
                    print(f"Error reading records for file {table}: {e}")
                    records_dict[table] = pd.DataFrame()  # Return empty DataFrame on error
            return records_dict
        
        query_template = top_records_query.get(db_type)
        if not query_template:
            raise ValueError(f"No top records query defined for database type: {db_type}")
        
        if schema_name is None:
            print(f"No schema provided for {db_type}, skipping records query")
            return {}
        
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            for table in tables:
                query = query_template.format(schema_name=schema_name, table_name=table)
                print(f"Executing records query for {db_type}, schema: {schema_name}, table: {table}")
                try:
                    df = pd.read_sql(query, conn)
                    records_dict[table] = df
                    print(f"Records retrieved for table {table}: {len(df)} rows")
                except Exception as e:
                    print(f"Error retrieving records for table {table}: {e}")
                    records_dict[table] = pd.DataFrame()  # Return empty DataFrame on error
        return records_dict
    except Exception as e:
        print(f"Error in get_top_records for {db_type}: {e}")
        return records_dict
    


# Build connection strings
#redshift_conn_str = build_connection_string('redshift', redshift_form_data)
#mssql_conn_str = build_connection_string('mssql_local', mssql_form_data)
#mssql_conn_str_alt = build_connection_string('mssql_local', mssql_form_data_alt)
#file_system_conn_str = build_connection_string('file_system', file_system_form_data)

'''# Test Redshift
print("Redshift Connection String:", redshift_conn_str)
print("Redshift Schemas:", get_schema_names(redshift_conn_str, 'redshift'))
redshift_schemas = get_schema_names(redshift_conn_str, 'redshift')
if redshift_schemas:
    schema = 'dwh_raw'  # Use the specific schema from your config
    print(f"Redshift Tables in {schema}:", get_table_names(redshift_conn_str, 'redshift', schema))

# Test SQL Server
print("\nSQL Server Connection String:", mssql_conn_str)
print("SQL Server Schemas:", get_schema_names(mssql_conn_str, 'mssql_local'))
mssql_schemas = get_schema_names(mssql_conn_str, 'mssql_local')
if mssql_schemas:
    schema = mssql_schemas[0]  # Example: Use the first schema (e.g., 'dbo')
    print(f"SQL Server Tables in {schema}:", get_table_names(mssql_conn_str, 'mssql_local', schema))

# Test SQL Server with named instance
print("\nSQL Server Connection String (SQLEXPRESS):", mssql_conn_str_alt)
print("SQL Server Schemas (SQLEXPRESS):", get_schema_names(mssql_conn_str_alt, 'mssql_local'))
mssql_schemas_alt = get_schema_names(mssql_conn_str_alt, 'mssql_local')
if mssql_schemas_alt:
    schema = mssql_schemas_alt[0]
    print(f"SQL Server Tables in {schema}:", get_table_names(mssql_conn_str_alt, 'mssql_local', schema))

# Test File System
print("\nFile System Connection String:", file_system_conn_str)
print("File System Schemas:", get_schema_names(file_system_conn_str, 'file_system'))
print("File System Files (Tables):", get_table_names(file_system_conn_str, 'file_system'))
'''
