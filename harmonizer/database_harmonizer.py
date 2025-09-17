from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
import pandas as pd
from flask import current_app
import logging

logger = logging.getLogger(__name__)

def get_source_db_connection(connection_string=None):
    """Create connection to source database"""
    if not connection_string:
        # Use the global connection string from main app
        connection_string = current_app.config.get('GLOBAL_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("No database connection string available")
    
    return create_engine(connection_string)

def get_mysql_connection():
    """Create connection to MySQL mapping database"""
    # Use the same database as the main app for simplicity
    return create_engine(current_app.config['SQLALCHEMY_DATABASE_URI'])

def get_table_names(connection_string=None, db_type='mysql', schema_name=None):
    """Get list of tables from source database"""
    engine = get_source_db_connection(connection_string)
    
    try:
        if db_type.lower() in ['mysql', 'mariadb']:
            if schema_name:
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_type = 'BASE TABLE'
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'schema': schema_name})
                    return [row[0] for row in result]
            else:
                inspector = inspect(engine)
                return inspector.get_table_names()
                
        elif db_type.lower() in ['mssql', 'sqlserver', 'azure_sql']:
            if schema_name:
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_type = 'BASE TABLE'
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'schema': schema_name})
                    return [row[0] for row in result]
            else:
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE'
                """)
                with engine.connect() as conn:
                    result = conn.execute(query)
                    return [row[0] for row in result]
                    
        elif db_type.lower() in ['postgresql', 'redshift']:
            if schema_name:
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_type = 'BASE TABLE'
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'schema': schema_name})
                    return [row[0] for row in result]
            else:
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """)
                with engine.connect() as conn:
                    result = conn.execute(query)
                    return [row[0] for row in result]
        
        else:
            # Fallback for other database types
            inspector = inspect(engine)
            return inspector.get_table_names()
            
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        return []

# ... (rest of the database functions remain the same as previous database.py) ...