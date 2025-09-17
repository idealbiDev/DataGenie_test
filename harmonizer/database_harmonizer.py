from flask import current_app
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def get_engine_db_connection():
    """Connect to the engine_db MySQL database where column_descriptions are stored"""
    db_config = current_app.config.get('ENGINE_DB_CONFIG')
    if not db_config:
        raise ValueError("Engine database configuration not found")
    
    # Use the same format as your db_config_local
    connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(connection_string)

def get_all_tables():
    """Get distinct table names from column_descriptions"""
    engine = get_engine_db_connection()
    
    try:
        query = text("SELECT DISTINCT table_name FROM column_descriptions ORDER BY table_name")
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row['table_name'] for row in result]
    except Exception as e:
        logger.error(f"Error getting tables from column_descriptions: {e}")
        return []

def get_table_columns(table_name):
    """Get all columns for a specific table from column_descriptions"""
    engine = get_engine_db_connection()
    
    try:
        query = text("""
            SELECT column_name, business_purpose, data_quality_rules, 
                   example_usage, issues, created_at, updated_at
            FROM column_descriptions 
            WHERE table_name = :table_name 
            ORDER BY column_name
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {'table_name': table_name})
            return [dict(row) for row in result]
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {e}")
        return []

def get_all_column_data():
    """Get all columns from all tables for AI analysis"""
    engine = get_engine_db_connection()
    
    try:
        query = text("""
            SELECT table_name, column_name, business_purpose, 
                   data_quality_rules, example_usage, issues
            FROM column_descriptions 
            ORDER BY table_name, column_name
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row) for row in result]
    except Exception as e:
        logger.error(f"Error getting all column data: {e}")
        return []

def save_column_mapping(mapping_data):
    """Save a new column mapping to column_mappings table"""
    engine = get_engine_db_connection()
    
    try:
        query = text("""
            INSERT INTO column_mappings 
            (source_table, source_column, target_table, target_column, 
             mapping_type, confidence_score, created_by)
            VALUES (:source_table, :source_column, :target_table, :target_column,
                    :mapping_type, :confidence_score, :created_by)
            ON DUPLICATE KEY UPDATE
            mapping_type = VALUES(mapping_type),
            confidence_score = VALUES(confidence_score),
            updated_at = CURRENT_TIMESTAMP
        """)
        
        with engine.connect() as conn:
            conn.execute(query, mapping_data)
            return True
            
    except Exception as e:
        logger.error(f"Error saving column mapping: {e}")
        return False

def get_existing_mappings():
    """Get all existing column mappings"""
    engine = get_engine_db_connection()
    
    try:
        query = text("""
            SELECT source_table, source_column, target_table, target_column,
                   mapping_type, confidence_score, created_by, created_at
            FROM column_mappings 
            ORDER BY created_at DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row) for row in result]
    except Exception as e:
        logger.error(f"Error getting existing mappings: {e}")
        return []