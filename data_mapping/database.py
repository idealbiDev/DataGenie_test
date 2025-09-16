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
    # Or you can use a separate database if needed
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

def get_column_metadata(connection_string=None, db_type='mysql', table_name=None, schema_name=None):
    """Get column metadata from source database"""
    if not table_name:
        return []
        
    engine = get_source_db_connection(connection_string)
    
    try:
        if db_type.lower() in ['mysql', 'mariadb']:
            if schema_name:
                query = text("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
                           COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    ORDER BY ORDINAL_POSITION
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'schema': schema_name, 'table': table_name})
                    return [dict(row) for row in result]
            else:
                query = text("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
                           COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT
                    FROM information_schema.columns
                    WHERE table_name = :table
                    ORDER BY ORDINAL_POSITION
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'table': table_name})
                    return [dict(row) for row in result]
                    
        elif db_type.lower() in ['mssql', 'sqlserver', 'azure_sql']:
            if schema_name:
                query = text("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
                           COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    ORDER BY ORDINAL_POSITION
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'schema': schema_name, 'table': table_name})
                    return [dict(row) for row in result]
            else:
                query = text("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
                           COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
                    FROM information_schema.columns
                    WHERE table_name = :table
                    ORDER BY ORDINAL_POSITION
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'table': table_name})
                    return [dict(row) for row in result]
                    
        elif db_type.lower() in ['postgresql', 'redshift']:
            if schema_name:
                query = text("""
                    SELECT column_name, data_type, is_nullable, character_maximum_length,
                           column_default, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    ORDER BY ordinal_position
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'schema': schema_name, 'table': table_name})
                    return [dict(row) for row in result]
            else:
                query = text("""
                    SELECT column_name, data_type, is_nullable, character_maximum_length,
                           column_default, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table
                    ORDER BY ordinal_position
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {'table': table_name})
                    return [dict(row) for row in result]
        
        else:
            # Fallback for other database types
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            return columns
            
    except Exception as e:
        logger.error(f"Error getting column metadata for {table_name}: {e}")
        return []

def get_sample_data(connection_string=None, db_type='mysql', table_name=None, schema_name=None, limit=5):
    """Get sample data from table"""
    if not table_name:
        return pd.DataFrame()
        
    engine = get_source_db_connection(connection_string)
    
    try:
        if db_type.lower() in ['mssql', 'sqlserver', 'azure_sql']:
            # SQL Server uses TOP instead of LIMIT
            if schema_name:
                full_table_name = f"{schema_name}.{table_name}"
            else:
                full_table_name = table_name
            query = f"SELECT TOP {limit} * FROM {full_table_name}"
            
        elif db_type.lower() in ['mysql', 'mariadb', 'postgresql', 'redshift']:
            if schema_name:
                full_table_name = f"{schema_name}.{table_name}"
            else:
                full_table_name = table_name
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
            
        else:
            # Generic approach
            if schema_name:
                full_table_name = f"{schema_name}.{table_name}"
            else:
                full_table_name = table_name
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        logger.error(f"Error getting sample data from {table_name}: {e}")
        return pd.DataFrame()

def save_column_descriptions(descriptions):
    """Save column descriptions to database"""
    from .models import ColumnDescription, db
    
    try:
        for desc in descriptions:
            # Check if description already exists
            existing = ColumnDescription.query.filter_by(
                TableName=desc['table_name'],
                ColumnName=desc['column_name']
            ).first()
            
            if existing:
                # Update existing
                existing.business_purpose = desc.get('business_purpose', '')
                existing.data_quality_rules = desc.get('data_quality_rules', '')
                existing.example_usage = desc.get('example_usage', '')
                existing.issues = desc.get('issues', '')
            else:
                # Create new
                new_desc = ColumnDescription(
                    TableName=desc['table_name'],
                    ColumnName=desc['column_name'],
                    business_purpose=desc.get('business_purpose', ''),
                    data_quality_rules=desc.get('data_quality_rules', ''),
                    example_usage=desc.get('example_usage', ''),
                    issues=desc.get('issues', '')
                )
                db.session.add(new_desc)
        
        db.session.commit()
        return True
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving column descriptions: {e}")
        return False