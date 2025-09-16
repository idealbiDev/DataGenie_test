from .db_manager import DBManager
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DictionaryStorage:
    @staticmethod
    def check_table_exists(cursor, table_name, schema='IDIMDM'):
        """Check if a table exists in the database."""
        try:
            cursor.execute("""
                SELECT 1 FROM sysobjects WHERE name = ? AND type = 'U'
            """, (table_name,))
            return cursor.fetchone() is not None
        except Exception as e:
            logging.error(f"Error checking table existence for {table_name}: {str(e)}")
            raise

    @staticmethod
    def save_to_db(table_name, dictionary, db_type='local'):
        """
        Save dictionary to data_dictionary and QualityReports tables.
        
        Parameters:
        - table_name (str): Name of the table.
        - dictionary (list): List of dictionaries with column metadata and quality metrics.
        - db_type (str): Database type ('local' for SQL Server, else Redshift).
        """
        conn = DBManager.get_dictionary_storage()
        cursor = conn.cursor()
        
        try:
            # Check table existence
            schema = 'IDIMDM' if db_type == 'local' else 'public'
            if not DictionaryStorage.check_table_exists(cursor, 'data_dictionary', schema):
                raise Exception("data_dictionary table does not exist")
            if not DictionaryStorage.check_table_exists(cursor, 'QualityReports', schema):
                raise Exception("QualityReports table does not exist")
            
            # Prepare DataFrames for bulk insert
            dict_df = pd.DataFrame([{
                'TableName': table_name,
                'ColumnName': item['ColumnName'],
                'ColumnDescription': item['ColumnDescription'],
                'ColumnDataType': item['ColumnDataType'],
                'Nullable': item['Nullable'],
                'CharacterMaximumLength': item.get('CharacterMaximumLength', None),
                'AuditDate': item['AuditDate'],
                'DataValidityConsistency': item.get('DataValidityConsistency', None),
                'DataInValidityConsistency': item.get('DataInValidityConsistency', None),
                'UniqueRecords': item.get('UniqueRecords', None),
                'DuplicateRecords': item.get('DuplicateRecords', None),
                'DataCompleteness': item.get('DataCompleteness', None),
                'NullCounts': item.get('NullCounts', None)
            } for item in dictionary])
            
            quality_df = pd.DataFrame([{
                'TableName': table_name,
                'ColumnName': item['ColumnName'],
                'DataValidityConsistency': item.get('DataValidityConsistency', None),
                'DataInValidityConsistency': item.get('DataInValidityConsistency', None),
                'UniqueRecords': item.get('UniqueRecords', None),
                'DuplicateRecords': item.get('DuplicateRecords', None),
                'DataCompleteness': item.get('DataCompleteness', None),
                'NullCounts': item.get('NullCounts', None),
                'AuditDate': item['AuditDate']
            } for item in dictionary])
            
            # Create SQLAlchemy engine
            conn_str = f"mssql+pyodbc://@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}?driver={os.getenv('DB_DRIVER')}&Trusted_Connection=yes"
            engine = create_engine(conn_str)
            
            # Save to data_dictionary (Option 1)
            dict_df.to_sql('data_dictionary', engine, schema=schema, if_exists='append', index=False)
            
            # Save to QualityReports (Option 2)
            quality_df.to_sql('QualityReports', engine, schema=schema, if_exists='append', index=False)
            
            conn.commit()
            logging.info(f"Successfully saved dictionary for {table_name} to data_dictionary and QualityReports")
        
        except Exception as e:
            conn.rollback()
            logging.error(f"Error saving dictionary to database: {str(e)}")
            raise
        finally:
            conn.close()