import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging
from .db_manager import DBManager
import json
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_quality_report(df, table_name, column_descriptions=None):
    """
    Generate a data quality report for a DataFrame with specified metrics.
    
    Parameters:
    - df (pd.DataFrame): Input DataFrame to analyze.
    - table_name (str): Name of the table.
    - column_descriptions (dict, optional): Dictionary mapping column names to descriptions.
    
    Returns:
    - pd.DataFrame: Data quality report with specified columns.
    """
    if column_descriptions is None:
        column_descriptions = {col: 'No description provided' for col in df.columns}
    
    report_data = {
        'TableName': [], 'ColumnName': [], 'ColumnDescription': [], 'ColumnDataType': [],
        'DataValidityConsistency': [], 'DataInValidityConsistency': [], 'UniqueRecords': [],
        'DuplicateRecords': [], 'DataCompleteness': [], 'NullCounts': [], 'AuditDate': []
    }
    
    audit_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_rows = len(df)
    
    for column in df.columns:
        report_data['TableName'].append(table_name)
        report_data['ColumnName'].append(column)
        report_data['ColumnDescription'].append(column_descriptions.get(column, 'No description provided'))
        report_data['ColumnDataType'].append(str(df[column].dtype))
        
        null_count = df[column].isnull().sum()
        report_data['NullCounts'].append(null_count)
        completeness = ((total_rows - null_count) / total_rows) * 100 if total_rows > 0 else 0
        report_data['DataCompleteness'].append(f"{completeness:.2f}%")
        
        unique_count = df[column].nunique()
        report_data['UniqueRecords'].append(unique_count)
        report_data['DuplicateRecords'].append(total_rows - unique_count if total_rows > unique_count else 0)
        
        valid_count = invalid_count = 0
        if df[column].dtype in ['int64', 'float64']:
            valid_count = len(df[df[column] >= 0]) if not df[column].isnull().all() else 0
            invalid_count = len(df[df[column] < 0]) if not df[column].isnull().all() else 0
        elif df[column].dtype == 'object':
            valid_count = len(df[df[column].str.strip().ne('') & df[column].notnull()]) if not df[column].isnull().all() else 0
            invalid_count = len(df[df[column].str.strip().eq('') & df[column].notnull()]) if not df[column].isnull().all() else 0
        elif 'datetime' in str(df[column].dtype):
            current_year = datetime.now().year
            valid_count = len(df[(df[column] >= '1900-01-01') & (df[column] <= f'{current_year}-12-31')]) if not df[column].isnull().all() else 0
            invalid_count = len(df[(df[column] < '1900-01-01') | (df[column] > f'{current_year}-12-31')]) if not df[column].isnull().all() else 0
        
        valid_count = min(valid_count, total_rows - null_count)
        invalid_count = min(invalid_count, total_rows - null_count)
        report_data['DataValidityConsistency'].append(f"{(valid_count / (total_rows - null_count)) * 100:.2f}%" if total_rows - null_count > 0 else "0.00%")
        report_data['DataInValidityConsistency'].append(f"{(invalid_count / (total_rows - null_count)) * 100:.2f}%" if total_rows - null_count > 0 else "0.00%")
        
        report_data['AuditDate'].append(audit_date)
    
    return pd.DataFrame(report_data)

def save_quality_report(report_df, db_type="local"):
    """
    Save quality report to the pre-existing QualityReports table.
    
    Parameters:
    - report_df (pd.DataFrame): Data quality report DataFrame.
    - db_type (str): Database type ('local' for SQL Server, else Redshift).
    """
    conn = DBManager.get_connection(db_type)
    cursor = conn.cursor()
    
    try:
        # Check table existence
        schema = 'IDIMDM' if db_type == 'local' else 'public'
        cursor.execute("SELECT 1 FROM sysobjects WHERE name = ? AND type = 'U'", ('QualityReports',))
        if not cursor.fetchone():
            logging.error("QualityReports table does not exist")
            raise Exception("QualityReports table does not exist")
        
        # Create SQLAlchemy engine for bulk insert
        conn_str = f"mssql+pyodbc://@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}?driver={os.getenv('DB_DRIVER')}&Trusted_Connection=yes"
        engine = create_engine(conn_str)
        
        # Save to QualityReports
        report_df.to_sql('QualityReports', engine, schema=schema, if_exists='append', index=False)
        
        conn.commit()
        logging.info(f"Successfully saved quality report for {report_df['TableName'].iloc[0]}")
    
    except Exception as e:
        conn.rollback()
        logging.error(f"Error saving quality report: {str(e)}")
        raise
    finally:
        conn.close()

def get_quality_history(table_name, db_type="local", limit=10):
    """
    Retrieve historical quality reports.
    
    Parameters:
    - table_name (str): Name of the table.
    - db_type (str): Database type ('local' for SQL Server, else Redshift).
    - limit (int): Number of records to return.
    
    Returns:
    - pd.DataFrame: Historical quality reports.
    """
    conn = DBManager.get_connection(db_type)
    
    try:
        if db_type == "local":
            query = """
            SELECT * FROM IDIMDM.QualityReports
            WHERE TableName = ?
            ORDER BY ReportDate DESC
            """
            params = (table_name,)
        else:  # Redshift
            query = """
            SELECT * FROM public.QualityReports
            WHERE table_name = %s
            ORDER BY ReportDate DESC
            LIMIT %s
            """
            params = (table_name, limit)
        
        df = pd.read_sql(query, conn, params=params)
        return df
    
    except Exception as e:
        logging.error(f"Error retrieving quality history for {table_name}: {str(e)}")
        raise
    finally:
        conn.close()





def get_dq_rules():
    dq_rules = {
        'DAMA': {
            'Completeness': {
                'Not Null': {'description': 'Ensures no null or missing values in the column.', 'params': None},
                'Completeness Ratio': {'description': 'Calculates the percentage of non-null values.', 'params': {'threshold': 'Minimum acceptable completeness percentage (0-100)'}}
            },
            'Uniqueness': {
                'Unique': {'description': 'Checks for duplicate values in the column.', 'params': None},
                'Primary Key Check': {'description': 'Verifies that the column (or combination) is unique across the table.', 'params': {'columns': 'List of columns forming the primary key'}}
            },
            'Consistency': {
                'Referential Integrity': {'description': 'Ensures foreign key values exist in the rseferenced table.', 'params': {'ref_table': 'Referenced table name', 'ref_column': 'Referenced column name'}},
                # Corrected line below: Removed 'twoiamas'
                'Cross-Field Consistency': {'description': 'Checks consistency between two columns (e.g., start_date < end_date).', 'params': {'column2': 'Second column name', 'condition': 'Comparison condition (e.g., <, >, =)'}}
            },
            'Accuracy': {
                'Value Range Check': {'description': 'Ensures values fall within a specified range.', 'params': {'min': 'Minimum value', 'max': 'Maximum value'}},
                'Domain Check': {'description': 'Ensures values belong to a predefined set (e.g., list of valid codes).', 'params': {'valid_values': 'List of acceptable values'}}
            },
            'Validity': {
                'Format Check': {'description': 'Validates column values against a regex pattern.', 'params': {'regex': 'Regular expression pattern'}},
                'Length Check': {'description': 'Ensures string length is within specified bounds.', 'params': {'min_length': 'Minimum length', 'max_length': 'Maximum length'}}
            },
            'Timeliness': {
                'Recency Check': {'description': 'Ensures data is within an acceptable time range.', 'params': {'date_column': 'Date column name', 'days_threshold': 'Maximum age in days'}}
            }
        },
        'ISO': {
            'Completeness': {
                'Mandatory Field Check': {'description': 'Ensures mandatory fields are populated (ISO 8000-110).', 'params': None}
            },
            'Accuracy': {
                'Precision Check': {'description': 'Ensures numeric values meet precision requirements (ISO 8000-120).', 'params': {'decimal_places': 'Maximum allowed decimal places'}},
                'Data Source Validation': {'description': 'Verifies data matches a trusted source (ISO 8000-130).', 'params': {'source_table': 'Trusted table name', 'source_column': 'Trusted column name'}}
            },
            'Consistency': {
                # Corrected line below: Removed 'ISO date' redundancy and extra character
                'Standardized Format': {'description': 'Ensures data adheres to standardized formats (ISO 8000-115).', 'params': {'format': 'Expected format (e.g., YYYY-MM-DD)'}}
            },
            'Portability': {
                'Encoding Check': {'description': 'Ensures text data uses a specific encoding (ISO 8000-140).', 'params': {'encoding': 'Expected encoding (e.g., UTF-8)'}}
            }
        },
        'Other': {
            'General Validation': {
                'Data Type Check': {'description': 'Ensures column values match the expected data type.', 'params': {'data_type': 'Expected type (e.g., integer, string, date)'}},
                'Outlier Detection': {'description': 'Identifies statistical outliers using standard deviation or IQR.', 'params': {'method': 'Outlier detection method (std, iqr)', 'threshold': 'Threshold multiplier'}}
            },
            'Business Rules': {
                'Custom SQL Check': {'description': 'Runs a custom SQL query to validate business rules.', 'params': {'sql_query': 'Custom SQL query returning non-compliant rows'}},
                'Regex Replace Validation': {'description': 'Checks if values remain valid after applying a regex replacement.', 'params': {'regex_find': 'Pattern to find', 'regex_replace': 'Replacement string'}}
            }
        }
    }

    return dq_rules
# Get list of tables
'''
def get_tables():
    tables = inspector.get_table_names()
    return jsonify(tables)

# Get columns for a selected table

def get_columns(table_name):
    columns = inspector.get_columns(table_name)
    return jsonify([col['name'] for col in columns])

# Get DQ rules

def get_rules():
    flat_rules = {}
    for standard, categories in DQ_RULES.items():
        for category, rules in categories.items():
            for rule_name, rule_info in rules.items():
                flat_rules[f"{standard}_{category}_{rule_name}"] = {
                    'description': rule_info['description'],
                    'params': rule_info['params'],
                    'standard': standard,
                    'category': category
                }
    return flat_rules



def run_dq():
    data = request.get_json()
    table_name = data['table']
    column_rules = data['column_rules']  # e.g., {'column1': {'rule': 'DAMA_Completeness_Not Null'}, 'column2': {'rule': 'DAMA_Validity_Format Check', 'params': {'regex': r'^\d+$'}}}

    results = []
    for column, rule_info in column_rules.items():
        rule = rule_info['rule']
        params = rule_info.get('params', {})
        result = execute_dq_rule(table_name, column, rule, params)
        results.append({'column': column, 'rule': rule, 'result': result})

    return jsonify(results)

# DQ rule execution (customize as needed)
def execute_dq_rule(table_name, column, rule, params):
    try:
        query = f"SELECT {column} FROM {table_name}"
        df = pd.read_sql(query, engine)

        if rule == 'DAMA_Completeness_Not Null':
            null_count = df[column].isna().sum()
            return f"{null_count} null values found"
        elif rule == 'DAMA_Completeness_Completeness Ratio':
            total_rows = df.shape[0]
            non_null = df[column].notna().sum()
            ratio = (non_null / total_rows) * 100
            threshold = float(params.get('threshold', 100))
            return f"Completeness: {ratio:.2f}% (Threshold: {threshold}%)"
        elif rule == 'DAMA_Uniqueness_Unique':
            duplicate_count = df[column].duplicated().sum()
            return f"{duplicate_count} duplicate values found"
        elif rule == 'DAMA_Validity_Format Check':
            regex = params.get('regex', '')
            invalid = df[~df[column].astype(str).str.match(regex, na=False)].shape[0]
            return f"{invalid} values do not match format"
        elif rule == 'DAMA_Accuracy_Value Range Check':
            min_val, max_val = float(params.get('min', float('-inf'))), float(params.get('max', float('inf')))
            out_of_range = df[~df[column].between(min_val, max_val, inclusive='both')].shape[0]
            return f"{out_of_range} values out of range [{min_val}, {max_val}]"
        elif rule == 'DAMA_Consistency_Referential Integrity':
            ref_table, ref_column = params['ref_table'], params['ref_column']
            ref_values = pd.read_sql(f"SELECT {ref_column} FROM {ref_table}", engine)[ref_column]
            invalid = df[~df[column].isin(ref_values)].shape[0]
            return f"{invalid} values not found in {ref_table}.{ref_column}"
        elif rule == 'ISO_Accuracy_Precision Check':
            decimal_places = int(params.get('decimal_places', 2))
            invalid = df[df[column].apply(lambda x: len(str(x).split('.')[-1]) > decimal_places if '.' in str(x) else False)].shape[0]
            return f"{invalid} values exceed {decimal_places} decimal places"
        # Add implementations for other rules as needed
        else:
            return "Rule not implemented"
    except Exception as e:
        return f"Error: {str(e)}"

'''