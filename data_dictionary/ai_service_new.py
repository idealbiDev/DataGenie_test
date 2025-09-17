import mysql.connector
from typing import Dict, List, Any
import pandas as pd
import requests
import hashlib
import pickle
import os
import re
import logging

# Database configuration (replace with your actual credentials)
db_config_local = {
    'host': 'localhost',
    'port': 3306,
    'database': 'datagenie',
    'user': 'datagenie',
    'password': 'P@ss1234'
}

# Cache directory
CACHE_DIR = 'ollama_cache'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def local_db_connection(db_config):
    """Establishes a connection to the local database using the provided config."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        db_version = cursor.fetchone()
        version_local = db_version[0]
        logging.info(f"Connected to MySQL version: {version_local}")
        return conn, cursor
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        raise

def get_cache_key(prompt: str) -> str:
    return hashlib.md5(prompt.encode()).hexdigest()

def load_from_cache(cache_key: str):
    cache_file = os.path.join(CACHE_DIR, f'{cache_key}.pkl')
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            return pickle.load(f)
    return None

def save_to_cache(cache_key: str, response: str):
    cache_file = os.path.join(CACHE_DIR, f'{cache_key}.pkl')
    with open(cache_file, 'wb') as f:
        pickle.dump(response, f)

def parse_markdown_description(description: str) -> dict:
    patterns = {
        'business_purpose': r'\*\*Business Purpose\*\*:\s*(.*?)(?=\n-|\n\n|$)',
        'data_quality_rules': r'\*\*Data Quality Rules\*\*:\s*(.*?)(?=\n-|\n\n|$)',
        'example_usage': r'\*\*Example Usage\*\*:\s*(.*?)(?=\n-|\n\n|$)',
        'issues': r'\*\*Known Issues/Limitations\*\*:\s*(.*?)(?=\n-|\n\n|$)'
    }
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, description, re.DOTALL)
        result[key] = match.group(1).strip() if match else ''
    return result

def generate_column_description(table_name: str, column_info: Dict[str, Any], sample_values: List[Any]) -> str:
    logging.info(f"Retrieved cached description for generate_column_description")
    prompt = f"""
    As a data governance expert, provide a concise description for:

    Table: {table_name}
    Column: {column_info['COLUMN_NAME']}
    Type: {column_info['DATA_TYPE']}
    Nullable: {column_info['IS_NULLABLE']}
    Max Length: {column_info.get('CHARACTER_MAXIMUM_LENGTH', 'N/A')}

    Sample Values: {sample_values[:3]}

    Include:
    - **Business Purpose**: Describe the column's role in business processes (1 sentence).
    - **Data Quality Rules**: List 1-2 rules to ensure data integrity (e.g., format, range, uniqueness).
    - **Example Usage**: Provide 1 example of how the column is used in analysis or operations.
    - **Known Issues/Limitations**: Identify 1-2 possible data quality issues.

    Format as markdown bullets, keep total length under 200 words.
    """

    cache_key = get_cache_key(prompt)
    cached = load_from_cache(cache_key)
    if cached:
        return cached

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "llama3",
                "prompt": prompt,
                "options": {"temperature": 0.2, "num_predict": 200},
                "stream": False
            },
            timeout=60
        )
        logging.info(f"Retrieved cached description for generate")
        if response.status_code != 200:
            raise Exception(f"Ollama error: {response.status_code} {response.text}")

        data = response.json()
        description = data.get("response", "").strip()
        save_to_cache(cache_key, description)
        return description
    except Exception as e:
        return f"""
- **Business Purpose**: Unable to determine purpose for {column_info['COLUMN_NAME']}.
- **Data Quality Rules**: Ensure data matches type {column_info['DATA_TYPE']}; check for nulls if {column_info['IS_NULLABLE']}='NO'.
- **Example Usage**: Analyze {column_info['COLUMN_NAME']} in {table_name} (e.g., count unique values).
- **Known Issues/Limitations**: Description generation failed: {str(e)}; sample values: {sample_values[:3]}.
"""

def generate_column_descriptions_for_tables(data_dict: Dict[str, pd.DataFrame],
                                           db_config: Dict[str, Any],
                                           schema_name: str = None):
    all_results = []

    for table_name, df in data_dict.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue

        # Build metadata DataFrame
        metadata_df = pd.DataFrame({
            'COLUMN_NAME': df.columns,
            'DATA_TYPE': [str(df[col].dtype) for col in df.columns],
            'IS_NULLABLE': ['YES' if df[col].isna().any() else 'NO' for col in df.columns],
            'CHARACTER_MAXIMUM_LENGTH': [None] * len(df.columns)
        })

        for _, column_info in metadata_df.iterrows():
            column_name = column_info['COLUMN_NAME']
            if column_name not in df.columns:
                continue

            sample_values = df[column_name].dropna().head(3).tolist()
            description = generate_column_description(table_name, column_info, sample_values)
            parsed = parse_markdown_description(description)

            all_results.append({
                'table_name': table_name,
                'column_name': column_name,
                'business_purpose': parsed.get('business_purpose', ''),
                'data_quality_rules': parsed.get('data_quality_rules', ''),
                'example_usage': parsed.get('example_usage', ''),
                'issues': parsed.get('issues', '')
            })

    # Insert into MySQL using mysql.connector
    if all_results:
        try:
            conn, cursor = local_db_connection(db_config)
            
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS column_descriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                business_purpose TEXT,
                data_quality_rules TEXT,
                example_usage TEXT,
                issues TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            
            # Insert data
            insert_query = """
            INSERT INTO column_descriptions 
            (table_name, column_name, business_purpose, data_quality_rules, example_usage, issues)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            for result in all_results:
                cursor.execute(insert_query, (
                    result['table_name'],
                    result['column_name'],
                    result['business_purpose'],
                    result['data_quality_rules'],
                    result['example_usage'],
                    result['issues']
                ))
            
            conn.commit()
            logging.info(f"Successfully inserted {len(all_results)} records into column_descriptions table")
            
            cursor.close()
            conn.close()
            
        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")
            # You might want to handle this error differently, e.g., return the results without inserting
            raise

    return all_results

# Usage example:
# results = generate_column_descriptions_for_tables(your_data_dict, db_config_local)