import mysql.connector
from typing import Dict, List, Any, Tuple, Optional, Union
import pandas as pd
import requests
import hashlib
import pickle
import os
import re
import logging
import time
import json
import threading
import traceback

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_dictionary_debug.log', mode='w'),  # Overwrite each run
        logging.StreamHandler()
    ]
)

# Create logger for this module
logger = logging.getLogger('DataDictionaryGenerator')

# Configuration
CACHE_DIR = 'ollama_cache'
OLLAMA_CONFIG = {
    'url': 'http://localhost:11434/api/generate',
    'model': 'llama3.2:1b',
    'timeout': 60,
    'max_workers': 3,
    'batch_size': 4,
    'retry_attempts': 3,
    'retry_delay': 2
}

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)
logger.info(f"Cache directory: {CACHE_DIR}")

class DatabaseConnection:
    """Database connection handler with connection pooling"""
    
    _connections = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_connection(cls, db_config: Dict[str, Any]):
        """Get or create a database connection"""
        config_key = json.dumps(db_config, sort_keys=True)
        
        with cls._lock:
            if config_key not in cls._connections:
                try:
                    logger.debug(f"Creating new database connection to {db_config.get('host', 'unknown')}")
                    conn = mysql.connector.connect(**db_config)
                    cls._connections[config_key] = conn
                    logger.info(f"Created new database connection for {db_config['host']}")
                except mysql.connector.Error as err:
                    logger.error(f"Database connection error: {err}")
                    logger.error(f"Connection config: {db_config}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error creating connection: {e}")
                    raise
        
        return cls._connections[config_key]
    
    @classmethod
    def close_all(cls):
        """Close all database connections"""
        with cls._lock:
            for config_key, conn in cls._connections.items():
                try:
                    conn.close()
                    logger.debug(f"Closed database connection: {config_key}")
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
            cls._connections.clear()
            logger.info("All database connections closed")

class OllamaClient:
    """Client for interacting with Ollama API with caching and retry logic"""
    
    @staticmethod
    def get_cache_key(prompt: str) -> str:
        """Generate a cache key from prompt"""
        return hashlib.md5(prompt.encode()).hexdigest()
    
    @staticmethod
    def load_from_cache(cache_key: str) -> Optional[str]:
        """Load response from cache"""
        cache_file = os.path.join(CACHE_DIR, f'{cache_key}.pkl')
        if os.path.exists(cache_file):
            try:
                logger.debug(f"Loading from cache: {cache_key}")
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"Cache load failed: {e}")
        else:
            logger.debug(f"Cache miss: {cache_key}")
        return None
    
    @staticmethod
    def save_to_cache(cache_key: str, response: str):
        """Save response to cache"""
        cache_file = os.path.join(CACHE_DIR, f'{cache_key}.pkl')
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(response, f)
            logger.debug(f"Saved to cache: {cache_key}")
        except Exception as e:
            logger.warning(f"Cache save failed: {e}")
    
    @staticmethod
    def parse_markdown_description(description: str) -> dict:
        """Parse markdown formatted description into structured data"""
        logger.debug(f"Parsing markdown description: {description[:100]}...")
        
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
            logger.debug(f"Parsed {key}: {result[key][:50]}...")
        
        return result
    
    @classmethod
    def test_ollama_connection(cls) -> bool:
        """Test if Ollama is running and accessible"""
        try:
            logger.info("Testing Ollama connection...")
            response = requests.get("http://localhost:11434/api/tags", timeout=10)
            if response.status_code == 200:
                logger.info(f"Ollama connection successful. Available models: {response.json()}")
                return True
            else:
                logger.error(f"Ollama connection failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Ollama connection test failed: {e}")
            return False
    
    @classmethod
    def generate_descriptions_batch(cls, columns_data: List[Tuple[str, Dict[str, Any], List[Any]]]) -> Dict[str, str]:
        """Generate descriptions for multiple columns in a single API call"""
        logger.info(f"generate_descriptions_batch called with {len(columns_data)} columns")
        
        if not columns_data:
            logger.warning("No columns data provided to generate_descriptions_batch")
            return {}
        
        # Test Ollama connection first
        if not cls.test_ollama_connection():
            logger.error("Ollama is not available. Using fallback responses.")
            return cls._generate_fallback_responses(columns_data)
        
        # Build batch prompt
        prompt = "As a data governance expert, provide concise descriptions for the following columns:\n\n"
        
        for table_name, column_info, sample_values in columns_data:
            prompt += f"""
Table: {table_name}
Column: {column_info['COLUMN_NAME']}
Type: {column_info['DATA_TYPE']}
Nullable: {column_info['IS_NULLABLE']}
Max Length: {column_info.get('CHARACTER_MAXIMUM_LENGTH', 'N/A')}
Sample Values: {sample_values[:5]}

"""
        
        prompt += """
For each column, include:
- **Business Purpose**: Describe the column's role in business processes (1 sentence).
- **Data Quality Rules**: List 1-2 rules to ensure data integrity (e.g., format, range, uniqueness).
- **Example Usage**: Provide 1 example of how the column is used in analysis or operations.
- **Known Issues/Limitations**: Identify 1-2 possible data quality issues.

Format as markdown bullets, keep each description under 200 words.
Clearly separate each column description with '---COLUMN---' marker.
"""
        
        logger.debug(f"Generated prompt for {len(columns_data)} columns")
        logger.debug(f"Prompt preview: {prompt[:200]}...")
        
        cache_key = cls.get_cache_key(prompt)
        cached = cls.load_from_cache(cache_key)
        if cached:
            logger.info(f"Using cached response for {len(columns_data)} columns")
            return cached
        
        # Generate with retry logic
        for attempt in range(OLLAMA_CONFIG['retry_attempts']):
            try:
                logger.info(f"Calling Ollama API for {len(columns_data)} columns (attempt {attempt + 1}/{OLLAMA_CONFIG['retry_attempts']})")
                
                # Log the exact request being made
                request_data = {
                    "model": OLLAMA_CONFIG['model'],
                    "prompt": prompt,
                    "options": {
                        "temperature": 0.2, 
                        "num_predict": 200 * len(columns_data),
                        "top_p": 0.9
                    },
                    "stream": False
                }
                logger.debug(f"Ollama request data: {json.dumps(request_data, indent=2)}")
                
                response = requests.post(
                    OLLAMA_CONFIG['url'],
                    json=request_data,
                    timeout=OLLAMA_CONFIG['timeout']
                )
                
                logger.debug(f"Ollama response status: {response.status_code}")
                logger.debug(f"Ollama response headers: {dict(response.headers)}")
                
                if response.status_code != 200:
                    error_msg = f"Ollama error: {response.status_code} {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                data = response.json()
                logger.debug(f"Ollama response JSON: {json.dumps(data, indent=2)}")
                
                description = data.get("response", "").strip()
                logger.debug(f"Raw response: {description[:200]}...")
                
                if not description:
                    raise Exception("Empty response from Ollama")
                
                # Parse the batch response
                descriptions = description.split('---COLUMN---')
                logger.debug(f"Split into {len(descriptions)} description parts")
                
                result = {}
                for i, desc in enumerate(descriptions):
                    if i < len(columns_data):
                        table_name, column_info, _ = columns_data[i]
                        column_name = column_info['COLUMN_NAME']
                        result[f"{table_name}.{column_name}"] = desc.strip()
                        logger.debug(f"Processed column {i+1}: {table_name}.{column_name}")
                
                # Save to cache
                cls.save_to_cache(cache_key, result)
                logger.info(f"Successfully generated descriptions for {len(columns_data)} columns")
                return result
                
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error to Ollama: {e}")
                logger.error("Is Ollama running? Try: ollama serve")
            except requests.exceptions.Timeout as e:
                logger.error(f"Timeout error with Ollama: {e}")
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Wait before retry
            if attempt < OLLAMA_CONFIG['retry_attempts'] - 1:
                wait_time = OLLAMA_CONFIG['retry_delay'] * (attempt + 1)
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        # If all attempts failed
        logger.error(f"All {OLLAMA_CONFIG['retry_attempts']} attempts failed for batch of {len(columns_data)} columns")
        return cls._generate_fallback_responses(columns_data)
    
    @classmethod
    def _generate_fallback_responses(cls, columns_data: List[Tuple[str, Dict[str, Any], List[Any]]]) -> Dict[str, str]:
        """Generate fallback responses when Ollama fails"""
        logger.warning("Generating fallback responses due to Ollama failure")
        result = {}
        for table_name, column_info, sample_values in columns_data:
            column_name = column_info['COLUMN_NAME']
            result[f"{table_name}.{column_name}"] = f"""
- **Business Purpose**: Unable to determine purpose for {column_name}.
- **Data Quality Rules**: Ensure data matches type {column_info['DATA_TYPE']}; check for nulls if {column_info['IS_NULLABLE']}='NO'.
- **Example Usage**: Analyze {column_name} in {table_name} (e.g., count unique values).
- **Known Issues/Limitations**: Description generation failed due to Ollama connection issues.
"""
        return result

class DataDictionaryGenerator:
    """Main class for generating data dictionary descriptions"""
    
    @staticmethod
    def parse_connection_string(conn_str: str) -> Dict[str, Any]:
        """Parse SQLAlchemy-style connection string to mysql.connector config"""
        logger.debug(f"Parsing connection string: {conn_str}")
        
        # Default config
        default_config = {
            'host': 'localhost',
            'port': 3306,
            'database': 'datagenie',
            'user': 'datagenie',
            'password': 'P@ss1234',
            'charset': 'utf8mb4',
            'autocommit': True
        }
        
        try:
            if 'mysql+pymysql://' in conn_str:
                # Extract the part after mysql+pymysql://
                parts = conn_str.replace('mysql+pymysql://', '').split('@')
                if len(parts) == 2:
                    # User/password part
                    user_pass = parts[0].split(':')
                    user = user_pass[0]
                    password = user_pass[1] if len(user_pass) > 1 else ''
                    
                    # Host/port/database part
                    host_db = parts[1].split('/')
                    host_port = host_db[0].split(':')
                    host = host_port[0]
                    port = int(host_port[1]) if len(host_port) > 1 else 3306
                    database = host_db[1] if len(host_db) > 1 else 'datagenie'
                    
                    config = {
                        'host': host,
                        'port': port,
                        'database': database,
                        'user': user,
                        'password': password,
                        'charset': 'utf8mb4',
                        'autocommit': True
                    }
                    logger.debug(f"Parsed connection config: {config}")
                    return config
        except Exception as e:
            logger.error(f"Error parsing connection string: {e}")
            logger.error(f"Using default config due to parsing error")
        
        logger.debug(f"Using default connection config: {default_config}")
        return default_config
    
    @staticmethod
    def prepare_column_data(data_dict: Dict[str, pd.DataFrame]) -> List[Tuple[str, Dict[str, Any], List[Any]]]:
        """Prepare column data for processing"""
        logger.debug(f"prepare_column_data called with data_dict type: {type(data_dict)}")
        
        all_columns_data = []
        
        # Handle case where data_dict might be a list or other type
        if not isinstance(data_dict, dict):
            logger.error(f"Expected dict but got {type(data_dict)}: {data_dict}")
            return all_columns_data
        
        logger.info(f"Processing {len(data_dict)} tables")
        
        for table_name, df in data_dict.items():
            logger.debug(f"Processing table: {table_name}")
            
            if not isinstance(df, pd.DataFrame):
                logger.warning(f"Skipping {table_name}: not a DataFrame (type: {type(df)})")
                continue
            
            if df.empty:
                logger.warning(f"Skipping {table_name}: DataFrame is empty")
                continue
            
            logger.debug(f"Table {table_name} has {len(df.columns)} columns and {len(df)} rows")
            
            for column_name in df.columns:
                try:
                    sample_values = df[column_name].dropna().head(5).tolist()
                    column_info = {
                        'COLUMN_NAME': column_name,
                        'DATA_TYPE': str(df[column_name].dtype),
                        'IS_NULLABLE': 'YES' if df[column_name].isna().any() else 'NO',
                        'CHARACTER_MAXIMUM_LENGTH': None
                    }
                    all_columns_data.append((table_name, column_info, sample_values))
                    logger.debug(f"Added column: {table_name}.{column_name} with {len(sample_values)} samples")
                except Exception as e:
                    logger.error(f"Error processing column {column_name} in table {table_name}: {e}")
        
        logger.info(f"Prepared {len(all_columns_data)} columns for processing")
        return all_columns_data
    
    @staticmethod
    def ensure_descriptions_table(db_config: Dict[str, Any]):
        """Ensure the column_descriptions table exists"""
        try:
            logger.debug(f"Ensuring table exists with config: {db_config}")
            conn = DatabaseConnection.get_connection(db_config)
            cursor = conn.cursor()
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS column_descriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                business_purpose TEXT,
                data_quality_rules TEXT,
                example_usage TEXT,
                issues TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY table_column_idx (table_name, column_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_query)
            cursor.close()
            logger.info("Ensured column_descriptions table exists")
            
        except Exception as e:
            logger.error(f"Error ensuring table exists: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    @staticmethod
    def save_descriptions_to_db(descriptions: List[Dict[str, Any]], db_config: Dict[str, Any]):
        """Save descriptions to database"""
        logger.debug(f"save_descriptions_to_db called with {len(descriptions)} descriptions")
        
        if not descriptions:
            logger.warning("No descriptions to save to database")
            return
        
        try:
            conn = DatabaseConnection.get_connection(db_config)
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO column_descriptions 
            (table_name, column_name, business_purpose, data_quality_rules, example_usage, issues)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            business_purpose = VALUES(business_purpose),
            data_quality_rules = VALUES(data_quality_rules),
            example_usage = VALUES(example_usage),
            issues = VALUES(issues),
            updated_at = CURRENT_TIMESTAMP
            """
            
            batch_size = 50
            for i in range(0, len(descriptions), batch_size):
                batch = descriptions[i:i + batch_size]
                values = [
                    (result['table_name'], result['column_name'], 
                     result['business_purpose'], result['data_quality_rules'],
                     result['example_usage'], result['issues'])
                    for result in batch
                ]
                logger.debug(f"Executing batch insert with {len(values)} rows")
                cursor.executemany(insert_query, values)
                conn.commit()
                logger.info(f"Saved {len(batch)} descriptions to database")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Don't raise here to allow the function to continue
    
    @classmethod
    def generate_column_descriptions_for_tables(cls, data_dict: Dict[str, pd.DataFrame],
                                               connection_string: str,
                                               db_type: str,
                                               schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Main function to generate column descriptions
        """
        logger.info("=" * 60)
        logger.info("STARTING COLUMN DESCRIPTION GENERATION")
        logger.info("=" * 60)
        
        logger.debug(f"Input parameters:")
        logger.debug(f"  data_dict type: {type(data_dict)}, keys: {list(data_dict.keys()) if isinstance(data_dict, dict) else 'N/A'}")
        logger.debug(f"  connection_string: {connection_string}")
        logger.debug(f"  db_type: {db_type}")
        logger.debug(f"  schema_name: {schema_name}")
        
        # Parse connection string
        db_config = cls.parse_connection_string(connection_string)
        logger.info(f"Using database config: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        
        # Validate input data
        if not data_dict or not isinstance(data_dict, dict):
            logger.error(f"Invalid data_dict: expected dict, got {type(data_dict)}")
            if hasattr(data_dict, '__len__'):
                logger.error(f"Data length: {len(data_dict)}")
            return []
        
        logger.info(f"Processing {len(data_dict)} tables")
        
        # Prepare all column data
        all_columns_data = cls.prepare_column_data(data_dict)
        if not all_columns_data:
            logger.warning("No column data to process")
            return []
        
        logger.info(f"Prepared {len(all_columns_data)} columns from {len(data_dict)} tables")
        
        # Ensure database table exists
        cls.ensure_descriptions_table(db_config)
        
        results = []
        total_columns = len(all_columns_data)
        processed_columns = 0
        
        logger.info(f"Processing {total_columns} columns from {len(data_dict)} tables")
        
        # Process columns in batches
        for i in range(0, total_columns, OLLAMA_CONFIG['batch_size']):
            batch = all_columns_data[i:i + OLLAMA_CONFIG['batch_size']]
            batch_num = i // OLLAMA_CONFIG['batch_size'] + 1
            total_batches = (total_columns + OLLAMA_CONFIG['batch_size'] - 1) // OLLAMA_CONFIG['batch_size']
            
            logger.info(f"Processing batch {batch_num}/{total_batches} with {len(batch)} columns")
            
            batch_descriptions = OllamaClient.generate_descriptions_batch(batch)
            
            for table_name, column_info, sample_values in batch:
                column_key = f"{table_name}.{column_info['COLUMN_NAME']}"
                description = batch_descriptions.get(column_key, "")
                parsed = OllamaClient.parse_markdown_description(description)
                
                result = {
                    'table_name': table_name,
                    'column_name': column_info['COLUMN_NAME'],
                    'business_purpose': parsed.get('business_purpose', ''),
                    'data_quality_rules': parsed.get('data_quality_rules', ''),
                    'example_usage': parsed.get('example_usage', ''),
                    'issues': parsed.get('issues', '')
                }
                results.append(result)
                
                logger.debug(f"Processed {table_name}.{column_info['COLUMN_NAME']}: "
                           f"business_purpose={bool(result['business_purpose'])}, "
                           f"data_quality_rules={bool(result['data_quality_rules'])}, "
                           f"example_usage={bool(result['example_usage'])}, "
                           f"issues={bool(result['issues'])}")
            
            processed_columns += len(batch)
            logger.info(f"Processed {processed_columns}/{total_columns} columns ({processed_columns/total_columns*100:.1f}%)")
            
            # Add a small delay between batches
            if i + OLLAMA_CONFIG['batch_size'] < total_columns:
                time.sleep(1)
        
        # Save to database
        cls.save_descriptions_to_db(results, db_config)
        
        logger.info("=" * 60)
        logger.info(f"COMPLETED PROCESSING {total_columns} COLUMNS")
        logger.info(f"Generated {len(results)} descriptions")
        logger.info("=" * 60)
        
        return results

# Cleanup on application exit
import atexit
atexit.register(DatabaseConnection.close_all)

