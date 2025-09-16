from cohere import Client
import os
from dotenv import load_dotenv
import pandas as pd
import logging
import hashlib
import pickle
import os.path

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Cohere client
co = Client(os.getenv('COHERE_API_KEY'))

# Cache directory
CACHE_DIR = 'cohere_cache'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_key(prompt):
    """Generate a cache key from the prompt."""
    return hashlib.md5(prompt.encode()).hexdigest()

def load_from_cache(cache_key):
    """Load response from cache if available."""
    cache_file = os.path.join(CACHE_DIR, f'{cache_key}.pkl')
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            return pickle.load(f)
    return None

def save_to_cache(cache_key, response):
    """Save response to cache."""
    cache_file = os.path.join(CACHE_DIR, f'{cache_key}.pkl')
    with open(cache_file, 'wb') as f:
        pickle.dump(response, f)

def generate_column_description(table_name, column_info, sample_values):
    """
    Generate a comprehensive column description using Cohere.
    
    Parameters:
    - table_name (str): Name of the table.
    - column_info (pd.Series): Column metadata (COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH).
    - sample_values (list): Sample data values.
    
    Returns:
    - str: Markdown-formatted description.
    """
    prompt = f"""
    As a data governance expert, provide a concise description for:
    
    Table: {table_name}
    Column: {column_info['COLUMN_NAME']}
    Type: {column_info['DATA_TYPE']}
    Nullable: {column_info['IS_NULLABLE']}
    Max Length: {column_info.get('CHARACTER_MAXIMUM_LENGTH', 'N/A')}
    
    Sample Values: {sample_values[:3]}
    
    Include:
    - Business purpose (1 sentence)
    - Data quality rules (1-2 rules)
    - Example usage in queries (1 example)
    - Known issues/limitations (1-2 issues)
    
    Format as markdown bullets, keep total length under 200 words.
    """
    
    cache_key = get_cache_key(prompt)
    cached_response = load_from_cache(cache_key)
    if cached_response:
        logging.info(f"Retrieved cached description for {column_info['COLUMN_NAME']} in {table_name}")
        return cached_response
    
    try:
        response = co.generate(
            model=os.getenv('COHERE_MODEL', 'command-r'),  # Default model if not set
            prompt=prompt,
            max_tokens=200,  # Reduced to ensure concise output
            temperature=0.2
        )
        description = response.generations[0].text.strip()
        save_to_cache(cache_key, description)
        logging.info(f"Generated description for {column_info['COLUMN_NAME']} in {table_name}")
        return description
    
    except Exception as e:
        logging.error(f"Error generating description for {column_info['COLUMN_NAME']} in {table_name}: {str(e)}")
        fallback = f"- **Business Purpose**: Unknown purpose for {column_info['COLUMN_NAME']}.\n- **Data Quality Rules**: Ensure non-null if required.\n- **Example Usage**: SELECT {column_info['COLUMN_NAME']} FROM {table_name};\n- **Issues**: Description generation failed."
        return fallback