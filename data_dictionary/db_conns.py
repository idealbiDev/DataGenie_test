import psycopg2
import ast
import pandas as pd

def redshift_connection(redshift_config):
    try:
        conn = psycopg2.connect(**redshift_config)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1')
            ORDER BY schema_name;
        """)
        
        result = cursor.fetchall()
        
        # Extract the schema names from the list of tuples
        schema_names = [schema[0] for schema in result]
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
        
        # Return the list of schema names
        return schema_names

    except Exception as e:
        print("Error connecting to Redshift:", e)
        return None # Return None in case of an error
    

def get_tables_in_schema_test(conn_params,query_template,tables,schema_name):
    dictionary = ast.literal_eval(conn_params)

    redshift_config ={'host':dictionary['hostname'],
            
            'port': dictionary['port'],
            'database': dictionary['database'],
            'user': dictionary['username'],
            'password': dictionary['password']}
    conn = psycopg2.connect(**redshift_config)
    cursor = conn.cursor()
    records_dict={}
    for table in tables:

        query = query_template.format(
            schema_name=schema_name,
            table_name=table
        )
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        records_dict[table]=df
        #print('NOW TESTING table: ',table,' : ',result )
    
    cursor.close()
    return records_dict
    


def get_tables_in_schema(conn_params, schema_name):
    """
    Connects to a Redshift database and returns a list of tables
    for a specified schema.

    Args:
        conn: A psycopg2 connection object.
        schema_name: The name of the schema to query.

    Returns:
        A list of table names (strings) or None if an error occurs.
    """
    dictionary= ast.literal_eval(conn_params)
    redshift_config ={'host':dictionary['hostname'],
            
            'port': dictionary['port'],
            'database': dictionary['database'],
            'user': dictionary['username'],
            'password': dictionary['password']}
    
    try:
        # Create a cursor object
        conn = psycopg2.connect(**redshift_config)
        cursor = conn.cursor()
       

        # Execute the query to get table names for the specified schema.
        # The schema_name parameter is passed safely to the query using %s.
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name limit 20;
        """, (schema_name,))
        
        # Fetch all results from the query
        result = cursor.fetchall()
        
        # Close the cursor
        cursor.close()
        
        # Extract the table names from the list of tuples and return as a list of strings
        table_names = [table[0] for table in result]
        
        
        return table_names
        
    except Exception as e:
        print(f"Error getting tables for schema '{schema_name}': {e}")
        return None