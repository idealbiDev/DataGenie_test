from flask import Blueprint, render_template, request, redirect, url_for,jsonify,session
import logging
from sqlalchemy import text
import pandas as pd
import os
from .db_utils import get_schemas, get_tables,getDQRules
from .db_manager import DBManager
from urllib.parse import quote_plus
from .db_select import *
from .ai_service_new import *
from .quality_service import get_dq_rules
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

data_dictionary_bp = Blueprint(
    'data_dictionary',
    __name__
    #,template_folder='templates',
   # static_folder='static'
)

@data_dictionary_bp.context_processor
def inject_common_variables():
    return {
        'product': 'Data Dictionary Generator',
        'username': session.get('username', 'N/A'),
        'company_name': session.get('company_name', 'N/A'),
        'tier_name': session.get('tier_name', 'N/A'),
        'products_opted': session.get('products_opted', [])
    }

# Database configuration
DB_TYPES = {
    'postgresql': {
        'driver': 'postgresql+psycopg2',
        'default_port': '5432',
        'connection_string': '{driver}://{username}:{password}@{hostname}:{port}/{database}',
        'fields': [
            {'name': 'hostname', 'label': 'Host', 'type': 'text', 'required': True},
            {'name': 'port', 'label': 'Port', 'type': 'number', 'default': '5432', 'required': True},
            {'name': 'database', 'label': 'Database Name', 'type': 'text', 'required': True},
            {'name': 'username', 'label': 'Username', 'type': 'text', 'required': True},
            {'name': 'password', 'label': 'Password', 'type': 'password', 'required': True},
        ]
    },
    'redshift': {
        'driver': 'redshift+psycopg2',
        'default_port': '5439',
        'connection_string': '{driver}://{username}:{password}@{hostname}:{port}/{database}',
        'fields': [
            {'name': 'hostname', 'label': 'Cluster Endpoint', 'type': 'text', 'required': True},
            {'name': 'port', 'label': 'Port', 'type': 'number', 'default': '5439', 'required': True},
            {'name': 'database', 'label': 'Database Name', 'type': 'text', 'required': True},
            {'name': 'username', 'label': 'Username', 'type': 'text', 'required': True},
            {'name': 'password', 'label': 'Password', 'type': 'password', 'required': True},
        ]
    },
    # ... other database types ...
}

# 1. Main Data Quality Monitor Dashboard
@data_dictionary_bp.route("/dictionary", methods=['GET'])
def dictionary():
    return render_template('data_dictionary/index.html')

# 2. Database Connection Wizard - Step 1: Introduction
@data_dictionary_bp.route("/dbselect", methods=['GET'])
def dbselect():
    return render_template('data_dictionary/wizard_step1.html')

# 3. Database Connection Wizard - Step 2: Database Type Selection
@data_dictionary_bp.route("/dbselect/type", methods=['GET', 'POST'])
def dbselect_type():
    db_type_list=get_db_types()
   # if request.method == 'POST':
        # Handle form submission from step 1
    #    return redirect(url_for('data_quality_monitor.dbselect_connection'))
    
    return render_template('data_dictionary/wizard_step2.html', db_type_list=db_type_list)

# 4. Database Connection Wizard - Step 3: Connection Details
@data_dictionary_bp.route("/dbselect/connection", methods=['GET', 'POST'])
def dbselect_connection():
    form_data = request.form.to_dict()
    db_type=form_data['db_type']
    db_config=get_db_properties(db_type)
    
    #if request.method == 'GET':
    #    db_type = request.args.get('db_type')
    #    if db_type not in DB_TYPES:
    #        return redirect(url_for('data_quality_monitor.dbselect_type'))
        
    #return db_config
    return render_template(
            'data_dictionary/wizard_step3.html',
           db_type=db_type,
            db_config=db_config
       )
    
   


@data_dictionary_bp.route("/dbselect/schema", methods=['GET','POST'])
def dbschema_select():
    db_type = request.form.get('db_type')
    form_data = request.form.to_dict()
     
    conn_str = build_connection_string(db_type, form_data)
    
    dbschema = get_schema_names(conn_str, db_type)
    #get_schema_names(file_system_conn_str, 'file_system')get_table_names(connection_string, db_type, schema_name=None)
    if db_type =='file_system':
        data_list = get_table_names(conn_str, db_type)
    else:
         data_list = dbschema #get_table_names(file_system_conn_str, 'file_system')
    return render_template(
            'data_dictionary/wizard_step4.html',
           db_type=db_type,
            dbschema=dbschema ,data_list=data_list,conn_str=conn_str,form_data=form_data
       )

@data_dictionary_bp.route("/dbselect/service", methods=['POST'])
def dbservice():
    form_data = request.form.to_dict()
    db_type = request.form.get('db_type')
    conn_str = request.form.get('conn_str')
    dbschema=request.form.get('schema')
    conn_params =request.form.get('conn_params') 
    if db_type =='file_system':
        data_list = get_table_names(conn_str, db_type)
    
    elif db_type =='redshift':
        
       
        data_list=get_tables_in_schema(conn_params, dbschema)
       
    else:
         
         data_list = get_table_names(conn_str, db_type,dbschema)
    print("1 ran")
    print(form_data)
    return render_template(
            'data_dictionary/wizard_step5.html',form_data=form_data,
          dbschema=dbschema ,db_type=db_type,conn_params=conn_params,
            data_list=data_list,conn_str=conn_str
       )


@data_dictionary_bp.route("/dbselect/tableselect", methods=['POST'])
def dbtableselect():
    db_type = request.form.get('db_type')
    form_data = request.form.to_dict()
    conn_str = request.form.get('conn_str')
    db_schema_name = request.form.get('schema')
    
    data_list = get_table_names(conn_str, db_type, db_schema_name)
        
  
    return render_template(
            'data_dictionary/wizard_step5tables.html',
           db_type=db_type,data_list=data_list,
            form_data=form_data)



@data_dictionary_bp.route("/dbselect/table", methods=['POST'])
def dbtable():
    form_data = request.form.to_dict()
    return render_template(
            'data_dictionary/wizard_step5.html',form_data=form_data
          # ,db_type=db_type,
          #  dbschema=dbschema ,data_list=data_list
       )

@data_dictionary_bp.route("/dbselect/doservice", methods=['POST'])
def doservice():

    form_data = request.form.to_dict()
    generate_dict_tables = request.form.getlist('generate_dict')
    run_dq_tables = request.form.getlist('run_dq')
    
    doservice_list =  {
        'dict_tables': generate_dict_tables,
        'dq_tables': run_dq_tables,
        'db_type' : request.form.get('db_type'),
       'conn_str' : request.form.get('conn_str'),
       'db_schema_name' : request.form.get('dbschema'),
       'conn_params' : request.form.get('conn_params')
    }
    dq_rules=getDQRules()
    data=get_top_records(doservice_list)
    print("ran")
    print(data)
    all_column_values = {
        df_name: {col: df[col].iloc[0] for col in df.columns}
        for df_name, df in data.items()
    }
    
  

    return render_template(
            'data_dictionary/wizard_data.html',data=data,form_data=form_data
           ,doservice_list=doservice_list,all_column_values=all_column_values,run_dq_tables=run_dq_tables,dq_rules=dq_rules
          #  dbschema=dbschema ,data_list=data_list
       )


@data_dictionary_bp.route('/dbdictionary', methods=['POST'])
def dbdictionary():
    """Flask route for generating data dictionary"""
    try:
        # Extract and process request data
        dict_tables_t = request.form.getlist('dict_tables')
        
        if not dict_tables_t:
            return jsonify({'error': 'No tables selected'}), 400
        
        # Parse table list - FIXED to handle different formats
        try:
            if isinstance(dict_tables_t[0], str):
                db_tables = eval(dict_tables_t[0])
            else:
                db_tables = dict_tables_t[0]
                
            # Ensure it's a list
            if not isinstance(db_tables, list):
                db_tables = [db_tables]
                
        except Exception as e:
            logging.error(f"Error parsing table list: {e}")
            return jsonify({'error': f'Invalid table format: {e}'}), 400
        
        # Get form fields
        conn_str = request.form.get('conn_str')
        dq_tables = request.form.get('dq_tables')
        db_schema_name = request.form.get('db_schema_name')
        db_type = request.form.get('db_type')
        conn_params = request.form.get('conn_params')
        
        # Construct service parameters
        doservice_list = {
            'dict_tables': db_tables,
            'dq_tables': dq_tables,
            'db_type': db_type,
            'conn_str': conn_str,
            'db_schema_name': db_schema_name,
            'conn_params': conn_params
        }
        
        logging.info(f"Starting data dictionary generation for tables: {db_tables}")
        
        # Get sample data - ensure it returns a dictionary
        data = get_top_records(doservice_list)
        
        # Validate that data is a dictionary
        if not isinstance(data, dict):
            logging.error(f"Expected dict from get_top_records, got {type(data)}")
            # Try to convert if it's a list of DataFrames
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], pd.DataFrame):
                data = {f"table_{i}": df for i, df in enumerate(data)}
            else:
                return jsonify({'error': 'Invalid data format returned from get_top_records'}), 500
        
        # Generate descriptions
        descriptions = DataDictionaryGenerator.generate_column_descriptions_for_tables(
            data_dict=data,
            connection_string=doservice_list['conn_str'],
            db_type=doservice_list['db_type'],
            schema_name=doservice_list['db_schema_name']
        )
        logging.info(f"Generated  column descriptions : {descriptions}")
        logging.info(f"Generated {len(descriptions)} column descriptions")
        
        return jsonify(descriptions)
        #return render_template('data_dictionary/wizard_data_new.html', descriptions=descriptions, data=data,
         #   doservice_list=doservice_list
       # )
        
    except Exception as e:
        logging.error(f"Error in dbdictionary route: {e}")
        return jsonify({'error': str(e)}), 500


@data_dictionary_bp.route('/testapp')
def testapp():
        
        return render_template('data_dictionary/testapp.html')

@data_dictionary_bp.route('/dataquality', methods=['POST'])
def dataquality():
   form_data = request.form.to_dict()
   dq_rules = get_dq_rules()
   conn_str = form_data["conn_str"]
   db_type = form_data["db_type"]
   table_name = form_data["table_name"]
   schema_name= form_data["db_schema_name"]
   columns= get_columns(conn_str, db_type, schema_name=schema_name, tables=table_name)
   #return form_data
   return render_template('data_dictionary/dataquality.html',columns=columns,dq_rules=dq_rules)

   
# Helper functions
def test_build_connection_string(db_type, form_data):
    DB_TYPES=['redshift', 'mssql_local', 'azure_sql','txt','csv']

    
    db_config=get_db_properties(db_type)
    if db_type not in DB_TYPES:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    #db_config = DB_TYPES[db_type]
    connection_template = db_config['connection_string']
    
    variables = {'driver': db_config['driver']}
    
    for field in db_config['fields']:
        field_name = field['name']
        if field_name in form_data:
            variables[field_name] = form_data[field_name]
    
    # Special handling for certain database types
    if db_type in ['sqlite', 'access']:
        variables['database'] = variables['database'].replace('\\', '/')
    
    # URL encode sensitive values
    for key in ['username', 'password']:
        if key in variables:
            variables[key] = quote_plus(variables[key])
    
    return connection_template.format(**variables)

def handle_connection_submission():
    try:
        db_type = request.form.get('db_type')
        form_data = request.form.to_dict()
        
        conn_str = build_connection_string(db_type, form_data)
        
        # Save connection to session or database
        # session['db_connection'] = conn_str
        
        # Redirect to next step in your workflow
        return redirect(url_for('data_dictionary.datamonitor'))
        
    except Exception as e:
        logging.error(f"Connection setup failed: {str(e)}")
        return render_template(
            'data_dictionary/error.html',
            message=f"Connection setup failed: {str(e)}"
        ), 400