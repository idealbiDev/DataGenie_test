# client/client_routes.py
from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify
from .client_functions import get_client_billing_summary, getclientdata,getSchemas,getListItems,getServers,connectServer
from .client_dq_data import *
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict
client_bp = Blueprint('home', __name__, template_folder='templates', static_folder='static')

@client_bp.app_template_filter('datetimeformat')
def datetimeformat(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return 'N/A'

@client_bp.app_template_filter('safe_percent')
def safe_percent(value):
    try:
        val = float(value)
        return max(0, min(val, 100))
    except (TypeError, ValueError):
        return 0
    

@client_bp.route("/")
def clientdashboard():
        data = getclientdata(session["company_id"])
        servers=getServers()

        return render_template(
        "client/client.html",product="Dashboard",servers=servers,
        username=session["username"],
        company_name=session["company_name"],
        tier_name=session["tier_name"],
        products_opted=session["products_opted"],
        company_type=session["company_type"],
       connections=data['connections'],
        data_groups=data['data_groups'],
        schemas=data['schemas'],
        tables=data['tables'],server_conn=session["server_conn"]
    )    
@client_bp.route("/home")
def clienthome():
    if "username" not in session:
        flash("Please log in first.", "danger")
        return redirect(url_for("login"))
    if session["company_type"] != "Billing":
        flash("Access restricted to Billing clients.", "danger")
        return redirect(url_for("dashboard"))
    
    # Example: Use helper function to get billing summary
    billing_summary = get_client_billing_summary(session["company_id"])
    data = getclientdata(session["company_id"])
    
    return render_template(
        "client/dashboard.html",product="Account Overview",
        username=session["username"],
        company_name=session["company_name"],
        tier_name=session["tier_name"],
        products_opted=session["products_opted"],
        company_type=session["company_type"],
       connections=data['connections'],
        data_groups=data['data_groups'],
        schemas=data['schemas'],
        tables=data['tables']
    )

@client_bp.route("/dataquality")
def dataquality():
    if "username" not in session:
        flash("Please log in first.", "danger")
        return redirect(url_for("login"))
    if session["company_type"] != "Billing":
        flash("Access restricted to Billing clients.", "danger")
        return redirect(url_for("dashboard"))
    
    # Example: Use helper function to get billing summary
    billing_summary = get_client_billing_summary(session["company_id"])
    data = getclientdata(session["company_id"])
    
    return render_template(
        "client/clientdashboard.html",product="Data Quality",
        username=session["username"],
        company_name=session["company_name"],
        tier_name=session["tier_name"],
        products_opted=session["products_opted"],
        company_type=session["company_type"],
       connections=data['connections'],
        data_groups=data['data_groups'],
        schemas=data['schemas'],
        tables=data['tables']
    )
@client_bp.route("/billing")
def client_billing():
    if "username" not in session:
        flash("Please log in first.", "danger")
        return redirect(url_for("login"))
    if session["company_type"] != "Billing":
        flash("Access restricted to Billing clients.", "danger")
        return redirect(url_for("dashboard"))
    
    # Fetch billing summary
    billing_summary = get_client_billing_summary(session["company_id"])
    
    return render_template(
        "client/clientbilling.html",product="Billing",
        username=session["username"],
        company_name=session["company_name"],
        tier_name=session["tier_name"],
        products_opted=session["products_opted"],
        company_type=session["company_type"],
        billing_summary=billing_summary
    )
@client_bp.route("/account")
def accountinfo():
    if "username" not in session:
        flash("Please log in first.", "danger")
        return redirect(url_for("login"))
    if session["company_type"] != "Billing":
        flash("Access restricted to Billing clients.", "danger")
        return redirect(url_for("dashboard"))
    
    # Fetch billing summary
   
    
    return render_template(
        "client/accountinfo.html",product="Account Information",
        username=session["username"],
        company_name=session["company_name"],
        tier_name=session["tier_name"],
        products_opted=session["products_opted"],
        company_type=session["company_type"]
    )


@client_bp.route("/settings")
def client_settings():
    if "username" not in session:
        flash("Please log in first.", "danger")
        return redirect(url_for("login"))
    if session["company_type"] != "Billing":
        flash("Access restricted to Billing clients.", "danger")
        return redirect(url_for("dashboard"))
    return render_template(
        "client/settings.html",product="Settings",
        username=session["username"],
        company_name=session["company_name"],
        tier_name=session["tier_name"],
        products_opted=session["products_opted"],
        company_type=session["company_type"]
    )
@client_bp.route("/server_connect", methods=['POST'])
def server_connect():
     if "username" not in session:
        return jsonify({'error': 'Unauthorized'}), 401
     else:
        if request.method == 'POST':

            server = request.form.get('svr_name')
            server_connect=connectServer(server)
            print(server_connect)
            session["server_conn"] = 1
            session["server"] = server_connect.get("name", "Unknown Server")
            session["server_type"] = server_connect.get("type_svr", "Unknown Server")
            print("server_type",session["server_type"])
            return redirect(url_for("client.clientdashboard"))
            #return jsonify({'server_connect':server_connect})
        

@client_bp.route("/filter_data", methods=['POST'])
def filter_data():
    if "username" not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    if session["company_type"] != "Billing":
        return jsonify({'error': 'Access restricted'}), 403
    
    filters = request.get_json()
    if not filters:
        return jsonify({'error': 'Invalid request'}), 400

    connection_id = filters.get('connection', 'all')
    data_group_id = filters.get('group', 'all')
    schema_id = filters.get('schema', 'all')
    table_id = filters.get('table', 'all')
    time_range = filters.get('time', 'current_month')

    # Fetch all data
    data = getclientdata(session["company_id"])
    connections = data['connections']
    data_groups = data['data_groups']
    schemas = data['schemas']
    tables = data['tables']

    # Filter data_groups
    filtered_data_groups = [
        dg for dg in data_groups
        if (connection_id == 'all' or dg['connection_id'] == connection_id)
    ]

    # Filter schemas
    filtered_schemas = [
        s for s in schemas
        if (connection_id == 'all' or s['connection_id'] == connection_id) and
           (data_group_id == 'all' or s['data_group_id'] == data_group_id)
    ]

    # Filter tables
    filtered_tables = [
        t for t in tables
        if (connection_id == 'all' or t['connection_id'] == connection_id) and
           (data_group_id == 'all' or t['data_group_id'] == data_group_id) and
           (schema_id == 'all' or t['schema_id'] == schema_id)
    ]

    # Filter connections based on selections
    filtered_connection_ids = set()
    if table_id != 'all':
        try:
            table = next(t for t in tables if t['id'] == table_id)
            filtered_connection_ids.add(table['connection_id'])
        except StopIteration:
            return jsonify({'error': 'Invalid table ID'}), 400
    elif schema_id != 'all':
        try:
            schema = next(s for s in schemas if s['id'] == schema_id)
            filtered_connection_ids.add(schema['connection_id'])
        except StopIteration:
            return jsonify({'error': 'Invalid schema ID'}), 400
    elif data_group_id != 'all':
        try:
            data_group = next(dg for dg in data_groups if dg['id'] == data_group_id)
            filtered_connection_ids.add(data_group['connection_id'])
        except StopIteration:
            return jsonify({'error': 'Invalid data group ID'}), 400
    else:
        filtered_connection_ids.update(c['id'] for c in connections)

    # Apply time filter
    now = datetime.now()
    if time_range == 'last_7_days':
        time_threshold = now - timedelta(days=7)
    elif time_range == 'last_14_days':
        time_threshold = now - timedelta(days=14)
    elif time_range == 'current_month':
        time_threshold = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif time_range == 'previous_month':
        last_month = now - relativedelta.relativedelta(months=1)
        time_threshold = last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return jsonify({'error': 'Invalid time range'}), 400

    filtered_connections = [
        c for c in connections
        if c['id'] in filtered_connection_ids and
           datetime.strptime(c['last_checked'], '%Y-%m-%d %H:%M:%S') >= time_threshold
    ]

    return jsonify({
        'connections': filtered_connections,
        'data_groups': filtered_data_groups,
        'schemas': filtered_schemas,
        'tables': filtered_tables
    })

@client_bp.route("/tables", methods=['GET', 'POST'])  # Handles both GET and POST
def client_tables():
    if request.method == 'POST':
        conn_id = request.form.get('conn_id')
        schemas=getdata_qualityResults()
        
       
        
       
       
       # return render_template('client/schemas.html', conn_id=conn_id,products_opted=session["products_opted"])  # Or redirect
        return render_template('client/schemas.html',
                               username=session["username"],
                               conn_id=conn_id,product="Data Quality",
        company_name=session["company_name"],
        products_opted=session["products_opted"],
        
                            schemas=schemas)
    
    # Handle GET requests (if someone visits /tables directly)
    return  redirect(url_for("dashboard"))

@client_bp.route('/schemas')
def list_schemas():
    schemas=getSchemas()
    list_items=getListItems()
    return render_template('client/schemas.html',username=session["username"],list_items=list_items,
        company_name=session["company_name"],products_opted=session["products_opted"],
                            schemas=list(schemas.keys()))

@client_bp.route('/listtables/<schema_name>')
def list_tables(schema_name):
    schemas=getSchemas()
    tables = list(schemas.get(schema_name, {}).keys())
    return render_template('client/tables.html', tables=tables, schema_name=schema_name)

@client_bp.route('/columns/<schema_name>/<table_name>')
def list_columns(schema_name, table_name):
    schemas=getSchemas()
    columns = schemas.get(schema_name, {}).get(table_name, {})
    return render_template('client/columns.html', products_opted=session["products_opted"],
                         columns=columns,
                         schema_name=schema_name,
                         table_name=table_name)

@client_bp.route('/edit-column/<schema_name>/<table_name>/<column_name>', methods=['GET', 'POST'])
def edit_column(schema_name, table_name, column_name):
    if request.method == 'POST':
        # Update column definition
        column_def = {
            "type_snapshot": {
                "column_type": request.form.get('column_type'),
                "nullable": request.form.get('nullable') == 'true'
            }
        }
        schemas=getSchemas()
        schemas[schema_name][table_name][column_name] = column_def
        return jsonify({"status": "success"})
    
    # GET request - return current column data
    column = schemas.get(schema_name, {}).get(table_name, {}).get(column_name, {})
    return render_template('client/column_edit_modal.html', 
                         column=column,
                         schema_name=schema_name,products_opted=session["products_opted"],
                         table_name=table_name,
                         column_name=column_name)
