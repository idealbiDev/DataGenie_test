# client/client_functions.py
import pyodbc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os



DB_SERVER = os.getenv("DB_SERVER")

def get_conn():
    try:
        # Attempt to establish a connection
        conn = pyodbc.connect(DB_SERVER)
        return conn
    except pyodbc.Error as e:
        # Raise the error to be caught in the route
        raise e
    
def get_client_billing_summary_old(company_id):
    """
    Fetches a billing summary for a company from UsageLogs.
    Returns a dictionary with total columns processed and cost.
    """
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 
                SUM(columns_processed) AS total_columns,
                SUM(cost) AS total_cost
            FROM ibisols.UsageLogs
            WHERE company_id = ?
            """,
            (company_id,)
        )
        result = cursor.fetchone()
        conn.close()
        return {
            "total_columns": result.total_columns or 0,
            "total_cost": result.total_cost or 0.0
        }
    except pyodbc.Error as e:
        return {"total_columns": 0, "total_cost": 0.0, "error": str(e)}
# Hardcoded billing summaries
billing_summaries = {
    5: {"total_columns": 1200, "total_cost": 96.00},  # Example for Beta LLC
    6: {"total_columns": 300, "total_cost": 30.00},   # Example for IdealBI Demo
}

def get_client_billing_summary(company_id):
    """
    Returns a hardcoded billing summary for a company.
    """
    return billing_summaries.get(company_id, {"total_columns": 0, "total_cost": 0.0})
    


def getclientdata(clientid):
    # Hardcoded connections with varied last_checked timestamps
    connections = [
        {
            'id': 'pg1',
            'name': 'PostgreSQL Analytics',
            'type': 'PostgreSQL',
            'icon': 'database',
            'color': 'blue',
            'schemas': 12,
            'tables': 145,
            'quality': {'good': 70, 'warning': 20, 'bad': 10},
            'last_checked': (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')  # July 27, 2025
        },
        {
            'id': 'ms1',
            'name': 'MySQL Sales DB',
            'type': 'MySQL',
            'icon': 'database',
            'color': 'green',
            'schemas': 8,
            'tables': 95,
            'quality': {'good': 85, 'warning': 10, 'bad': 5},
            'last_checked': (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S')  # July 19, 2025
        },
        {
            'id': 'or1',
            'name': 'Oracle Finance',
            'type': 'Oracle',
            'icon': 'database',
            'color': 'red',
            'schemas': 15,
            'tables': 200,
            'quality': {'good': 60, 'warning': 25, 'bad': 15},
            'last_checked': (datetime.now() - relativedelta(months=1) + timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')  # June 24, 2025
        },
        {
            'id': 'ss1',
            'name': 'SQL Server CRM',
            'type': 'SQL Server',
            'icon': 'database',
            'color': 'purple',
            'schemas': 10,
            'tables': 120,
            'quality': {'good': 75, 'warning': 15, 'bad': 10},
            'last_checked': (datetime.now() - timedelta(days=12)).strftime('%Y-%m-%d %H:%M:%S')  # July 17, 2025
        },
        {
            'id': 'md1',
            'name': 'MongoDB Logs',
            'type': 'MongoDB',
            'icon': 'database',
            'color': 'teal',
            'schemas': 5,
            'tables': 50,
            'quality': {'good': 90, 'warning': 8, 'bad': 2},
            'last_checked': (datetime.now() - relativedelta(months=1) + timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')  # June 29, 2025
        }
    ]
    
    # Data groups, each linked to a connection
    data_groups = [
        {'id': 'dg1', 'name': 'Customer Data', 'connection_id': 'pg1'},
        {'id': 'dg2', 'name': 'Sales Data', 'connection_id': 'pg1'},
        {'id': 'dg3', 'name': 'Marketing Data', 'connection_id': 'pg1'},
        {'id': 'dg4', 'name': 'Inventory Data', 'connection_id': 'ms1'},
        {'id': 'dg5', 'name': 'Order Data', 'connection_id': 'ms1'},
        {'id': 'dg6', 'name': 'Financial Records', 'connection_id': 'or1'},
        {'id': 'dg7', 'name': 'Budget Data', 'connection_id': 'or1'},
        {'id': 'dg8', 'name': 'CRM Contacts', 'connection_id': 'ss1'},
        {'id': 'dg9', 'name': 'Support Tickets', 'connection_id': 'ss1'},
        {'id': 'dg10', 'name': 'Log Analytics', 'connection_id': 'md1'},
        {'id': 'dg11', 'name': 'Error Logs', 'connection_id': 'md1'}
    ]
    
    # Schemas, each linked to a connection and data group
    schemas = [
        # For pg1: Customer Data (dg1)
        {'id': 'sch1', 'name': 'public', 'connection_id': 'pg1', 'data_group_id': 'dg1'},
        {'id': 'sch2', 'name': 'analytics', 'connection_id': 'pg1', 'data_group_id': 'dg1'},
        # For pg1: Sales Data (dg2)
        {'id': 'sch3', 'name': 'sales', 'connection_id': 'pg1', 'data_group_id': 'dg2'},
        {'id': 'sch4', 'name': 'reports', 'connection_id': 'pg1', 'data_group_id': 'dg2'},
        # For pg1: Marketing Data (dg3)
        {'id': 'sch5', 'name': 'campaigns', 'connection_id': 'pg1', 'data_group_id': 'dg3'},
        {'id': 'sch6', 'name': 'metrics', 'connection_id': 'pg1', 'data_group_id': 'dg3'},
        # For ms1: Inventory Data (dg4)
        {'id': 'sch7', 'name': 'inventory', 'connection_id': 'ms1', 'data_group_id': 'dg4'},
        {'id': 'sch8', 'name': 'stock', 'connection_id': 'ms1', 'data_group_id': 'dg4'},
        # For ms1: Order Data (dg5)
        {'id': 'sch9', 'name': 'orders', 'connection_id': 'ms1', 'data_group_id': 'dg5'},
        {'id': 'sch10', 'name': 'fulfillment', 'connection_id': 'ms1', 'data_group_id': 'dg5'},
        # For or1: Financial Records (dg6)
        {'id': 'sch11', 'name': 'finance', 'connection_id': 'or1', 'data_group_id': 'dg6'},
        {'id': 'sch12', 'name': 'accounts', 'connection_id': 'or1', 'data_group_id': 'dg6'},
        # For or1: Budget Data (dg7)
        {'id': 'sch13', 'name': 'budgets', 'connection_id': 'or1', 'data_group_id': 'dg7'},
        {'id': 'sch14', 'name': 'forecasts', 'connection_id': 'or1', 'data_group_id': 'dg7'},
        # For ss1: CRM Contacts (dg8)
        {'id': 'sch15', 'name': 'contacts', 'connection_id': 'ss1', 'data_group_id': 'dg8'},
        {'id': 'sch16', 'name': 'leads', 'connection_id': 'ss1', 'data_group_id': 'dg8'},
        # For ss1: Support Tickets (dg9)
        {'id': 'sch17', 'name': 'tickets', 'connection_id': 'ss1', 'data_group_id': 'dg9'},
        {'id': 'sch18', 'name': 'issues', 'connection_id': 'ss1', 'data_group_id': 'dg9'},
        # For md1: Log Analytics (dg10)
        {'id': 'sch19', 'name': 'logs', 'connection_id': 'md1', 'data_group_id': 'dg10'},
        {'id': 'sch20', 'name': 'metrics', 'connection_id': 'md1', 'data_group_id': 'dg10'},
        # For md1: Error Logs (dg11)
        {'id': 'sch21', 'name': 'errors', 'connection_id': 'md1', 'data_group_id': 'dg11'},
        {'id': 'sch22', 'name': 'alerts', 'connection_id': 'md1', 'data_group_id': 'dg11'}
    ]
    
    # Tables, each linked to a connection, data group, and schema
    tables = [
        # For pg1, dg1, sch1 (public)
        {'id': 'tbl1', 'name': 'customers', 'connection_id': 'pg1', 'data_group_id': 'dg1', 'schema_id': 'sch1'},
        {'id': 'tbl2', 'name': 'customer_profiles', 'connection_id': 'pg1', 'data_group_id': 'dg1', 'schema_id': 'sch1'},
        # For pg1, dg1, sch2 (analytics)
        {'id': 'tbl3', 'name': 'user_analytics', 'connection_id': 'pg1', 'data_group_id': 'dg1', 'schema_id': 'sch2'},
        {'id': 'tbl4', 'name': 'session_data', 'connection_id': 'pg1', 'data_group_id': 'dg1', 'schema_id': 'sch2'},
        # For pg1, dg2, sch3 (sales)
        {'id': 'tbl5', 'name': 'transactions', 'connection_id': 'pg1', 'data_group_id': 'dg2', 'schema_id': 'sch3'},
        {'id': 'tbl6', 'name': 'orders', 'connection_id': 'pg1', 'data_group_id': 'dg2', 'schema_id': 'sch3'},
        # For pg1, dg2, sch4 (reports)
        {'id': 'tbl7', 'name': 'sales_reports', 'connection_id': 'pg1', 'data_group_id': 'dg2', 'schema_id': 'sch4'},
        {'id': 'tbl8', 'name': 'revenue', 'connection_id': 'pg1', 'data_group_id': 'dg2', 'schema_id': 'sch4'},
        # For pg1, dg3, sch5 (campaigns)
        {'id': 'tbl9', 'name': 'campaigns', 'connection_id': 'pg1', 'data_group_id': 'dg3', 'schema_id': 'sch5'},
        {'id': 'tbl10', 'name': 'ad_impressions', 'connection_id': 'pg1', 'data_group_id': 'dg3', 'schema_id': 'sch5'},
        # For pg1, dg3, sch6 (metrics)
        {'id': 'tbl11', 'name': 'marketing_metrics', 'connection_id': 'pg1', 'data_group_id': 'dg3', 'schema_id': 'sch6'},
        {'id': 'tbl12', 'name': 'click_data', 'connection_id': 'pg1', 'data_group_id': 'dg3', 'schema_id': 'sch6'},
        # For ms1, dg4, sch7 (inventory)
        {'id': 'tbl13', 'name': 'stock_levels', 'connection_id': 'ms1', 'data_group_id': 'dg4', 'schema_id': 'sch7'},
        {'id': 'tbl14', 'name': 'warehouse', 'connection_id': 'ms1', 'data_group_id': 'dg4', 'schema_id': 'sch7'},
        # For ms1, dg4, sch8 (stock)
        {'id': 'tbl15', 'name': 'inventory_logs', 'connection_id': 'ms1', 'data_group_id': 'dg4', 'schema_id': 'sch8'},
        {'id': 'tbl16', 'name': 'stock_movement', 'connection_id': 'ms1', 'data_group_id': 'dg4', 'schema_id': 'sch8'},
        # For ms1, dg5, sch9 (orders)
        {'id': 'tbl17', 'name': 'customer_orders', 'connection_id': 'ms1', 'data_group_id': 'dg5', 'schema_id': 'sch9'},
        {'id': 'tbl18', 'name': 'order_details', 'connection_id': 'ms1', 'data_group_id': 'dg5', 'schema_id': 'sch9'},
        # For ms1, dg5, sch10 (fulfillment)
        {'id': 'tbl19', 'name': 'shipments', 'connection_id': 'ms1', 'data_group_id': 'dg5', 'schema_id': 'sch10'},
        {'id': 'tbl20', 'name': 'delivery_status', 'connection_id': 'ms1', 'data_group_id': 'dg5', 'schema_id': 'sch10'},
        # For or1, dg6, sch11 (finance)
        {'id': 'tbl21', 'name': 'transactions', 'connection_id': 'or1', 'data_group_id': 'dg6', 'schema_id': 'sch11'},
        {'id': 'tbl22', 'name': 'invoices', 'connection_id': 'or1', 'data_group_id': 'dg6', 'schema_id': 'sch11'},
        # For or1, dg6, sch12 (accounts)
        {'id': 'tbl23', 'name': 'accounts_payable', 'connection_id': 'or1', 'data_group_id': 'dg6', 'schema_id': 'sch12'},
        {'id': 'tbl24', 'name': 'accounts_receivable', 'connection_id': 'or1', 'data_group_id': 'dg6', 'schema_id': 'sch12'},
        # For or1, dg7, sch13 (budgets)
        {'id': 'tbl25', 'name': 'budget_plans', 'connection_id': 'or1', 'data_group_id': 'dg7', 'schema_id': 'sch13'},
        {'id': 'tbl26', 'name': 'expenditures', 'connection_id': 'or1', 'data_group_id': 'dg7', 'schema_id': 'sch13'},
        # For or1, dg7, sch14 (forecasts)
        {'id': 'tbl27', 'name': 'financial_forecasts', 'connection_id': 'or1', 'data_group_id': 'dg7', 'schema_id': 'sch14'},
        {'id': 'tbl28', 'name': 'projections', 'connection_id': 'or1', 'data_group_id': 'dg7', 'schema_id': 'sch14'},
        # For ss1, dg8, sch15 (contacts)
        {'id': 'tbl29', 'name': 'contacts', 'connection_id': 'ss1', 'data_group_id': 'dg8', 'schema_id': 'sch15'},
        {'id': 'tbl30', 'name': 'customer_info', 'connection_id': 'ss1', 'data_group_id': 'dg8', 'schema_id': 'sch15'},
        # For ss1, dg8, sch16 (leads)
        {'id': 'tbl31', 'name': 'leads', 'connection_id': 'ss1', 'data_group_id': 'dg8', 'schema_id': 'sch16'},
        {'id': 'tbl32', 'name': 'prospects', 'connection_id': 'ss1', 'data_group_id': 'dg8', 'schema_id': 'sch16'},
        # For ss1, dg9, sch17 (tickets)
        {'id': 'tbl33', 'name': 'support_tickets', 'connection_id': 'ss1', 'data_group_id': 'dg9', 'schema_id': 'sch17'},
        {'id': 'tbl34', 'name': 'ticket_history', 'connection_id': 'ss1', 'data_group_id': 'dg9', 'schema_id': 'sch17'},
        # For ss1, dg9, sch18 (issues)
        {'id': 'tbl35', 'name': 'issues', 'connection_id': 'ss1', 'data_group_id': 'dg9', 'schema_id': 'sch18'},
        {'id': 'tbl36', 'name': 'resolutions', 'connection_id': 'ss1', 'data_group_id': 'dg9', 'schema_id': 'sch18'},
        # For md1, dg10, sch19 (logs)
        {'id': 'tbl37', 'name': 'access_logs', 'connection_id': 'md1', 'data_group_id': 'dg10', 'schema_id': 'sch19'},
        {'id': 'tbl38', 'name': 'event_logs', 'connection_id': 'md1', 'data_group_id': 'dg10', 'schema_id': 'sch19'},
        # For md1, dg10, sch20 (metrics)
        {'id': 'tbl39', 'name': 'performance_metrics', 'connection_id': 'md1', 'data_group_id': 'dg10', 'schema_id': 'sch20'},
        {'id': 'tbl40', 'name': 'usage_stats', 'connection_id': 'md1', 'data_group_id': 'dg10', 'schema_id': 'sch20'},
        # For md1, dg11, sch21 (errors)
        {'id': 'tbl41', 'name': 'error_logs', 'connection_id': 'md1', 'data_group_id': 'dg11', 'schema_id': 'sch21'},
        {'id': 'tbl42', 'name': 'crash_reports', 'connection_id': 'md1', 'data_group_id': 'dg11', 'schema_id': 'sch21'},
        # For md1, dg11, sch22 (alerts)
        {'id': 'tbl43', 'name': 'alerts', 'connection_id': 'md1', 'data_group_id': 'dg11', 'schema_id': 'sch22'},
        {'id': 'tbl44', 'name': 'notifications', 'connection_id': 'md1', 'data_group_id': 'dg11', 'schema_id': 'sch22'}
    ]
    
    # Return dictionary with all data
    return {
        'connections': connections,
        'data_groups': data_groups,
        'schemas': schemas,
        'tables': tables
    }

def getSchemas():
    schemas = {
                "public": {
                    "users": {
                        "id": {"type_snapshot": {"column_type": "INT", "nullable": False}},
                        "name": {"type_snapshot": {"column_type": "VARCHAR", "nullable": True}}
                    },
                    "orders": {
                        "id": {"type_snapshot": {"column_type": "INT", "nullable": False}},
                        "amount": {"type_snapshot": {"column_type": "FLOAT", "nullable": True}}
                    }
                },
                "sales": {
                    "customers": {
                        "cust_id": {"type_snapshot": {"column_type": "UUID", "nullable": False}},
                        "join_date": {"type_snapshot": {"column_type": "DATE", "nullable": True}}
                    }
                }
            }
    return schemas


def getListItems():
    list_items = [
        {
            'title': 'Item 1',
            'content': '<p>This is content for item 1</p><ul><li>List item</li></ul>'
        },
        {
            'title': 'Item 2',
            'content': '<p>Different content for item 2</p>'
        },
        # Add more items as needed
    ]

    return list_items

def getServers():
    servers = [
            {"name": "Analytics Server", "icon": "fa-database","svr_name":"anl","type_svr":"redshift"},
            {"name": "Reporting Server", "icon": "fa-chart-line","svr_name":"rps","type_svr":"ms_sql"},
            {"name": "AI Engine", "icon": "fa-brain","svr_name":"aie","type_svr":"ms_sql"},
        ]
    return servers


def connectServer(svr_name):
    servers = [
        {"name": "Analytics Server", "icon": "fa-database", "svr_name": "anl","type_svr":"redshift"},
        {"name": "Reporting Server", "icon": "fa-chart-line", "svr_name": "rps","type_svr":"ms_sql"},
        {"name": "AI Engine", "icon": "fa-brain", "svr_name": "aie","type_svr":"ms_sql"},
    ]

    for server in servers:
        if server["svr_name"] == svr_name:
            return server

    return None  # if no match found
