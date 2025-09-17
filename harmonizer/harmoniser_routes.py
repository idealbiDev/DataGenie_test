from flask import render_template, jsonify, request, current_app
from . import data_mapping_bp
from .models import db  # Only import db here
import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

@data_mapping_bp.route('/')
def index():
    """Data mapping tool main page"""
    return render_template('data_mapping/index.html')

@data_mapping_bp.route('/api/tables', methods=['GET'])
def get_tables():
    """Get list of tables from source database using global connection"""
    try:
        # Use the global connection string from main app
        connection_string = current_app.config.get('GLOBAL_CONNECTION_STRING')
        if not connection_string:
            return jsonify({'error': 'No database connection configured'}), 400
        
        db_type = request.args.get('db_type', 'mysql')  # Default to mysql
        schema_name = request.args.get('schema_name')
        
        from .database import get_table_names
        tables = get_table_names(connection_string, db_type, schema_name)
        return jsonify(tables)
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        return jsonify({'error': str(e)}), 500

@data_mapping_bp.route('/api/columns/<table_name>', methods=['GET'])
def get_columns(table_name):
    """Get columns for a specific table with descriptions"""
    try:
        connection_string = current_app.config.get('GLOBAL_CONNECTION_STRING')
        if not connection_string:
            return jsonify({'error': 'No database connection configured'}), 400
        
        db_type = request.args.get('db_type', 'mysql')
        schema_name = request.args.get('schema_name')
        
        from .database import get_column_metadata, get_sample_data, save_column_descriptions
        from .models import ColumnDescription
        
        # Get column metadata from source database
        column_metadata = get_column_metadata(connection_string, db_type, table_name, schema_name)
        
        # Get sample data
        sample_df = get_sample_data(connection_string, db_type, table_name, schema_name)
        
        # Get existing descriptions from our database
        existing_descriptions = ColumnDescription.query.filter_by(
            table_name=table_name
        ).all()
        desc_dict = {desc.column_name: desc for desc in existing_descriptions}
        
        columns = []
        for col_meta in column_metadata:
            # Handle different database column name cases
            column_name = get_column_name_from_metadata(col_meta)
            if not column_name:
                continue
                
            sample_values = []
            
            if not sample_df.empty and column_name in sample_df.columns:
                sample_values = sample_df[column_name].dropna().head(3).tolist()
            
            # Get or generate description
            if column_name in desc_dict:
                desc = desc_dict[column_name]
                description = {
                    'business_purpose': desc.business_purpose,
                    'data_quality_rules': desc.data_quality_rules,
                    'example_usage': desc.example_usage,
                    'issues': desc.issues
                }
            else:
                from .ai_harmonizer_service import ai_service
                # Generate new description using AI
                ai_description = ai_service.generate_column_description(
                    table_name, col_meta, sample_values
                )
                description = parse_markdown_description(ai_description)
                
                # Save to database
                save_column_descriptions([{
                    'table_name': table_name,
                    'column_name': column_name,
                    'business_purpose': description.get('business_purpose', ''),
                    'data_quality_rules': description.get('data_quality_rules', ''),
                    'example_usage': description.get('example_usage', ''),
                    'issues': description.get('issues', '')
                }])
            
            columns.append({
                'column_name': column_name,
                'data_type': col_meta.get('DATA_TYPE', col_meta.get('data_type', '')),
                'is_nullable': col_meta.get('IS_NULLABLE', col_meta.get('is_nullable', 'YES')),
                'max_length': col_meta.get('CHARACTER_MAXIMUM_LENGTH', col_meta.get('character_maximum_length')),
                'sample_values': sample_values,
                **description
            })
        
        return jsonify(columns)
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {e}")
        return jsonify({'error': str(e)}), 500

@data_mapping_bp.route('/api/ai/suggestions', methods=['POST'])
def get_ai_suggestions():
    """Get AI-powered mapping suggestions"""
    try:
        data = request.json
        source_table = data.get('source_table')
        target_tables = data.get('target_tables', [])
        
        if not source_table or not target_tables:
            return jsonify({'error': 'Missing source_table or target_tables'}), 400
        
        from .models import ColumnDescription
        from .ai_harmonizer_service import ai_service
        
        # Get all column descriptions
        all_columns = {}
        all_tables = [source_table] + target_tables
        
        for table in all_tables:
            descriptions = ColumnDescription.query.filter_by(table_name=table).all()
            all_columns[table] = [
                {
                    'column_name': desc.column_name,
                    'business_purpose': desc.business_purpose,
                    'data_type': 'Unknown',
                    'example_usage': desc.example_usage
                }
                for desc in descriptions
            ]
        
        # Get AI suggestions
        suggestions = ai_service.suggest_mappings(source_table, target_tables, all_columns)
        return jsonify(suggestions)
    except Exception as e:
        logger.error(f"Error getting AI suggestions: {e}")
        return jsonify({'error': str(e)}), 500

@data_mapping_bp.route('/api/mappings', methods=['GET', 'POST'])
def handle_mappings():
    """Get or save column mappings"""
    if request.method == 'GET':
        try:
            from .models import ColumnMapping
            mappings = ColumnMapping.query.all()
            return jsonify([{
                'source_table': m.source_table,
                'source_column': m.source_column,
                'target_table': m.target_table,
                'target_column': m.target_column,
                'mapping_type': m.mapping_type,
                'confidence_score': m.confidence_score,
                'created_at': m.created_at.isoformat() if m.created_at else None
            } for m in mappings])
        except Exception as e:
            logger.error(f"Error getting mappings: {e}")
            return jsonify({'error': str(e)}), 500
    
    else:  # POST
        try:
            from .models import ColumnMapping
            data = request.json
            mapping = ColumnMapping(
                source_table=data['source_table'],
                source_column=data['source_column'],
                target_table=data['target_table'],
                target_column=data['target_column'],
                mapping_type=data.get('mapping_type', 'exact'),
                confidence_score=data.get('confidence_score', 1.0),
                created_by=data.get('created_by', 'system')
            )
            db.session.add(mapping)
            db.session.commit()
            return jsonify({'success': True, 'id': mapping.id})
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error saving mapping: {e}")
            return jsonify({'error': str(e)}), 500

@data_mapping_bp.route('/api/sessions', methods=['POST'])
def create_session():
    """Create a new mapping session"""
    try:
        from .models import MappingSession
        data = request.json
        session = MappingSession(
            session_name=data['name'],
            description=data.get('description', ''),
            created_by=data.get('created_by', 'user')
        )
        db.session.add(session)
        db.session.commit()
        return jsonify({'success': True, 'session_id': session.id})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating session: {e})
        return jsonify({'error': str(e)}), 500

def get_column_name_from_metadata(col_meta):
    """Extract column name from metadata, handling different database cases"""
    # Try different possible column name keys
    possible_keys = [
        'COLUMN_NAME', 'column_name', 'Column_Name',
        'COLUMN', 'column', 'Column',
        'name', 'NAME', 'Name'
    ]
    
    for key in possible_keys:
        if key in col_meta:
            return col_meta[key]
    
    # If no standard key found, try to find any key that might contain the name
    for key, value in col_meta.items():
        if isinstance(value, str) and any(term in key.lower() for term in ['name', 'column']):
            return value
    
    return None

def parse_markdown_description(description):
    """Parse markdown description into components"""
    patterns = {
        'business_purpose': r'\*\*Business Purpose\*\*:\s*(.*?)(?=\n-|\n\n|$)',
        'data_quality_rules': r'\*\*Data Quality Rules\*\*:\s*(.*?)(?=\n-|\n\n|$)',
        'example_usage': r'\*\*Example Usage\*\*:\s*(.*?)(?=\n-|\n\n|$)',
        'issues': r'\*\*Known Issues/Limitations\*\*:\s*(.*?)(?=\n-|\n\n|$)'
    }
    
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, description, re.DOTALL | re.IGNORECASE)
        result[key] = match.group(1).strip() if match else 'Not specified'
    
    return result