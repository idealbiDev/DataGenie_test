from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from flask import current_app

# Create SQLAlchemy instance
db = SQLAlchemy()

class ColumnDescription(db.Model):
    __tablename__ = 'column_descriptions'
    
    id = db.Column(db.Integer, primary_key=True)
    table_name = db.Column(db.String(255), nullable=False)
    column_name = db.Column(db.String(255), nullable=False)
    business_purpose = db.Column(db.Text)
    data_quality_rules = db.Column(db.Text)
    example_usage = db.Column(db.Text)
    issues = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('table_name', 'column_name', name='unique_table_column'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'table_name': self.table_name,
            'column_name': self.column_name,
            'business_purpose': self.business_purpose,
            'data_quality_rules': self.data_quality_rules,
            'example_usage': self.example_usage,
            'issues': self.issues,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class ColumnMapping(db.Model):
    __tablename__ = 'column_mappings'
    
    id = db.Column(db.Integer, primary_key=True)
    source_table = db.Column(db.String(255), nullable=False)
    source_column = db.Column(db.String(255), nullable=False)
    target_table = db.Column(db.String(255), nullable=False)
    target_column = db.Column(db.String(255), nullable=False)
    mapping_type = db.Column(db.Enum('exact', 'fuzzy', 'transformed'), default='exact')
    transformation_rule = db.Column(db.Text)
    confidence_score = db.Column(db.Float, default=1.0)
    created_by = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('source_table', 'source_column', 'target_table', 'target_column', 
                           name='unique_mapping'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'source_table': self.source_table,
            'source_column': self.source_column,
            'target_table': self.target_table,
            'target_column': self.target_column,
            'mapping_type': self.mapping_type,
            'transformation_rule': self.transformation_rule,
            'confidence_score': self.confidence_score,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class MappingSession(db.Model):
    __tablename__ = 'mapping_sessions'
    
    id = db.Column(db.Integer, primary_key=True)
    session_name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    created_by = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'session_name': self.session_name,
            'description': self.description,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'is_active': self.is_active
        }