from flask import Blueprint

harmonizer_bp = Blueprint('harmonizer', __name__,
                          template_folder='templates',
                          static_folder='static',
                          url_prefix='/harmonizer')

# Import routes after creating the blueprint to avoid circular imports
from . import harmonizer_routes