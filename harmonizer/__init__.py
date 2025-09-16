from flask import Blueprint

data_mapping_bp = Blueprint('data_mapping', __name__,
                          template_folder='templates',
                          static_folder='static',
                          url_prefix='/data-mapping')

# Don't import anything else here to avoid circular imports