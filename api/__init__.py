from flask import Blueprint

api_bp = Blueprint('api', __name__, url_prefix='/api')

from api import logs
from api import metrics
from api import dead_queue
