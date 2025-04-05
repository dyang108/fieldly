"""Basic API endpoints for health checks and status."""

from flask import Blueprint

api_bp = Blueprint('api', __name__)


@api_bp.route('/api/ping')
def ping():
    """Health check endpoint."""
    return 'pong' 