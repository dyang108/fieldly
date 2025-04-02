from flask import Flask
from .schemas import schemas_bp
from .datasets import datasets_bp
from .uploads import uploads_bp
from .ai import ai_bp


def register_blueprints(app: Flask) -> None:
    """
    Register all blueprints with the Flask app
    
    Args:
        app: Flask application instance
    """
    app.register_blueprint(schemas_bp)
    app.register_blueprint(datasets_bp)
    app.register_blueprint(uploads_bp)
    app.register_blueprint(ai_bp)


__all__ = ['register_blueprints'] 