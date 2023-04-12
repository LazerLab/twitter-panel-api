"""Package initialization and metadata."""
from flask import Flask

from .endpoints import public_api

__version__ = "0.4.2"


def create_app() -> Flask:
    """
    Instantiate the Flask application object.
    """
    app = Flask(__name__)
    app.register_blueprint(public_api)

    return app
