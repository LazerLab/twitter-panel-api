"""Package initialization and metadata."""
import json
import os

from flask import Flask

from .endpoints import public_api

__version__ = "0.6.0"

default_settings = {
    "MIN_DISPLAYED_USERS": 10,
    "MAX_CROSS_SECTIONS": 2,
    "EXPLICIT_ZEROS": True,
    "ELASTICSEARCH_URL": "http://localhost:9200/",
    "DATABASE_URL": "postgresql://postgres:password@localhost:5432/database",
    "TWEETS": {"SOURCE": "elasticsearch"},
    "VOTERS": {"SOURCE": "database"},
}


def create_app(**kwargs) -> Flask:
    """
    Instantiate the Flask application object.
    """
    app = Flask(__name__)

    app.config.from_mapping(default_settings)

    config_file = os.environ.get("API_CONFIG")
    if config_file is not None:
        app.config.from_file(config_file, json.load)

    app.config.update(kwargs)

    app.register_blueprint(public_api)

    return app
