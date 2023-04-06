"""
Define config values for the API.
"""
import json
import os
from typing import Any, Mapping

_default_config = {
    "user_count_privacy_threshold": 10,
    "cross_sections_limit": 2,
    "flask": {"SECRET_KEY": "flask_secret_key"},
    "elasticsearch": {
        "hosts": ["https://elastic:password@localhost:9200/"],
    },
    "postgresql": {
        "host": "localhost",
        "port": "5200",
        "database": "voters",
        "user": "postgres",
    },
}


class Config(dict):
    """
    DO NOT USE OUTSIDE THIS FILE!
    Configuration for the API.
    """

    def __init__(self, *, path=None, env=True, fallback=True):
        if fallback:
            self.update(_default_config)
        if env:
            env_config = os.environ.get("CONFIG")
            if env_config and os.path.exists(env_config):
                self.update(Config.parse_file(env_config))
        if path and os.path.exists(path):
            self.update(Config.parse_file(path))

    @staticmethod
    def parse_file(path: str) -> Mapping[str, Any]:
        """
        Parse a config JSON file into a Python dict.
        """
        return json.load(open(path, encoding="utf-8"))


CONFIG = Config()


def get_config_value(key: str, default: Any = None):
    """
    Get a value from the global API config.
    """
    return CONFIG.get(key, default)
