"""Sinker settings."""
import logging.config
import os

from environs import Env

env = Env()
env.read_env(path=os.path.join(os.getcwd(), ".env"))

DEFAULT_SCHEMA = "public"
SCHEMA_TABLE_DELIMITER = "."

# Sinker:
# path to the views/indices configs
SINKER_DEFINITIONS_PATH = env.str("SINKER_DEFINITIONS_PATH", default=None)
SINKER_SCHEMA = env.str("SINKER_SCHEMA", default=DEFAULT_SCHEMA)
SINKER_REPLICATION_SLOT = env.str("SINKER_REPLICATION_SLOT", default="sinker")
SINKER_TODO_TABLE = env.str("SINKER_TODO_TABLE", default="todo")
SINKER_POLL_INTERVAL = env.int("SINKER_POLL_INTERVAL", default=10)

# Elasticsearch:
ELASTICSEARCH_CHUNK_SIZE = env.int("ELASTICSEARCH_CHUNK_SIZE", default=100)
ELASTICSEARCH_HOST = env.str("ELASTICSEARCH_HOST", default="localhost")
ELASTICSEARCH_MAX_RETRIES = env.int("ELASTICSEARCH_MAX_RETRIES", default=5)
ELASTICSEARCH_PASSWORD = env.str("ELASTICSEARCH_PASSWORD", default=None)
ELASTICSEARCH_PORT = env.int("ELASTICSEARCH_PORT", default=9200)
ELASTICSEARCH_RAISE_ON_ERROR = env.bool("ELASTICSEARCH_RAISE_ON_ERROR", default=True)
ELASTICSEARCH_RAISE_ON_EXCEPTION = env.bool("ELASTICSEARCH_RAISE_ON_EXCEPTION", default=True)
ELASTICSEARCH_SCHEME = env.str("ELASTICSEARCH_SCHEME", default="http")
ELASTICSEARCH_SSL_SHOW_WARN = env.bool("ELASTICSEARCH_SSL_SHOW_WARN", default=False)
ELASTICSEARCH_TIMEOUT = env.float("ELASTICSEARCH_TIMEOUT", default=60)
ELASTICSEARCH_USER = env.str("ELASTICSEARCH_USER", default=None)
ELASTICSEARCH_VERIFY_CERTS = env.bool("ELASTICSEARCH_VERIFY_CERTS", default=True)

ELASTICSEARCH_BULK_KWARGS = dict(
    chunk_size=ELASTICSEARCH_CHUNK_SIZE,
    max_retries=ELASTICSEARCH_MAX_RETRIES,
    raise_on_error=ELASTICSEARCH_RAISE_ON_ERROR,
    raise_on_exception=ELASTICSEARCH_RAISE_ON_EXCEPTION,
)

# Postgres:
PGHOST = env.str("PGHOST", default="localhost")
PGPASSWORD = env.str("PGPASSWORD", default=None)
PGPORT = env.int("PGPORT", default=5432)
PGSSLMODE = env.str("PGSSLMODE", default=None)
PGSSLROOTCERT = env.str("PGSSLROOTCERT", default=None)
PGUSER = env.str("PGUSER")
PGCHUNK_SIZE = env.int("PGCHUNK_SIZE", default=2000)

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
            },
        },
        "loggers": {
            "": {
                "handlers": ["console"],
                "level": env.str("SINKER_LOG_LEVEL", default="INFO"),
                "propagate": True,
            },
        },
    }
)
