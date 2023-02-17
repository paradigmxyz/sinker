from elasticsearch import Elasticsearch

from .settings import (
    ELASTICSEARCH_VERIFY_CERTS,
    ELASTICSEARCH_SCHEME,
    ELASTICSEARCH_USER,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_SSL_SHOW_WARN,
    ELASTICSEARCH_TIMEOUT,
)


def get_client() -> Elasticsearch:
    es_url = (
        f"{ELASTICSEARCH_SCHEME}://{ELASTICSEARCH_USER}:{ELASTICSEARCH_PASSWORD}"
        f"@{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"
    )
    return Elasticsearch(
        es_url,
        verify_certs=ELASTICSEARCH_VERIFY_CERTS,
        ssl_show_warn=ELASTICSEARCH_SSL_SHOW_WARN,
        request_timeout=ELASTICSEARCH_TIMEOUT,
    )
