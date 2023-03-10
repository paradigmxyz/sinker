#!/usr/bin/env bash
set -euxo pipefail

printenv

# Wait until Elasticsearch and Postgres are available.
until pg_isready -h "$PGHOST"; do
  echo "Waiting for Postgres..."
  sleep 1
done
ES_CREDS="$ELASTICSEARCH_USER:$ELASTICSEARCH_PASSWORD"
ES_ENDPOINT="$ELASTICSEARCH_SCHEME://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT"
until curl -k -u "$ES_CREDS" "$ES_ENDPOINT"; do
  echo "Waiting for Elasticsearch..."
  sleep 1
done

sinker