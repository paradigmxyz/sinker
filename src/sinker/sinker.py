import json
import logging
import os
import re
from typing import Iterable, Dict, Any

import psycopg
from elasticsearch.helpers import bulk

import sinker.query_templates as q
from .es import get_client
from .settings import (
    SINKER_DEFINITIONS_PATH,
    DEFAULT_SCHEMA,
    SINKER_SCHEMA,
    SINKER_TODO_TABLE,
    ELASTICSEARCH_BULK_KWARGS,
    PGCHUNK_SIZE,
    SCHEMA_TABLE_DELIMITER,
)

logger = logging.getLogger(__name__)

BACKFILL_CURSOR_NAME = "backfill"
TABLE_RE = re.compile(r"from\s\"?(\S+)\b", re.I)


class Sinker:
    def __init__(self, view, index):
        """
        :param view: Postgres materialized view name
        :param index: Elasticsearch index name
        """
        self.view: str = view
        self.index: str = index
        self.parent_table: str = ""  # defined during setup process

    def setup(self):
        """
        Creates materialized views with supporting functions and triggers, and creates Elasticsearch indices
        """
        self.setup_pg()
        self.setup_es()

    def setup_es(self) -> None:
        logger.info(f"Setting up the {self.index} Elasticsearch index")
        self.recreate_index()
        self.backfill_index()

        # optionally enable ES replicas here

    def backfill_index(self) -> None:
        # populate ES index with initial data from materialized view
        logger.info(f"Populating {self.index} with initial data from {self.view}")
        added_docs, _ = bulk(get_client(), self.backfill_stream(), **ELASTICSEARCH_BULK_KWARGS)
        logger.info(f"Added {added_docs} documents to {self.index}")

    def backfill_stream(self) -> Iterable[Dict[str, Any]]:
        """
        Uses a server-side cursor to stream the data from Postgres in PGCHUNK_SIZE chunks, yielding an Elasticsearch
        bulk "index" action for each row.
        """
        schema_view_name: str = f"{SINKER_SCHEMA}.{self.view}"
        query: str = q.BACKFILL_QUERY.format(schema_view_name)
        with psycopg.connect() as conn:
            with conn.cursor(name=BACKFILL_CURSOR_NAME) as cursor:
                cursor.itersize = PGCHUNK_SIZE
                cursor.execute(query)
                for doc_id, doc in cursor:
                    yield {"_id": doc_id, "_index": self.index, "_source": doc}

    def recreate_index(self) -> None:
        es = get_client()
        # read Elasticsearch index definition file
        index_mapping_path: str = os.path.join(os.getcwd(), SINKER_DEFINITIONS_PATH, f"{self.index}.json")
        with open(index_mapping_path, "r") as f:
            index_body = json.load(f)
            # (re)create index in Elasticsearch
            es.indices.delete(index=self.index, ignore_unavailable=True)
            es.indices.create(
                index=self.index,
                mappings=index_body["mappings"],
                settings=index_body["settings"],
            )

    def setup_pg(self):
        """
        Creates materialized views with supporting functions and triggers, and sets the parent table that drives the
        materialized view
        """
        logger.info(f"Setting up the {self.view} materialized view")
        ddl_list: list[str] = list()
        # read SQL view file
        view_sql_path: str = os.path.join(os.getcwd(), SINKER_DEFINITIONS_PATH, f"{self.view}.sql")
        with open(view_sql_path, "r") as f:
            view_select_query: str = f.read()
        schema_view_name: str = f"{SINKER_SCHEMA}.{self.view}"
        drop_view: str = q.DROP_VIEW.format(schema_view_name)
        ddl_list.append(drop_view)
        create_view: str = q.CREATE_VIEW.format(schema_view_name, view_select_query)
        ddl_list.append(create_view)
        create_index: str = q.CREATE_VIEW_INDEX.format(self.view, schema_view_name)
        ddl_list.append(create_index)
        # Get constituent tables from SQL query and create function and triggers for them
        plpgsql: str = f"{schema_view_name}_fn"
        create_function: str = q.CREATE_FUNCTION.format(plpgsql, SINKER_SCHEMA, SINKER_TODO_TABLE, schema_view_name)
        ddl_list.append(create_function)
        schema_tables: list[Any] = TABLE_RE.findall(view_select_query)
        for schema_table in schema_tables:
            schema, _, table = schema_table.rpartition(SCHEMA_TABLE_DELIMITER)
            schema = schema or DEFAULT_SCHEMA
            trigger_name: str = f"{SINKER_SCHEMA}_{self.view}_{schema}_{table}"
            create_trigger: str = q.CREATE_TRIGGER.format(trigger_name, schema, table, plpgsql)
            ddl_list.append(create_trigger)
        create_todo_entry: str = q.CREATE_TODO_ENTRY.format(SINKER_SCHEMA, SINKER_TODO_TABLE, schema_view_name)
        ddl_list.append(create_todo_entry)
        psycopg.connect(autocommit=True).execute("; ".join(ddl_list))
        # The last table is the top-level table that gets DELETE events with an ID in the replication slot.
        # The materialized views do not contain the ID of the doc being deleted,
        # so we'll use this table's delete events as a proxy.
        # lsn,xid,data
        # 0/24EDA4D8,17393,BEGIN 17393
        # 0/24EDA4D8,17393,"table public.""Foo"": DELETE: id[text]:'91754ea9-2983-4cf7-bdf9-fc23d2386d90'"
        # 0/24EDC1B0,17393,COMMIT 17393
        # 0/24EDC228,17394,BEGIN 17394
        # 0/24EF0D60,17394,table sinker.foo_mv: DELETE: (no-tuple-data)
        # 0/24EF4718,17394,COMMIT 17394
        self.parent_table = schema_tables[-1]

    def refresh_view(self) -> None:
        logger.info(f"Refreshing the {self.view} materialized view")
        refresh_view_query: str = q.REFRESH_VIEW.format(SINKER_SCHEMA, self.view)
        psycopg.connect(autocommit=True).execute(refresh_view_query)
