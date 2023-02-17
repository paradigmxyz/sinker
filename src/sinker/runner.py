import json
import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any

import psycopg
from elasticsearch.helpers import bulk

import sinker.query_templates as q
from .bulk_action_generator import BulkActionGenerator
from .es import get_client
from .settings import (
    SINKER_DEFINITIONS_PATH,
    SINKER_SCHEMA,
    SINKER_REPLICATION_SLOT,
    SINKER_TODO_TABLE,
    SINKER_POLL_INTERVAL,
    ELASTICSEARCH_BULK_KWARGS,
)
from .sinker import Sinker, SCHEMA_TABLE_DELIMITER

logger = logging.getLogger(__name__)


class Runner:
    def __init__(self):
        # What are you sinking about?
        with open(f"{SINKER_DEFINITIONS_PATH}/views_to_indices.json") as f:
            views_to_indices = json.load(f)

        # set up table to track materialized views that need updating
        ddl_list = [
            q.DROP_TODO_TABLE.format(SINKER_SCHEMA, SINKER_TODO_TABLE),
            q.CREATE_TODO_TABLE.format(SINKER_SCHEMA, SINKER_TODO_TABLE),
        ]
        psycopg.connect(autocommit=True).execute("; ".join(ddl_list))
        self.views_to_sinkers: dict[str, Sinker] = {
            view: Sinker(view, index) for (view, index) in views_to_indices.items()
        }

        # set up materialized views and Elasticsearch indices, and populate them with initial data
        with ThreadPoolExecutor(max_workers=len(self.views_to_sinkers)) as executor:
            for sinker in self.views_to_sinkers.values():
                executor.submit(sinker.setup)

        parent_tables_to_indices: dict[str, str] = {
            sinker.parent_table: sinker.index for sinker in self.views_to_sinkers.values()
        }

        # set up replication slot
        drop_slot: str = q.DROP_SLOT.format(SINKER_REPLICATION_SLOT)
        with psycopg.connect(autocommit=True) as conn:
            check_slot_format = q.CHECK_SLOT.format(SINKER_REPLICATION_SLOT)
            count_tuple = conn.execute(check_slot_format).fetchone()
            if count_tuple and count_tuple[0] > 0:
                conn.execute(drop_slot)
            create_slot: str = q.CREATE_SLOT.format(SINKER_REPLICATION_SLOT)
            conn.execute(create_slot)

        self.bulk_gen: BulkActionGenerator = BulkActionGenerator(views_to_indices, parent_tables_to_indices)

    def run(self):
        logger.info("We are sinking!")
        while True:
            self.iterate()

    def iterate(self):
        # pop any materialized views that need refreshing
        views: list[tuple[Any, ...]] = (
            psycopg.connect(autocommit=True)
            .execute(q.POP_TODO_ENTRIES.format(SINKER_SCHEMA, SINKER_TODO_TABLE))
            .fetchall()
        )
        # ---------------------------------------
        # a triggered update here will cause a new entry to be added to the table
        # to be processed on a subsequent loop. However, the actual change will get reflected in the materialized
        # view when it gets refreshed below, and will get propagated to Elasticsearch on this loop. When the
        # enqueued update is popped on the next iteration, it could be a harmless no-op if no additional changes
        # occurred because the materialized view will already be up-to-date.
        # ---------------------------------------
        if not views:
            logger.debug("Nothing is something worth doing.")
            sleep(SINKER_POLL_INTERVAL)
            return
        with ThreadPoolExecutor(max_workers=len(views)) as executor:
            for view_tuple in views:
                view: str = view_tuple[0].split(SCHEMA_TABLE_DELIMITER)[1]
                sinker: Sinker = self.views_to_sinkers[view]
                executor.submit(sinker.refresh_view)
                logger.debug(f"{view} view is refreshed")

        # ---------------------------------------
        # a triggered update from here on will cause a new materialized view entry to be added to the "end"
        # of the table to be processed on the next loop iteration.
        # ---------------------------------------

        logger.info("Processing replication slot entries...")
        # Loop over available xid's in replication slot that came from refreshing the materialized view. Any other
        # changes to anything in the entire database will be ignored. It's possible that the replication slot will
        # have accrued entries that are unrelated, and these will need to be scanned through and dropped first.
        # This can happen when the tables that have sinker triggers have no activity but the database sees other
        # activity, like inserts into a schema/table you aren't synchronizing to Elasticsearch. If you are worried
        # about your replication slot growing too large during a scenario like this, you can periodically trigger
        # a materialized view refresh to clear out the slot (see the CREATE_TODO_ENTRY query template).
        processed_tuples, _ = bulk(get_client(), self.bulk_gen.generate_actions(), **ELASTICSEARCH_BULK_KWARGS)
        logger.info(f"Processed {processed_tuples} tuples from replication slot")
