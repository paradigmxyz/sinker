import logging
import re
from dataclasses import dataclass
from logging import Logger
from typing import Iterable, Dict, Any, Match, Optional

import psycopg

import sinker.query_templates as q
from sinker.settings import SINKER_REPLICATION_SLOT, PGCHUNK_SIZE

logger: Logger = logging.getLogger(__name__)

# Replication slot pattern
SLOT_RE = re.compile(
    r"""table\s\"?(?P<schema>[\w-]+)\"?.\"?(?P<table>[\w-]+)\"?:\s
        (?P<tg_op>[A-Z]+):\sid\[text]:'(?P<id>[\w-]+)'""",
    re.X,
)
GET_CURSOR_NAME = "get_changes"


@dataclass
class BulkActionGenerator:
    views_to_indices: dict
    parent_tables_to_indices: dict

    def generate_actions(self) -> Iterable[Dict[str, Any]]:
        with psycopg.connect() as conn:
            with conn.cursor(name=GET_CURSOR_NAME) as cursor:
                # num of tuples to fetch over the wire at a time, not transactions. A transaction can contain
                # many tuples.
                cursor.itersize = PGCHUNK_SIZE
                # gather all pending transactions on server-side cursor, which has the side effect of
                # truncating the replication slot
                cursor.execute(q.GET_ALL_CHANGES.format(SINKER_REPLICATION_SLOT))
                for xid, lsn, data in cursor:
                    logger.debug(f"Got LSN {lsn} for xid {xid} with data {data}")
                    match: Optional[Match[str]] = SLOT_RE.search(data)
                    if match:
                        logger.debug(f"LSN entry {lsn} matches pattern")
                        slot_dict: dict[str, str] = match.groupdict()
                        slot_dict["table"] = slot_dict["table"].replace('"', "")
                        if slot_dict["table"] in self.views_to_indices and slot_dict["tg_op"] == "INSERT":
                            doc: str = data.split("doc[json]:")[1].replace("'", "")
                            logger.debug(
                                f"Putting doc {slot_dict['id']} from {slot_dict['table']}"
                                f" into {self.views_to_indices[slot_dict['table']]}"
                            )
                            yield self.index_action(doc, slot_dict)
                        elif slot_dict["table"] in self.parent_tables_to_indices and slot_dict["tg_op"] == "DELETE":
                            logger.debug(
                                f"Deleting doc {slot_dict['id']}"
                                f" from {self.parent_tables_to_indices[slot_dict['table']]}"
                            )
                            yield self.delete_action(slot_dict)
                        else:
                            logger.debug(f"Ignoring LSN {lsn}")

    def delete_action(self, slot_dict):
        """
        Generate a delete action for a particular Elasticsearch document
        based on the replication slot entry for the parent table e.g.
            table public.foo: DELETE: id[text]:'a-1'
        :param slot_dict: The dict representing the replication slot entry
        :return: The delete action for use in the Elasticsearch bulk operation
        """
        delete_action: dict[str, str] = {
            "_op_type": "delete",
            "_index": self.parent_tables_to_indices[slot_dict["table"]],
            "_id": slot_dict["id"],
        }
        return delete_action

    def index_action(self, doc: str, slot_dict: dict) -> dict[str, Any]:
        """
        Generate an index action (insert or overwrite what's already in Elasticsearch for that ID)
        based on the replication slot entry for the materialized view e.g.
            table sinker.foo_mv: INSERT: id[text]:'a-1' doc[json]:'{"name" : "Foo Bar"}'

        :param doc: The document JSON string to index in Elasticsearch
        :param slot_dict: The dict representing the replication slot entry
        :return: The index action for use in the Elasticsearch bulk operation
        """
        index_action: dict[str, Any] = {
            "_index": self.views_to_indices[slot_dict["table"]],
            "_id": slot_dict["id"],
            "_source": doc,
        }
        return index_action
