import re

from typing import Iterable

TABLE_RE = re.compile(r"from\s\"?(\S+)\b", re.I)


def generate_schema_tables(view_select_query: str) -> Iterable[str]:
    """
    Given a view select query, return a list of unique tables that are referenced in the query
    in the order they were encountered.
    Skip anything that looks like a function call.
    :param view_select_query: The select query from the view
    """
    seen: set = set()
    for table_candidate in TABLE_RE.findall(view_select_query):
        if "(" not in table_candidate:
            if table_candidate not in seen:
                seen.add(table_candidate)
                yield table_candidate
