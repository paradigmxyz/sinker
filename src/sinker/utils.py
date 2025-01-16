from typing import Set, Tuple

import sqlglot
from sqlglot.expressions import Table, CTE


def parse_schema_tables(view_select_query: str) -> Tuple[str, Set[str]]:
    """
    Given a view select query, return a primary parent table and the set of unique tables that are referenced in the
    query. Skip anything that looks like a function call.
    :param view_select_query: The select query from the view
    """
    parsed = sqlglot.parse_one(view_select_query)
    parent_table = parsed.find(Table)
    if parent_table is None:
        raise ValueError("No table found in the query")
    tables = {table.name for table in parsed.find_all(Table) if table.name}
    ctes = {cte.alias for cte in parsed.find_all(CTE)}
    schema_tables = tables - ctes
    return parent_table.name, schema_tables
