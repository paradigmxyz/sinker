CHECK_SLOT = "SELECT count(*) FROM PG_REPLICATION_SLOTS where slot_name='{}'"
DROP_SLOT = "select pg_drop_replication_slot('{}')"
CREATE_SLOT = "select pg_create_logical_replication_slot('{}', 'test_decoding')"

DROP_TODO_TABLE = "drop table if exists {}.{}"
CREATE_TODO_TABLE = """create table {}.{} (
        mv text primary key not null,
        created timestamp not null default now())"""

DROP_VIEW = "drop materialized view if exists {}"
CREATE_VIEW = "create materialized view {} (id, doc) as {}"
CREATE_VIEW_INDEX = "create unique index {}_id on {} (id)"
REFRESH_VIEW = "refresh materialized view concurrently {}.{}"

CREATE_FUNCTION = """create or replace function {} ()
RETURNS TRIGGER AS $$
BEGIN
    insert into {}.{} (mv) values ('{}') on conflict do nothing;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

CREATE_TRIGGER = """create or replace trigger {}
after insert or update or delete on {}."{}"
for each row
execute procedure {}();
"""

CREATE_TODO_ENTRY = "insert into {}.{} (mv) values ('{}')"
POP_TODO_ENTRIES = "delete from {}.{} returning mv"
BACKFILL_QUERY = "SELECT id, doc FROM {}"

GET_ALL_CHANGES = "SELECT xid, lsn, data FROM pg_logical_slot_get_changes('{}', NULL, NULL)"
