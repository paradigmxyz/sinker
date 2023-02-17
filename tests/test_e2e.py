from time import sleep

import elasticsearch
import psycopg
import pytest

from sinker.es import get_client
from sinker.runner import Runner

# pytest_plugins = ["docker_compose"]
DROP_SINKER_SCHEMA = """drop schema if exists sinker cascade"""
CREATE_SINKER_SCHEMA = """create schema sinker"""
DROP_PERSON_TABLE = """drop table if exists public.person"""
CREATE_PERSON_TABLE = """create table public.person (
        id text primary key not null,
        name text not null,
        created timestamp not null default now())"""
INSERT_PERSON_1 = """insert into public.person (id, name) values ('p-1', 'John')"""


# Invoking this fixture: 'function_scoped_container_getter' starts all services
@pytest.fixture(scope="function")
def wait_for_pg_es():
    """Wait for Postgres and Elasticsearch to become responsive"""
    ddl_list = [DROP_SINKER_SCHEMA, CREATE_SINKER_SCHEMA, DROP_PERSON_TABLE, CREATE_PERSON_TABLE, INSERT_PERSON_1]
    while True:
        try:
            psycopg.connect(autocommit=True).execute("; ".join(ddl_list))
            break
        except psycopg.OperationalError:
            print("Not yet")
            sleep(1)
    get_client().cluster.health(wait_for_status="green")


def test_end_to_end(wait_for_pg_es):
    runner = Runner()
    es = get_client()
    # verify the initial data made it into Elasticsearch
    doc = es.get(index="people", id="p-1")
    assert doc["_source"]["name"] == "John"

    # update the data in Postgres
    psycopg.connect(autocommit=True).execute("update public.person set name = 'Jane' where id = 'p-1'")
    runner.iterate()
    # verify the update made it into Elasticsearch
    doc = es.get(index="people", id="p-1")
    assert doc["_source"]["name"] == "Jane"

    # delete the data in Postgres
    psycopg.connect(autocommit=True).execute("delete from public.person where id = 'p-1'")
    runner.iterate()
    # verify the doc got deleted from Elasticsearch
    with pytest.raises(elasticsearch.NotFoundError):
        es.get(index="people", id="p-1")
