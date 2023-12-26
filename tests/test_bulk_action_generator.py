import pytest

from sinker.bulk_action_generator import BulkActionGenerator


@pytest.fixture
def bulk_action_generator() -> BulkActionGenerator:
    v2i = dict(foo_mv="foo_index")
    pt2i = dict(foo_table="foo_index")
    return BulkActionGenerator(
        views_to_indices=v2i,
        parent_tables_to_indices=pt2i)


def test_index_action(bulk_action_generator: BulkActionGenerator) -> None:
    doc = '{"name" : "Foo Bar"}'
    slot_dict = dict(table="foo_mv", id="a-1")
    expected = {"_index": "foo_index", "_id": "a-1", "_source": doc}
    assert bulk_action_generator.index_action(doc, slot_dict) == expected


def test_delete_action(bulk_action_generator: BulkActionGenerator) -> None:
    slot_dict = dict(table="foo_table", id="a-1")
    expected = {"_op_type": "delete", "_index": "foo_index", "_id": "a-1"}
    assert bulk_action_generator.delete_action(slot_dict) == expected
