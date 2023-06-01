from sinker.utils import generate_schema_tables


def test_generate_schema_tables():
    view_select_query = """select id,
       json_build_object(
               'name', "name",
               'emailDomains',(select array_agg(split_part(email, '@', 2)) FROM unnest(emails) as email),
               ) as "person"
        from "person" 
        """
    assert list(generate_schema_tables(view_select_query)) == ["person"]
