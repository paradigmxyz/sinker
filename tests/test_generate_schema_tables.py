from sinker.utils import generate_schema_tables


def test_generate_schema_tables():
    view_select_query = """select id,
       json_build_object(
               'name', "name",
               'otherEmailDomains',(select array_agg(split_part(email, '@', 2)) FROM unnest(emails) as email),
               'emailDomains', (select array_agg(split_part(value, '@', 2))
                    from "EmailAddress" EA where "personId"="Person".id),
               'emailAddresses', (select array_agg(value) from "EmailAddress" EA where "personId"="Person".id),
               ) as "person"
        from "person"
        """
    assert list(generate_schema_tables(view_select_query)) == ["EmailAddress", "person"]
