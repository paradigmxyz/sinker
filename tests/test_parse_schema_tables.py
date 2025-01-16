from sinker.utils import parse_schema_tables


def test_parse_schema_tables():
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
    parent_table, schema_tables = parse_schema_tables(view_select_query)
    assert parent_table == "person"
    assert schema_tables == {"EmailAddress", "person"}


def test_parse_schema_tables_with_cte():
    view_select_query = """
        WITH
        attendees AS (
            SELECT DISTINCT ON (a."personId", a."hostedEventId")
                a."hostedEventId",
                a.status,
                e.value as email,
                p."primaryOrganizationId"
            FROM "HostedEventAttendance" a
            JOIN "Person" p ON a."personId" = p.id
            JOIN "EmailAddress" e ON p.id = e."personId"
            GROUP BY
                a."personId",
                a."hostedEventId",
                a.status,
                e.value,
                p."primaryOrganizationId"
        )
        SELECT
            id,
            json_build_object(
                'summary', "name",
                'startTime', "timestamp",
                'attendees', (
                    SELECT json_agg(json_build_object('email', attendees.email, 'eventResponse', attendees.status))
                        AS formatted_attendees
                    FROM attendees
                    WHERE attendees."hostedEventId" = "HostedEvent".id
                ),
                'organizationIds',
                (
                    SELECT array_agg(attendees."primaryOrganizationId")
                    FROM attendees
                    WHERE attendees."hostedEventId" = "HostedEvent".id
                )
            ) AS "hosted_events"
        FROM
            "HostedEvent"
    """
    parent_table, schema_tables = parse_schema_tables(view_select_query)
    assert parent_table == "HostedEvent"
    assert schema_tables == {"EmailAddress", "HostedEvent", "HostedEventAttendance", "Person"}


def test_parse_schema_tables_handles_blank_table_name():
    view_select_query = """
        select id,
               json_build_object(
                       'summary', "summary",
                       'startTime', "start_time",
                       'organizerEmail', "organizerEmail",
                       'attendees', (select json_agg(json_build_object('email', key, 'eventResponse', value))
                            as formatted_attendees
                                     from (select id, key, value
                                           from "googleEvents",
                                               jsonb_each_text(attendees) as kv(key, value)) as subquery
                                     where id = "googleEvents".id),
                       'organizationIds', (select array_agg("_NotesToOrganization"."B")
                                           from "_NotesToOrganization"
                                                    left join public."Notes" N on "_NotesToOrganization"."A" = N.id
                                           where "googleEventId" = "googleEvents".id)
               ) as "google_events"
        from "googleEvents";
    """
    parent_table, schema_tables = parse_schema_tables(view_select_query)
    assert parent_table == "googleEvents"
    # parsed.find_all(Table) returns a Table object with a blank '' name
    assert schema_tables == {"googleEvents", "_NotesToOrganization", "Notes"}


def test_error_handling_on_query_with_no_table():
    view_select_query = """select 1"""
    try:
        parse_schema_tables(view_select_query)
    except ValueError as e:
        assert str(e) == "No table found in the query"
    else:
        assert False, "Expected ValueError"
