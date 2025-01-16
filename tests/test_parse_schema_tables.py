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
