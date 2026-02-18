from sqlalchemy import Engine, text

from sql_queries import TASK_2_QUERY


async def test_query_2(engine: Engine) -> None:
    query = """
    SELECT flight_no, COUNT(flight_no) as count
    FROM flights
    GROUP BY flight_no
    HAVING COUNT(flight_no) < 50
    ORDER BY count DESC
    LIMIT 3;
    """
    #  flight_no | count
    # -----------+-------
    #  PG0260    |    27
    #  PG0371    |    27
    #  PG0310    |    27
    async with engine.connect() as conn:
        res = await conn.execute(text(query))

    assert res.all() == [
        ("PG0260", 27),
        ("PG0371", 27),
        ("PG0310", 27),
    ]
