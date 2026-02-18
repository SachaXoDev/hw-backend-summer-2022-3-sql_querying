import datetime

from sqlalchemy import Engine, text

from sql_queries import TASK_1_QUERY


async def test_query_1(engine: Engine) -> None:
    query = """
    SELECT flight_no, ( scheduled_arrival - scheduled_departure) as duration
    FROM flights
    ORDER BY duration ASC
    LIMIT 5;
    """
    #  flight_no | duration
    # -----------+----------
    #  PG0235    | 00:25:00
    #  PG0234    | 00:25:00
    #  PG0233    | 00:25:00
    #  PG0235    | 00:25:00
    #  PG0234    | 00:25:00
    async with engine.connect() as conn:
        res = await conn.execute(text(query))

    assert res.all() == [
        ("PG0235", datetime.timedelta(seconds=1500)),
        ("PG0234", datetime.timedelta(seconds=1500)),
        ("PG0233", datetime.timedelta(seconds=1500)),
        ("PG0235", datetime.timedelta(seconds=1500)),
        ("PG0234", datetime.timedelta(seconds=1500)),
    ]
