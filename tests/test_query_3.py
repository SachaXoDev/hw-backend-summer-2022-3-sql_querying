from sqlalchemy import Engine, text

from sql_queries import TASK_3_QUERY


async def test_query_3(engine: Engine) -> None:
    query = """
    SELECT COUNT(f.flight_id) as count
    FROM flights f
    JOIN airports_data a_dep ON f.departure_airport = a_dep.airport_code
    JOIN airports_data a_arr ON f.arrival_airport = a_arr.airport_code
    WHERE a_dep.timezone = a_arr.timezone;
    """
    #  count
    # --------
    #  16824
    async with engine.connect() as conn:
        res = await conn.execute(text(query))

    assert res.scalar() == 16824
