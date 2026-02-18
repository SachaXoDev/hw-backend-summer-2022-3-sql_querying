import os
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
import yaml
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine


@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "demo"
    user: str | None = None
    password: str | None = None


@pytest.fixture
def config() -> DatabaseConfig:
    config_path = os.environ.get("CONFIGPATH", "./tests/config.yml")
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    return DatabaseConfig(**raw_config["database"])


@pytest.fixture
async def engine(config: DatabaseConfig) -> AsyncIterator[AsyncEngine]:
    """Create async database engine."""
    engine = create_async_engine(
        URL.create(
            drivername="postgresql+asyncpg",
            host=config.host,
            database=config.database,
            username=config.user,
            password=config.password,
            port=config.port,
        ),
        echo=True,
    )

    yield engine

    await engine.dispose()
