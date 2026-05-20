"""Database engine and session management utilities.

All request handlers receive sessions from this module. Keeping session
construction here ensures consistent transaction boundaries and connection
pool behavior across the service layer.
"""

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

settings = get_settings()

engine = create_engine(
    settings.sqlalchemy_url,
    pool_pre_ping=True,
    pool_recycle=3600,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session for one request lifecycle."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
