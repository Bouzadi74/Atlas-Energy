from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


def build_engine(database_url: str) -> Engine:
    """Create the application database engine without opening a connection eagerly."""

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create short-lived, transaction-scoped SQLAlchemy sessions."""

    return sessionmaker(bind=engine, expire_on_commit=False)

