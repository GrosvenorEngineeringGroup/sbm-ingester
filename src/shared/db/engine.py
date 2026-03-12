"""Database engine and session management."""

import os

from sqlalchemy import Engine, text
from sqlmodel import Session, SQLModel, create_engine


def get_engine() -> Engine:
    """Create a SQLAlchemy engine from DATABASE_URL environment variable."""
    url = os.environ["DATABASE_URL"]
    return create_engine(url, echo=False)


def get_session() -> Session:
    """Return a new database session."""
    return Session(get_engine())


def create_all_tables() -> None:
    """Create all tables defined in SQLModel metadata."""
    from shared.db.models import Bill, Meter, Site  # noqa: F401

    engine = get_engine()

    # Create partial unique indexes and composite unique constraints
    # that SQLModel doesn't natively support
    SQLModel.metadata.create_all(engine)

    with engine.begin() as conn:
        # Partial unique index on sites.building_id (WHERE NOT NULL)
        conn.execute(
            text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_sites_building_id
                ON sites (building_id) WHERE building_id IS NOT NULL
            """)
        )
        # Partial unique index on sites.neptune_id (WHERE NOT NULL)
        conn.execute(
            text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_sites_neptune_id
                ON sites (neptune_id) WHERE neptune_id IS NOT NULL
            """)
        )
        # Partial unique index on meters.neptune_id (WHERE NOT NULL)
        conn.execute(
            text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_meters_neptune_id
                ON meters (neptune_id) WHERE neptune_id IS NOT NULL
            """)
        )
        # Performance index on bills.bill_date (meter_id already covered by PK prefix)
        conn.execute(
            text("""
                CREATE INDEX IF NOT EXISTS idx_bills_bill_date
                ON bills (bill_date)
            """)
        )

    print("All tables and indexes created successfully.")


def print_schema() -> None:
    """Print the DDL for all tables (does not execute against database)."""
    from sqlalchemy.schema import CreateIndex, CreateTable

    from shared.db.models import Bill, Meter, Site

    for model in [Site, Meter, Bill]:
        table = model.__table__
        print(CreateTable(table).compile().string)
        for index in table.indexes:
            print(CreateIndex(index).compile().string)
        print()

    # Print additional partial indexes that are created manually
    print("-- Additional partial unique indexes (created in create_all_tables):")
    print(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_sites_building_id ON sites (building_id) WHERE building_id IS NOT NULL;"
    )
    print("CREATE UNIQUE INDEX IF NOT EXISTS idx_sites_neptune_id ON sites (neptune_id) WHERE neptune_id IS NOT NULL;")
    print(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_meters_neptune_id ON meters (neptune_id) WHERE neptune_id IS NOT NULL;"
    )
    print("CREATE INDEX IF NOT EXISTS idx_bills_bill_date ON bills (bill_date);")
