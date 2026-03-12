"""Meter model - NMI/meter level information."""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from shared.db.models.bill import Bill
    from shared.db.models.site import Site


class Meter(SQLModel, table=True):
    __tablename__ = "meters"

    id: int | None = Field(default=None, primary_key=True)  # Auto-increment PK
    neptune_id: str | None = Field(
        default=None, max_length=100, sa_column_kwargs={"unique": False}
    )  # Neptune vertex ID (populated later)
    site_id: int = Field(foreign_key="sites.id")  # FK → sites
    identifier: str = Field(max_length=50, sa_column_kwargs={"unique": True})  # Business ID, e.g. NMI "4103815184"
    identifier_type: str = Field(max_length=20)  # e.g. "NMI"
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )

    site: Optional["Site"] = Relationship(back_populates="meters")
    bills: list["Bill"] = Relationship(back_populates="meter")
