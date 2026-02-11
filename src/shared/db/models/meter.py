"""Meter model - NMI/meter level information."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from shared.db.models.bill import Bill
    from shared.db.models.site import Site


class Meter(SQLModel, table=True):
    __tablename__ = "meters"

    identifier: str = Field(max_length=50, primary_key=True)
    neptune_id: str | None = Field(default=None, max_length=100, sa_column_kwargs={"unique": False})
    site_id: int = Field(foreign_key="sites.id")
    identifier_type: str = Field(max_length=20)
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )

    site: Site | None = Relationship(back_populates="meters")
    bills: list[Bill] = Relationship(back_populates="meter")
