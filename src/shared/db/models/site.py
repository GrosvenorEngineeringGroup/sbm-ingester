"""Site model - building/location level information."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from shared.db.models.meter import Meter


class Site(SQLModel, table=True):
    __tablename__ = "sites"

    id: int | None = Field(default=None, primary_key=True)
    neptune_id: str | None = Field(default=None, max_length=100, sa_column_kwargs={"unique": False})
    name: str = Field(max_length=200)
    address: str | None = Field(default=None, max_length=300)
    building_id: str | None = Field(default=None, max_length=50)
    client_id: str | None = Field(default=None, max_length=100)
    country: str = Field(max_length=10)
    state: str | None = Field(default=None, max_length=20)
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )

    meters: list[Meter] = Relationship(back_populates="site")
