"""Site model - building/location level information."""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from shared.db.models.meter import Meter


class Site(SQLModel, table=True):
    __tablename__ = "sites"

    id: int | None = Field(default=None, primary_key=True)  # Auto-increment PK
    neptune_id: str | None = Field(
        default=None, max_length=100, sa_column_kwargs={"unique": False}
    )  # Neptune vertex ID (populated later)
    name: str = Field(max_length=200)  # e.g. "BUN AUS Alexandria"
    address: str | None = Field(default=None, max_length=300)  # e.g. "8-40 Euston Road"
    building_id: str | None = Field(
        default=None, max_length=50
    )  # Client-specific reference, e.g. BidEnergy "Site Reference 3"
    client_id: str | None = Field(default=None, max_length=100)  # External system ID (populated later)
    country: str = Field(max_length=10)  # e.g. "AU"
    state: str | None = Field(default=None, max_length=20)  # e.g. "AU:NSW"
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )

    meters: list["Meter"] = Relationship(back_populates="site")
