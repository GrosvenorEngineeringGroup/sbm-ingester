"""Bill model - monthly usage and spend data."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from shared.db.models.meter import Meter


class Bill(SQLModel, table=True):
    __tablename__ = "bills"

    id: int | None = Field(default=None, primary_key=True)
    meter_id: str = Field(max_length=50, foreign_key="meters.identifier")
    bill_date: date
    retailer: str | None = Field(default=None, max_length=100)

    # Actual Usage
    peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    off_peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    shoulder_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    # Estimated Usage
    estimated_peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_off_peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_shoulder_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_estimated_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    usage_unit: str = Field(default="kWh", max_length=20)

    # Actual Spend
    energy_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    network_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    environmental_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    metering_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    other_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_spend: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    # Estimated Spend
    estimated_energy_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_network_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_environmental_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_metering_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_other_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_estimated_spend: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    spend_currency: str = Field(default="AUD", max_length=10)

    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )

    meter: Meter | None = Relationship(back_populates="bills")
