"""Bill model - monthly usage and spend data."""

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from shared.db.models.meter import Meter


class Bill(SQLModel, table=True):
    __tablename__ = "bills"

    meter_id: int = Field(foreign_key="meters.id", primary_key=True)  # FK → meters, part of composite PK
    bill_date: date = Field(primary_key=True)  # Bill date, part of composite PK
    retailer: str | None = Field(default=None, max_length=100)  # e.g. "SnowyEnergy"

    # Actual Usage (kWh)
    peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    off_peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    shoulder_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_greenpower_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    # Estimated Usage (kWh)
    estimated_peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_off_peak_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_shoulder_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_estimated_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_estimated_greenpower_usage: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    usage_unit: str = Field(default="kWh", max_length=20)  # Unit for usage fields

    # Actual Spend (AUD)
    energy_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    network_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    environmental_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    metering_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    other_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_spend: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    greenpower_spend: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    # Estimated Spend (AUD)
    estimated_energy_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_network_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_environmental_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_metering_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    estimated_other_charge: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)
    total_estimated_spend: Decimal = Field(default=Decimal("0"), max_digits=14, decimal_places=2)

    spend_currency: str = Field(default="AUD", max_length=10)  # Currency for spend fields

    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"server_default": "now()"},
    )

    meter: Optional["Meter"] = Relationship(back_populates="bills")
