"""Database models package."""

from shared.db.models.bill import Bill
from shared.db.models.meter import Meter
from shared.db.models.site import Site

# Rebuild models to resolve forward references across files
Site.model_rebuild()
Meter.model_rebuild()
Bill.model_rebuild()

__all__ = ["Bill", "Meter", "Site"]
