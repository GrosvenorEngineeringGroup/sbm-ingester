"""Non-NEM file parsers, organised by source platform."""

from __future__ import annotations

import pandas as pd

ParserResult = list[tuple[str, pd.DataFrame]]
