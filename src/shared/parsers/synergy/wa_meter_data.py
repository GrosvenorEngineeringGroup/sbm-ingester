"""Synergy WA "meter data" archiver / sentinel handler.

External producer: Synergy's WA portal drops files into newTBP/ with names
``Meter_Data_WA (AU)_Electricity_<epoch>_<timestamp>.csv``. The current
production payload is a 56-byte sentinel CSV indicating "no data found" for
the queried period; the file is classified as ``processed_empty`` and moved
to newP/ (with no rows written to the Hudi data lake).

Real-data files have not been observed in production. If Synergy starts
emitting them, the strict header match in this parser will raise
``NotRelevantParser``; dispatch will exhaust all remaining parsers and
raise ``ParserError("No Valid Parser Found")``, which ``ingest_file``
catches as ``status="parse_failed"`` and routes the file to newParseErr/.
That accumulation (and the resulting ParseErrorFiles alarm) is the signal
to add real-data parsing logic here.

Drift is therefore loud-by-design once it occurs at any non-trivial rate.
This is acceptable because the Synergy WA producer is well-bounded; a
small number of drift events (one or two during a producer change) will
not meaningfully impact the alarm baseline.
"""

from __future__ import annotations

from pathlib import Path

from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserOutcome

logger = Logger(service="synergy-wa-meter-data-parser", child=True)

FILENAME_PREFIX = "Meter_Data_WA (AU)_Electricity_"
SENTINEL_HEADER = "Unnamed: 0,NMI,Unnamed: 2"


def synergy_wa_meter_data_parser(file_name: str) -> ParserOutcome:
    path = Path(file_name)
    if not path.name.startswith(FILENAME_PREFIX):
        raise NotRelevantParser("Not a Synergy WA meter data file")

    try:
        with path.open(encoding="utf-8-sig") as f:
            first_line = f.readline().strip()
    except (OSError, UnicodeDecodeError) as e:
        raise NotRelevantParser(f"Synergy WA file not readable as text: {e}") from e

    if first_line != SENTINEL_HEADER:
        raise NotRelevantParser(f"Synergy WA file format drifted. First line: {first_line!r}")

    logger.info(
        "synergy_wa_no_data_sentinel",
        extra={"file": file_name},
    )
    return ParserOutcome(status="processed_empty", reason="no_data_sentinel")
