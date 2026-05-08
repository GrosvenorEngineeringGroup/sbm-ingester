"""SourceFile: an S3 object reference identifying one input file.

Used as the ``data_keyword_argument`` for Powertools idempotency in
``functions.file_processor.pipeline.ingest_file``. Powertools natively
supports plain dataclasses as idempotency-key payloads — its
``_prepare_data`` (in ``aws_lambda_powertools/utilities/idempotency/base.py``)
detects ``__dataclass_fields__`` and calls ``dataclasses.asdict(data)``, which
works on ``frozen=True, slots=True`` instances because ``asdict`` iterates
``__dataclass_fields__`` rather than ``__dict__``. No custom serializer is
needed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceFile:
    """An S3 object reference identifying one input file."""

    bucket: str
    key: str
