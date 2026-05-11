"""Dispatcher for non-NEM file parsers."""

from aws_lambda_powertools import Logger

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ParserResult,
    ProcessingError,
)
from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk
from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser
from shared.parsers.optima.bunnings_billing import bunnings_billing_parser
from shared.parsers.optima.demand import demand_parser
from shared.parsers.optima.interval import interval_parser
from shared.parsers.optima.racv_billing import racv_billing_parser
from shared.parsers.racv.elec import racv_elec_parser
from shared.parsers.racv.noosa_solar import noosa_solar_parser

logger = Logger(service="non-nem-parsers", child=True)

PARSERS = [
    noosa_solar_parser,
    envizi_vertical_parser_water,
    envizi_vertical_parser_electricity,
    racv_elec_parser,
    racv_billing_parser,
    bunnings_billing_parser,
    demand_parser,
    interval_parser,
    envizi_vertical_parser_water_bulk,
    green_square_private_wire_schneider_comx_parser,
]


def _as_outcome(result: ParserOutcome | ParserResult) -> ParserOutcome:
    if isinstance(result, ParserOutcome):
        return result
    return ParserOutcome(status="processed", dataframes=result)


def dispatch_non_nem(file_name: str) -> ParserOutcome:
    for parser in PARSERS:
        try:
            return _as_outcome(parser(file_name))
        except NotRelevantParser as e:
            logger.debug(
                "Parser not relevant",
                extra={"parser": parser.__name__, "file": file_name, "error": str(e)},
            )
        except (ParserError, ProcessingError):
            raise
        except Exception as e:
            logger.exception(
                "Unexpected parser failure",
                extra={"parser": parser.__name__, "file": file_name, "error": str(e)},
            )
            raise ParserError(f"Unexpected parser failure in {parser.__name__}: {e}") from e

    logger.error("No valid parser found", extra={"file": file_name})
    raise ParserError(f"dispatch_non_nem: {file_name}: No Valid Parser Found")


def get_non_nem_df(file_name: str) -> ParserResult:
    return dispatch_non_nem(file_name).dataframes
