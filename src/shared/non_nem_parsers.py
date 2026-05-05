import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk
from shared.parsers.optima.bunnings_billing import bunnings_billing_parser
from shared.parsers.optima.interval import interval_parser
from shared.parsers.optima.racv_billing import racv_billing_parser
from shared.parsers.racv.elec import racv_elec_parser
from shared.parsers.racv.noosa_solar import noosa_solar_parser

logger = Logger(service="non-nem-parsers", child=True)

# Type alias for parser return type
ParserResult = list[tuple[str, pd.DataFrame]]

# ---------------------- Parsers ---------------------- #


def green_square_private_wire_schneider_comx_parser(file_name: str, error_file_path: str) -> ParserResult:
    first_rows = pd.read_csv(file_name, header=None, nrows=2)
    if first_rows.iloc[1, 0] != "ComX510_Green_Square":
        raise Exception("Not Relevant Parser For File")

    site_name = first_rows.iloc[1, 4].replace(" ", "")
    raw_df = pd.read_csv(file_name, header=6, skip_blank_lines=False)

    if "Active energy (Wh)" in raw_df.columns:
        raw_df = raw_df[pd.to_numeric(raw_df["Active energy (Wh)"], errors="coerce").notnull()]
        raw_df["Active energy (Wh)"] = raw_df["Active energy (Wh)"].astype(float) / 1000
        energy_col = "Active energy (Wh)"
    elif "Active energy (kWh)" in raw_df.columns:
        raw_df = raw_df[pd.to_numeric(raw_df["Active energy (kWh)"], errors="coerce").notnull()]
        raw_df["Active energy (kWh)"] = raw_df["Active energy (kWh)"].astype(float)
        energy_col = "Active energy (kWh)"
    else:
        raise Exception("Missing Active energy column in file.")

    raw_df["Local Time Stamp"] = pd.to_datetime(raw_df["Local Time Stamp"], dayfirst=True)

    buf_df = raw_df[["Local Time Stamp", energy_col]].rename(
        columns={"Local Time Stamp": "t_start", energy_col: "E1_kWh"}
    )
    buf_df = buf_df.set_index("t_start")

    return [(f"GPWComX_{site_name}", buf_df)]


# ---------------------- Dispatcher ---------------------- #


def get_non_nem_df(file_name: str, error_file_path: str) -> ParserResult:
    parsers = [
        noosa_solar_parser,  # Must be first — checks filename, fast rejection
        envizi_vertical_parser_water,
        envizi_vertical_parser_electricity,
        racv_elec_parser,
        racv_billing_parser,  # RACV — unchanged
        bunnings_billing_parser,  # NEW — Bunnings billing
        interval_parser,
        envizi_vertical_parser_water_bulk,
        green_square_private_wire_schneider_comx_parser,
    ]

    for parser in parsers:
        try:
            return parser(file_name, error_file_path)
        except Exception as e:
            logger.debug("Parser failed", extra={"parser": parser.__name__, "file": file_name, "error": str(e)})

    # If no parser succeeded, log the error and raise an exception
    logger.error("No valid parser found", extra={"file": file_name})
    raise Exception(f"get_non_nem_df: {file_name}: No Valid Parser Found")
