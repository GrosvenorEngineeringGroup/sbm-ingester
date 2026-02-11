from pathlib import Path

import boto3
import pandas as pd
from aws_lambda_powertools import Logger

logger = Logger(service="non-nem-parsers", child=True)

# Type alias for parser return type
ParserResult = list[tuple[str, pd.DataFrame]]

# ---------------------- Parsers ---------------------- #


def envizi_vertical_parser_water(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[
            raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "Consumption", "Consumption Unit"]
        ]

        unit_count = buf_df["Consumption Unit"].nunique()
        if unit_count != 1:
            logger.error(
                "envizi_vertical_parser_water: Multiple units", extra={"file": file_name, "unit_count": unit_count}
            )

        unit = buf_df["Consumption Unit"].iloc[0]
        buf_df = buf_df[["Interval_Start", "Consumption"]].rename(
            columns={"Interval_Start": "t_start", "Consumption": f"E1_{unit}"}
        )
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs


def envizi_vertical_parser_water_bulk(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Date_Time"] = pd.to_datetime(raw_df["Date_Time"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Date_Time", "kL"]]
        buf_df = buf_df.rename(columns={"Date_Time": "t_start", "kL": "E1_kL"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs


def envizi_vertical_parser_electricity(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "kWh"]]
        buf_df = buf_df.rename(columns={"Interval_Start": "t_start", "kWh": "E1_kWh"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs


def optima_usage_and_spend_to_s3(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    if "RACV-Usage and Spend Report" not in file_name:
        raise Exception("Not Valid Optima Usage And Spend File")

    # boto3 will use IAM role or env vars — no hardcoding creds
    s3 = boto3.client("s3")
    S3_BUCKET = "gegoptimareports"
    S3_KEY = "usageAndSpendReports/racvUsageAndSpend.csv"

    with Path(file_name).open("rb") as file:
        file_data = file.read()

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=file_data)
    return []


def racv_elec_parser(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name, skiprows=[0, 1])
    cols = [x for x in raw_df.columns if "kWh" in x or x in ["Date", "Start Time"]]
    meter_cols = [x for x in cols if "kWh" in x]

    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])

    dfs: ParserResult = []
    for mn in meter_cols:
        buf_df = raw_df[["Interval_Start", mn]].rename(columns={"Interval_Start": "t_start", mn: "E1_kWh"})
        buf_df = buf_df.set_index("t_start")

        # Daily aggregation to filter out invalid days
        daily_sum = buf_df.resample("D").sum(numeric_only=True)
        non_zero_dates = daily_sum[daily_sum["E1_kWh"] != 0].index
        buf_df = buf_df[buf_df.index.normalize().isin(non_zero_dates)]

        if not non_zero_dates.empty:
            dfs.append((f"Optima_{mn.split(' ')[0]}", buf_df))

    if dfs:
        return dfs
    raise Exception(f"No Valid Data in file: {file_name}")


def optima_parser(file_name: str, error_file_path: str) -> ParserResult:
    raw_df = pd.read_csv(file_name)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
    raw_df["Identifier"] = raw_df["Identifier"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Identifier"].unique()):
        base_df = raw_df.loc[raw_df["Identifier"] == name].copy()

        # Build output DataFrame with t_start as index
        output_df = base_df[["Interval_Start"]].copy()
        output_df = output_df.rename(columns={"Interval_Start": "t_start"})

        # Add Usage column as E1_kWh if present
        if "Usage" in raw_df.columns:
            output_df["E1_kWh"] = base_df["Usage"].values

        # Add Generation column as B1_kWh if present
        if "Generation" in raw_df.columns:
            output_df["B1_kWh"] = base_df["Generation"].values

        output_df = output_df.set_index("t_start")
        dfs.append((f"Optima_{name}", output_df))

    return dfs


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
        envizi_vertical_parser_water,
        envizi_vertical_parser_electricity,
        racv_elec_parser,
        optima_usage_and_spend_to_s3,
        optima_parser,
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
