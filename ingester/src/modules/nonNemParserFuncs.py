from pathlib import Path

import boto3
import pandas as pd

from modules.common import CloudWatchLogger

parse_error_log = CloudWatchLogger("sbm-ingester-parse-error-log")

# Type alias for parser return type
ParserResult = list[tuple[str, pd.DataFrame]]

# ---------------------- Parsers ---------------------- #


def enviziVerticalParserWater(fileName: str, errorFilePath: str) -> ParserResult:
    if "OptimaGenerationData" in fileName:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(fileName)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        bufDF = raw_df.loc[
            raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "Consumption", "Consumption Unit"]
        ]

        unitCount = bufDF["Consumption Unit"].nunique()
        if unitCount != 1:
            parse_error_log.log(
                f"enviziVerticalParserWater: {fileName} - File has meter with multiple units: {unitCount}"
            )

        unit = bufDF["Consumption Unit"].iloc[0]
        bufDF = bufDF[["Interval_Start", "Consumption"]].rename(
            columns={"Interval_Start": "t_start", "Consumption": f"E1_{unit}"}
        )
        bufDF = bufDF.set_index("t_start")
        dfs.append((f"Envizi_{name}", bufDF))

    return dfs


def enviziVerticalParserWaterBulk(fileName: str, errorFilePath: str) -> ParserResult:
    if "OptimaGenerationData" in fileName:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(fileName)
    raw_df["Date_Time"] = pd.to_datetime(raw_df["Date_Time"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        bufDF = raw_df.loc[raw_df["Serial_No"] == name, ["Date_Time", "kL"]]
        bufDF = bufDF.rename(columns={"Date_Time": "t_start", "kL": "E1_kL"})
        bufDF = bufDF.set_index("t_start")
        dfs.append((f"Envizi_{name}", bufDF))

    return dfs


def enviziVerticalParserElectricity(fileName: str, errorFilePath: str) -> ParserResult:
    if "OptimaGenerationData" in fileName:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(fileName)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        bufDF = raw_df.loc[raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "kWh"]]
        bufDF = bufDF.rename(columns={"Interval_Start": "t_start", "kWh": "E1_kWh"})
        bufDF = bufDF.set_index("t_start")
        dfs.append((f"Envizi_{name}", bufDF))

    return dfs


def optimaUsageAndSpendToS3(fileName: str, errorFilePath: str) -> ParserResult:
    if "OptimaGenerationData" in fileName:
        raise Exception("Not Relevant Parser For File")

    if "RACV-Usage and Spend Report" not in fileName:
        raise Exception("Not Valid Optima Usage And Spend File")

    # boto3 will use IAM role or env vars — no hardcoding creds
    s3 = boto3.client("s3")
    S3_BUCKET = "gegoptimareports"
    S3_KEY = "usageAndSpendReports/racvUsageAndSpend.csv"

    with Path(fileName).open("rb") as file:
        file_data = file.read()

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=file_data)
    return []


def racvElecParser(fileName: str, errorFilePath: str) -> ParserResult:
    if "OptimaGenerationData" in fileName:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(fileName, skiprows=[0, 1])
    cols = [x for x in raw_df.columns if "kWh" in x or x in ["Date", "Start Time"]]
    meterCols = [x for x in cols if "kWh" in x]

    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])

    dfs: ParserResult = []
    for mn in meterCols:
        bufDF = raw_df[["Interval_Start", mn]].rename(columns={"Interval_Start": "t_start", mn: "E1_kWh"})
        bufDF = bufDF.set_index("t_start")

        # Daily aggregation to filter out invalid days
        daily_sum = bufDF.resample("D").sum(numeric_only=True)
        non_zero_dates = daily_sum[daily_sum["E1_kWh"] != 0].index
        bufDF = bufDF[bufDF.index.normalize().isin(non_zero_dates)]

        if not non_zero_dates.empty:
            dfs.append((f"Optima_{mn.split(' ')[0]}", bufDF))

    if dfs:
        return dfs
    raise Exception(f"No Valid Data in file: {fileName}")


def optimaGenerationDataParser(fileName: str, errorFilePath: str) -> ParserResult:
    raw_df = pd.read_csv(fileName)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
    raw_df["Identifier"] = raw_df["Identifier"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Identifier"].unique()):
        bufDF = raw_df.loc[raw_df["Identifier"] == name, ["Interval_Start", "Generation"]]
        bufDF = bufDF.rename(columns={"Interval_Start": "t_start", "Generation": "B1_kWh"})
        bufDF = bufDF.set_index("t_start")
        dfs.append((f"Optima_{name}", bufDF))

    return dfs


def greenSquarePrivateWireSchneiderComXParser(fileName: str, errorFilePath: str) -> ParserResult:
    first_rows = pd.read_csv(fileName, header=None, nrows=2)
    if first_rows.iloc[1, 0] != "ComX510_Green_Square":
        raise Exception("Not Relevant Parser For File")

    siteName = first_rows.iloc[1, 4].replace(" ", "")
    raw_df = pd.read_csv(fileName, header=6, skip_blank_lines=False)

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

    bufDF = raw_df[["Local Time Stamp", energy_col]].rename(
        columns={"Local Time Stamp": "t_start", energy_col: "E1_kWh"}
    )
    bufDF = bufDF.set_index("t_start")

    return [(f"GPWComX_{siteName}", bufDF)]


# ---------------------- Dispatcher ---------------------- #


def nonNemParsersGetDf(fileName: str, errorFilePath: str) -> ParserResult:
    parsers = [
        enviziVerticalParserWater,
        enviziVerticalParserElectricity,
        racvElecParser,
        optimaUsageAndSpendToS3,
        optimaGenerationDataParser,
        enviziVerticalParserWaterBulk,
        greenSquarePrivateWireSchneiderComXParser,
    ]

    for parser in parsers:
        try:
            return parser(fileName, errorFilePath)
        except Exception as e:
            parse_error_log.log(f"Parser {parser.__name__} failed for file {fileName}: {e}")

    # If no parser succeeded, log the error and raise an exception
    parse_error_log.log(f"nonNemParsersGetDf: {fileName}: No Valid Parser Found")
    raise Exception(f"nonNemParsersGetDf: {fileName}: No Valid Parser Found")
