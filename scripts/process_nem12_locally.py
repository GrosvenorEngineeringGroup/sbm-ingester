#!/usr/bin/env python3
"""
Local NEM12 file processor.

Processes large NEM12 files locally and uploads results to S3 data lake.
Use this for files that are too large for Lambda processing.

Usage:
    uv run scripts/process_nem12_locally.py <nem12_file> [--dry-run]

Example:
    uv run scripts/process_nem12_locally.py /path/to/5MINNEM12MDFF.csv --dry-run
    uv run scripts/process_nem12_locally.py /path/to/5MINNEM12MDFF.csv
"""

import argparse
import sys
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import boto3

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from shared.nem_adapter import output_as_data_frames

# Constants
OUTPUT_BUCKET = "hudibucketsrc"
OUTPUT_PREFIX = "sensorDataFiles"
MAPPINGS_BUCKET = "sbm-file-ingester"
MAPPINGS_KEY = "nem12_mappings.json"
NMI_DATA_STREAM_COMBINED = ["E1", "E2", "B1", "Q1", "K1"]
AWS_PROFILE = "geg"


def load_nem12_mappings(s3_client: boto3.client) -> dict[str, str]:
    """Load NEM12 to Neptune ID mappings from S3."""
    print(f"Loading mappings from s3://{MAPPINGS_BUCKET}/{MAPPINGS_KEY}...")
    response = s3_client.get_object(Bucket=MAPPINGS_BUCKET, Key=MAPPINGS_KEY)
    import json

    mappings = json.loads(response["Body"].read().decode("utf-8"))
    print(f"  Loaded {len(mappings)} mappings")
    return mappings


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def process_nem12_file(
    file_path: str,
    nem12_mappings: dict[str, str],
    s3_client: boto3.client,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict:
    """Process NEM12 file and upload results to S3."""
    stats = {
        "nmis_total": 0,
        "nmis_mapped": 0,
        "nmis_unmapped": 0,
        "monitor_points": 0,
        "readings_total": 0,
        "files_uploaded": 0,
        "unmapped_nmis": [],
    }

    print(f"\nParsing NEM12 file: {file_path}")
    parse_start = time.time()
    dfs = output_as_data_frames(file_path, split_days=True)
    parse_duration = time.time() - parse_start
    total_nmis = len(dfs)
    print(f"  Found {total_nmis} NMI entries (parsed in {format_duration(parse_duration)})")

    # Track unique NMIs
    seen_nmis = set()
    batch_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    process_start = time.time()

    for idx, (nmi, df) in enumerate(dfs, 1):
        seen_nmis.add(nmi)

        # Progress indicator
        elapsed = time.time() - process_start
        if idx > 1:
            eta = (elapsed / (idx - 1)) * (total_nmis - idx + 1)
            eta_str = f", ETA: {format_duration(eta)}"
        else:
            eta_str = ""

        if verbose:
            print(f"\n[{idx}/{total_nmis}] Processing NMI: {nmi}{eta_str}")

        # Reset index if t_start is the index
        if "t_start" not in df.columns and df.index.name == "t_start":
            df = df.reset_index()

        channels_processed = []
        channels_unmapped = []

        for col in df.columns:
            suffix = col.split("_")[0]
            if suffix not in NMI_DATA_STREAM_COMBINED:
                continue

            monitor_point_name = f"{nmi}-{suffix}"
            neptune_id = nem12_mappings.get(monitor_point_name)

            if neptune_id is None:
                channels_unmapped.append(suffix)
                if nmi not in [n for n, _ in stats["unmapped_nmis"]]:
                    stats["unmapped_nmis"].append((nmi, monitor_point_name))
                continue

            # Extract unit from column name
            unit_name = col.split("_")[1].lower() if "_" in col else "kwh"

            # Build output DataFrame
            output_df = df[["t_start", col]].copy()
            output_df["sensorId"] = neptune_id
            output_df["unit"] = unit_name
            output_df = output_df.rename(columns={"t_start": "ts", col: "val"})
            output_df["its"] = output_df["ts"]
            output_df = output_df[["sensorId", "ts", "val", "unit", "its"]]

            # Format timestamps
            output_df["ts"] = output_df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
            output_df["its"] = output_df["its"].dt.strftime("%Y-%m-%d %H:%M:%S")

            stats["monitor_points"] += 1
            stats["readings_total"] += len(output_df)

            # Upload to S3
            s3_key = f"{OUTPUT_PREFIX}/{neptune_id}_{batch_timestamp}.csv"

            if dry_run:
                channels_processed.append(f"{suffix}({len(output_df)} rows)")
            else:
                csv_buffer = StringIO()
                output_df.to_csv(csv_buffer, index=False)
                s3_client.put_object(
                    Bucket=OUTPUT_BUCKET,
                    Key=s3_key,
                    Body=csv_buffer.getvalue(),
                )
                stats["files_uploaded"] += 1
                channels_processed.append(f"{suffix}({len(output_df)} rows)")

        # Print channel summary for this NMI
        if verbose:
            if channels_processed:
                print(f"  ✓ Channels: {', '.join(channels_processed)}")
            if channels_unmapped:
                print(f"  ✗ Unmapped: {', '.join(channels_unmapped)}")

    # Final timing
    total_duration = time.time() - process_start
    print(f"\nProcessing completed in {format_duration(total_duration)}")

    stats["nmis_total"] = len(seen_nmis)
    stats["nmis_mapped"] = stats["nmis_total"] - len({n for n, _ in stats["unmapped_nmis"]})
    stats["nmis_unmapped"] = len({n for n, _ in stats["unmapped_nmis"]})

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Process NEM12 file locally and upload to S3")
    parser.add_argument("file", help="Path to NEM12 file")
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("-q", "--quiet", action="store_true", help="Minimal output (no per-NMI progress)")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    print("=" * 60)
    print("Local NEM12 Processor")
    print(f"File: {file_path}")
    print(f"Dry run: {args.dry_run}")
    print(f"Verbose: {not args.quiet}")
    print("=" * 60)

    # Initialize S3 client
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client("s3")

    # Load mappings
    nem12_mappings = load_nem12_mappings(s3_client)

    # Process file
    stats = process_nem12_file(str(file_path), nem12_mappings, s3_client, dry_run=args.dry_run, verbose=not args.quiet)

    # Print summary
    print("\n" + "=" * 60)
    print("Processing Summary")
    print("=" * 60)
    print(f"Total NMIs:           {stats['nmis_total']}")
    print(f"Mapped NMIs:          {stats['nmis_mapped']}")
    print(f"Unmapped NMIs:        {stats['nmis_unmapped']}")
    print(f"Monitor Points:       {stats['monitor_points']}")
    print(f"Total Readings:       {stats['readings_total']:,}")
    print(f"Files Uploaded:       {stats['files_uploaded']}")

    if stats["unmapped_nmis"]:
        print(f"\nUnmapped NMIs ({len(stats['unmapped_nmis'])} total):")
        for _nmi, mp in stats["unmapped_nmis"][:10]:
            print(f"  - {mp}")
        if len(stats["unmapped_nmis"]) > 10:
            print(f"  ... and {len(stats['unmapped_nmis']) - 10} more")

    print("=" * 60)


if __name__ == "__main__":
    main()
