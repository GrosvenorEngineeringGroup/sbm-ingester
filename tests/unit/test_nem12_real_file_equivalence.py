"""Test streaming vs batch parser equivalence using real NEM12 files.

This test verifies that the streaming parser produces IDENTICAL final output
files as the batch parser - same number of files, same content, same order.

These tests are marked as slow and skipped by default.
Run with: pytest -m slow
"""

from pathlib import Path

import pandas as pd
import pytest

# Mark entire module as slow - skipped by default
pytestmark = pytest.mark.slow

# Fixture directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Real NEM12 test files
REAL_NEM12_FILES = [
    FIXTURES_DIR / "nem12_real_5min_large.csv",  # 79MB, 2025-08-01 to 2026-01-04
    FIXTURES_DIR / "nem12_real_5min_2023.csv",  # 48MB, 2023-09-01 to 2024-01-01
]

# NMI data stream suffixes (from app.py)
NMI_DATA_STREAM_SUFFIX = [
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "J",
    "K",
    "L",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "G",
    "H",
    "Y",
    "M",
    "W",
    "V",
    "Z",
]
NMI_DATA_STREAM_CHANNEL = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
]
NMI_DATA_STREAM_COMBINED = frozenset(i + j for i in NMI_DATA_STREAM_SUFFIX for j in NMI_DATA_STREAM_CHANNEL)


def get_available_real_files():
    """Get list of available real NEM12 files."""
    return [f for f in REAL_NEM12_FILES if f.exists()]


def simulate_file_processing(dfs_iterable, nem12_mappings: dict[str, str]) -> list[pd.DataFrame]:
    """
    Simulate the file processing logic from app.py.

    This replicates the exact transformation done in parse_and_write_data():
    - For each NMI/suffix, create output DataFrame with sensorId, ts, val, unit, its
    - Only process columns that match NMI_DATA_STREAM_COMBINED
    - Only include data with valid Neptune ID mappings

    Args:
        dfs_iterable: Iterator of (nmi, df) tuples from parser
        nem12_mappings: Dict mapping "NMI-suffix" to Neptune IDs

    Returns:
        List of output DataFrames (what would be written to S3)
    """
    output_dfs = []

    for nmi, df in dfs_iterable:
        # Reset index if t_start is the index
        if "t_start" not in df.columns and df.index.name == "t_start":
            df = df.reset_index()

        for col in df.columns:
            suffix = col.split("_")[0]
            if suffix not in NMI_DATA_STREAM_COMBINED:
                continue

            monitor_point_name = f"{nmi}-{suffix}"
            neptune_id = nem12_mappings.get(monitor_point_name)

            if neptune_id is None:
                continue

            # Extract unit from column name
            unit_name = col.split("_")[1].lower() if "_" in col else "kwh"

            # Build output DataFrame
            output_df = df[["t_start", col]].rename(columns={"t_start": "ts", col: "val"})
            output_df = output_df.copy()  # Avoid SettingWithCopyWarning
            output_df["sensorId"] = neptune_id
            output_df["unit"] = unit_name
            output_df["its"] = output_df["ts"]
            output_df = output_df[["sensorId", "ts", "val", "unit", "its"]]

            # Format timestamps
            output_df["ts"] = output_df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
            output_df["its"] = output_df["its"].dt.strftime("%Y-%m-%d %H:%M:%S")

            output_dfs.append(output_df)

    return output_dfs


def create_mock_mappings(dfs_iterable) -> dict[str, str]:
    """
    Create mock NEM12 mappings for all NMI-suffix combinations in the file.

    This ensures we process ALL data (not skip due to missing mappings).
    """
    mappings = {}

    for nmi, df in dfs_iterable:
        for col in df.columns:
            suffix = col.split("_")[0]
            if suffix in NMI_DATA_STREAM_COMBINED:
                key = f"{nmi}-{suffix}"
                if key not in mappings:
                    mappings[key] = f"neptune-{key}"

    return mappings


@pytest.fixture(scope="module", params=get_available_real_files(), ids=lambda p: p.name)
def real_nem12_file(request):
    """Parametrized fixture for real NEM12 files."""
    return str(request.param)


@pytest.fixture(scope="module")
def nem12_mappings(real_nem12_file):
    """Create mappings by scanning the file first."""
    from shared.nem_adapter import output_as_data_frames

    # Use batch parser to create mappings (need full data to get all NMI-suffix combos)
    dfs = output_as_data_frames(real_nem12_file, split_days=True)
    return create_mock_mappings(dfs)


@pytest.fixture(scope="module")
def batch_outputs(real_nem12_file, nem12_mappings):
    """Process file using batch parser and return output DataFrames."""
    from shared.nem_adapter import output_as_data_frames

    dfs = output_as_data_frames(real_nem12_file, split_days=True)
    return simulate_file_processing(dfs, nem12_mappings)


@pytest.fixture(scope="module")
def stream_outputs(real_nem12_file, nem12_mappings):
    """Process file using streaming parser and return output DataFrames."""
    from shared.nem_adapter import stream_as_data_frames

    dfs = stream_as_data_frames(real_nem12_file, split_days=True)
    return simulate_file_processing(dfs, nem12_mappings)


@pytest.mark.skipif(
    not get_available_real_files(),
    reason="No real NEM12 files found in fixtures directory",
)
class TestFinalOutputEquivalence:
    """Test that streaming and batch parsers produce identical final output files."""

    def test_same_number_of_output_dataframes(self, batch_outputs, stream_outputs) -> None:
        """Test that both parsers produce same number of output DataFrames."""
        assert len(batch_outputs) == len(stream_outputs), (
            f"Output count mismatch: batch={len(batch_outputs)}, stream={len(stream_outputs)}"
        )
        print(f"\nBoth parsers produced {len(batch_outputs)} output DataFrames")

    def test_same_total_rows(self, batch_outputs, stream_outputs) -> None:
        """Test that total row count is identical."""
        batch_rows = sum(len(df) for df in batch_outputs)
        stream_rows = sum(len(df) for df in stream_outputs)

        assert batch_rows == stream_rows, f"Total row count mismatch: batch={batch_rows}, stream={stream_rows}"
        print(f"\nBoth parsers produced {batch_rows} total rows")

    def test_same_sensor_ids(self, batch_outputs, stream_outputs) -> None:
        """Test that same sensorIds are produced."""
        batch_sensors = set()
        stream_sensors = set()

        for df in batch_outputs:
            batch_sensors.update(df["sensorId"].unique())
        for df in stream_outputs:
            stream_sensors.update(df["sensorId"].unique())

        assert batch_sensors == stream_sensors, (
            f"SensorId mismatch:\n"
            f"Only in batch: {batch_sensors - stream_sensors}\n"
            f"Only in stream: {stream_sensors - batch_sensors}"
        )
        print(f"\nBoth parsers produced {len(batch_sensors)} unique sensorIds")

    def test_each_output_dataframe_identical(self, batch_outputs, stream_outputs) -> None:
        """Test that each output DataFrame is identical (in order)."""
        assert len(batch_outputs) == len(stream_outputs)

        mismatches = []

        for i, (batch_df, stream_df) in enumerate(zip(batch_outputs, stream_outputs)):
            try:
                pd.testing.assert_frame_equal(
                    batch_df.reset_index(drop=True),
                    stream_df.reset_index(drop=True),
                    check_exact=False,
                    rtol=1e-10,
                )
            except AssertionError as e:
                mismatches.append(f"DataFrame {i}: {str(e)[:200]}")

        if mismatches:
            pytest.fail(
                f"Found {len(mismatches)} mismatched DataFrames:\n"
                + "\n".join(mismatches[:5])
                + (f"\n... and {len(mismatches) - 5} more" if len(mismatches) > 5 else "")
            )

    def test_concatenated_output_identical(self, batch_outputs, stream_outputs) -> None:
        """Test that concatenated output (simulating S3 batch write) is identical."""
        if not batch_outputs or not stream_outputs:
            pytest.skip("No output DataFrames to compare")

        batch_merged = pd.concat(batch_outputs, ignore_index=True)
        stream_merged = pd.concat(stream_outputs, ignore_index=True)

        # Sort by sensorId and ts for deterministic comparison
        batch_sorted = batch_merged.sort_values(["sensorId", "ts"]).reset_index(drop=True)
        stream_sorted = stream_merged.sort_values(["sensorId", "ts"]).reset_index(drop=True)

        pd.testing.assert_frame_equal(
            batch_sorted,
            stream_sorted,
            check_exact=False,
            rtol=1e-10,
        )

        print(f"\nMerged output: {len(batch_merged)} rows, {batch_merged['sensorId'].nunique()} sensors")

    def test_csv_output_identical(self, batch_outputs, stream_outputs, tmp_path) -> None:
        """Test that CSV file output is byte-for-byte identical."""
        if not batch_outputs or not stream_outputs:
            pytest.skip("No output DataFrames to compare")

        # Merge and sort for deterministic output
        batch_merged = pd.concat(batch_outputs, ignore_index=True)
        stream_merged = pd.concat(stream_outputs, ignore_index=True)

        batch_sorted = batch_merged.sort_values(["sensorId", "ts"]).reset_index(drop=True)
        stream_sorted = stream_merged.sort_values(["sensorId", "ts"]).reset_index(drop=True)

        # Write to CSV
        batch_csv = tmp_path / "batch_output.csv"
        stream_csv = tmp_path / "stream_output.csv"

        batch_sorted.to_csv(batch_csv, index=False)
        stream_sorted.to_csv(stream_csv, index=False)

        # Compare file contents
        batch_content = batch_csv.read_text()
        stream_content = stream_csv.read_text()

        assert batch_content == stream_content, (
            f"CSV content differs!\n"
            f"Batch file size: {len(batch_content)} bytes\n"
            f"Stream file size: {len(stream_content)} bytes"
        )

        print(f"\nCSV output identical: {len(batch_content)} bytes")

    def test_per_sensor_data_identical(self, batch_outputs, stream_outputs) -> None:
        """Test that data for each sensor is identical."""
        if not batch_outputs or not stream_outputs:
            pytest.skip("No output DataFrames to compare")

        batch_merged = pd.concat(batch_outputs, ignore_index=True)
        stream_merged = pd.concat(stream_outputs, ignore_index=True)

        batch_by_sensor = {
            sensor: df.sort_values("ts").reset_index(drop=True) for sensor, df in batch_merged.groupby("sensorId")
        }
        stream_by_sensor = {
            sensor: df.sort_values("ts").reset_index(drop=True) for sensor, df in stream_merged.groupby("sensorId")
        }

        assert set(batch_by_sensor.keys()) == set(stream_by_sensor.keys())

        for sensor in batch_by_sensor:
            pd.testing.assert_frame_equal(
                batch_by_sensor[sensor],
                stream_by_sensor[sensor],
                check_exact=False,
                rtol=1e-10,
            )


@pytest.mark.skipif(
    not get_available_real_files(),
    reason="No real NEM12 files found in fixtures directory",
)
class TestIntermediateDataEquivalence:
    """Test intermediate parsing results for debugging."""

    def test_same_nmis_parsed(self, real_nem12_file) -> None:
        """Test that both parsers find the same NMIs."""
        from shared.nem_adapter import output_as_data_frames, stream_as_data_frames

        batch_dfs = output_as_data_frames(real_nem12_file, split_days=True)
        stream_dfs = list(stream_as_data_frames(real_nem12_file, split_days=True))

        batch_nmis = sorted(set(nmi for nmi, df in batch_dfs))
        stream_nmis = sorted(set(nmi for nmi, df in stream_dfs))

        assert batch_nmis == stream_nmis, (
            f"NMI mismatch:\n"
            f"Only in batch: {set(batch_nmis) - set(stream_nmis)}\n"
            f"Only in stream: {set(stream_nmis) - set(batch_nmis)}"
        )

    def test_same_columns_per_nmi(self, real_nem12_file) -> None:
        """Test that both parsers produce same columns for each NMI."""
        from shared.nem_adapter import output_as_data_frames, stream_as_data_frames

        batch_dfs = {nmi: df for nmi, df in output_as_data_frames(real_nem12_file, split_days=True)}
        stream_dfs = {nmi: df for nmi, df in stream_as_data_frames(real_nem12_file, split_days=True)}

        for nmi in batch_dfs:
            assert nmi in stream_dfs, f"NMI {nmi} missing from stream output"
            batch_cols = set(batch_dfs[nmi].columns)
            stream_cols = set(stream_dfs[nmi].columns)

            assert batch_cols == stream_cols, (
                f"Column mismatch for NMI {nmi}:\n"
                f"Only in batch: {batch_cols - stream_cols}\n"
                f"Only in stream: {stream_cols - batch_cols}"
            )

    def test_same_row_count_per_nmi(self, real_nem12_file) -> None:
        """Test that both parsers produce same row count for each NMI."""
        from shared.nem_adapter import output_as_data_frames, stream_as_data_frames

        batch_dfs = {nmi: df for nmi, df in output_as_data_frames(real_nem12_file, split_days=True)}
        stream_dfs = {nmi: df for nmi, df in stream_as_data_frames(real_nem12_file, split_days=True)}

        for nmi in batch_dfs:
            batch_rows = len(batch_dfs[nmi])
            stream_rows = len(stream_dfs[nmi])

            assert batch_rows == stream_rows, (
                f"Row count mismatch for NMI {nmi}: batch={batch_rows}, stream={stream_rows}"
            )
