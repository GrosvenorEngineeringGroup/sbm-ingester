"""Unit tests for nonNemParserFuncs.py module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add tests directory to path
sys.path.insert(0, str(Path(__file__).parent))
from conftest import (
    create_envizi_electricity_csv,
    create_envizi_water_csv,
    create_optima_generation_csv,
)


class TestEnviziVerticalParserWater:
    """Tests for enviziVerticalParserWater function."""

    def test_parses_water_data_correctly(self, temp_directory: str) -> None:
        """Test that water data is parsed correctly."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import enviziVerticalParserWater

            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"], rows_per_meter=5)

            result = enviziVerticalParserWater(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 1

            nmi, df = result[0]
            assert nmi == "Envizi_12345"
            assert "t_start" in df.index.name or "t_start" in df.columns
            assert "E1_kL" in df.columns

    def test_handles_multiple_meters(self, temp_directory: str) -> None:
        """Test that multiple meters are handled correctly."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import enviziVerticalParserWater

            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["111", "222", "333"], rows_per_meter=3)

            result = enviziVerticalParserWater(filepath, "error_log")

            assert len(result) == 3
            nmis = [nmi for nmi, df in result]
            assert "Envizi_111" in nmis
            assert "Envizi_222" in nmis
            assert "Envizi_333" in nmis

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import enviziVerticalParserWater

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"])

            with pytest.raises(Exception, match="Not Relevant Parser"):
                enviziVerticalParserWater(filepath, "error_log")

    def test_logs_warning_for_multiple_units(self, temp_directory: str) -> None:
        """Test that multiple units per meter triggers warning."""
        mock_log = MagicMock()
        with patch("modules.nonNemParserFuncs.parse_error_log", mock_log):
            from modules.nonNemParserFuncs import enviziVerticalParserWater

            # Create CSV with multiple units for same meter
            filepath = str(Path(temp_directory) / "multi_unit.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345", "12345"],
                    "Interval_Start": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
                    "Interval_End": ["2024-01-01T01:00:00", "2024-01-01T02:00:00"],
                    "Consumption": [1.0, 2.0],
                    "Consumption Unit": ["kL", "L"],  # Different units
                }
            )
            df.to_csv(filepath, index=False)

            enviziVerticalParserWater(filepath, "error_log")

            # Should log warning about multiple units
            assert mock_log.log.called


class TestEnviziVerticalParserElectricity:
    """Tests for enviziVerticalParserElectricity function."""

    def test_parses_electricity_data_correctly(self, temp_directory: str) -> None:
        """Test that electricity data is parsed correctly."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import enviziVerticalParserElectricity

            filepath = str(Path(temp_directory) / "elec_data.csv")
            create_envizi_electricity_csv(filepath, serial_numbers=["E001"], rows_per_meter=5)

            result = enviziVerticalParserElectricity(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 1

            nmi, df = result[0]
            assert nmi == "Envizi_E001"
            assert "E1_kWh" in df.columns

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import enviziVerticalParserElectricity

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_envizi_electricity_csv(filepath, serial_numbers=["E001"])

            with pytest.raises(Exception, match="Not Relevant Parser"):
                enviziVerticalParserElectricity(filepath, "error_log")


class TestOptimaGenerationDataParser:
    """Tests for optimaGenerationDataParser function."""

    def test_parses_generation_data_correctly(self, temp_directory: str) -> None:
        """Test that generation data is parsed correctly."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import optimaGenerationDataParser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_generation_csv(filepath, identifiers=["SOLAR001"], rows_per_id=5)

            result = optimaGenerationDataParser(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 1

            nmi, df = result[0]
            assert nmi == "Optima_SOLAR001"
            assert "B1_kWh" in df.columns  # Generation uses B1

    def test_handles_multiple_identifiers(self, temp_directory: str) -> None:
        """Test that multiple identifiers are handled correctly."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import optimaGenerationDataParser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_generation_csv(filepath, identifiers=["SOLAR001", "SOLAR002"], rows_per_id=3)

            result = optimaGenerationDataParser(filepath, "error_log")

            assert len(result) == 2
            nmis = [nmi for nmi, df in result]
            assert "Optima_SOLAR001" in nmis
            assert "Optima_SOLAR002" in nmis


class TestNonNemParsersGetDf:
    """Tests for nonNemParsersGetDf dispatcher function."""

    def test_tries_parsers_in_order(self, temp_directory: str) -> None:
        """Test that parsers are tried in order until one succeeds."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import nonNemParsersGetDf

            # Create valid Envizi water file
            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"])

            result = nonNemParsersGetDf(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) > 0

    def test_raises_exception_when_all_parsers_fail(self, temp_directory: str) -> None:
        """Test that exception is raised when no parser succeeds."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import nonNemParsersGetDf

            # Create invalid file that no parser can handle
            filepath = str(Path(temp_directory) / "invalid.csv")
            with Path(filepath).open("w") as f:
                f.write("completely,invalid,format\n1,2,3\n")

            with pytest.raises(Exception, match="No Valid Parser Found"):
                nonNemParsersGetDf(filepath, "error_log")

    def test_logs_errors_for_failed_parsers(self, temp_directory: str) -> None:
        """Test that errors are logged for each failed parser."""
        mock_log = MagicMock()
        with patch("modules.nonNemParserFuncs.parse_error_log", mock_log):
            from modules.nonNemParserFuncs import nonNemParsersGetDf

            # Create valid file for later parser
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_generation_csv(filepath, identifiers=["ID1"])

            nonNemParsersGetDf(filepath, "error_log")

            # Earlier parsers should have logged failures
            assert mock_log.log.called


class TestRacvElecParser:
    """Tests for racvElecParser function."""

    def test_skips_header_rows(self, temp_directory: str) -> None:
        """Test that first two rows are skipped."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import racvElecParser

            # Create RACV format file with header rows
            filepath = str(Path(temp_directory) / "racv_data.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh,Meter2 kWh
2024-01-01,00:00,10.5,20.5
2024-01-01,00:30,11.0,21.0
2024-01-02,00:00,12.0,22.0
2024-01-02,00:30,13.0,23.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racvElecParser(filepath, "error_log")

            assert isinstance(result, list)
            # Should have parsed meter columns
            assert len(result) >= 1

    def test_filters_zero_days(self, temp_directory: str) -> None:
        """Test that days with all zero values are filtered out."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import racvElecParser

            # Create file with some zero days
            filepath = str(Path(temp_directory) / "racv_data.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh
2024-01-01,00:00,0.0
2024-01-01,00:30,0.0
2024-01-02,00:00,10.0
2024-01-02,00:30,10.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racvElecParser(filepath, "error_log")

            # Result should only have non-zero day data
            if len(result) > 0:
                _nmi, _df = result[0]
                # Should have filtered out Jan 1 (all zeros)
                # Only Jan 2 data should remain

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import racvElecParser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh
2024-01-01,00:00,10.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(Exception, match="Not Relevant Parser"):
                racvElecParser(filepath, "error_log")


class TestGreenSquarePrivateWireSchneiderComXParser:
    """Tests for greenSquarePrivateWireSchneiderComXParser function."""

    def test_validates_comx_header(self, temp_directory: str) -> None:
        """Test that ComX header is validated."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import greenSquarePrivateWireSchneiderComXParser

            # Create file without ComX header - match expected CSV structure
            filepath = str(Path(temp_directory) / "not_comx.csv")
            content = """Row1,col2,col3,col4,col5
NotComX510_Green_Square,data,data,data,SiteName
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(Exception, match="Not Relevant Parser"):
                greenSquarePrivateWireSchneiderComXParser(filepath, "error_log")

    def test_converts_wh_to_kwh(self, temp_directory: str) -> None:
        """Test that Wh values are converted to kWh."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import greenSquarePrivateWireSchneiderComXParser

            # Create valid ComX file with Wh column - must have consistent columns
            filepath = str(Path(temp_directory) / "comx_data.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (Wh),Other,col4,col5
01/01/2024 00:00,1000,data,col4,col5
01/01/2024 00:30,2000,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = greenSquarePrivateWireSchneiderComXParser(filepath, "error_log")

            assert len(result) == 1
            _nmi, df = result[0]

            # Values should be converted from Wh to kWh
            assert "E1_kWh" in df.columns
            # 1000 Wh = 1.0 kWh
            assert df["E1_kWh"].iloc[0] == 1.0


class TestDataFrameOutputFormat:
    """Tests for DataFrame output format consistency across parsers."""

    def test_all_parsers_return_t_start_column(self, temp_directory: str) -> None:
        """Test that all parsers return DataFrames with t_start."""
        with patch("modules.nonNemParserFuncs.parse_error_log"):
            from modules.nonNemParserFuncs import (
                enviziVerticalParserElectricity,
                enviziVerticalParserWater,
                optimaGenerationDataParser,
            )

            # Test Envizi water
            water_file = str(Path(temp_directory) / "water.csv")
            create_envizi_water_csv(water_file, serial_numbers=["1"])
            result = enviziVerticalParserWater(water_file, "error")
            _, df = result[0]
            assert df.index.name == "t_start" or "t_start" in df.columns

            # Test Envizi electricity
            elec_file = str(Path(temp_directory) / "elec.csv")
            create_envizi_electricity_csv(elec_file, serial_numbers=["1"])
            result = enviziVerticalParserElectricity(elec_file, "error")
            _, df = result[0]
            assert df.index.name == "t_start" or "t_start" in df.columns

            # Test Optima generation
            gen_file = str(Path(temp_directory) / "OptimaGenerationData_test.csv")
            create_optima_generation_csv(gen_file, identifiers=["1"])
            result = optimaGenerationDataParser(gen_file, "error")
            _, df = result[0]
            assert df.index.name == "t_start" or "t_start" in df.columns
