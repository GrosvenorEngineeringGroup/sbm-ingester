"""Dispatcher compatibility tests for standard non-NEM parser outcome gates."""

from unittest.mock import patch


def _write_demand_no_data_csv(path) -> None:
    path.write_text(
        "\n".join(
            [
                'Commodities:,"Electricity"',
                'Sites (NMIs):,"0000005438UN02B"',
                'Status:,"Active"',
                "Country:, New Zealand",
                "Start:,01-May-2026",
                "End:,03-May-2026",
                "",
                "",
                "No data found",
            ]
        )
    )


def test_demand_no_data_file_routes_past_broad_envizi_gates(tmp_path) -> None:
    from shared.non_nem_parsers import get_non_nem_outcome

    path = tmp_path / "Bunnings_Demand_Profile.csv"
    _write_demand_no_data_csv(path)

    with patch("shared.non_nem_parsers.logger") as logger:
        result = get_non_nem_outcome(str(path))

    attempted_parsers = [call.kwargs["extra"]["parser"] for call in logger.debug.call_args_list]
    assert "envizi_vertical_parser_electricity" in attempted_parsers
    assert result.status == "processed_empty"
    assert result.reason == "no_data_sentinel"


def test_legacy_get_non_nem_df_unwraps_routed_empty_outcome(tmp_path) -> None:
    from shared.non_nem_parsers import get_non_nem_df

    path = tmp_path / "Bunnings_Demand_Profile.csv"
    _write_demand_no_data_csv(path)

    result = get_non_nem_df(str(path))

    assert not result
