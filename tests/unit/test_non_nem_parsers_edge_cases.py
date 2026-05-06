"""Dispatcher compatibility edge-case tests for standard non-NEM parser outcomes."""

from shared.parsers import NotRelevantParser, ParserOutcome


def test_processed_empty_outcome_stops_dispatch(tmp_path, monkeypatch) -> None:
    from shared.non_nem_parsers import get_non_nem_outcome

    calls: list[str] = []

    def first_parser(file_name: str, error_file_path: str) -> ParserOutcome:
        calls.append("first")
        raise NotRelevantParser("not mine")

    def empty_parser(file_name: str, error_file_path: str) -> ParserOutcome:
        calls.append("empty")
        return ParserOutcome(status="processed_empty", reason="all_zero_valid")

    def later_parser(file_name: str, error_file_path: str) -> ParserOutcome:
        calls.append("later")
        raise AssertionError("dispatcher should stop after processed_empty")

    monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, empty_parser, later_parser])

    result = get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

    assert calls == ["first", "empty"]
    assert result.status == "processed_empty"
    assert result.reason == "all_zero_valid"


def test_legacy_get_non_nem_df_returns_empty_list_for_processed_empty(tmp_path, monkeypatch) -> None:
    from shared.non_nem_parsers import get_non_nem_df

    def parser(file_name: str, error_file_path: str) -> ParserOutcome:
        return ParserOutcome(status="processed_empty", reason="no_valid_point_rows")

    monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

    result = get_non_nem_df(str(tmp_path / "file.csv"), "error_log")

    assert result == []
