"""Shared fixtures for parsers/optima tests."""

import pytest


@pytest.fixture
def write_demand_csv(tmp_path):
    """Factory fixture: write a synthetic Demand Profile CSV, return path."""

    def _write(filename="Bunnings_Demand_Profile.csv", rows=None, body_override=None):
        csv_path = tmp_path / filename
        if body_override is not None:
            csv_path.write_text(body_override)
            return csv_path

        rows = (
            rows
            if rows is not None
            else [
                ("4001260599", "01-Feb-2026 00:00:00", "5.24", "10.48", "10.48", "1.0000"),
                ("4001260599", "01-Feb-2026 00:30:00", "5.21", "10.42", "10.42", "1.0000"),
                ("4001260599", "01-Feb-2026 05:30:00", "29.56", "59.12", "67.18", "0.8800"),
            ]
        )
        body_lines = [
            'Commodities:,"Electricity"',
            'Sites (NMIs):,"4001260599"',
            'Status:,"Active"',
            "Country:, Australia",
            "Start:,01-Feb-2026",
            "End:,30-Apr-2026",
            "",
            "",
            "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name",
        ]
        for nmi, ts, e, kw, kva, pf in rows:
            body_lines.append(f"Bunnings Australia,{nmi},NMI,{ts},{e},{kw},{kva},{pf},BUN AUS Forbes")
        csv_path.write_text("\n".join(body_lines))
        return csv_path

    return _write
