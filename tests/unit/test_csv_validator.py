import pytest

from app.services.csv_validator import CsvValidator
from tests.conftest import csv_bytes, make_rows


@pytest.fixture
def validator() -> CsvValidator:
    return CsvValidator(max_rows=20, max_bytes=4096)


def codes(result, *, warnings: bool = False) -> list[str]:  # type: ignore[no-untyped-def]
    issues = result.report.warnings if warnings else result.report.errors
    return [i.code for i in issues]


def test_valid_file_parses_and_normalises(validator: CsvValidator) -> None:
    result = validator.validate(
        csv_bytes('  General Hospital ,"12  Main St,\n Springfield", 555-0100', "Clinic,1 Road,")
    )
    assert result.report.valid
    assert result.report.total_rows == result.report.valid_rows == 2
    first, second = result.rows
    assert (first.name, first.address, first.phone) == (
        "General Hospital",
        "12 Main St, Springfield",
        "555-0100",
    )
    assert second.phone is None  # empty optional field becomes None


def test_header_is_case_and_whitespace_insensitive_and_bom_tolerant(
    validator: CsvValidator,
) -> None:
    content = "﻿ Name , ADDRESS ,Phone\r\nA,B,555-1234\r\n".encode()
    result = validator.validate(content)
    assert result.report.valid, result.report.errors
    assert result.rows[0].name == "A"


def test_phone_column_is_optional(validator: CsvValidator) -> None:
    result = validator.validate(csv_bytes("A,1 Main St", header="name,address"))
    assert result.report.valid
    assert result.rows[0].phone is None


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"", "empty_file"),
        (b"  \n\n ", "empty_file"),
        (b"name,address\n", "no_data_rows"),
        (b"\xff\xfe\x00n\x00a", "invalid_encoding"),
        (b"name,address\nA\x00,B\n", "binary_content"),
        (b"name,phone\nA,555-0100\n", "missing_column"),
        (b"hospital,address\nA,B\n", "unknown_column"),
        (b"name,address,name\nA,B,C\n", "duplicate_column"),
    ],
)
def test_file_level_errors(validator: CsvValidator, content: bytes, expected: str) -> None:
    result = validator.validate(content)
    assert not result.report.valid
    assert expected in codes(result)
    assert result.file_errors


def test_semicolon_separated_file_gets_a_hint(validator: CsvValidator) -> None:
    result = validator.validate(b"name;address;phone\nA;B;C\n")
    assert "comma-separated" in result.report.errors[0].message


def test_header_less_file_reports_missing_columns(validator: CsvValidator) -> None:
    result = validator.validate(b"General Hospital,1 Main St,555-0100\n")
    assert {"unknown_column", "missing_column"} <= set(codes(result))


def test_too_many_rows(validator: CsvValidator) -> None:
    result = validator.validate(csv_bytes(*make_rows(21)))
    assert codes(result) == ["too_many_rows"]
    assert result.report.total_rows == 21


def test_exactly_max_rows_is_ok(validator: CsvValidator) -> None:
    assert validator.validate(csv_bytes(*make_rows(20))).report.valid


def test_file_too_large_short_circuits() -> None:
    small = CsvValidator(max_rows=20, max_bytes=1024)
    result = small.validate(csv_bytes(*make_rows(200)))
    assert codes(result) == ["file_too_large"]


@pytest.mark.parametrize(
    ("filename", "content_type", "expected"),
    [
        ("hospitals.xlsx", "text/csv", "invalid_file_extension"),
        ("hospitals.csv", "application/pdf", "invalid_content_type"),
        ("hospitals.csv", "image/png", "invalid_content_type"),
    ],
)
def test_rejects_non_csv_uploads(
    validator: CsvValidator, filename: str, content_type: str, expected: str
) -> None:
    result = validator.validate(
        csv_bytes("A,B,555-0100"), filename=filename, content_type=content_type
    )
    assert expected in codes(result)


@pytest.mark.parametrize(
    "content_type",
    ["text/csv", "text/csv; charset=utf-8", "application/vnd.ms-excel", "application/octet-stream"],
)
def test_accepts_real_world_csv_content_types(validator: CsvValidator, content_type: str) -> None:
    result = validator.validate(
        csv_bytes("A,B,555-0100"), filename="UPPER.CSV", content_type=content_type
    )
    assert result.report.valid


def test_row_level_errors_are_reported_per_row(validator: CsvValidator) -> None:
    long_name = "X" * 201
    result = validator.validate(
        csv_bytes(
            "Good,1 Main St,555-0100",
            ",2 Main St,",
            "No Address,,",
            "Bad Phone,3 Main St,call me",
            f"{long_name},4 Main St,",
            "Extra,5 Main St,555-0105,surprise",
            "Short Phone,6 Main St,12",
        )
    )
    report = result.report
    assert not report.valid
    assert (report.total_rows, report.valid_rows, report.invalid_rows) == (7, 1, 6)
    by_row = {e.row: e.code for e in report.errors}
    assert by_row == {
        2: "missing_required_field",
        3: "missing_required_field",
        4: "invalid_phone",
        5: "value_too_long",
        6: "unexpected_value",
        7: "invalid_phone",
    }
    assert not result.file_errors
    assert [r.row for r in result.valid_rows] == [1]
    assert [p.valid for p in report.rows] == [True] + [False] * 6


def test_blank_lines_are_ignored_with_warning_and_row_numbers_stay_dense(
    validator: CsvValidator,
) -> None:
    result = validator.validate(b"name,address\n\nA,1 St\n , \nB,2 St\n")
    assert result.report.valid
    assert [(r.row, r.line) for r in result.rows] == [(1, 3), (2, 5)]
    assert codes(result, warnings=True) == ["blank_lines_ignored"]


def test_duplicate_rows_warn_but_stay_valid(validator: CsvValidator) -> None:
    result = validator.validate(csv_bytes("A,1 Main St,555-0100", "a , 1 MAIN st,555-0100"))
    assert result.report.valid
    [warning] = result.report.warnings
    assert (warning.code, warning.row) == ("duplicate_row", 2)


def test_unnamed_trailing_column_is_ignored(validator: CsvValidator) -> None:
    result = validator.validate(b"name,address,phone,\nA,B,555-0100,\n")
    assert result.report.valid
    assert "unnamed_column_ignored" in codes(result, warnings=True)


@pytest.mark.parametrize("phone", ["+1 (555) 010-0100", "555.010.0100", "555-0100 x12", "5550100"])
def test_accepts_common_phone_formats(validator: CsvValidator, phone: str) -> None:
    assert validator.validate(csv_bytes(f"A,B,{phone}")).report.valid


def test_malformed_csv_is_reported() -> None:
    # The stdlib parser raises csv.Error for fields beyond its 128 KiB field limit.
    validator = CsvValidator(max_rows=20, max_bytes=512 * 1024)
    result = validator.validate(b'name,address\n"' + b"x" * 140_000 + b'",B\n')
    assert codes(result) == ["malformed_csv"]
    assert result.report.errors[0].line == 2
