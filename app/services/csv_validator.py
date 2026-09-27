"""CSV parsing and validation. Pure (no I/O); the same code backs `/validate` and `/bulk`.

Issues are classified as:
* file-level errors (row = None): the file cannot be processed at all (bad headers, too big...);
* row-level errors: that row cannot be sent upstream;
* warnings: processable but suspicious (duplicates, ignored blank lines...).
"""

import csv
import io
import re
from dataclasses import dataclass, field

from app.models.schemas import RowPreview, ValidationIssue, ValidationReport

REQUIRED_COLUMNS = ("name", "address")
OPTIONAL_COLUMNS = ("phone",)
KNOWN_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
EXPECTED_HEADER = ",".join(KNOWN_COLUMNS)

MAX_LENGTHS = {"name": 200, "address": 500, "phone": 40}

# Browsers and curl label CSV uploads inconsistently: Windows sends `application/vnd.ms-excel`,
# curl sends `application/octet-stream` for unknown extensions. Accept those; reject types that
# are clearly something else (images, PDFs, JSON, spreadsheets...).
ALLOWED_CONTENT_TYPES = frozenset(
    {
        "text/csv",
        "application/csv",
        "text/x-csv",
        "application/x-csv",
        "text/comma-separated-values",
        "text/plain",
        "application/vnd.ms-excel",
        "application/octet-stream",
    }
)

_PHONE_RE = re.compile(r"^\+?[0-9().\-\s/]+(\s*(x|ext\.?|#)\s*\d+)?$", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ParsedRow:
    row: int
    line: int
    name: str
    address: str
    phone: str | None
    errors: tuple[ValidationIssue, ...] = ()

    @property
    def is_valid(self) -> bool:
        return not self.errors


@dataclass
class CsvValidationResult:
    report: ValidationReport
    rows: list[ParsedRow] = field(default_factory=list)

    @property
    def file_errors(self) -> list[ValidationIssue]:
        return [e for e in self.report.errors if e.row is None]

    @property
    def valid_rows(self) -> list[ParsedRow]:
        return [r for r in self.rows if r.is_valid]


def _normalise(value: str | None) -> str:
    """Trim and collapse internal whitespace (incl. newlines inside quoted cells)."""
    return _WHITESPACE_RE.sub(" ", value or "").strip()


class CsvValidator:
    def __init__(self, *, max_rows: int, max_bytes: int) -> None:
        self.max_rows = max_rows
        self.max_bytes = max_bytes

    def validate(
        self, content: bytes, *, filename: str | None = None, content_type: str | None = None
    ) -> CsvValidationResult:
        errors: list[ValidationIssue] = []
        warnings: list[ValidationIssue] = []

        def done(rows: list[ParsedRow] | None = None) -> CsvValidationResult:
            rows = rows or []
            for r in rows:
                errors.extend(r.errors)
            errors.sort(key=lambda e: (e.row is not None, e.row or 0))
            valid_count = sum(1 for r in rows if r.is_valid)
            report = ValidationReport(
                valid=not errors,
                total_rows=len(rows),
                valid_rows=valid_count,
                invalid_rows=len(rows) - valid_count,
                errors=errors,
                warnings=warnings,
                rows=[
                    RowPreview(
                        row=r.row,
                        line=r.line,
                        name=r.name,
                        address=r.address,
                        phone=r.phone,
                        valid=r.is_valid,
                    )
                    for r in rows
                ],
            )
            return CsvValidationResult(report=report, rows=rows)

        # --- 1. transport-level checks ---------------------------------------------------------
        if filename and not filename.lower().endswith(".csv"):
            errors.append(
                ValidationIssue(
                    code="invalid_file_extension",
                    message=f"File '{filename}' must have a .csv extension.",
                )
            )
        if content_type:
            base_type = content_type.split(";")[0].strip().lower()
            if base_type not in ALLOWED_CONTENT_TYPES:
                errors.append(
                    ValidationIssue(
                        code="invalid_content_type",
                        message=f"Content type '{base_type}' is not a CSV type.",
                    )
                )
        if len(content) > self.max_bytes:
            errors.append(
                ValidationIssue(
                    code="file_too_large",
                    message=f"File is {len(content)} bytes; the limit is {self.max_bytes} bytes.",
                )
            )
            return done()
        try:
            text = content.decode("utf-8-sig")  # transparently strips a UTF-8 BOM
        except UnicodeDecodeError as exc:
            errors.append(
                ValidationIssue(
                    code="invalid_encoding",
                    message=f"File is not valid UTF-8 (byte offset {exc.start}).",
                )
            )
            return done()
        if "\x00" in text:
            errors.append(
                ValidationIssue(code="binary_content", message="File looks binary, not CSV.")
            )
            return done()
        if not text.strip():
            errors.append(ValidationIssue(code="empty_file", message="File is empty."))
            return done()
        if errors:  # wrong extension/content type: don't bother parsing
            return done()

        # --- 2. header -------------------------------------------------------------------------
        reader = csv.reader(io.StringIO(text, newline=""))
        header: list[str] | None = None
        header_line = 0
        rows: list[ParsedRow] = []
        blank_lines: list[int] = []
        seen: dict[tuple[str, str, str], int] = {}
        last_line = 0
        try:
            for record in reader:
                start_line, last_line = last_line + 1, reader.line_num
                if not any(cell.strip() for cell in record):
                    if header is not None:
                        blank_lines.append(start_line)
                    continue
                if header is None:
                    header_line = start_line
                    header = self._check_header(record, header_line, errors, warnings)
                    if header is None:
                        return done()
                    continue
                parsed = self._parse_row(record, header, len(rows) + 1, start_line)
                self._check_duplicate(parsed, seen, warnings)
                rows.append(parsed)
        except csv.Error as exc:
            errors.append(
                ValidationIssue(
                    code="malformed_csv",
                    message=f"Malformed CSV near line {reader.line_num}: {exc}.",
                    line=reader.line_num,
                )
            )
            return done(rows)

        # --- 3. file-level row checks ----------------------------------------------------------
        if header is not None and not rows:
            errors.append(
                ValidationIssue(
                    code="no_data_rows",
                    message="The file has a header but no hospital rows.",
                    line=header_line,
                )
            )
        if len(rows) > self.max_rows:
            errors.append(
                ValidationIssue(
                    code="too_many_rows",
                    message=f"File has {len(rows)} hospital rows; the maximum is {self.max_rows}.",
                )
            )
        if blank_lines:
            warnings.append(
                ValidationIssue(
                    code="blank_lines_ignored",
                    message=f"Ignored {len(blank_lines)} blank line(s): "
                    + ", ".join(map(str, blank_lines[:10]))
                    + ("..." if len(blank_lines) > 10 else ""),
                )
            )
        return done(rows)

    # --- helpers -------------------------------------------------------------------------------

    @staticmethod
    def _check_header(
        record: list[str],
        line: int,
        errors: list[ValidationIssue],
        warnings: list[ValidationIssue],
    ) -> list[str] | None:
        """Normalise the header (case, whitespace, stray BOM). Returns None if unusable."""
        header = [cell.strip().lstrip("﻿").strip().lower() for cell in record]
        issues: list[ValidationIssue] = []

        for position, column in enumerate(header, start=1):
            if column == "":
                warnings.append(
                    ValidationIssue(
                        code="unnamed_column_ignored",
                        message=f"Column {position} has no header and will be ignored.",
                        line=line,
                    )
                )
            elif column not in KNOWN_COLUMNS:
                hint = ""
                if ";" in column or "\t" in column:
                    hint = " The file must be comma-separated."
                issues.append(
                    ValidationIssue(
                        code="unknown_column",
                        message=f"Unknown column '{column}'. Expected header: {EXPECTED_HEADER}."
                        + hint,
                        column=column,
                        line=line,
                    )
                )
        named = [c for c in header if c]
        for column in sorted({c for c in named if named.count(c) > 1}):
            issues.append(
                ValidationIssue(
                    code="duplicate_column",
                    message=f"Column '{column}' appears more than once.",
                    column=column,
                    line=line,
                )
            )
        for column in REQUIRED_COLUMNS:
            if column not in header:
                issues.append(
                    ValidationIssue(
                        code="missing_column",
                        message=f"Required column '{column}' is missing. "
                        f"Expected header: {EXPECTED_HEADER}.",
                        column=column,
                        line=line,
                    )
                )
        errors.extend(issues)
        return None if issues else header

    def _parse_row(self, record: list[str], header: list[str], row: int, line: int) -> ParsedRow:
        issues: list[ValidationIssue] = []
        values: dict[str, str] = {}
        for position, cell in enumerate(record):
            column = header[position] if position < len(header) else ""
            if column:
                values[column] = _normalise(cell)
            elif cell.strip():
                issues.append(
                    ValidationIssue(
                        code="unexpected_value",
                        message=f"Value in column {position + 1}, which has no header.",
                        row=row,
                        line=line,
                    )
                )

        for column in REQUIRED_COLUMNS:
            if not values.get(column):
                issues.append(
                    ValidationIssue(
                        code="missing_required_field",
                        message=f"'{column}' is required.",
                        row=row,
                        line=line,
                        column=column,
                    )
                )
        for column, limit in MAX_LENGTHS.items():
            value = values.get(column, "")
            if len(value) > limit:
                issues.append(
                    ValidationIssue(
                        code="value_too_long",
                        message=f"'{column}' is {len(value)} characters; the limit is {limit}.",
                        row=row,
                        line=line,
                        column=column,
                    )
                )
        phone = values.get("phone") or None
        if phone and (not _PHONE_RE.match(phone) or sum(ch.isdigit() for ch in phone) < 3):
            issues.append(
                ValidationIssue(
                    code="invalid_phone",
                    message=f"'{phone}' is not a valid phone number.",
                    row=row,
                    line=line,
                    column="phone",
                )
            )
        return ParsedRow(
            row=row,
            line=line,
            name=values.get("name", ""),
            address=values.get("address", ""),
            phone=phone,
            errors=tuple(issues),
        )

    @staticmethod
    def _check_duplicate(
        parsed: ParsedRow,
        seen: dict[tuple[str, str, str], int],
        warnings: list[ValidationIssue],
    ) -> None:
        if not parsed.name or not parsed.address:
            return
        key = (parsed.name.casefold(), parsed.address.casefold(), parsed.phone or "")
        if key in seen:
            warnings.append(
                ValidationIssue(
                    code="duplicate_row",
                    message=f"Row {parsed.row} duplicates row {seen[key]}; "
                    "both will be created if processed.",
                    row=parsed.row,
                    line=parsed.line,
                )
            )
        else:
            seen[key] = parsed.row
