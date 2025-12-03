"""Tests for timestamp and type conversion functions."""

from datetime import datetime, timezone

import pytest

from qhist_db.parsers import (
    parse_timestamp,
    parse_int,
    parse_float,
    parse_job_id,
    parse_job_record,
    parse_date_string,
    date_range,
)


class TestParseTimestamp:
    """Tests for parse_timestamp function."""

    def test_iso_format_without_timezone(self):
        """ISO format without TZ should be treated as Mountain Time."""
        result = parse_timestamp("2025-01-15T10:00:00")
        assert result is not None
        assert result.tzinfo == timezone.utc
        # Mountain Standard Time is UTC-7 in winter
        assert result.hour == 17  # 10:00 MST = 17:00 UTC

    def test_iso_format_with_timezone(self):
        """ISO format with TZ should convert to UTC."""
        result = parse_timestamp("2025-01-15T10:00:00-0700")
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 17

    def test_space_separated_format(self):
        """Space-separated format should work."""
        result = parse_timestamp("2025-01-15 10:00:00")
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_empty_value(self):
        """Empty string should return None."""
        assert parse_timestamp("") is None
        assert parse_timestamp(None) is None

    def test_invalid_format(self):
        """Invalid format should return None."""
        assert parse_timestamp("not-a-date") is None
        assert parse_timestamp("2025/01/15") is None

    def test_summer_time_dst(self):
        """During MDT (summer), offset should be UTC-6."""
        # July 15 is during MDT
        result = parse_timestamp("2025-07-15T10:00:00")
        assert result is not None
        assert result.hour == 16  # 10:00 MDT = 16:00 UTC


class TestParseInt:
    """Tests for parse_int function."""

    def test_valid_integer(self):
        """Valid integer strings should parse correctly."""
        assert parse_int("123") == 123
        assert parse_int(456) == 456
        assert parse_int("0") == 0

    def test_empty_value(self):
        """Empty values should return None."""
        assert parse_int("") is None
        assert parse_int(None) is None

    def test_invalid_value(self):
        """Invalid values should return None."""
        assert parse_int("abc") is None
        assert parse_int("12.5") is None  # float string

    def test_large_integer(self):
        """Large integers should parse without overflow."""
        assert parse_int("9223372036854775807") == 9223372036854775807


class TestParseFloat:
    """Tests for parse_float function."""

    def test_valid_float(self):
        """Valid float strings should parse correctly."""
        assert parse_float("123.45") == 123.45
        assert parse_float("0.0") == 0.0
        assert parse_float(3.14) == 3.14

    def test_integer_as_float(self):
        """Integer values should convert to float."""
        assert parse_float("123") == 123.0
        assert parse_float(456) == 456.0

    def test_empty_value(self):
        """Empty values should return None."""
        assert parse_float("") is None
        assert parse_float(None) is None

    def test_invalid_value(self):
        """Invalid values should return None."""
        assert parse_float("abc") is None


class TestParseJobId:
    """Tests for parse_job_id function."""

    def test_simple_job_id(self):
        """Simple numeric job ID should parse correctly."""
        assert parse_job_id("123456") == 123456
        assert parse_job_id(789012) == 789012

    def test_array_job_id(self):
        """Array job IDs should extract base ID."""
        assert parse_job_id("6049117[28]") == 6049117
        assert parse_job_id("123456[0]") == 123456

    def test_empty_value(self):
        """Empty values should return None."""
        assert parse_job_id("") is None
        assert parse_job_id(None) is None

    def test_invalid_value(self):
        """Invalid values should return None."""
        assert parse_job_id("abc") is None


class TestParseJobRecord:
    """Tests for parse_job_record function."""

    def test_basic_parsing(self, sample_job_record):
        """Basic job record should parse all fields correctly."""
        result = parse_job_record(sample_job_record, "123456.desched1")

        assert result["job_id"] == "123456.desched1"
        assert result["short_id"] == 123456
        assert result["user"] == "testuser"
        assert result["account"] == "NCAR0001"
        assert result["queue"] == "main"
        assert result["status"] == "0"
        assert result["name"] == "test_job"

    def test_timestamp_parsing(self, sample_job_record):
        """Timestamps should be parsed and converted to UTC."""
        result = parse_job_record(sample_job_record, "123456.desched1")

        assert result["submit"] is not None
        assert result["submit"].tzinfo == timezone.utc
        assert result["eligible"] is not None
        assert result["start"] is not None
        assert result["end"] is not None

    def test_time_conversion(self, sample_job_record):
        """Hours should be converted to seconds."""
        result = parse_job_record(sample_job_record, "123456.desched1")

        # 1.0 hours = 3600 seconds
        assert result["elapsed"] == 3600
        # 2.0 hours = 7200 seconds
        assert result["walltime"] == 7200
        # 0.5 hours = 1800 seconds
        assert result["cputime"] == 1800

    def test_memory_conversion(self, sample_job_record):
        """GB should be converted to bytes."""
        result = parse_job_record(sample_job_record, "123456.desched1")

        # 100 GB = 100 * 1024^3 bytes
        assert result["reqmem"] == 100 * 1024 * 1024 * 1024
        # 50 GB
        assert result["memory"] == 50 * 1024 * 1024 * 1024
        # 60 GB
        assert result["vmemory"] == 60 * 1024 * 1024 * 1024

    def test_resource_allocation(self, sample_job_record):
        """Resource allocation fields should be parsed."""
        result = parse_job_record(sample_job_record, "123456.desched1")

        assert result["numcpus"] == 256
        assert result["numgpus"] == 0
        assert result["numnodes"] == 2
        assert result["mpiprocs"] == 128
        assert result["ompthreads"] == 1

    def test_missing_fields(self):
        """Missing fields should result in None values."""
        minimal_record = {"short_id": "999"}
        result = parse_job_record(minimal_record)

        assert result["short_id"] == 999
        assert result["user"] is None
        assert result["account"] is None
        assert result["elapsed"] is None
        assert result["memory"] is None


class TestParseDateString:
    """Tests for parse_date_string function."""

    def test_valid_date(self):
        """Valid YYYY-MM-DD should parse correctly."""
        result = parse_date_string("2025-01-15")
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_invalid_date(self):
        """Invalid format should raise ValueError."""
        with pytest.raises(ValueError):
            parse_date_string("not-a-date")


class TestDateRange:
    """Tests for date_range function."""

    def test_single_day(self):
        """Same start and end should yield one date."""
        result = list(date_range("2025-01-15", "2025-01-15"))
        assert result == ["2025-01-15"]

    def test_multiple_days(self):
        """Should yield all dates inclusive."""
        result = list(date_range("2025-01-15", "2025-01-17"))
        assert result == ["2025-01-15", "2025-01-16", "2025-01-17"]

    def test_month_boundary(self):
        """Should handle month boundaries correctly."""
        result = list(date_range("2025-01-30", "2025-02-02"))
        assert len(result) == 4
        assert result[0] == "2025-01-30"
        assert result[-1] == "2025-02-02"

    def test_year_boundary(self):
        """Should handle year boundaries correctly."""
        result = list(date_range("2024-12-30", "2025-01-02"))
        assert len(result) == 4
        assert "2024-12-31" in result
        assert "2025-01-01" in result
