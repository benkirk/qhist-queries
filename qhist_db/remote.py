"""SSH command execution for remote qhist queries."""

import json
import subprocess
from typing import Iterator

from .parsers import ALL_FIELDS, parse_job_record

# Default SSH timeout in seconds (5 minutes)
SSH_TIMEOUT = 300


def run_qhist_command(
    machine: str,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    timeout: int = SSH_TIMEOUT,
) -> subprocess.CompletedProcess:
    """Run qhist command on remote machine via SSH.

    Args:
        machine: Machine name ('casper' or 'derecho')
        period: Single date in YYYYMMDD format
        start_date: Start date for range (YYYYMMDD)
        end_date: End date for range (YYYYMMDD)
        timeout: SSH command timeout in seconds

    Returns:
        CompletedProcess with command output

    Raises:
        RuntimeError: If SSH command fails
        subprocess.TimeoutExpired: If command exceeds timeout
    """
    # Build the qhist command
    # qhist uses -p/--period with format: YYYYMMDD for single day, YYYYMMDD-YYYYMMDD for range
    cmd = ["ssh", machine, "qhist", "-J", f"-f={ALL_FIELDS}"]

    if period:
        cmd.extend(["-p", period])
    elif start_date and end_date:
        cmd.extend(["-p", f"{start_date}-{end_date}"])
    elif start_date:
        # From start_date to today
        cmd.extend(["-p", f"{start_date}-"])
    elif end_date:
        # Up to end_date (use days parameter instead)
        cmd.extend(["-p", end_date])

    # Run the command with timeout
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.returncode != 0:
        raise RuntimeError(f"qhist command failed: {result.stderr}")

    return result


def fetch_jobs_ssh(
    machine: str,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    timeout: int = SSH_TIMEOUT,
) -> Iterator[dict]:
    """Fetch job records from a remote machine via SSH.

    Args:
        machine: Machine name ('casper' or 'derecho')
        period: Single date in YYYYMMDD format
        start_date: Start date for range (YYYYMMDD)
        end_date: End date for range (YYYYMMDD)
        timeout: SSH command timeout in seconds

    Yields:
        Parsed job record dictionaries

    Raises:
        RuntimeError: If qhist command fails or JSON parsing fails
    """
    result = run_qhist_command(machine, period, start_date, end_date, timeout)

    # Parse JSON output - qhist outputs a single JSON object with nested Jobs
    # Structure: { "timestamp": ..., "Jobs": { "jobid": {...}, ... } }
    if not result.stdout.strip():
        return

    try:
        data = json.loads(result.stdout)
        jobs = data.get("Jobs", {})
        for job_id, job_data in jobs.items():
            yield parse_job_record(job_data, full_job_id=job_id)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse qhist JSON output: {e}")
