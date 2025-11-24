"""Charging rules for HPC resources by machine and queue.

This module defines charging calculations for NCAR HPC clusters. Each machine
has different charging rules based on queue type and resource usage.

Constants are defined once and used by both Python functions (for ad-hoc
calculations) and SQL view generation (for bulk queries in the database).
"""

from typing import Callable

# Type alias for charging function
ChargingFunc = Callable[[dict], float]

# ============================================================================
# Machine-specific constants
# ============================================================================

# Derecho cluster
DERECHO_CORES_PER_NODE = 128  # CPU cores per compute node
DERECHO_GPUS_PER_NODE = 4     # GPUs per GPU node

# Memory conversion
BYTES_PER_GB = 1024 * 1024 * 1024  # 1 GB in bytes

# Time conversion
SECONDS_PER_HOUR = 3600


# ============================================================================
# Python charging functions (for ad-hoc calculations)
# ============================================================================

def derecho_charge(job: dict) -> dict:
    """Calculate charge metrics for a Derecho job.

    Derecho tracks CPU-hours, GPU-hours, and memory-hours.
    CPU/GPU hours depend on queue type (dev vs production).

    Note: This Python function is available for ad-hoc calculations but is not
    used in the normal data pipeline. The primary charging source is the SQL
    view v_jobs_charged, created by generate_derecho_view_sql() below, which
    provides better performance for bulk queries.

    Args:
        job: Job record dict with elapsed, numnodes, numcpus, numgpus, memory, queue

    Returns:
        Dict with cpu_hours, gpu_hours, and memory_hours
    """
    elapsed = job.get("elapsed") or 0
    numnodes = job.get("numnodes") or 0
    numcpus = job.get("numcpus") or 0
    numgpus = job.get("numgpus") or 0
    memory = job.get("memory") or 0  # in bytes
    queue = (job.get("queue") or "").lower()

    is_gpu_queue = "gpu" in queue
    is_dev_queue = "dev" in queue

    # CPU hours: dev queues use actual CPUs, production uses cores per node
    if is_dev_queue:
        cpu_hours = elapsed * numcpus / SECONDS_PER_HOUR
    else:
        cpu_hours = elapsed * numnodes * DERECHO_CORES_PER_NODE / SECONDS_PER_HOUR

    # GPU hours: only for GPU queues; dev uses actual GPUs, production uses GPUs per node
    if is_gpu_queue:
        if is_dev_queue:
            gpu_hours = elapsed * numgpus / SECONDS_PER_HOUR
        else:
            gpu_hours = elapsed * numnodes * DERECHO_GPUS_PER_NODE / SECONDS_PER_HOUR
    else:
        gpu_hours = 0.0

    # Memory hours: GB-hours based on actual memory used
    memory_hours = elapsed * memory / (SECONDS_PER_HOUR * BYTES_PER_GB)

    return {
        "cpu_hours": cpu_hours,
        "gpu_hours": gpu_hours,
        "memory_hours": memory_hours,
    }


def casper_charge(job: dict) -> dict:
    """Calculate charge metrics for a Casper job.

    Casper tracks CPU-hours, memory-hours, and GPU-hours (when GPUs used).

    Note: This Python function is available for ad-hoc calculations but is not
    used in the normal data pipeline. The primary charging source is the SQL
    view v_jobs_charged, created by generate_casper_view_sql() below, which
    provides better performance for bulk queries.

    Args:
        job: Job record dict with elapsed, numcpus, numgpus, memory

    Returns:
        Dict with cpu_hours, memory_hours, and gpu_hours
    """
    elapsed = job.get("elapsed") or 0
    numcpus = job.get("numcpus") or 0
    numgpus = job.get("numgpus") or 0
    memory = job.get("memory") or 0  # in bytes

    return {
        "cpu_hours": elapsed * numcpus / SECONDS_PER_HOUR,
        "gpu_hours": elapsed * numgpus / SECONDS_PER_HOUR,
        "memory_hours": elapsed * memory / (SECONDS_PER_HOUR * BYTES_PER_GB),
    }


# ============================================================================
# SQL view generation (uses same constants as Python functions)
# ============================================================================

def generate_derecho_view_sql() -> str:
    """Generate Derecho charging view SQL from Python constants.

    This ensures the SQL view uses the same charging logic as the Python
    function, avoiding duplication and potential inconsistencies.

    Returns:
        SQL CREATE VIEW statement for Derecho charging
    """
    return f"""
CREATE VIEW IF NOT EXISTS v_jobs_charged AS
SELECT *,
  CASE
    WHEN queue LIKE '%dev%'
      THEN elapsed * COALESCE(numcpus, 0) / {SECONDS_PER_HOUR}.0
    ELSE elapsed * COALESCE(numnodes, 0) * {DERECHO_CORES_PER_NODE} / {SECONDS_PER_HOUR}.0
  END AS cpu_hours,
  CASE
    WHEN queue LIKE '%gpu%' AND queue LIKE '%dev%'
      THEN elapsed * COALESCE(numgpus, 0) / {SECONDS_PER_HOUR}.0
    WHEN queue LIKE '%gpu%'
      THEN elapsed * COALESCE(numnodes, 0) * {DERECHO_GPUS_PER_NODE} / {SECONDS_PER_HOUR}.0
    ELSE 0.0
  END AS gpu_hours,
  elapsed * COALESCE(memory, 0) / ({SECONDS_PER_HOUR}.0 * {BYTES_PER_GB}) AS memory_hours
FROM jobs;
"""


def generate_casper_view_sql() -> str:
    """Generate Casper charging view SQL from Python constants.

    This ensures the SQL view uses the same charging logic as the Python
    function, avoiding duplication and potential inconsistencies.

    Returns:
        SQL CREATE VIEW statement for Casper charging
    """
    return f"""
CREATE VIEW IF NOT EXISTS v_jobs_charged AS
SELECT *,
  elapsed * COALESCE(numcpus, 0) / {SECONDS_PER_HOUR}.0 AS cpu_hours,
  elapsed * COALESCE(numgpus, 0) / {SECONDS_PER_HOUR}.0 AS gpu_hours,
  elapsed * COALESCE(memory, 0) / ({SECONDS_PER_HOUR}.0 * {BYTES_PER_GB}) AS memory_hours
FROM jobs;
"""


# Pre-generated SQL for backwards compatibility
DERECHO_VIEW_SQL = generate_derecho_view_sql()
CASPER_VIEW_SQL = generate_casper_view_sql()

VIEW_SQL = {
    "derecho": DERECHO_VIEW_SQL,
    "casper": CASPER_VIEW_SQL,
}


def get_view_sql(machine: str) -> str:
    """Get the SQL to create the charging view for a machine.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        SQL CREATE VIEW statement
    """
    return VIEW_SQL.get(machine, DERECHO_VIEW_SQL)
