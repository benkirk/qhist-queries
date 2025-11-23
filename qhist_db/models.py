"""SQLAlchemy ORM models for HPC job history data."""

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Job(Base):
    """Job record from an HPC cluster.

    Each machine (casper, derecho) has its own database file with a 'jobs' table.
    """

    __tablename__ = "jobs"

    # Primary key - full job ID including array index (e.g., "6049117[28]")
    id = Column(Text, primary_key=True)

    # Base job ID as integer for efficient queries (array index stripped)
    short_id = Column(Integer, index=True)

    # Job identification
    name = Column(Text)
    user = Column(Text, index=True)
    account = Column(Text, index=True)

    # Queue and status
    queue = Column(Text, index=True)
    status = Column(Text, index=True)

    # Timestamps (stored in UTC)
    submit = Column(DateTime, index=True)
    eligible = Column(DateTime)
    start = Column(DateTime, index=True)
    end = Column(DateTime, index=True)

    # Time metrics (in seconds)
    elapsed = Column(Integer)
    walltime = Column(Integer)
    cputime = Column(Integer)

    # Resource allocation
    numcpus = Column(Integer)
    numgpus = Column(Integer)
    numnodes = Column(Integer)
    mpiprocs = Column(Integer)
    ompthreads = Column(Integer)

    # Memory (in bytes)
    reqmem = Column(BigInteger)
    memory = Column(BigInteger)
    vmemory = Column(BigInteger)

    # Resource types
    cputype = Column(Text)
    gputype = Column(Text)
    resources = Column(Text)
    ptargets = Column(Text)

    # Performance metrics
    cpupercent = Column(Float)
    avgcpu = Column(Float)
    count = Column(Integer)

    __table_args__ = (
        # Existing composite indexes
        Index("ix_jobs_user_account", "user", "account"),
        Index("ix_jobs_submit_end", "submit", "end"),
        # Date-filtered aggregation indexes
        Index("ix_jobs_user_submit", "user", "submit"),
        Index("ix_jobs_account_submit", "account", "submit"),
        Index("ix_jobs_queue_submit", "queue", "submit"),
    )

    def __repr__(self):
        return f"<Job(id='{self.id}', user='{self.user}', status='{self.status}')>"

    def to_dict(self):
        """Convert job record to dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
