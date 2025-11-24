# QHist-Queries Refactoring Plan

This document tracks the remaining work for the qhist-queries codebase after completing Phases 1-4.

## Completed Work (Phases 1-4)

### Phase 1: Foundation ✓
- Added `pyproject.toml` for modern Python packaging
- Added `requirements.txt` for pip users
- Fixed timezone handling (Mountain Time → UTC conversion using `zoneinfo`)
- Documented charging functions as available for ad-hoc use
- Fixed cross-platform path handling in sync_jobs.py

### Phase 2: Testing Infrastructure ✓
- Created `tests/` directory with pytest configuration
- Added 57 tests covering:
  - Timestamp parsing and timezone conversion
  - Type conversion (int, float, job ID)
  - Charging calculations for Derecho and Casper
  - ORM models and constraints
  - Summary generation
  - Date range utilities

### Phase 3: Code Organization ✓
- Split `sync.py` (528→280 lines) into:
  - `parsers.py` (258 lines) - Field parsing and type conversion
  - `remote.py` (100 lines) - SSH command execution with 5-minute timeout
  - `sync.py` (280 lines) - Orchestration only
- Consolidated date utilities in `parsers.py`
- All modules now under 300 lines

### Phase 4: Quality Improvements ✓
- Added `logging.py` module with configurable logger
- Extracted machine constants:
  - `DERECHO_CORES_PER_NODE = 128`
  - `DERECHO_GPUS_PER_NODE = 4`
  - `BYTES_PER_GB`, `SECONDS_PER_HOUR`
- SQL views now generated from Python constants (single source of truth)

---

## Current State

**Project Size**: ~1,270 lines of Python across 10 modules
**Test Coverage**: 57 tests, covering all core functionality
**Module Sizes**: All modules ≤ 280 lines

### Module Structure
```
qhist_db/
├── __init__.py      (15 lines)  - Package exports
├── charging.py     (185 lines)  - Charging rules and SQL generation
├── database.py     (112 lines)  - Connection management
├── logging.py       (58 lines)  - Logging configuration
├── models.py       (128 lines)  - SQLAlchemy ORM models
├── parsers.py      (258 lines)  - Field parsing utilities
├── remote.py       (100 lines)  - SSH command execution
├── summary.py      (134 lines)  - Daily summary generation
└── sync.py         (280 lines)  - Sync orchestration
```

---

## Phase 5: Features & Scalability (Future)

The following items are planned for future implementation as needed.

### 5.1 Query Interface

Add a Python API for common queries:

```python
# qhist_db/queries.py
class JobQueries:
    def __init__(self, session: Session):
        self.session = session

    def jobs_by_user(self, user: str, start: date = None, end: date = None):
        """Get all jobs for a user, optionally filtered by date range."""
        query = self.session.query(Job).filter(Job.user == user)
        if start:
            query = query.filter(Job.end >= start)
        if end:
            query = query.filter(Job.end <= end)
        return query.all()

    def usage_summary(self, account: str, start: date, end: date):
        """Get usage summary for an account over a date range."""
        ...
```

### 5.2 CI/CD

Consider adding GitHub Actions for:
- Code coverage reporting

---

## Success Metrics (Completed)

After completing Phases 1-4:
- [x] Package installable via `pip install -e .`
- [x] Test coverage for core modules (57 tests)
- [x] No single module > 300 lines
- [x] All timestamps correctly in UTC
- [ ] CI/CD running tests on push (optional - Phase 5)
