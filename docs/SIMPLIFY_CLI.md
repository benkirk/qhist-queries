# Refactoring Plan: queries.py and cli.py

## Overview

Refactor `qhist_db/queries.py` (1496 lines) and `qhist_db/cli.py` (586 lines) to reduce duplication and improve maintainability.

**Goals:**
- Reduce code duplication by 40-50%
- Simplify complex query methods (especially `usage_history`)
- Make adding new reports easier (<10 lines vs current 15-20)
- Maintain output format compatibility (DAT, JSON, CSV, MD)

**Expected Results:**
- Total LOC: 2082 → ~1500 (-28%)
- queries.py: 1496 → ~950 (-36%)
- cli.py: 586 → ~350 (-40%)
- New: query_builders.py: ~200 lines

## Part 1: Extract Query Builder Utilities

### Step 1: Create query_builders.py

**File:** `qhist_db/query_builders.py` (NEW)

**Purpose:** Eliminate duplication in period handling, quarter aggregation, and resource type resolution.

**Key Classes:**

1. **PeriodGrouper** - Handles day/month/quarter grouping
   - `get_period_func(period, date_column)` - Returns SQLAlchemy period function
   - `aggregate_quarters(monthly_data, count_field, grouping_fields)` - Converts monthly counts to quarterly
   - `aggregate_quarters_distinct(monthly_data, entity_field)` - Converts monthly distinct entities to quarterly

2. **ResourceTypeResolver** - Maps resource types to queues and hour fields
   - `resolve(resource_type, machine, JobCharged)` - Returns (queues, hours_field) tuple
   - Supports: 'cpu', 'gpu', 'all'
   - Machine-aware (casper vs derecho)

**Impact:** Eliminates period handling duplication across 8 methods, quarter logic across 3 methods, resource type logic across 6 methods.

### Step 2: Add tests for query_builders.py

**File:** `tests/test_query_builders.py` (NEW)

**Coverage:**
- PeriodGrouper.get_period_func() - all periods (day/month/quarter) + invalid
- PeriodGrouper.aggregate_quarters() - simple counts + grouping fields
- PeriodGrouper.aggregate_quarters_distinct() - unique entity counting
- ResourceTypeResolver.resolve() - cpu/gpu/all + invalid
- Edge cases: year boundaries, Q1/Q4 transitions, null handling

**Tests:** ~15-20 unit tests

## Part 2: Refactor queries.py

### Step 3: Simplify QueryConfig range definitions

**File:** `qhist_db/queries.py`

**Location:** Lines 66-90 (QueryConfig class)

**Change:** Add helper method to generate ranges from boundaries:

```python
@staticmethod
def _make_ranges(boundaries: List[int]) -> List[Tuple[int, int]]:
    """Generate range tuples from boundary list."""
    ranges = []
    prev = 1
    for bound in boundaries:
        if prev == bound:
            ranges.append((bound, bound))
            prev = bound + 1
        else:
            ranges.append((prev, bound))
            prev = bound + 1
    return ranges

GPU_RANGES = _make_ranges([4, 8, 16, 32, 64, 96, 128, 256, 320])
NODE_RANGES = _make_ranges([1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048])
CORE_RANGES = _make_ranges([1, 2, 4, 8, 16, 32, 48, 64, 96, 128])
```

**Impact:** 25 lines → 18 lines, clearer pattern

### Step 4: Decompose usage_history() method

**File:** `qhist_db/queries.py`

**Location:** Lines 684-786 (103 lines)

**Strategy:** Break into 6 focused methods:

1. `usage_history()` - Main coordinator (30 lines)
2. `_build_date_filter(start, end)` - Extract date filter logic (5 lines)
3. `_usage_history_total_users(period_format, date_filter)` - User count subquery (8 lines)
4. `_usage_history_total_projects(period_format, date_filter)` - Project count subquery (8 lines)
5. `_usage_history_resource_stats(resource_type, period_format, date_filter)` - CPU/GPU stats subquery (15 lines)
6. `_join_usage_history_results(users_sq, projects_sq, cpu_sq, gpu_sq)` - Join and format (25 lines)

**Impact:** 103 lines → 6 methods (10-30 lines each), improved testability

### Step 5: Update period-based query methods

**File:** `qhist_db/queries.py`

**Methods to update (5 total):**
1. `job_durations()` (lines 487-553)
2. `job_memory_per_rank()` (lines 555-630)
3. `jobs_per_user_account_by_period()` (lines 1054-1108)
4. `unique_projects_by_period()` (lines 1110-1173)
5. `unique_users_by_period()` (lines 1175-1239)

**Changes per method:**
- Import: `from .query_builders import PeriodGrouper, ResourceTypeResolver`
- Replace period handling: Use `PeriodGrouper.get_period_func(period, Job.end)`
- Replace resource type logic: Use `ResourceTypeResolver.resolve(resource_type, self.machine, JobCharged)`
- Replace quarter aggregation: Use `PeriodGrouper.aggregate_quarters()` or `aggregate_quarters_distinct()`

**Impact:** ~10-15 lines saved per method, ~60 lines total

## Part 3: Refactor cli.py

### Step 6: Add ColumnSpecs factory class

**File:** `qhist_db/cli.py`

**Location:** Insert before RESOURCE_REPORTS (around line 175)

**Purpose:** Reusable column spec patterns to reduce RESOURCE_REPORTS duplication.

**Factory Methods (7 total):**

1. `usage_counts(label="User-ids", label_width=15)` - For pie charts
   - Returns: [label, usage_hours, job_count] columns
   - Used by: 6 pie chart reports

2. `range_waits(range_label)` - For wait time reports
   - Returns: [range_label, avg_wait_hours, job_count] columns
   - Used by: 4 wait reports

3. `range_sizes(range_label)` - For job size reports
   - Returns: [range_label, job_count, user_count, hours] columns
   - Used by: 4 size reports

4. `duration_buckets()` - For duration histograms
   - Returns: [date, <30s, 30s-30m, ..., >18h] columns
   - Used by: 2 duration reports

5. `memory_per_rank_buckets()` - For memory-per-rank histograms
   - Returns: [date, <128MB, 128MB-512MB, ..., >256GB] columns
   - Used by: 2 memory reports

6. `usage_history()` - For usage history report
   - Returns: [Date, #-Users, #-Proj, ...] columns (11 columns)
   - Used by: 1 usage history report

**Implementation:** ~100 lines total

### Step 7: Simplify RESOURCE_REPORTS list

**File:** `qhist_db/cli.py`

**Location:** Lines 176-460 (285 lines)

**Changes:** Update all 18 report configs to use ColumnSpecs factories

**Transformation pattern:**

Before (12 lines):
```python
ReportConfig(
    command_name="pie-proj-cpu",
    description="CPU usage by project (account)",
    query_method="usage_by_group",
    query_params={"resource_type": "cpu", "group_by": "account"},
    filename_base="pie_proj_cpu",
    columns=[
        ColumnSpec("label", "Accounts", 15, "s"),
        ColumnSpec("usage_hours", "Usage", 15, ".1f"),
        ColumnSpec("job_count", "Counts", 0, ""),
    ]
),
```

After (7 lines):
```python
ReportConfig(
    command_name="pie-proj-cpu",
    description="CPU usage by project (account)",
    query_method="usage_by_group",
    query_params={"resource_type": "cpu", "group_by": "account"},
    filename_base="pie_proj_cpu",
    columns=ColumnSpecs.usage_counts(label="Accounts")
),
```

**Impact:**
- Pie charts (6): 12 → 7 lines each (30 lines saved)
- Durations (2): 17 → 7 lines each (20 lines saved)
- Memory (2): 19 → 7 lines each (24 lines saved)
- Waits (4): 11 → 7 lines each (16 lines saved)
- Sizes (4): 11 → 7 lines each (16 lines saved)
- Usage history (1): 20 → 7 lines (13 lines saved)
- **Total: ~120 lines saved (42% reduction)**

## Implementation Sequence

### Phase 1: Foundation (Steps 1-2)
1. Create `query_builders.py` with PeriodGrouper and ResourceTypeResolver
2. Add `test_query_builders.py` with comprehensive unit tests
3. **Checkpoint:** All new tests pass

### Phase 2: Query Refactoring (Steps 3-5)
4. Simplify QueryConfig range definitions (Step 3)
5. Decompose `usage_history()` into helper methods (Step 4)
6. Update 5 period-based methods to use query_builders (Step 5)
7. **Checkpoint:** All tests in `test_queries.py` pass (409 tests)

### Phase 3: CLI Refactoring (Steps 6-7)
8. Add ColumnSpecs factory class (Step 6)
9. Update all 18 RESOURCE_REPORTS configs (Step 7)
10. **Checkpoint:** CLI integration tests pass, output files identical

### Phase 4: Validation
11. Run full test suite (`pytest tests/ -v`)
12. Generate sample reports and verify byte-for-byte match with originals
13. Update documentation and add CHANGELOG entry

## Critical Files

### New Files
- `qhist_db/query_builders.py` - Period/resource utilities (~200 lines)
- `tests/test_query_builders.py` - Unit tests (~150 lines)

### Modified Files
- `qhist_db/queries.py` - Query refactoring (1496 → ~950 lines)
  - Lines 66-90: QueryConfig ranges
  - Lines 684-786: usage_history decomposition
  - Lines 487-553, 555-630, 1054-1239: Period-based methods
- `qhist_db/cli.py` - Report config simplification (586 → ~350 lines)
  - Add ColumnSpecs class (~100 lines)
  - Lines 176-460: RESOURCE_REPORTS updates

### Verification Files
- `tests/test_queries.py` - Must pass unchanged (409 tests)
- `tests/test_cli.py` - Must pass if exists

## Risk Mitigation

### High Risk: usage_history() refactoring
- **Risk:** Complex 103-line query with many test dependencies
- **Mitigation:**
  - Extract one helper at a time
  - Keep old code commented during development
  - Run tests after each helper
  - Manual verification of output files

### Medium Risk: Quarter aggregation logic
- **Risk:** Date math is subtle, off-by-one errors
- **Mitigation:**
  - Extensive unit tests with known inputs/outputs
  - Test edge cases (year boundaries, Q1→Q2→Q3→Q4)
  - Compare against existing implementation

### Low Risk: Column spec factories
- **Risk:** Typos in mechanical transformation
- **Mitigation:**
  - One report type at a time
  - Integration test comparing output files
  - Code review

## Success Criteria

✅ All existing tests pass unchanged (409 tests in test_queries.py)
✅ All CLI commands produce identical output files (byte-for-byte)
✅ New tests for query_builders.py (15-20 tests)
✅ Total LOC reduction: 2082 → ~1500 (-28%)
✅ Duplication reduction: 70-85% in affected areas
✅ No breaking changes to public API

## Backward Compatibility

**Maintained:**
- All public method signatures in JobQueries class
- All CLI command names and options
- All output file formats (DAT, JSON, CSV, MD)
- All output file names

**Changed (internal only):**
- Private method names (prefixed with `_`)
- Internal implementation details
- QueryConfig range generation (values unchanged)

## Next Steps After Approval

1. Implement Phase 1 (foundation) - Est. 4-6 hours
2. Run checkpoint tests
3. Implement Phase 2 (queries) - Est. 12-15 hours
4. Run checkpoint tests
5. Implement Phase 3 (CLI) - Est. 4-6 hours
6. Run full validation
7. Update documentation
