# Refactoring Plan: qhist-queries Code Consolidation & Enhancement

## Overview

**Goal**: Eliminate ~1,700 lines of code duplication and add new capabilities
**Approach**: Clean reimplementation with new API (no backward compatibility required)
**Estimated Time**: 6 hours
**Risk Level**: Medium (breaking changes, but tests will validate)

## Code Duplication Analysis

### Current State
- **cli.py** (594 lines): 15 commands with 100% duplication in session management, context extraction, file I/O
- **queries.py** (1,442 lines): ~1,100 lines of duplicated logic across 30+ methods
  - Date filtering repeated 19 times
  - CPU/GPU query pairs (10 methods) differ only in queue names and metric fields
  - Range-based queries (6 methods) share 95% identical structure

### Target State
- **cli.py**: ~250 lines (58% reduction) - metadata-driven command generation
- **queries.py**: ~600 lines (58% reduction) - factory-based query methods with clean API
- **New files**: Export formatters, enhanced query capabilities

---

## Implementation Phases

### Phase 1: Query Layer Refactoring (queries.py)

**File**: `qhist_db/queries.py`

#### 1.1 Add Configuration Class (15 min)

Create `QueryConfig` class with all constants:
```python
class QueryConfig:
    # Queue definitions
    CPU_QUEUES = ['cpu', 'cpudev']
    GPU_QUEUES = ['gpu', 'gpudev', 'pgpu']

    # Resource ranges
    GPU_RANGES = [(4, 4), (8, 8), (9, 16), (17, 32), (33, 64), (65, 96),
                  (97, 128), (129, 256), (257, 320)]
    NODE_RANGES = [(1, 1), (2, 2), (3, 4), (5, 8), (9, 16), (17, 32),
                   (33, 64), (65, 128), (129, 256), (257, 512), (513, 1024), (1025, 2048)]
    CORE_RANGES = [(1, 1), (2, 2), (3, 4), (5, 8), (9, 16), (17, 32),
                   (33, 48), (49, 64), (65, 96), (97, 128)]
    MEMORY_RANGES = [(1, 10), (11, 50), (51, 100), (101, 500), (501, 1000)]

    # Duration buckets (seconds)
    DURATION_BUCKETS = {
        "<30s": lambda: Job.elapsed < 30,
        "30s-30m": lambda: and_(Job.elapsed >= 30, Job.elapsed < 1800),
        "30-60m": lambda: and_(Job.elapsed >= 1800, Job.elapsed < 3600),
        "1-5h": lambda: and_(Job.elapsed >= 3600, Job.elapsed < 18000),
        "5-12h": lambda: and_(Job.elapsed >= 18000, Job.elapsed < 43200),
        "12-18h": lambda: and_(Job.elapsed >= 43200, Job.elapsed < 64800),
        ">18h": lambda: Job.elapsed >= 64800,
    }
```

**Test**: Verify imports work

#### 1.2 Add Core Helper Methods (20 min)

Add to `JobQueries` class:
- `_apply_date_filter(query, start, end)` - Consistent date filtering
- `_build_range_case(ranges, overflow, field)` - Range bucketing CASE statements
- `_build_range_ordering(ranges, overflow, column)` - Natural range ordering

**Test**: Unit tests for each helper

#### 1.3 Create Generic Factory Methods (45 min)

**New Query Methods** (replacing all old duplicated methods):

1. **`usage_by_group(resource_type, group_by, start, end)`**
   - Replaces: `pie_user_gpu`, `pie_user_cpu`, `pie_group_gpu`, `pie_group_cpu`
   - Parameters:
     - `resource_type`: 'cpu' | 'gpu' | 'all'
     - `group_by`: 'user' | 'account'
   - Returns: `[{label, usage_hours, job_count}]`

2. **`job_waits_by_resource(resource_type, range_type, start, end)`**
   - Replaces: `gpu_job_waits_by_gpu_ranges`, `cpu_job_waits_by_node_ranges`, `job_waits_by_core_ranges`
   - Parameters:
     - `resource_type`: 'cpu' | 'gpu' | 'all'
     - `range_type`: 'gpu' | 'node' | 'core' | 'memory'
   - Returns: `[{range_label, avg_wait_hours, job_count}]`

3. **`job_sizes_by_resource(resource_type, range_type, start, end)`**
   - Replaces: `gpu_job_sizes_by_gpu_ranges`, `cpu_job_sizes_by_node_ranges`, `job_sizes_by_core_ranges`
   - Similar parameter structure
   - Returns: `[{range_label, job_count, user_count, hours}]`

4. **`job_durations_by_day(resource_type, start, end)`**
   - Replaces: `gpu_job_durations_by_day`, `cpu_job_durations_by_day`
   - Parameters: `resource_type`: 'cpu' | 'gpu' | 'all'
   - Returns: `[{date, <30s, 30s-30m, ...}]`

5. **`usage_history(start, end, machines=None)`** ✨ NEW
   - Enhanced version of `usage_history_by_day`
   - Can aggregate multiple machines if `machines=['casper', 'derecho']`
   - Returns combined daily statistics

**Test**: Integration tests for each new method

#### 1.4 Remove Old Methods (10 min)

**Delete completely**:
- All pie chart methods (4 methods)
- All wait methods (3 methods)
- All size methods (3 methods)
- All duration methods (2 methods)
- Old implementations of `usage_history_by_day`

Keep these unchanged:
- `jobs_by_user`, `jobs_by_account`, `jobs_by_queue` (still useful as-is)
- `usage_summary`, `user_summary` (still useful as-is)
- Period-based methods (`jobs_per_user_account_by_period`, etc.)

**Test**: Ensure deleted methods are gone, remaining methods still work

---

### Phase 2: CLI Layer Refactoring (cli.py)

**File**: `qhist_db/cli.py`

#### 2.1 Create Report Configuration (30 min)

Add dataclasses:
```python
@dataclass
class ColumnSpec:
    key: str          # Dict key from query result
    header: str       # Column header
    width: int        # Column width (0 = last column)
    format: str       # Format spec: "s", ".1f", ".4f", ""

@dataclass
class ReportConfig:
    command_name: str
    description: str
    query_method: str
    query_params: Dict[str, Any]  # Parameters to pass to query method
    filename_base: str
    columns: List[ColumnSpec]
```

Define **RESOURCE_REPORTS** list with all 15 reports using new query methods:
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
)
# ... repeat for all 15 reports
```

**Test**: Verify configuration parsing

#### 2.2 Create Command Factory (20 min)

```python
def create_resource_command(config: ReportConfig):
    """Generate Click command from configuration."""
    @click.pass_context
    def command_func(ctx):
        # Extract context
        start_date = ctx.obj['start_date']
        end_date = ctx.obj['end_date']
        machine = ctx.obj['machine']
        output_dir = ctx.obj['output_dir']

        # Execute query
        session = get_session(machine)
        queries = JobQueries(session)
        query_func = getattr(queries, config.query_method)
        data = query_func(**config.query_params, start=start_date, end=end_date)

        # Write output
        filepath = _write_report(data, config, machine, start_date, end_date, output_dir)
        click.echo(f"Report saved to {filepath}")
        session.close()

    return command_func
```

**Test**: Test one command end-to-end

#### 2.3 Replace Individual Commands (15 min)

**Delete** all 15 individual `@resource.command()` functions (lines 137-589)

**Add** dynamic registration:
```python
for report_config in RESOURCE_REPORTS:
    command = create_resource_command(report_config)
    resource.command(report_config.command_name)(command)
```

**Test**: Verify all 15 commands still work

---

### Phase 3: Add Export Capabilities ✨ (60 min)

**New File**: `qhist_db/exporters.py`

#### 3.1 Create Export Classes

```python
class ReportExporter(ABC):
    @abstractmethod
    def export(self, data: List[Dict], columns: List[ColumnSpec], filepath: str):
        """Export data in specific format."""
        pass

class DatExporter(ReportExporter):
    """Fixed-width .dat format (existing)."""
    def export(self, data, columns, filepath):
        # Current implementation
        pass

class JSONExporter(ReportExporter):
    """JSON format for programmatic access."""
    def export(self, data, columns, filepath):
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)

class CSVExporter(ReportExporter):
    """CSV format for Excel/spreadsheet import."""
    def export(self, data, columns, filepath):
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[c.key for c in columns])
            writer.writeheader()
            writer.writerows(data)

class MarkdownExporter(ReportExporter):
    """Markdown tables for documentation."""
    def export(self, data, columns, filepath):
        # Generate markdown table
        pass
```

#### 3.2 Update CLI to Support Formats

Add `--format` option to resource group:
```python
@click.option("--format", type=click.Choice(["dat", "json", "csv", "md"]),
              default="dat", help="Output format")
```

Update command factory to use exporters

**Test**: Generate same report in all 4 formats

---

### Phase 4: Add Memory-Based Queries ✨ (30 min)

**File**: `qhist_db/queries.py`

Add new query capabilities using existing factories:
```python
def memory_job_waits(self, start, end):
    """Job wait times by memory requirement."""
    return self.job_waits_by_resource(
        resource_type='all',
        range_type='memory',
        start=start,
        end=end
    )

def memory_job_sizes(self, start, end):
    """Job sizes by memory requirement."""
    return self.job_sizes_by_resource(
        resource_type='all',
        range_type='memory',
        start=start,
        end=end
    )
```

**File**: `qhist_db/cli.py`

Add two new CLI commands:
- `memory-job-waits`
- `memory-job-sizes`

**Test**: Run new memory commands, verify output

---

### Phase 5: Multi-Machine Aggregation ✨ (45 min)

**File**: `qhist_db/queries.py`

Add class method for multi-machine queries:
```python
@classmethod
def multi_machine_query(cls, machines: List[str], method_name: str, **kwargs):
    """Execute query across multiple machines and aggregate results."""
    all_results = []
    for machine in machines:
        session = get_session(machine)
        queries = cls(session)
        method = getattr(queries, method_name)
        results = method(**kwargs)

        # Tag with machine name
        for row in results:
            row['machine'] = machine
        all_results.extend(results)
        session.close()

    return all_results
```

**File**: `qhist_db/cli.py`

Enhance `--machine` option to support multiple:
```python
@click.option("-m", "--machine",
              type=click.Choice(["casper", "derecho", "all"]),
              multiple=True,
              default=["derecho"])
```

Update command factory to handle multiple machines

**Test**: Query both Casper and Derecho, verify aggregated output

---

### Phase 6: Documentation & Testing (60 min)

#### 6.1 Update Documentation

**File**: `README.md`

Update sections:
- CLI Tool examples with new query methods
- Add export format examples
- Add multi-machine query examples
- Update Python API examples with new methods

**File**: `docs/query_api.md` ✨ NEW

Create comprehensive API documentation:
- All new query methods with examples
- Parameter descriptions
- Return value schemas
- Migration guide from old methods

#### 6.2 Create Comprehensive Tests

**File**: `tests/test_queries_refactored.py` ✨ NEW

Tests for:
- All helper methods
- All factory methods
- All new query methods
- Edge cases (empty results, null dates, invalid parameters)

**File**: `tests/test_cli_refactored.py` ✨ NEW

Tests for:
- Command registration (verify all 15 commands exist)
- Output format generation (all 4 formats)
- Multi-machine queries
- File writing and cleanup

**File**: `tests/test_exporters.py` ✨ NEW

Tests for all export formats

**Target**: >90% code coverage on new code

#### 6.3 Update Existing Tests

**File**: `tests/test_queries.py`

- Remove tests for deleted methods
- Update tests for modified methods
- Add new test cases

**File**: `tests/test_models.py`, `tests/test_charging.py`, etc.

- Verify still pass after changes
- No modifications needed (these test database models)

---

## Implementation Checklist

### Phase 1: Query Layer ✅
- [ ] Add QueryConfig class with constants
- [ ] Add helper methods (_apply_date_filter, _build_range_case, _build_range_ordering)
- [ ] Create new factory-based query methods
- [ ] Test new methods work correctly
- [ ] Delete old duplicated methods
- [ ] Run test suite (ensure existing tests updated)

### Phase 2: CLI Layer ✅
- [ ] Create ReportConfig and ColumnSpec dataclasses
- [ ] Define RESOURCE_REPORTS list with all 15 reports
- [ ] Create command factory function
- [ ] Delete individual command functions
- [ ] Add dynamic command registration
- [ ] Test all CLI commands still work

### Phase 3: Export Capabilities ✅
- [ ] Create exporters.py with 4 export classes
- [ ] Update CLI to support --format option
- [ ] Test all export formats
- [ ] Generate sample outputs in each format

### Phase 4: Memory Queries ✅
- [ ] Add memory_job_waits and memory_job_sizes methods
- [ ] Add CLI commands for memory queries
- [ ] Test memory-based reports

### Phase 5: Multi-Machine ✅
- [ ] Add multi_machine_query class method
- [ ] Update --machine option to support multiple/all
- [ ] Update command factory for multi-machine
- [ ] Test queries across both machines

### Phase 6: Documentation & Testing ✅
- [ ] Update README.md with new examples
- [ ] Create docs/query_api.md
- [ ] Create new test files for refactored code
- [ ] Update existing tests
- [ ] Achieve >90% code coverage
- [ ] Run full test suite

---

## Critical Files to Modify

### Core Implementation
1. **qhist_db/queries.py** (1,442 → 600 lines)
   - Add QueryConfig, helpers, factories
   - Remove 12 old methods
   - Add enhanced capabilities

2. **qhist_db/cli.py** (594 → 250 lines)
   - Add metadata structures
   - Replace 15 commands with factory
   - Add export format support

### New Files
3. **qhist_db/exporters.py** (~150 lines)
   - Export format implementations

4. **docs/query_api.md** (~300 lines)
   - Comprehensive API documentation

### Testing
5. **tests/test_queries_refactored.py** (~400 lines)
   - Tests for new query methods

6. **tests/test_cli_refactored.py** (~200 lines)
   - Tests for CLI refactoring

7. **tests/test_exporters.py** (~150 lines)
   - Tests for export formats

### Documentation
8. **README.md** (updates)
   - New CLI examples
   - New API examples
   - Export format documentation

---

## Risk Mitigation

### Breaking Changes
- **Risk**: Old query methods removed
- **Mitigation**: Clear documentation of new API, examples in README

### Test Coverage
- **Risk**: Tests may break during refactoring
- **Mitigation**: Incremental approach with testing at each phase

### Performance
- **Risk**: New abstraction layers might slow queries
- **Mitigation**: Same SQL generated, benchmark critical queries

### Data Validation
- **Risk**: Different output formats might have bugs
- **Mitigation**: Extensive testing of all formats, compare with original .dat files

---

## Success Criteria

- ✅ All 15 CLI commands work with new implementation
- ✅ Output .dat files byte-for-byte identical to originals
- ✅ New export formats (JSON, CSV, MD) work correctly
- ✅ Memory-based queries produce valid output
- ✅ Multi-machine aggregation works for both Casper and Derecho
- ✅ Code reduced by ~1,100 lines (58% reduction)
- ✅ Test coverage >90% on new code
- ✅ Documentation complete and accurate
- ✅ All tests pass (pytest tests/ -v)

---

## Estimated Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| 1. Query Layer Refactoring | 90 min | 1.5 hrs |
| 2. CLI Layer Refactoring | 65 min | 2.6 hrs |
| 3. Export Capabilities | 60 min | 3.6 hrs |
| 4. Memory Queries | 30 min | 4.1 hrs |
| 5. Multi-Machine | 45 min | 4.8 hrs |
| 6. Documentation & Testing | 60 min | 5.8 hrs |
| **Buffer** | 15 min | **6.0 hrs** |

Total: **6 hours**
