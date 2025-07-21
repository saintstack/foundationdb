**Test Execution Control:**
- **`TH_KILL_SECONDS`**: Timeout for individual tests in seconds (default: `1800`)
- **`TH_BUGGIFY`**: Buggify mode (`on`, `off`, or `random`, default: `random`)
- **`TH_USE_VALGRIND`**: Run tests under valgrind (`true`/`false`, default: `false`)
- **`TH_LONG_RUNNING`**: Enable long-running test mode (`true`/`false`, default: `false`)

**Optional Configuration:**

## Determinism Analysis

TestHarness2 includes built-in support for analyzing determinism check failures. When a determinism check fails, the system automatically preserves trace files from both runs for comparison.

### How It Works

1. **Before determinism check**: Initial run trace files are automatically backed up
2. **During determinism check**: The second run proceeds normally, overwriting trace files
3. **After failed determinism check**: Both sets of traces are organized for analysis

### Analysis Files

When determinism checks fail, analysis files are created in:
```
<run_temp_dir>/determinism_analysis_{uid}/
├── initial_run/          # Trace files from first run
├── determinism_check/    # Trace files from second run  
└── README.txt           # Analysis instructions
```

### Usage

To analyze determinism failures:
```bash
python3 contrib/TestHarness2/analyze_determinism_failure.py determinism_analysis_{uid}/initial_run/ determinism_analysis_{uid}/determinism_check/
```

For more details, see [DETERMINISM_ANALYSIS.md](DETERMINISM_ANALYSIS.md).
