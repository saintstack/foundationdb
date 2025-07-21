# Determinism Analysis Tools

This directory contains tools for analyzing determinism failures in FoundationDB simulation tests.

## Files

- `analyze_determinism_failure.py` - Script for comparing trace files from determinism check failures
- `test_harness/run.py` - Main TestHarness2 implementation with determinism analysis support

## How Determinism Analysis Works

When a determinism check fails, TestHarness2 automatically preserves trace files from both runs for analysis:

1. **Before determinism check**: Initial run trace files are backed up to a temporary location
2. **During determinism check**: The second run overwrites the trace files (normal behavior)
3. **After failed determinism check**: Both sets of traces are organized into an analysis directory

The resulting directory structure:

```
determinism_analysis_{uid}/
├── initial_run/
│   └── trace.*.json        # Trace files from the first run
├── determinism_check/
│   └── trace.*.json        # Trace files from the second run
└── README.txt             # Analysis instructions
```

## Using the Analysis Tools

### Automatic Analysis (Recommended)

When a determinism check fails, follow the instructions in the generated `README.txt` file:

```bash
python3 contrib/TestHarness2/analyze_determinism_failure.py determinism_analysis_{uid}/initial_run/ determinism_analysis_{uid}/determinism_check/
```

### Manual Analysis

You can also use `analyze_determinism_failure.py` to analyze any two sets of trace files:

```bash
python3 contrib/TestHarness2/analyze_determinism_failure.py <path_to_initial_traces>/ <path_to_determinism_check_traces>/
```

## Understanding the Output

The comparison script analyzes:

- **S3 Operations**: Compares bulk dump file names and operations
- **Key Events**: Compares important simulation events like ConnectToDatabase, CommitQueueNewLog, etc.
- **Event Sequences**: Identifies where the two runs diverged

### Success Indicators

- ✅ Same files used in both runs
- ✅ Identical event counts
- ✅ Matching key events

### Failure Indicators

- ❌ Different files used (different version numbers in bulk dump files)
- ❌ Different event counts
- ❌ Mismatched key events (different cluster files, versions, etc.)

## Troubleshooting

If you see determinism failures:

1. **Check for different bulk dump version numbers** - This indicates the database progressed through different versions
2. **Look for different cluster file paths** - This suggests the simulation used different cluster configurations
3. **Compare ConnectToDatabase events** - Different timestamps or cluster files indicate simulation state divergence

## Implementation Notes

The determinism analysis is designed to have minimal interference with the actual determinism check:

- Initial trace files are backed up before the determinism check runs
- The determinism check runs normally, overwriting trace files as expected
- Only when a determinism check fails are the backed up files organized for analysis
- Temporary backup files are automatically cleaned up after analysis setup
- All analysis files are stored in a dedicated directory to avoid interference 