# Restore Validation Test Fix - Cycle Workload Configuration

## Problem

The `RestoreValidation_Simple.toml` and `RestoreValidation.toml` tests were failing with:
- `BytesWritten="0"` during restore operations
- `RestoreValidationNoRestoredDataFound` errors

**Root Cause**: The `Cycle` workload was not writing any transactions because its configuration parameters were too low.

## Analysis

### Observed Behavior
Running with seed `98332948`:
1. Cycle workload ran from 110s-140s (30 second duration)
2. **ZERO `CycleUpdate` events** - no transactions were written
3. Backup completed successfully at 181s (but backed up an empty database)
4. Restore completed with `BytesWritten="0"` at 183s (nothing to restore)
5. `RestoreValidation` waited 5 minutes looking for data at `restored/` prefix
6. Test timed out with `future_version` error at 337s

### Root Cause
The `Cycle` workload configuration in our test files was too conservative:
```toml
nodeCount = 10000
transactionsPerSecond = 1000.0
```

Comparing with the working `BackupCorrectness.toml`:
```toml
nodeCount = 30000
transactionsPerSecond = 2500.0
```

The lower parameters, combined with specific random seeds, result in the Cycle workload not actually writing any data.

## Solution

Updated both test files to use the same Cycle parameters as `BackupCorrectness.toml`:

### RestoreValidation_Simple.toml
```toml
[[test.workload]]
testName = 'Cycle'
nodeCount = 30000           # Was: 10000
transactionsPerSecond = 2500.0  # Was: 1000.0
testDuration = 30.0
expectedRate = 0
```

### RestoreValidation.toml
```toml
[[test.workload]]
testName = 'Cycle'
nodeCount = 30000           # Was: 10000
transactionsPerSecond = 2500.0  # Was: 1000.0
testDuration = 20.0
expectedRate = 0
```

## Code Changes Verified

All restore-related code changes are correct and present:

1. ✅ **`fdbserver/workloads/BackupCorrectness.actor.cpp`**
   - All 4 `restore()` call sites pass `self->addPrefix` and `self->removePrefix`
   - `clearAndRestoreSystemKeys` (line 568-569)
   - Per-range restore (line 777-778)
   - Multi-range single tag restore (line 801-802)
   - Abort/retry path restore #1 (line 849-850)
   - Abort/retry path restore #2 (line 882-883)

2. ✅ **`fdbclient/FileBackupAgent.actor.cpp`**
   - Tenant validation skip when `addPrefix` is set (line 4528)
   - Correct application of prefix during restore (line 4534)

3. ✅ **`tests/fast/RestoreValidation_Simple.toml`** and **`tests/fast/RestoreValidation.toml`**
   - `tenantModes = ['disabled']` to prevent tenant validation issues
   - `addPrefix = 'restored/'` correctly configured

## Expected Behavior After Fix

With the increased Cycle parameters:
1. Cycle workload will write ~75K transactions during its `testDuration`
2. Backup will capture this data
3. Restore will write data to `restored/` prefix with `BytesWritten > 0`
4. `RestoreValidation` workload will find data and trigger audit
5. Audit will compare original data vs restored data
6. Test will pass with `AuditPhase::Complete`

## Testing

To test the fix:
```bash
cd ~/build_output
ctest -R RestoreValidation_Simple -V
```

Or with a specific seed:
```bash
~/build_output/bin/fdbserver -r simulation -f tests/fast/RestoreValidation_Simple.toml --logdir ./logs -s <seed>
```

## Notes

- The test still uses `--add-prefix 'restored/'` (not `\\xff\\x02/rlog/`) due to TOML limitations
- The audit code automatically detects which prefix has data (dual-prefix support)
- The `future_version` error at 337s was a timeout symptom, not the root cause
- All other SevErrors in `errors.txt` were from the same underlying issue (empty backup)

