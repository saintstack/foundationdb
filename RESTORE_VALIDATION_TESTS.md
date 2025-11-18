# Restore Validation Simulation Tests

## Overview

Two simulation tests have been created to test the restore validation feature that compares restored backup data with the original source data within the same cluster.

## Test Files Created

### 1. RestoreValidation Workload
**File:** `fdbserver/workloads/RestoreValidation.actor.cpp`

**Purpose:** A test workload that triggers and monitors restore validation audits.

**Functionality:**
- Waits for a specified time after test start (to allow restore to complete)
- Triggers an `audit_storage validate_restore` command via ManagementAPI
- Monitors the audit progress by polling audit states
- Verifies that the audit completes successfully with no errors
- Reports errors if validation fails or times out

**Key Parameters:**
- `validateAfter` (default: 50.0) - Delay before starting validation
- `expectedPhase` (default: 2 = Complete) - Expected final audit phase
- `expectSuccess` (default: true) - Whether to expect successful validation
- `checkInterval` (default: 5.0) - How often to check audit progress
- `maxWaitTime` (default: 300.0) - Maximum time to wait for completion

### 2. RestoreValidation.toml
**File:** `tests/fast/RestoreValidation.toml`

**Purpose:** Comprehensive simulation test with chaos/attrition workloads.

**Test Workflow:**
1. Write test data using `Cycle` workload (10,000 keys)
2. Backup data to file using `BackupAndRestoreCorrectness` workload
3. Restore data with `addPrefix = '\xff\x02/rlog/'` (validation prefix)
4. Run `RestoreValidation` workload to trigger and verify validation
5. Add stress workloads (RandomClogging, Rollback, Attrition)

**Configuration:**
- Test class: `Backup`
- Backup agents: `BackupToFile`
- Timeout: 3600 seconds (1 hour)
- Knobs: `restore_validation_enabled=1`, `restore_validation=1`

### 3. RestoreValidation_Simple.toml
**File:** `tests/fast/RestoreValidation_Simple.toml`

**Purpose:** Simplified test without chaos workloads for faster testing.

**Test Workflow:**
1. Write test data using `Cycle` workload (5,000 keys)
2. Backup data to file
3. Restore data with validation prefix
4. Run `RestoreValidation` workload to verify

**Configuration:**
- Test class: `Backup`
- Backup agents: `BackupToFile`
- Timeout: 1800 seconds (30 minutes)
- Config: `single` (simplified cluster configuration)
- Knobs: `restore_validation_enabled=1`, `restore_validation=1`
- `runConsistencyCheck = false` (because data exists at two prefixes)

## Build Integration

The tests are registered in `tests/CMakeLists.txt`:
```cmake
add_fdb_test(TEST_FILES fast/RestoreValidation.toml)
add_fdb_test(TEST_FILES fast/RestoreValidation_Simple.toml)
```

The workload is automatically discovered by the build system via `fdb_find_sources()`.

## Running the Tests

### Locally (Simulation)
These tests are designed to run in simulation on another system (per project workflow).

The test names in the simulation framework are:
- `fast/RestoreValidation`
- `fast/RestoreValidation_Simple`

### Manual Testing
For manual testing, see `TESTING_GUIDE.md` which includes:
- Setting up a local cluster
- Running backup agents
- Performing backup and restore
- Manually triggering validation with `fdbcli audit_storage validate_restore`
- Monitoring progress with `fdbcli get_audit_status`

## Expected Behavior

### Success Case
1. Backup completes successfully
2. Restore writes data to `\xff\x02/rlog/` prefix
3. `RestoreValidation` workload triggers audit
4. Audit compares source data (normal keys) with restored data (prefixed keys)
5. Audit completes with `Phase=2` (Complete) and no errors
6. Test passes

### Failure Cases
The validation will fail and report errors if:
- **Value mismatch**: A key has different values in source vs. restored data
- **Missing key**: A key exists in source but not in restored data
- **Range violation**: Validation range is not within `normalKeys`
- **Timeout**: Validation takes longer than `maxWaitTime`

## Implementation Status

All components are now implemented and ready for testing:

✅ Core validation logic in `storageserver.actor.cpp`  
✅ CLI commands (`audit_storage`, `get_audit_status`)  
✅ Client knobs (`RESTORE_VALIDATION_ENABLED`, `RESTORE_VALIDATION`)  
✅ `restoreLogKeys` system key range  
✅ `RestoreValidation` workload  
✅ Simulation test configurations  
✅ Build integration  
✅ Documentation  

## Next Steps

1. **Run tests in simulation** to verify the feature works end-to-end
2. **Analyze any failures** and iterate on the implementation
3. **Add more test variations** if needed (e.g., with encryption, tenants)
4. **Performance testing** to measure validation overhead

## Related Documentation

- `TESTING_GUIDE.md` - Manual testing procedures
- `IMPLEMENTATION_SUMMARY.md` - Technical implementation details
- `BUILD_FIX_SUMMARY.md` - Build issues and resolutions
- `restore_validation_implementation_status.md` - Feature implementation status

