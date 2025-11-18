# Restore Validation Test Failure Analysis

## Problem Summary
The `RestoreValidation_Simple.toml` test is failing on Linux (Joshua) with two error types:
1. **`RestoreValidationNoRestoredDataFound`** (most common) - Validation workload times out because no data exists at `restored/` prefix
2. **`RestoreValidationUnexpectedPhase`** (1 instance) - Audit ran but failed with "Unknown error" (Phase 4 = Error)

## Root Cause
**Restore operations are completing with `BytesWritten="0"`**, meaning data is not being written to the prefixed location (`restored/`).

## Critical Fixes Applied

###1. Tenant Validation Skip (`fdbclient/FileBackupAgent.actor.cpp`, line ~4528)
```cpp
// Skip tenant validation when restoring with a prefix
// Prefixed keys (e.g., 'restored/' or '\xff\x02/rlog/') are not tenant keys
if (tenantCache.present() && addPrefix.get() == StringRef()) {
    validTenantCheckFutures.push_back(_validTenantAccess(
        StringRef(arena,
                  data[i].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get())),
        tenantCache.get()));
}
```

### 2. System Key Restore Prefix Fix (`fdbserver/workloads/BackupCorrectness.actor.cpp`, lines ~568-569)
**CRITICAL FIX**: `clearAndRestoreSystemKeys` was passing empty prefixes instead of `self->addPrefix`/`self->removePrefix`:

```cpp
wait(success(backupAgent->restore(cx,
                                  cx,
                                  restoreTag,
                                  KeyRef(lastBackupContainer->getURL()),
                                  lastBackupContainer->getProxy(),
                                  systemRestoreRanges,
                                  WaitForComplete::True,
                                  targetVersion,
                                  Verbose::True,
                                  self->addPrefix,      // FIXED: was Key()
                                  self->removePrefix,   // FIXED: was Key()
                                  self->locked,
                                  UnlockDB::True,
                                  OnlyApplyMutationLogs::False,
                                  InconsistentSnapshotOnly::False,
                                  ::invalidVersion,
                                  lastBackupContainer->getEncryptionKeyFileName())));
```

### 3. Skip Clearing System Keys for Prefixed Restores (`BackupCorrectness.actor.cpp`, lines ~548-556)
```cpp
// Skip clearing if addPrefix is set (restore to different prefix)
if (self->addPrefix.size() == 0) {
    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        for (auto& range : systemRestoreRanges)
            tr->clear(range);
        return Void();
    }));
}
```

### 4. Tenant Mode Disabled in TOML Files
Both test files now have:
```toml
[configuration]
config = 'single'  # or 'triple' for RestoreValidation.toml
tenantModes = ['disabled']
```

### 5. Helper Function Refactoring (`fdbserver/storageserver.actor.cpp`)
Extracted repeated `GetKeyValuesRequest` building code into `issueGetKeyValuesRequest()` helper.

## Verification Checklist

To confirm all fixes are in the Linux binary, verify:

1. **Tenant validation skip** in `fdbclient/FileBackupAgent.actor.cpp` line ~4528:
   ```bash
   grep -A 3 "Skip tenant validation when restoring with a prefix" fdbclient/FileBackupAgent.actor.cpp
   ```

2. **System key restore prefix** in `fdbserver/workloads/BackupCorrectness.actor.cpp` line ~568:
   ```bash
   grep -B 10 -A 2 "self->addPrefix, // FIXED" fdbserver/workloads/BackupCorrectness.actor.cpp
   ```

3. **TOML tenant mode** in both test files:
   ```bash
   grep -A 2 "\[configuration\]" tests/fast/RestoreValidation_Simple.toml
   ```

4. **Binary rebuild timestamp**:
   ```bash
   ls -lh ~/build_output/bin/fdbserver  # Check if newer than the fixes
   ```

## Expected Test Flow

1. **Cycle workload** (0-30s): Writes 10K nodes at 1K TPS
2. **Backup** (35s): `BackupAndRestoreCorrectness` starts backup
3. **Restore** (70s): Restore to `restored/` prefix
   - Should see `BytesWritten > 0` in trace logs
   - Both user data and system keys restored with prefix
4. **Validation** (80s): `RestoreValidation` workload checks for data at `restored/` prefix
   - Triggers `AuditType::ValidateRestore` audit
   - Polls for completion
   - Expects Phase 2 (Complete)

## Debugging Commands

If test still fails, check:

```bash
# Check restore progress
grep "BytesWritten" errors.txt | grep -v "MovingData"

# Check if data was found at restored/ prefix
grep "RestoreValidationRestoredDataFound" errors.txt

# Check audit phase
grep "RestoreValidationUnexpectedPhase\|RestoreValidationSuccess" errors.txt

# Check backup/restore workload events
grep "BackupCorrectness" errors.txt | grep "backup\|restore"
```

## Next Steps

1. **Rebuild on Linux** with all fixes
2. **Run test** on Joshua
3. **Check** for `BytesWritten > 0` in restore progress
4. **If still failing**, collect full trace logs for the failing test run (not just errors.txt)

