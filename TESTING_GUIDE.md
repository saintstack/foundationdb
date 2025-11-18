# Restore Validation Testing Guide

## Prerequisites

### 1. Complete the Build
```bash
cd ~/build_output
cmake --build . --target fdbserver fdbcli fdbbackup -j4
```

Wait for the build to complete successfully.

### 2. Set Up a Test Cluster

#### Option A: Local Single-Node Cluster (Easiest)
```bash
# Create a data directory
mkdir -p ~/fdb_test_data

# Create cluster file
echo "test:test@127.0.0.1:4500" > ~/fdb_test.cluster

# Start fdbserver with validation knobs
~/build_output/bin/fdbserver -p 127.0.0.1:4500 -d ~/fdb_test_data \
  -C ~/fdb_test.cluster \
  --knob-RESTORE_VALIDATION_ENABLED=1 \
  --knob-RESTORE_VALIDATION=1 &

# Wait a moment, then configure the database
sleep 3
~/build_output/bin/fdbcli -C ~/fdb_test.cluster --exec "configure new single memory"

# **IMPORTANT**: Start backup agent (required for backups to work)
sleep 2
~/build_output/bin/fdbserver -p 127.0.0.1:4501 -d ~/fdb_test_data/backup_agent \
  -C ~/fdb_test.cluster &

sleep 2
echo "Cluster ready with backup agent"
```

#### Option B: Use Simulation (Recommended for Development)
```bash
# Create a simulation test directory
cd /Users/stack/checkouts/fdb/foundationdb/tests

# Run a simple backup test first to verify basic functionality
~/build_output/bin/fdbserver -r simulation -f BackupS3BlobCorrectness.toml -s 12345
```

## Testing Workflow

### Phase 1: Set Up Test Data

#### 1.1 Write Some Test Data
```bash
~/build_output/bin/fdbcli -C ~/fdb_test.cluster

# In fdbcli:
fdb> writemode on
fdb> set testkey1 testvalue1
fdb> set testkey2 testvalue2
fdb> set testkey3 testvalue3
fdb> set mykey myvalue

# Verify data
fdb> getrange "" "\xff"
```

#### 1.2 Set Validation Knobs
You need to enable the validation knobs. Create a file `~/fdb_knobs.txt`:
```
RESTORE_VALIDATION_ENABLED=1
RESTORE_VALIDATION=1
```

Then restart fdbserver with knobs:
```bash
# Kill existing server
pkill fdbserver

# Restart with knobs
~/build_output/bin/fdbserver -p 127.0.0.1:4500 -d ~/fdb_test_data \
  -C ~/fdb_test.cluster --knob-RESTORE_VALIDATION_ENABLED=1 \
  --knob-RESTORE_VALIDATION=1 &
```

### Phase 2: Backup

#### 2.1 Create Backup
```bash
# Create backup directory
mkdir -p ~/fdb_backup

# Start backup
~/build_output/bin/fdbbackup start -C ~/fdb_test.cluster \
  -d file:///Users/stack/fdb_backup -z

# Wait for backup to complete
~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster
```

Wait until status shows backup is "restorable" or "running differential".

#### 2.2 Stop Backup (for testing)
```bash
# Stop backup and wait for it to finalize
~/build_output/bin/fdbbackup discontinue -C ~/fdb_test.cluster
sleep 2
~/build_output/bin/fdbbackup wait -C ~/fdb_test.cluster

# Verify backup is now restorable
~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster
```

**Note**: The backup MUST show a restorable version before you can restore from it.

### Phase 3: Restore to Validation Prefix

#### 3.1 Restore with Prefix
**Critical**: Use the `\xff\x02/rlog/` prefix for restored data:

```bash
# Use fdbrestore (not fdbbackup restore)
~/build_output/bin/fdbrestore start \
  -r file:///Users/stack/fdb_backup \
  --dest-cluster-file ~/fdb_test.cluster \
  --add-prefix "\xff\x02/rlog/" \
  -w

# Alternative: If using backup agent for restore
~/build_output/bin/fdbbackup restore -C ~/fdb_test.cluster \
  -r file:///Users/stack/fdb_backup \
  --add-prefix "\xff\x02/rlog/" \
  --wait-for-done
```

**Note**: The `\xff\x02/rlog/` is the `restoreLogKeys` prefix defined in the code.

#### 3.2 Verify Restored Data Exists
```bash
~/build_output/bin/fdbcli -C ~/fdb_test.cluster

# In fdbcli, enable system keys to see restored data
fdb> option on ACCESS_SYSTEM_KEYS
fdb> getrange "\xff\x02/rlog/" "\xff\x02/rlog0"

# You should see your keys with the prefix
# e.g., "\xff\x02/rlog/testkey1" -> "testvalue1"
```

### Phase 4: Run Validation

#### 4.1 Start Validation Audit
```bash
~/build_output/bin/fdbcli -C ~/fdb_test.cluster

# Start validation for entire key range
fdb> audit_storage validate_restore "" "\xff"
```

This returns an Audit ID. **Save this ID!**

Example output:
```
Audit ID: 12345678-1234-5678-1234-567812345678
```

#### 4.2 Monitor Progress
```bash
# Check overall status
fdb> get_audit_status validate_restore id <AuditID>

# Check detailed progress (shows which ranges are complete)
fdb> get_audit_status validate_restore progress <AuditID>

# Check for any errors
fdb> get_audit_status validate_restore phase error
```

#### 4.3 Wait for Completion
Keep checking status until the audit completes. For a small dataset, this should take seconds to minutes.

### Phase 5: Verify Results

#### 5.1 Check Audit Status
```bash
fdb> get_audit_status validate_restore id <AuditID>
```

**Expected Output (Success)**:
```
Audit result is:
AuditStorageState: [ID]: <AuditID>, [Range]: ["","\\xff"), [Type]: 5, [Phase]: 2
```

Where:
- Type: 5 = ValidateRestore
- Phase: 2 = Complete (no errors)

**If Phase: 3 = Error**, there were validation failures!

#### 5.2 Check Trace Logs
Look for validation events in the server logs:
```bash
grep "AuditRestore" ~/fdb_test_data/*.log | tail -20
```

Look for:
- `SSAuditRestoreBegin` - Validation started
- `SSAuditRestoreComplete` - Validation finished successfully
- `SSAuditRestoreError` - Validation found errors (check details!)

### Phase 6: Test Error Detection

#### 6.1 Introduce an Error
Let's test that validation actually detects mismatches:

```bash
~/build_output/bin/fdbcli -C ~/fdb_test.cluster

# Modify a source key
fdb> writemode on
fdb> set testkey1 MODIFIED_VALUE

# DO NOT modify the restored version
```

#### 6.2 Run Validation Again
```bash
fdb> audit_storage validate_restore "" "\xff"
# Save new AuditID

fdb> get_audit_status validate_restore id <NewAuditID>
```

#### 6.3 Verify Error is Detected
This time, you should see:
- Phase: 3 (Error)
- Error message indicating value mismatch for "testkey1"

Check logs:
```bash
grep "SSAuditRestoreError" ~/fdb_test_data/*.log | tail -5
```

You should see something like:
```
Value Mismatch for Key testkey1: source value: MODIFIED_VALUE, restored value: testvalue1
```

### Phase 7: Cleanup

#### 7.1 Clear Restored Data
```bash
~/build_output/bin/fdbcli -C ~/fdb_test.cluster

fdb> option on ACCESS_SYSTEM_KEYS
fdb> writemode on
fdb> clearrange "\xff\x02/rlog/" "\xff\x02/rlog0"
```

#### 7.2 Verify Cleanup
```bash
fdb> getrange "\xff\x02/rlog/" "\xff\x02/rlog0"
# Should return empty
```

## Quick Test Script

Here's a complete test script you can run:

```bash
#!/bin/bash
set -e

CLUSTER=~/fdb_test.cluster
FDBCLI=~/build_output/bin/fdbcli
FDBSERVER=~/build_output/bin/fdbserver
FDBBACKUP=~/build_output/bin/fdbbackup
FDBRESTORE=~/build_output/bin/fdbrestore
BACKUP_DIR=file:///Users/stack/fdb_backup_test
DATA_DIR=~/fdb_test_data

echo "=== Step 0: Setup cluster ==="
mkdir -p $DATA_DIR
echo "test:test@127.0.0.1:4500" > $CLUSTER

# Start main server
$FDBSERVER -p 127.0.0.1:4500 -d $DATA_DIR \
  -C $CLUSTER \
  --knob-RESTORE_VALIDATION_ENABLED=1 \
  --knob-RESTORE_VALIDATION=1 &
sleep 3

# Configure database
$FDBCLI -C $CLUSTER --exec "configure new single memory"
sleep 2

# **CRITICAL**: Start backup agent
$FDBSERVER -p 127.0.0.1:4501 -d $DATA_DIR/backup_agent -C $CLUSTER &
sleep 2

echo "=== Step 1: Write test data ==="
$FDBCLI -C $CLUSTER --exec "writemode on; set test1 value1; set test2 value2; set test3 value3"

echo "=== Step 2: Create backup ==="
$FDBBACKUP start -C $CLUSTER -d $BACKUP_DIR -z
echo "Waiting for backup to become restorable..."
sleep 15
$FDBBACKUP discontinue -C $CLUSTER
sleep 2
$FDBBACKUP wait -C $CLUSTER
echo "Backup status:"
$FDBBACKUP status -C $CLUSTER

echo "=== Step 3: Restore to validation prefix ==="
$FDBRESTORE start -r $BACKUP_DIR \
  --dest-cluster-file $CLUSTER \
  --add-prefix "\\xff\\x02/rlog/" \
  -w

echo "=== Step 4: Verify restored data ==="
$FDBCLI -C $CLUSTER --exec "option on ACCESS_SYSTEM_KEYS; getrange \"\\xff\\x02/rlog/\" \"\\xff\\x02/rlog0\" 5"

echo "=== Step 5: Run validation ==="
AUDIT_ID=$($FDBCLI -C $CLUSTER --exec "audit_storage validate_restore \"\" \"\\xff\"" 2>&1 | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
echo "Audit ID: $AUDIT_ID"

echo "=== Step 6: Wait and check results ==="
sleep 5
$FDBCLI -C $CLUSTER --exec "get_audit_status validate_restore id $AUDIT_ID"

echo "=== Step 7: Cleanup ==="
$FDBCLI -C $CLUSTER --exec "option on ACCESS_SYSTEM_KEYS; writemode on; clearrange \"\\xff\\x02/rlog/\" \"\\xff\\x02/rlog0\""

# Stop servers
pkill -9 fdbserver

echo "=== Test Complete! ==="
```

Save this as `test_restore_validation.sh`, make it executable, and run:
```bash
chmod +x test_restore_validation.sh
./test_restore_validation.sh
```

## Troubleshooting

### Issue: "not_implemented" Error
**Symptom**: Validation immediately fails with "not implemented"
**Cause**: Build didn't include the updated storageserver.actor.cpp
**Fix**: Rebuild fdbserver and restart

### Issue: "restore_destination_not_empty" Error
**Symptom**: Restore fails saying destination is not empty
**Cause**: Knobs not set correctly
**Fix**: Ensure `RESTORE_VALIDATION_ENABLED=1` and `RESTORE_VALIDATION=1`

### Issue: Backup Shows "No Backup Agents Responding"
**Symptom**: Backup submitted but stays in "just started" state forever
**Cause**: No backup agent running
**Fix**: 
```bash
# Start a backup agent (just another fdbserver process)
~/build_output/bin/fdbserver -p 127.0.0.1:4501 \
  -d ~/fdb_test_data/backup_agent \
  -C ~/fdb_test.cluster &
```

### Issue: Backup Not Restorable
**Symptom**: Restore fails with "not restorable to any version"
**Cause**: Backup hasn't completed or saved a snapshot yet
**Fix**:
- Wait longer (10-30 seconds minimum)
- Check `fdbbackup status` for restorable version
- Ensure backup agent is running

### Issue: No Progress Updates
**Symptom**: Audit status stays in "Running" phase forever
**Cause**: Storage servers may not have the shard containing your key range
**Fix**: 
```bash
# Check if data distribution is working
fdbcli> status details
# Look for storage servers and their shard assignments
```

### Issue: Cannot See Restored Data
**Symptom**: `getrange "\xff\x02/rlog/"` returns empty
**Cause**: Need to enable system keys access
**Fix**: 
```bash
fdb> option on ACCESS_SYSTEM_KEYS
```

### Issue: Validation Completes but No Logs
**Symptom**: Can't find trace events
**Cause**: Logs may be in different location
**Fix**:
```bash
# Find log location
ps aux | grep fdbserver
# Look for -L or --logdir parameter

# Or check default locations
ls -ltr /var/log/foundationdb/
ls -ltr ~/fdb_test_data/*.log
```

## Advanced Testing

### Test with Simulation
Create a simulation test file `RestoreValidationTest.toml`:

```toml
[[test]]
testTitle = 'RestoreValidationTest'
timeout = 7200

[[test.workload]]
testName = 'BackupS3BlobCorrectness'
backupURL = 's3://simulation-backup/'
# Add your test parameters
```

Run:
```bash
~/build_output/bin/fdbserver -r simulation -f RestoreValidationTest.toml
```

### Test with Different Key Ranges
```bash
# Test a specific range
fdb> audit_storage validate_restore "a" "m"

# Test with binary keys
fdb> audit_storage validate_restore "\x00" "\x10"
```

### Load Testing
```bash
# Write lots of data
for i in {1..10000}; do
  echo "set key_$i value_$i" | fdbcli -C $CLUSTER --exec
done

# Then backup, restore, and validate
```

## Expected Performance

- **Small dataset (100s of keys)**: Seconds
- **Medium dataset (10K keys)**: 1-5 minutes
- **Large dataset (1M+ keys)**: Hours (rate limited)

Rate limiting is controlled by `AUDIT_STORAGE_RATE_PER_SERVER_MAX` (default: 50MB/s per server).

## Success Criteria

✅ Validation should:
1. Complete without errors for matching data
2. Detect value mismatches
3. Detect missing keys in restored data
4. Report progress correctly
5. Persist audit state across failures
6. Work on any key range within normalKeys

## Next Steps

After manual testing succeeds:
1. Run simulation tests
2. Test with production backup data (in staging)
3. Test with large datasets
4. Test failure scenarios (server crashes during validation)
5. Performance benchmarking

Good luck! 🚀

