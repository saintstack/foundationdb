.. _restore-validation-testing:

##################################
Restore Validation Testing Guide
##################################

This guide provides step-by-step instructions for testing the restore validation feature in FoundationDB.

Quick Setup
===========

1. Build Required Binaries
---------------------------

::

    cd ~/build_output
    cmake --build . --target fdbserver fdbcli fdbbackup backup_agent -j4

2. Start Test Cluster
---------------------

::

    # Create directories and cluster file
    mkdir -p ~/fdb_test_data ~/fdb_backup
    echo "test:test@127.0.0.1:4500" > ~/fdb_test.cluster

    # Start fdbserver with validation knobs (note: lowercase with underscores)
    ~/build_output/bin/fdbserver -p 127.0.0.1:4500 -d ~/fdb_test_data \
      -C ~/fdb_test.cluster \
      --knob-restore_validation_enabled=1 \
      --knob-restore_validation=1 &

    # Configure database
    sleep 3
    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster --exec "configure new single memory"

    # Start backup agent (required for backups)
    sleep 2
    ~/build_output/bin/backup_agent -C ~/fdb_test.cluster &

    sleep 2
    echo "Cluster ready"

Testing Workflow
================

Phase 1: Set Up Test Data
--------------------------

Write Some Test Data
^^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # In fdbcli:
    fdb> writemode on
    fdb> set testkey1 testvalue1
    fdb> set testkey2 testvalue2
    fdb> set testkey3 testvalue3
    fdb> set mykey myvalue

    # Verify data
    fdb> getrange "" "\xff"

Phase 2: Backup
---------------

Start Backup
^^^^^^^^^^^^

::

    # Start backup (directory already created in setup)
    ~/build_output/bin/fdbbackup start -C ~/fdb_test.cluster \
      -d file:///Users/stack/fdb_backup -z

    # Check status - wait until backup is restorable
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

Wait approximately 15-30 seconds until status shows backup is "restorable". The output should include:

- ``BackupURL: file:///Users/stack/fdb_backup/backup-<timestamp>``
- Status showing "completed" or "running differential"

Verify Backup is Ready
^^^^^^^^^^^^^^^^^^^^^^

::

    # Get the specific backup URL from the status output
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

    # Verify it's restorable (use the actual backup URL from status)
    ~/build_output/bin/fdbbackup describe \
      -d file:///Users/stack/fdb_backup/backup-<timestamp>

Look for ``Restorable: true`` and ``MaxRestorableVersion`` in the output.

Phase 3: Restore to Validation Prefix
--------------------------------------

Restore with Prefix
^^^^^^^^^^^^^^^^^^^

**Important**: Use the SPECIFIC backup URL from the previous step (not the parent directory):

::

    # Use fdbrestore with the actual backup URL
    ~/build_output/bin/fdbrestore start \
      -r file:///Users/stack/fdb_backup/backup-<timestamp> \
      --dest-cluster-file ~/fdb_test.cluster \
      --add-prefix "\xff\x02/rlog/" \
      -w

.. warning::
   Using ``file:///Users/stack/fdb_backup`` (parent dir) instead of the full backup path will fail with "not restorable to any version".

The ``\xff\x02/rlog/`` prefix is the ``restoreLogKeys`` range where validation looks for restored data.

Verify Restored Data Exists
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # In fdbcli, enable system keys to see restored data
    fdb> option on ACCESS_SYSTEM_KEYS
    fdb> getrange "\xff\x02/rlog/" "\xff\x02/rlog0"

    # You should see your keys with the prefix
    # e.g., "\xff\x02/rlog/testkey1" -> "testvalue1"

Phase 4: Run Validation
------------------------

Start Validation Audit
^^^^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # Start validation for entire key range
    fdb> audit_storage validate_restore "" "\xff"

This returns an Audit ID. **Save this ID!**

Example output::

    Audit ID: 12345678-1234-5678-1234-567812345678

Monitor Progress
^^^^^^^^^^^^^^^^

::

    # Check overall status
    fdb> get_audit_status validate_restore id <AuditID>

    # Check detailed progress (shows which ranges are complete)
    fdb> get_audit_status validate_restore progress <AuditID>

    # Check for any errors
    fdb> get_audit_status validate_restore phase error

Wait for Completion
^^^^^^^^^^^^^^^^^^^

Keep checking status until the audit completes. For a small dataset, this should take seconds to minutes.

Phase 5: Verify Results
------------------------

Check Audit Status
^^^^^^^^^^^^^^^^^^

::

    fdb> get_audit_status validate_restore id <AuditID>

**Expected Output (Success)**::

    Audit result is:
    AuditStorageState: [ID]: <AuditID>, [Range]: ["","\\xff"), [Type]: 5, [Phase]: 2

Where:

- Type: 5 = ValidateRestore
- Phase: 2 = Complete (no errors)

**If Phase: 3 = Error**, there were validation failures!

Check Trace Logs
^^^^^^^^^^^^^^^^

Look for validation events in the server logs::

    grep "AuditRestore" ~/fdb_test_data/*.log | tail -20

Look for:

- ``SSAuditRestoreBegin`` - Validation started
- ``SSAuditRestoreComplete`` - Validation finished successfully
- ``SSAuditRestoreError`` - Validation found errors (check details!)

Phase 6: Testing a Failed Audit
--------------------------------

To verify that the audit correctly detects mismatches, you can intentionally modify the source data and rerun the audit.

Modify Source Data
^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    fdb> writemode on

    # Modify one of the original source values to create a mismatch
    fdb> set testkey1 "modified_value"

    # Verify the change
    fdb> get testkey1

This creates a mismatch because:

- Current source data: ``testkey1`` = ``modified_value`` (modified after backup)
- Restored data: ``\xff\x02/rlog/testkey1`` = ``testvalue1`` (from backup)

Run Audit Again
^^^^^^^^^^^^^^^

::

    # Start a new validation audit
    fdb> audit_storage validate_restore "" "\xff"

Save the new Audit ID returned.

Check for Expected Failure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    # Monitor the audit status
    fdb> get_audit_status validate_restore id <NewAuditID>

**Expected Output (Failure)**::

    Audit result is:
    AuditStorageState: [ID]: <AuditID>, [Range]: ["","\\xff"), [Type]: 5, [Phase]: 3

Where:

- Phase: 3 = Error (validation found mismatches)

Check Error Details
^^^^^^^^^^^^^^^^^^^

::

    # Check detailed error information
    fdb> get_audit_status validate_restore phase error

    # Check trace logs for specific error details
    grep "SSAuditRestoreError" ~/fdb_test_data/*.log | tail -20

The logs should show which key had a mismatch and what the differing values were.

Restore Correct Source Data for Next Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    # Restore the original source value to match the backup
    fdb> writemode on
    fdb> set testkey1 testvalue1

    # Verify the restoration
    fdb> get testkey1

Phase 7: Understanding Audit Design and Limitations
----------------------------------------------------

.. important::
   The restore validation audit is designed to run immediately after a restore operation to verify the restore process didn't corrupt data. It compares the current database state with the restored data at the time of the audit.

What the Audit Validates
^^^^^^^^^^^^^^^^^^^^^^^^^

The audit ensures:

✅ The restore process correctly copied data from backup to the restored location

✅ No data corruption occurred during the restore operation

✅ All keys in the specified range were restored correctly

What the Audit Does NOT Validate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The audit has these design limitations:

❌ It does NOT detect if source data changed after the backup was created

❌ It does NOT validate the backup's original state

❌ It compares current source data to restored data, not backup data to restored data

**Why This Matters**:

If you:

1. Create a backup with ``testkey1=value1``
2. Modify source data to ``testkey1=value2``
3. Restore the backup (restores ``testkey1=value1`` to ``\xff\x02/rlog/``)
4. Run the audit

The audit will report an ERROR because current source (``value2``) doesn't match restored (``value1``). This is expected behavior - the audit validates restore integrity by comparing current state to restored state.

When to Use Restore Validation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use restore validation:

✅ Immediately after completing a restore operation

✅ To verify the restore process worked correctly

✅ To ensure no corruption during data transfer

Do NOT use restore validation:

❌ To verify backup data integrity (use backup verification tools instead)

❌ To check if source data matches the backup (they're expected to diverge)

❌ As a long-term consistency check between source and restored data

Phase 8: Cleanup
-----------------

Clear Restored Data
^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    fdb> option on ACCESS_SYSTEM_KEYS
    fdb> writemode on
    fdb> clearrange "\xff\x02/rlog/" "\xff\x02/rlog0"

Verify Cleanup
^^^^^^^^^^^^^^

::

    fdb> getrange "\xff\x02/rlog/" "\xff\x02/rlog0"
    # Should return empty

Quick Test Script
=================

Here's a complete test script you can run::

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

    # Start main server with validation knobs (lowercase!)
    $FDBSERVER -p 127.0.0.1:4500 -d $DATA_DIR \
      -C $CLUSTER \
      --knob-restore_validation_enabled=1 \
      --knob-restore_validation=1 &
    sleep 3

    # Configure database
    $FDBCLI -C $CLUSTER --exec "configure new single memory"
    sleep 2

    # Start backup agent (required for backups)
    backup_agent -C $CLUSTER &
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

Save this as ``test_restore_validation.sh``, make it executable, and run::

    chmod +x test_restore_validation.sh
    ./test_restore_validation.sh

Troubleshooting
===============

"not_implemented" Error
------------------------

**Symptom**: Validation immediately fails with "not implemented"

**Cause**: Build didn't include the updated storageserver.actor.cpp

**Fix**: Rebuild fdbserver and restart

"restore_destination_not_empty" Error
--------------------------------------

**Symptom**: Restore fails saying destination is not empty

**Cause**: Knobs not set correctly

**Fix**: Ensure ``RESTORE_VALIDATION_ENABLED=1`` and ``RESTORE_VALIDATION=1``

"No backup agents are responding"
----------------------------------

**Symptom**: After running ``fdbbackup start``, you see this message

**Cause**: No backup agent process running to execute the backup

**Fix**::

    # Start backup agent daemon
    ~/build_output/bin/backup_agent -C ~/fdb_test.cluster &

    # Wait a moment, then check backup status
    sleep 5
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

"The specified backup is not restorable to any version"
--------------------------------------------------------

**Symptom**: Restore fails immediately with this error

**Causes**:

1. **Wrong backup URL**: Using parent directory instead of specific backup path
2. **Backup not complete**: Backup hasn't finished creating a restorable snapshot

**Fix**::

    # 1. Get the correct backup URL from status
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster
    # Look for: BackupURL: file:///.../backup-<timestamp>

    # 2. Use that EXACT URL in restore command
    ~/build_output/bin/fdbrestore start \
      -r file:///Users/stack/fdb_backup/backup-2025-11-18-09-36-09.156836 \
      --dest-cluster-file ~/fdb_test.cluster \
      --add-prefix "\xff\x02/rlog/" -w

Backup Not Restorable
----------------------

**Symptom**: Restore fails with "not restorable to any version"

**Cause**: Backup hasn't completed or saved a snapshot yet

**Fix**:

- Wait longer (10-30 seconds minimum)
- Check ``fdbbackup status`` for restorable version
- Ensure backup agent is running

No Progress Updates
-------------------

**Symptom**: Audit status stays in "Running" phase forever

**Cause**: Storage servers may not have the shard containing your key range

**Fix**::

    # Check if data distribution is working
    fdbcli> status details
    # Look for storage servers and their shard assignments

Cannot See Restored Data
-------------------------

**Symptom**: ``getrange "\xff\x02/rlog/"`` returns empty

**Cause**: Need to enable system keys access

**Fix**::

    fdb> option on ACCESS_SYSTEM_KEYS

Validation Completes but No Logs
---------------------------------

**Symptom**: Can't find trace events

**Cause**: Logs may be in different location

**Fix**::

    # Find log location
    ps aux | grep fdbserver
    # Look for -L or --logdir parameter

    # Or check default locations
    ls -ltr /var/log/foundationdb/
    ls -ltr ~/fdb_test_data/*.log

Advanced Testing
================

Test with Simulation
---------------------

Create a simulation test file ``RestoreValidationTest.toml``::

    [[test]]
    testTitle = 'RestoreValidationTest'
    timeout = 7200

    [[test.workload]]
    testName = 'BackupS3BlobCorrectness'
    backupURL = 's3://simulation-backup/'
    # Add your test parameters

Run::

    ~/build_output/bin/fdbserver -r simulation -f RestoreValidationTest.toml

Test with Different Key Ranges
-------------------------------

::

    # Test a specific range
    fdb> audit_storage validate_restore "a" "m"

    # Test with binary keys
    fdb> audit_storage validate_restore "\x00" "\x10"

Load Testing
------------

::

    # Write lots of data
    for i in {1..10000}; do
      echo "set key_$i value_$i" | fdbcli -C $CLUSTER --exec
    done

    # Then backup, restore, and validate

Expected Performance
====================

- **Small dataset (100s of keys)**: Seconds
- **Medium dataset (10K keys)**: 1-5 minutes
- **Large dataset (1M+ keys)**: Hours (rate limited)

Rate limiting is controlled by ``AUDIT_STORAGE_RATE_PER_SERVER_MAX`` (default: 50MB/s per server).

Success Criteria
================

✅ Validation should:

1. Complete without errors for matching data
2. Detect value mismatches
3. Detect missing keys in restored data
4. Report progress correctly
5. Persist audit state across failures
6. Work on any key range within normalKeys

Next Steps
==========

After manual testing succeeds:

1. Run simulation tests
2. Test with production backup data (in staging)
3. Test with large datasets
4. Test failure scenarios (server crashes during validation)
5. Performance benchmarking

Good luck! 🚀