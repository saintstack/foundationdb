#!/bin/bash

# BulkLoad to Restore Integration Test Script
# Tests the complete backup → restore cycle using BulkDump/BulkLoad

set -e

echo "🚀 Testing BulkLoad to Restore Integration"
echo "========================================="

# Configuration
BACKUP_DIR="/tmp/fdb_bulkload_integration_test"
CLUSTER_FILE="./fdb.cluster"
BACKUP_TAG="bulkload_integration"
RESTORE_TAG="bulkload_restore"

# Clean up previous test
rm -rf "$BACKUP_DIR"
mkdir -p "$BACKUP_DIR"

echo "📝 Step 1: Creating test data..."
./bin/fdbcli --exec "set integration_key_001 integration_value_001"
./bin/fdbcli --exec "set integration_key_002 integration_value_002"  
./bin/fdbcli --exec "set integration_key_003 integration_value_003"
./bin/fdbcli --exec "set integration_key_004 integration_value_004"
./bin/fdbcli --exec "set integration_key_005 integration_value_005"

echo "💾 Step 2: Creating BulkDump backup (new default mode)..."
./bin/fdbbackup start \
  -d "file://$BACKUP_DIR" \
  --mode bulkdump \
  -t "$BACKUP_TAG" \
  --waitfordone

echo "✅ Backup completed. Checking backup contents..."
ls -la "$BACKUP_DIR/"
echo ""

echo "🗑️  Step 3: Clearing database..."
./bin/fdbcli --exec "clearrange '' \xFF"

# Verify database is empty
EMPTY_CHECK=$(./bin/fdbcli --exec "getrange '' \xFF" | wc -l)
if [ "$EMPTY_CHECK" -eq 0 ]; then
    echo "✅ Database cleared successfully"
else
    echo "❌ Warning: Database may not be empty"
fi

echo "🔄 Step 4: Testing BulkLoad restore (new default behavior)..."
./bin/fdbrestore start \
  -r "file://$BACKUP_DIR" \
  --dest-cluster-file "$CLUSTER_FILE" \
  -t "$RESTORE_TAG" \
  --waitfordone

echo "🔍 Step 5: Validating restored data..."
VALIDATION_PASSED=true

for i in {1..5}; do
    KEY="integration_key_00$i"
    EXPECTED="integration_value_00$i"
    ACTUAL=$(./bin/fdbcli --exec "get $KEY" 2>/dev/null | tail -1 || echo "NOT_FOUND")
    
    if [ "$ACTUAL" = "$EXPECTED" ]; then
        echo "✅ $KEY: $ACTUAL (MATCH)"
    else
        echo "❌ $KEY: Expected '$EXPECTED', got '$ACTUAL' (MISMATCH)"
        VALIDATION_PASSED=false
    fi
done

echo ""
echo "🔄 Step 6: Testing backward compatibility with --rangefile..."
./bin/fdbcli --exec "clearrange '' \xFF"

# Test traditional restore using rangefile parameter
./bin/fdbrestore start \
  -r "file://$BACKUP_DIR" \
  --rangefile "file://$BACKUP_DIR/kvranges" \
  --dest-cluster-file "$CLUSTER_FILE" \
  -t "${RESTORE_TAG}_traditional" \
  --waitfordone

echo "🔍 Step 7: Validating traditional restore..."
TRADITIONAL_PASSED=true

for i in {1..5}; do
    KEY="integration_key_00$i"
    EXPECTED="integration_value_00$i"
    ACTUAL=$(./bin/fdbcli --exec "get $KEY" 2>/dev/null | tail -1 || echo "NOT_FOUND")
    
    if [ "$ACTUAL" = "$EXPECTED" ]; then
        echo "✅ $KEY: $ACTUAL (MATCH)"
    else
        echo "❌ $KEY: Expected '$EXPECTED', got '$ACTUAL' (MISMATCH)" 
        TRADITIONAL_PASSED=false
    fi
done

echo ""
echo "📊 Final Results:"
echo "=================="
if [ "$VALIDATION_PASSED" = true ] && [ "$TRADITIONAL_PASSED" = true ]; then
    echo "🎉 SUCCESS: BulkLoad to Restore integration working correctly!"
    echo "   ✅ BulkDump backup completed"
    echo "   ✅ BulkLoad restore successful" 
    echo "   ✅ Data integrity verified"
    echo "   ✅ Backward compatibility confirmed"
    echo ""
    echo "🚀 The BulkLoad to Restore integration is ready for production!"
    exit 0
else
    echo "❌ FAILURE: Integration test failed"
    if [ "$VALIDATION_PASSED" = false ]; then
        echo "   ❌ BulkLoad restore data mismatch"
    fi
    if [ "$TRADITIONAL_PASSED" = false ]; then
        echo "   ❌ Traditional restore data mismatch"  
    fi
    exit 1
fi