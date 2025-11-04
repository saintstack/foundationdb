# FoundationDB Backup v3: Comprehensive Testing Framework

## Testing Strategy Overview

The Progressive Shard ID System introduces significant architectural changes that require comprehensive testing across multiple dimensions:

1. **Unit Testing**: Individual component validation
2. **Integration Testing**: Component interaction validation  
3. **Performance Testing**: Scalability and efficiency validation
4. **Correctness Testing**: Data integrity and consistency validation
5. **Chaos Testing**: Fault tolerance and recovery validation
6. **Migration Testing**: v2→v3 transition validation

## 1. Unit Testing Framework

### Shard ID Generation Testing

```cpp
class ShardIdGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        ShardIdGenerator::reset(); // Start with clean state
    }
    
    void TearDown() override {
        ShardIdGenerator::reset();
    }
};

TEST_F(ShardIdGeneratorTest, GeneratesUniqueIds) {
    std::set<uint64_t> generatedIds;
    
    // Generate 100,000 IDs
    for (int i = 0; i < 100000; ++i) {
        uint64_t id = ShardIdGenerator::generateShardId();
        EXPECT_TRUE(generatedIds.insert(id).second) 
            << "Duplicate shard ID generated: " << id;
    }
    
    EXPECT_EQ(generatedIds.size(), 100000);
}

TEST_F(ShardIdGeneratorTest, MonotonicGeneration) {
    std::vector<uint64_t> ids;
    
    for (int i = 0; i < 1000; ++i) {
        ids.push_back(ShardIdGenerator::generateShardId());
    }
    
    // Verify strictly increasing
    for (size_t i = 1; i < ids.size(); ++i) {
        EXPECT_GT(ids[i], ids[i-1]) 
            << "Non-monotonic ID sequence at position " << i;
    }
}

TEST_F(ShardIdGeneratorTest, PersistenceRecovery) {
    // Generate some IDs
    auto id1 = ShardIdGenerator::generateShardId();
    auto id2 = ShardIdGenerator::generateShardId();
    
    // Simulate restart by reinitializing with last used ID
    ShardIdGenerator::initializeFromSystemKeyspace(id2);
    
    // Next ID should be greater than id2
    auto id3 = ShardIdGenerator::generateShardId();
    EXPECT_GT(id3, id2);
}
```

### Shard Evolution Testing

```cpp
class ShardEvolutionTest : public ::testing::Test {
protected:
    std::unique_ptr<MockDatabase> db;
    std::unique_ptr<ShardEvolutionManager> manager;
    
    void SetUp() override {
        db = std::make_unique<MockDatabase>();
        manager = std::make_unique<ShardEvolutionManager>(db.get());
    }
};

TEST_F(ShardEvolutionTest, CreateShard) {
    KeyRange range("a", "z");
    Version version = 1000;
    
    uint64_t shardId = manager->createShard(range, version);
    
    auto evolution = manager->getShardEvolution(shardId);
    EXPECT_EQ(evolution.shardId, shardId);
    EXPECT_EQ(evolution.createVersion, version);
    EXPECT_EQ(evolution.currentRange, range);
    EXPECT_TRUE(evolution.isActive);
    EXPECT_FALSE(evolution.parentShardId.present());
}

TEST_F(ShardEvolutionTest, SplitShard) {
    // Create initial shard
    KeyRange initialRange("a", "z");
    Version createVersion = 1000;
    uint64_t parentId = manager->createShard(initialRange, createVersion);
    
    // Split the shard
    Key splitPoint = "m";
    Version splitVersion = 2000;
    auto [leftId, rightId] = manager->splitShard(parentId, splitPoint, splitVersion);
    
    // Verify parent is marked inactive
    auto parentEvolution = manager->getShardEvolution(parentId);
    EXPECT_FALSE(parentEvolution.isActive);
    EXPECT_EQ(parentEvolution.childShardIds.size(), 2);
    EXPECT_TRUE(std::find(parentEvolution.childShardIds.begin(), 
                         parentEvolution.childShardIds.end(), leftId) != 
                parentEvolution.childShardIds.end());
    
    // Verify children are created correctly
    auto leftEvolution = manager->getShardEvolution(leftId);
    EXPECT_EQ(leftEvolution.currentRange, KeyRange("a", "m"));
    EXPECT_EQ(leftEvolution.parentShardId.get(), parentId);
    EXPECT_TRUE(leftEvolution.isActive);
    
    auto rightEvolution = manager->getShardEvolution(rightId);
    EXPECT_EQ(rightEvolution.currentRange, KeyRange("m", "z"));
    EXPECT_EQ(rightEvolution.parentShardId.get(), parentId);
    EXPECT_TRUE(rightEvolution.isActive);
}

TEST_F(ShardEvolutionTest, MergeShards) {
    // Create two adjacent shards
    uint64_t leftId = manager->createShard(KeyRange("a", "m"), 1000);
    uint64_t rightId = manager->createShard(KeyRange("m", "z"), 1000);
    
    // Merge them
    Version mergeVersion = 2000;
    uint64_t mergedId = manager->mergeShards({leftId, rightId}, mergeVersion);
    
    // Verify merged shard
    auto mergedEvolution = manager->getShardEvolution(mergedId);
    EXPECT_EQ(mergedEvolution.currentRange, KeyRange("a", "z"));
    EXPECT_TRUE(mergedEvolution.isActive);
    
    // Verify parent shards are inactive
    auto leftEvolution = manager->getShardEvolution(leftId);
    auto rightEvolution = manager->getShardEvolution(rightId);
    EXPECT_FALSE(leftEvolution.isActive);
    EXPECT_FALSE(rightEvolution.isActive);
}
```

### Restore Lookup Testing

```cpp
class RestoreLookupTest : public ::testing::Test {
protected:
    std::unique_ptr<MockS3Storage> storage;
    std::unique_ptr<RestoreLookupService> lookupService;
    
    void SetUp() override {
        storage = std::make_unique<MockS3Storage>();
        lookupService = std::make_unique<RestoreLookupService>(storage.get());
        
        // Set up test data
        setupTestBackupData();
    }
    
    void setupTestBackupData() {
        // Create version snapshots
        VersionSnapshot snapshot1000{
            .version = 1000,
            .shardMapping = {
                {1, KeyRange("", "key-100")},
                {2, KeyRange("key-100", "key-200")}
            }
        };
        storage->writeVersionSnapshot(1000, snapshot1000);
        
        // Create shard evolution data
        ShardEvolution shard1{
            .shardId = 1,
            .createVersion = 500,
            .currentRange = KeyRange("", "key-100"),
            .isActive = true
        };
        storage->writeShardEvolution(1, shard1);
    }
};

TEST_F(RestoreLookupTest, FindFilesForSimpleRange) {
    KeyRange restoreRange("key-050", "key-150");
    Version restoreVersion = 1000;
    
    auto files = lookupService->findFilesForRestore(restoreRange, restoreVersion);
    
    // Should find files from both shard 1 and shard 2
    EXPECT_GE(files.backupFiles.size(), 2);
    
    bool foundShard1 = std::any_of(files.backupFiles.begin(), files.backupFiles.end(),
        [](const std::string& file) { return file.find("shard-000001") != std::string::npos; });
    bool foundShard2 = std::any_of(files.backupFiles.begin(), files.backupFiles.end(),
        [](const std::string& file) { return file.find("shard-000002") != std::string::npos; });
    
    EXPECT_TRUE(foundShard1);
    EXPECT_TRUE(foundShard2);
}

TEST_F(RestoreLookupTest, HandleShardSplit) {
    // Add split event to shard 1
    ShardEvolution shard1Split{
        .shardId = 1,
        .createVersion = 500,
        .currentRange = KeyRange("", "key-100"),
        .childShardIds = {10, 11},
        .isActive = false
    };
    storage->writeShardEvolution(1, shard1Split);
    
    // Add child shards
    ShardEvolution shard10{
        .shardId = 10,
        .createVersion = 1500,
        .currentRange = KeyRange("", "key-050"),
        .parentShardId = 1,
        .isActive = true
    };
    storage->writeShardEvolution(10, shard10);
    
    // Test restore at version after split
    KeyRange restoreRange("key-025", "key-075");
    Version restoreVersion = 2000;
    
    auto files = lookupService->findFilesForRestore(restoreRange, restoreVersion);
    
    // Should find files from original shard 1 and new shard 10
    bool foundOriginalShard = std::any_of(files.backupFiles.begin(), files.backupFiles.end(),
        [](const std::string& file) { 
            return file.find("shard-000001") != std::string::npos; 
        });
    bool foundSplitShard = std::any_of(files.backupFiles.begin(), files.backupFiles.end(),
        [](const std::string& file) { 
            return file.find("shard-000010") != std::string::npos; 
        });
    
    EXPECT_TRUE(foundOriginalShard);
    EXPECT_TRUE(foundSplitShard);
}
```

## 2. Integration Testing Framework

### End-to-End Backup and Restore Testing

```cpp
class BackupRestoreIntegrationTest : public ::testing::Test {
protected:
    std::unique_ptr<TestCluster> cluster;
    std::unique_ptr<MockS3Storage> storage;
    
    void SetUp() override {
        cluster = std::make_unique<TestCluster>(3); // 3-node cluster
        storage = std::make_unique<MockS3Storage>();
        
        // Enable v3 backup system
        cluster->enableV3Backup(storage.get());
    }
    
    void populateTestData() {
        auto db = cluster->getDatabase();
        auto tr = db->createTransaction();
        
        // Write test data across multiple shards
        for (int i = 0; i < 10000; ++i) {
            std::string key = fmt::format("key-{:06d}", i);
            std::string value = fmt::format("value-{:06d}", i);
            tr->set(key, value);
            
            if (i % 100 == 99) {
                tr->commit().wait();
                tr = db->createTransaction();
            }
        }
        tr->commit().wait();
    }
};

TEST_F(BackupRestoreIntegrationTest, BasicBackupAndRestore) {
    // Step 1: Populate test data
    populateTestData();
    
    // Step 2: Wait for backup to capture data
    cluster->waitForBackupToReachVersion(cluster->getCurrentVersion());
    
    // Step 3: Clear a range
    KeyRange testRange("key-001000", "key-002000");
    cluster->clearRange(testRange);
    
    // Step 4: Restore the range
    Version restoreVersion = cluster->getCurrentVersion() - 1000; // Before clear
    cluster->restoreRange(testRange, restoreVersion);
    
    // Step 5: Verify data is restored
    auto db = cluster->getDatabase();
    auto tr = db->createTransaction();
    
    for (int i = 1000; i < 2000; ++i) {
        std::string key = fmt::format("key-{:06d}", i);
        std::string expectedValue = fmt::format("value-{:06d}", i);
        
        auto value = tr->get(key).wait();
        EXPECT_TRUE(value.present()) << "Key not found: " << key;
        EXPECT_EQ(value.get().toString(), expectedValue) << "Wrong value for key: " << key;
    }
}

TEST_F(BackupRestoreIntegrationTest, RestoreAfterShardSplit) {
    // Step 1: Populate initial data
    populateTestData();
    Version backupVersion = cluster->getCurrentVersion();
    
    // Step 2: Wait for backup
    cluster->waitForBackupToReachVersion(backupVersion);
    
    // Step 3: Force shard split
    cluster->forceSplitShard(KeyRange("key-001000", "key-009000"), "key-005000");
    
    // Step 4: Write more data to ensure split is processed
    populateTestData();
    
    // Step 5: Restore data from before the split
    KeyRange restoreRange("key-003000", "key-007000");
    cluster->restoreRange(restoreRange, backupVersion);
    
    // Step 6: Verify data integrity
    verifyRangeData(restoreRange, backupVersion);
}
```

### Concurrent Operations Testing

```cpp
class ConcurrentOperationsTest : public ::testing::Test {
protected:
    std::unique_ptr<TestCluster> cluster;
    
    void SetUp() override {
        cluster = std::make_unique<TestCluster>(5); // Larger cluster for concurrency
        cluster->enableV3Backup();
    }
};

TEST_F(ConcurrentOperationsTest, ConcurrentBackupAndShardOperations) {
    // Start background backup
    auto backupFuture = std::async(std::launch::async, [this]() {
        return cluster->runContinuousBackup(300); // 5 minutes
    });
    
    // Concurrent shard operations
    std::vector<std::future<void>> operations;
    
    // Start data writes
    operations.push_back(std::async(std::launch::async, [this]() {
        for (int i = 0; i < 100000; ++i) {
            cluster->writeRandomData();
            if (i % 1000 == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }));
    
    // Start shard splits
    operations.push_back(std::async(std::launch::async, [this]() {
        for (int i = 0; i < 10; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            cluster->forceSplitRandomShard();
        }
    }));
    
    // Start shard merges
    operations.push_back(std::async(std::launch::async, [this]() {
        for (int i = 0; i < 5; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(20));
            cluster->forceMergeAdjacentShards();
        }
    }));
    
    // Wait for all operations to complete
    for (auto& op : operations) {
        op.wait();
    }
    
    // Stop backup and verify consistency
    backupFuture.wait();
    cluster->validateBackupConsistency();
}
```

## 3. Performance Testing Framework

### Backup Performance Testing

```cpp
class BackupPerformanceTest : public ::testing::Test {
protected:
    std::unique_ptr<TestCluster> cluster;
    PerformanceMetrics metrics;
    
    void SetUp() override {
        cluster = std::make_unique<TestCluster>(10); // Large cluster
        cluster->enableV3Backup();
        metrics.reset();
    }
};

TEST_F(BackupPerformanceTest, ThroughputComparison) {
    const int duration_seconds = 300; // 5 minutes
    const int writes_per_second = 10000;
    
    // Test v2 backup performance
    cluster->enableV2Backup();
    auto v2Metrics = runPerformanceTest(duration_seconds, writes_per_second);
    
    // Test v3 backup performance
    cluster->enableV3Backup();
    auto v3Metrics = runPerformanceTest(duration_seconds, writes_per_second);
    
    // Compare results
    EXPECT_LE(v3Metrics.averageLatency, v2Metrics.averageLatency * 1.1) 
        << "v3 backup latency significantly worse than v2";
    
    EXPECT_GE(v3Metrics.throughput, v2Metrics.throughput * 0.9)
        << "v3 backup throughput significantly worse than v2";
    
    // Log detailed metrics
    LOG(INFO) << "v2 Metrics: " << v2Metrics.toString();
    LOG(INFO) << "v3 Metrics: " << v3Metrics.toString();
}

TEST_F(BackupPerformanceTest, RestorePerformanceImprovement) {
    // Set up test scenario
    populateLargeDataset(1000000); // 1M keys
    cluster->waitForBackupCompletion();
    
    // Test small range restore (expected to show dramatic improvement)
    KeyRange smallRange = selectRandomRange(4096); // 4KB range
    
    auto v2RestoreTime = measureV2RestoreTime(smallRange);
    auto v3RestoreTime = measureV3RestoreTime(smallRange);
    
    // v3 should be significantly faster for small ranges
    double improvement = v2RestoreTime / v3RestoreTime;
    EXPECT_GT(improvement, 100.0) << "Expected >100x improvement for small range restore";
    
    LOG(INFO) << fmt::format("Restore improvement: {}x (v2: {}s, v3: {}s)", 
                            improvement, v2RestoreTime, v3RestoreTime);
}

struct PerformanceMetrics {
    double averageLatency;
    double p99Latency;
    double throughput;
    uint64_t totalOperations;
    std::vector<double> latencyDistribution;
    
    std::string toString() const {
        return fmt::format("avg_lat: {:.2f}ms, p99_lat: {:.2f}ms, "
                          "throughput: {:.0f} ops/s, total: {}", 
                          averageLatency, p99Latency, throughput, totalOperations);
    }
};
```

### Scalability Testing

```cpp
class ScalabilityTest : public ::testing::Test {
public:
    void testShardScaling() {
        std::vector<int> shardCounts = {1000, 10000, 100000, 250000};
        
        for (int shardCount : shardCounts) {
            auto cluster = createClusterWithShards(shardCount);
            cluster->enableV3Backup();
            
            // Measure backup performance
            auto metrics = measureBackupPerformance(cluster.get(), 60); // 1 minute test
            
            // Measure restore lookup performance
            auto lookupMetrics = measureRestoreLookupPerformance(cluster.get(), 1000);
            
            LOG(INFO) << fmt::format("Shards: {}, Backup: {}, Lookup: {:.2f}ms", 
                                    shardCount, metrics.toString(), lookupMetrics.averageTime);
            
            // Performance should scale linearly or better
            EXPECT_LT(lookupMetrics.averageTime, 10.0) << "Lookup time too high for " << shardCount << " shards";
        }
    }
};
```

## 4. Correctness Testing Framework

### Data Integrity Testing

```cpp
class DataIntegrityTest : public ::testing::Test {
protected:
    std::unique_ptr<TestCluster> cluster;
    
    void SetUp() override {
        cluster = std::make_unique<TestCluster>(5);
        cluster->enableV3Backup();
    }
};

TEST_F(DataIntegrityTest, CompleteDatasetIntegrity) {
    // Create a known dataset with checksums
    DatasetGenerator generator;
    auto dataset = generator.createChecksummedDataset(100000);
    
    // Write dataset to cluster
    cluster->writeDataset(dataset);
    Version backupVersion = cluster->getCurrentVersion();
    cluster->waitForBackupToReachVersion(backupVersion);
    
    // Clear all data
    cluster->clearAllData();
    
    // Restore from backup
    cluster->restoreAllData(backupVersion);
    
    // Verify complete dataset integrity
    auto restoredDataset = cluster->readAllData();
    EXPECT_TRUE(generator.validateChecksums(restoredDataset))
        << "Dataset integrity check failed after restore";
    
    EXPECT_EQ(dataset.size(), restoredDataset.size())
        << "Dataset size mismatch after restore";
}

TEST_F(DataIntegrityTest, PartialRestoreIntegrity) {
    // Create test data
    auto dataset = createLargeDataset(1000000);
    cluster->writeDataset(dataset);
    Version backupVersion = cluster->getCurrentVersion();
    cluster->waitForBackupToReachVersion(backupVersion);
    
    // Test multiple partial restores
    for (int i = 0; i < 100; ++i) {
        KeyRange randomRange = selectRandomRange(dataset);
        
        // Clear range
        cluster->clearRange(randomRange);
        
        // Restore range
        cluster->restoreRange(randomRange, backupVersion);
        
        // Verify range integrity
        auto rangeData = cluster->readRange(randomRange);
        auto expectedData = getExpectedDataForRange(dataset, randomRange);
        
        EXPECT_EQ(rangeData, expectedData) 
            << "Range integrity check failed for range " << randomRange.toString();
    }
}
```

### Mutation Ordering Testing

```cpp
class MutationOrderingTest : public ::testing::Test {
public:
    TEST_F(MutationOrderingTest, VersionOrderingPreserved) {
        // Create ordered mutations
        std::vector<OrderedMutation> mutations;
        for (int i = 0; i < 10000; ++i) {
            mutations.push_back({
                .version = 1000 + i,
                .key = fmt::format("key-{:06d}", i % 100), // Overlapping keys
                .value = fmt::format("value-{}", i)
            });
        }
        
        // Apply mutations
        cluster->applyMutations(mutations);
        cluster->waitForBackupCompletion();
        
        // Restore and verify ordering
        for (int testRun = 0; testRun < 10; ++testRun) {
            KeyRange testRange = selectRandomRange();
            cluster->clearRange(testRange);
            cluster->restoreRange(testRange, 1000 + 9999);
            
            // Verify final values are from latest mutations
            auto finalData = cluster->readRange(testRange);
            for (const auto& kv : finalData) {
                auto expectedValue = getLatestValueForKey(mutations, kv.key);
                EXPECT_EQ(kv.value, expectedValue)
                    << "Mutation ordering violation for key " << kv.key;
            }
        }
    }
};
```

## 5. Chaos Testing Framework

### Fault Injection Testing

```cpp
class ChaosTest : public ::testing::Test {
protected:
    std::unique_ptr<ChaosCluster> cluster;
    
    void SetUp() override {
        cluster = std::make_unique<ChaosCluster>(7); // Larger cluster for fault tolerance
        cluster->enableV3Backup();
        cluster->enableChaosMode();
    }
};

TEST_F(ChaosTest, NetworkPartitions) {
    // Start continuous backup
    cluster->startContinuousBackup();
    
    // Inject network partitions
    cluster->injectNetworkPartition(0.1); // 10% packet loss
    
    // Continue operations for 10 minutes
    cluster->runChaosOperations(600);
    
    // Heal network
    cluster->healNetworkPartition();
    
    // Verify backup consistency
    cluster->validateBackupConsistency();
    
    // Test restore after chaos
    KeyRange testRange = cluster->selectRandomRange();
    Version restoreVersion = cluster->getRandomVersion();
    cluster->restoreRange(testRange, restoreVersion);
    cluster->validateRangeIntegrity(testRange, restoreVersion);
}

TEST_F(ChaosTest, NodeFailures) {
    cluster->startContinuousBackup();
    
    // Randomly kill and restart nodes
    for (int i = 0; i < 20; ++i) {
        cluster->killRandomNode();
        std::this_thread::sleep_for(std::chrono::seconds(30));
        cluster->restartKilledNode();
        std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    
    cluster->validateBackupConsistency();
}

TEST_F(ChaosTest, DiskFailures) {
    cluster->startContinuousBackup();
    
    // Simulate disk failures
    cluster->injectDiskFailures(0.05); // 5% write failure rate
    
    cluster->runChaosOperations(300);
    
    cluster->healDiskFailures();
    cluster->validateBackupConsistency();
}
```

## 6. Migration Testing Framework

### v2 to v3 Migration Testing

```cpp
class MigrationTest : public ::testing::Test {
protected:
    std::unique_ptr<TestCluster> cluster;
    
    void SetUp() override {
        cluster = std::make_unique<TestCluster>(5);
        // Start with v2 backup system
        cluster->enableV2Backup();
    }
};

TEST_F(MigrationTest, GradualMigration) {
    // Phase 1: Create v2 backup data
    auto dataset = createLargeDataset(500000);
    cluster->writeDataset(dataset);
    cluster->waitForV2BackupCompletion();
    
    // Phase 2: Enable dual mode
    cluster->enableDualMode();
    
    // Phase 3: Continue operations in dual mode
    auto additionalData = createLargeDataset(100000);
    cluster->writeDataset(additionalData);
    cluster->waitForDualModeSync();
    
    // Phase 4: Switch to v3 only
    cluster->switchToV3Only();
    
    // Phase 5: Verify both v2 and v3 restores work
    KeyRange v2Range = selectRangeFromDataset(dataset);
    KeyRange v3Range = selectRangeFromDataset(additionalData);
    
    // Test v2 restore (should still work)
    cluster->clearRange(v2Range);
    cluster->restoreRangeV2(v2Range, getVersionForDataset(dataset));
    validateRangeData(v2Range, dataset);
    
    // Test v3 restore (should work better)
    cluster->clearRange(v3Range);
    cluster->restoreRangeV3(v3Range, getVersionForDataset(additionalData));
    validateRangeData(v3Range, additionalData);
}

TEST_F(MigrationTest, RollbackCapability) {
    // Start migration
    cluster->enableDualMode();
    
    // Generate data in dual mode
    auto dataset = createLargeDataset(100000);
    cluster->writeDataset(dataset);
    
    // Simulate v3 failure requiring rollback
    cluster->simulateV3Failure();
    cluster->rollbackToV2();
    
    // Verify v2 backup is still functional
    cluster->validateV2BackupFunctionality();
    
    // Verify data integrity after rollback
    auto currentData = cluster->readAllData();
    validateDatasetIntegrity(currentData, dataset);
}
```

## 7. Test Execution Framework

### Automated Test Suite

```cpp
class BackupV3TestSuite {
public:
    static void runAllTests() {
        // Unit tests
        runUnitTests();
        
        // Integration tests
        runIntegrationTests();
        
        // Performance tests
        runPerformanceTests();
        
        // Correctness tests
        runCorrectnessTests();
        
        // Chaos tests
        runChaosTests();
        
        // Migration tests
        runMigrationTests();
        
        // Generate test report
        generateTestReport();
    }
    
private:
    static void runUnitTests() {
        ::testing::InitGoogleTest();
        auto result = RUN_ALL_TESTS();
        EXPECT_EQ(result, 0) << "Unit tests failed";
    }
    
    static void generateTestReport() {
        TestReport report{
            .timestamp = std::chrono::system_clock::now(),
            .totalTests = getTotalTestCount(),
            .passedTests = getPassedTestCount(),
            .failedTests = getFailedTestCount(),
            .performanceMetrics = getPerformanceResults(),
            .coverageMetrics = getCoverageResults()
        };
        
        report.writeToFile("backup_v3_test_report.json");
        report.generateHTMLReport("backup_v3_test_report.html");
    }
};
```

### Continuous Integration Pipeline

```yaml
# .github/workflows/backup-v3-tests.yml
name: FoundationDB Backup v3 Tests

on:
  push:
    paths:
      - 'fdbserver/backup/**'
      - 'fdbclient/backup/**'
  pull_request:
    paths:
      - 'fdbserver/backup/**'

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build FoundationDB
        run: make -j$(nproc)
      - name: Run Unit Tests
        run: ./bin/backup_v3_unit_tests
        
  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v3
      - name: Setup Test Cluster
        run: ./scripts/setup_test_cluster.sh
      - name: Run Integration Tests
        run: ./bin/backup_v3_integration_tests
        timeout-minutes: 60
        
  performance-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v3
      - name: Setup Performance Cluster
        run: ./scripts/setup_performance_cluster.sh
      - name: Run Performance Tests
        run: ./bin/backup_v3_performance_tests
      - name: Upload Performance Results
        uses: actions/upload-artifact@v3
        with:
          name: performance-results
          path: performance_results.json
          
  chaos-tests:
    runs-on: ubuntu-latest
    needs: integration-tests
    steps:
      - uses: actions/checkout@v3
      - name: Setup Chaos Cluster
        run: ./scripts/setup_chaos_cluster.sh
      - name: Run Chaos Tests
        run: ./bin/backup_v3_chaos_tests
        timeout-minutes: 120
```

## 8. Test Coverage Metrics

### Coverage Requirements

```cpp
class CoverageValidator {
public:
    struct CoverageMetrics {
        double lineCoverage;
        double branchCoverage;
        double functionCoverage;
        std::map<std::string, double> componentCoverage;
    };
    
    static void validateCoverage() {
        auto metrics = generateCoverageReport();
        
        // Minimum coverage requirements
        EXPECT_GE(metrics.lineCoverage, 0.85) << "Line coverage below 85%";
        EXPECT_GE(metrics.branchCoverage, 0.80) << "Branch coverage below 80%";
        EXPECT_GE(metrics.functionCoverage, 0.90) << "Function coverage below 90%";
        
        // Component-specific requirements
        EXPECT_GE(metrics.componentCoverage["ShardIdGenerator"], 0.95);
        EXPECT_GE(metrics.componentCoverage["ShardEvolutionManager"], 0.90);
        EXPECT_GE(metrics.componentCoverage["RestoreLookupService"], 0.85);
        EXPECT_GE(metrics.componentCoverage["BackupWorker"], 0.80);
    }
};
```

This comprehensive testing framework ensures the Progressive Shard ID System is thoroughly validated across all dimensions: functionality, performance, correctness, fault tolerance, and migration safety.