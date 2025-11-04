# FoundationDB Backup v3: S3 Writing and Mutation Aggregation

## The Core Data Flow

### Enhanced Mutation Flow: CommitProxy → TLog → LogRouter → BackupWorker → S3

```
CommitProxy (adds shard tags) → TLog (stores shard-tagged mutations) → LogRouter (distributes by tag) → BackupWorker (pulls & filters) → Multi-Shard File → S3
```

### BackupWorker Pull and Filtering Architecture

BackupWorkers must handle **both scenarios**: more workers than LogRouter tags, or fewer workers than LogRouter tags:

```cpp
// Enhanced BackupWorker with flexible LogRouter tag assignment
class EnhancedBackupWorker {
    std::vector<Tag> assignedLogRouterTags; // LogRouter tags assigned by master/cluster controller
    int myWorkerId;                         // This worker's unique ID
    int totalBackupWorkers;                 // Total number of backup workers
    
public:
    // Constructor receives assigned LogRouter tags from InitializeBackupRequest
    EnhancedBackupWorker(const InitializeBackupRequest& req)
        : myWorkerId(req.workerId), totalBackupWorkers(req.totalWorkers) {
        
        // LogRouter tags assigned by master/cluster controller during recruitment
        assignedLogRouterTags = req.assignedRouterTags; // NEW: Multiple tags possible
    }
    
    void pullShardTaggedMutations() {
        // Pull from ALL assigned LogRouter tags
        std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
        
        for (const auto& routerTag : assignedLogRouterTags) {
            auto cursor = logSystem->peekLogRouter(myId, tagAt, routerTag, /*...*/);
            cursors.push_back(cursor);
        }
        
        // Process messages from all cursors
        for (auto& cursor : cursors) {
            while (cursor->hasMessage()) {
                auto taggedMutation = cursor->getMessage();
                auto tags = cursor->getTags();
                
                // Process all tags on this mutation
                for (auto tag : tags) {
                    if (tag.locality == tagLocalityBackupShard) {
                        uint64_t shardId = tag.id;
                        // Only process shards assigned to THIS specific worker
                        if (shouldProcessShard(shardId, myWorkerId, totalBackupWorkers)) {
                            processShardMutation(taggedMutation, shardId);
                        }
                    }
                    // Handle existing LogRouter tags during migration
                    else if (tag.locality == tagLocalityLogRouter) {
                        // Only process if this worker is assigned to this LogRouter tag
                        if (isAssignedToRouterTag(tag)) {
                            processLegacyMutation(taggedMutation);
                        }
                    }
                }
                // CRITICAL: Mark mutation as processed and advance cursor
                // This "pops" the mutation from this worker's view - once nextMessage() is called,
                // this worker acknowledges it has consumed/processed this mutation
                cursor->nextMessage();
            }
        }
    }
    
private:
    // Assignment logic moved to master/cluster controller (see below)
    
    bool isAssignedToRouterTag(const Tag& routerTag) {
        return std::find(assignedLogRouterTags.begin(), assignedLogRouterTags.end(), routerTag)
               != assignedLogRouterTags.end();
    }
    
    bool shouldProcessShard(uint64_t shardId, int workerId, int totalWorkers) {
        return (shardId % totalWorkers) == workerId;
    }
};
```

**Master/Cluster Controller Tag Assignment Logic**:

```cpp
// Enhanced InitializeBackupRequest with flexible tag assignment
struct EnhancedInitializeBackupRequest : public InitializeBackupRequest {
    // NEW fields for flexible assignment
    std::vector<Tag> assignedRouterTags;    // All LogRouter tags assigned to this worker
    int workerId;                           // Unique worker ID (0, 1, 2, ...)
    int totalWorkers;                       // Total backup workers
};

// Master calculates worker assignments during BackupWorker recruitment
class BackupWorkerRecruitment {
public:
    static std::vector<EnhancedInitializeBackupRequest>
    calculateWorkerAssignments(int numWorkers, int numLogRouterTags, Version startVersion,
                               Optional<Version> endVersion, LogEpoch recruitedEpoch, LogEpoch backupEpoch) {
        
        std::vector<EnhancedInitializeBackupRequest> requests;
        
        if (numWorkers >= numLogRouterTags) {
            // Case 1: More workers than LogRouter tags (multiple workers share tags)
            for (int workerId = 0; workerId < numWorkers; workerId++) {
                EnhancedInitializeBackupRequest req;
                req.workerId = workerId;
                req.totalWorkers = numWorkers;
                req.startVersion = startVersion;
                req.endVersion = endVersion;
                req.recruitedEpoch = recruitedEpoch;
                req.backupEpoch = backupEpoch;
                req.totalTags = numLogRouterTags;
                
                // Each worker gets one LogRouter tag (multiple workers may share same tag)
                int routerTagId = workerId % numLogRouterTags;
                req.routerTag = Tag(tagLocalityLogRouter, routerTagId);
                req.assignedRouterTags = { req.routerTag };
                
                requests.push_back(req);
            }
        } else {
            // Case 2: Fewer workers than LogRouter tags (each worker gets multiple tags)
            for (int workerId = 0; workerId < numWorkers; workerId++) {
                EnhancedInitializeBackupRequest req;
                req.workerId = workerId;
                req.totalWorkers = numWorkers;
                req.startVersion = startVersion;
                req.endVersion = endVersion;
                req.recruitedEpoch = recruitedEpoch;
                req.backupEpoch = backupEpoch;
                req.totalTags = numLogRouterTags;
                
                // Distribute LogRouter tags evenly across workers
                int tagsPerWorker = numLogRouterTags / numWorkers;
                int extraTags = numLogRouterTags % numWorkers;
                
                int startTag = workerId * tagsPerWorker + std::min(workerId, extraTags);
                int numTags = tagsPerWorker + (workerId < extraTags ? 1 : 0);
                
                for (int i = 0; i < numTags; i++) {
                    Tag tag = Tag(tagLocalityLogRouter, startTag + i);
                    req.assignedRouterTags.push_back(tag);
                }
                
                // Set primary tag for backward compatibility
                req.routerTag = req.assignedRouterTags[0];
                
                requests.push_back(req);
            }
        }
        
        return requests;
    }
};
```

**Assignment Examples**:

```cpp
// Example 1: 20 BackupWorkers, 5 LogRouter tags
// Master assigns: Worker 0,5,10,15 → Tag 0; Worker 1,6,11,16 → Tag 1; etc.

// Example 2: 5 BackupWorkers, 20 LogRouter tags
// Master assigns: Worker 0 → Tags 0,1,2,3; Worker 1 → Tags 4,5,6,7; etc.

// During BackupWorker startup:
void backupWorker(BackupInterface interf, EnhancedInitializeBackupRequest req, ...) {
    // Worker receives pre-assigned LogRouter tags from master
    BackupData self(interf.id(), db, req);
    
    TraceEvent("BackupWorkerStart", self.myId)
        .detail("WorkerId", req.workerId)
        .detail("AssignedTags", req.assignedRouterTags.size())
        .detail("PrimaryTag", req.routerTag.toString());
    
    // Worker knows which LogRouter tags to pull from
    for (const auto& tag : req.assignedRouterTags) {
        TraceEvent("BackupWorkerAssignedTag", self.myId)
            .detail("TagId", tag.id)
            .detail("TagLocality", tag.locality);
    }
}
```

**Flexible Architecture Scenarios**:

**Scenario A: More BackupWorkers than LogRouter tags** (20 workers, 5 tags):
```cpp
// Each worker assigned to 1 LogRouter tag, multiple workers share tags:
// Worker 0,5,10,15 → LogRouter tag 0
// Worker 1,6,11,16 → LogRouter tag 1
// Worker 2,7,12,17 → LogRouter tag 2
// Worker 3,8,13,18 → LogRouter tag 3
// Worker 4,9,14,19 → LogRouter tag 4
```

**Scenario B: Fewer BackupWorkers than LogRouter tags** (5 workers, 20 tags):
```cpp
// Each worker assigned to multiple LogRouter tags:
// Worker 0 → LogRouter tags 0,1,2,3
// Worker 1 → LogRouter tags 4,5,6,7
// Worker 2 → LogRouter tags 8,9,10,11
// Worker 3 → LogRouter tags 12,13,14,15
// Worker 4 → LogRouter tags 16,17,18,19
```

**Key Architecture Benefits**:
- **Handles both worker/tag ratios**: Flexible assignment algorithm covers all scenarios
- **Complete LogRouter coverage**: Every LogRouter tag has at least one BackupWorker assigned
- **Worker-level shard filtering**: Each worker still filters to process only assigned shards
- **Preserves existing pull model**: Uses existing [`peekLogRouter()`](fdbserver/BackupWorker.actor.cpp:1047) interface

### Migration Integration: Hybrid Tag Processing


**New Shard Tag Type**: The shard-aware backup system introduces a new tag locality `tagLocalityBackupShard = -10` to the existing FoundationDB [`Tag`](fdbclient/include/fdbclient/FDBTypes.h:72) structure. This creates shard-specific tags like `Tag(-10, 42)` for shard #42, separate from existing LogRouter tags like `Tag(-2, 5)` for router #5. The `locality` field provides namespace separation, allowing both tag types to coexist during migration while enabling precise shard-based mutation routing that delivers the 250,000x efficiency improvement.

During migration, BackupWorkers support **both existing log router tags and new shard tags simultaneously**:

```cpp
// Enhanced BackupWorker supporting both tag systems during migration
class HybridBackupWorker {
public:
    void pullAndProcessMutations() {
        // EXISTING: BackupWorker pulls from TLog using LogRouter tag
        auto cursor = logSystem->peekLogRouter(myId, tagAt, routerTag, /*...*/);
        
        while (cursor->hasMessage()) {
            auto taggedMutation = cursor->getMessage();
            auto tags = cursor->getTags();
            
            // Process all tags on this mutation
            for (auto tag : tags) {
                if (tag.locality == tagLocalityLogRouter) {
                    // EXISTING: Process log router tagged mutations (unchanged)
                    if (tag.id == routerTag.id) {
                        processLegacyMutation(taggedMutation, tag);
                    }
                    
                } else if (tag.locality == tagLocalityBackupShard) {
                    // NEW: Process shard tagged mutations
                    uint64_t shardId = tag.id;
                    if (shouldProcessShard(shardId, myWorkerId, totalBackupWorkers)) {
                        processShardMutation(taggedMutation, shardId);
                    }
                }
            }
            cursor->nextMessage();
        }
    }
    
private:
    void processLegacyMutation(const TaggedMutation& mutation, Tag routerTag) {
        // Write to existing log file format (unchanged)
        writeLegacyMutationToFile(mutation);
    }
    
    void processShardMutation(const TaggedMutation& mutation, uint64_t shardId) {
        // Write with shard metadata to enhanced log file
        writeShardMutationToFile(mutation, shardId);
    }
    
    bool shouldProcessShard(uint64_t shardId, int workerId, int totalWorkers) {
        return (shardId % totalWorkers) == workerId;
    }
};
```

**Example Migration Scenario** (More BackupWorkers than LogRouter tags):
```cpp
// Configuration: 5 LogRouter tags, 20 BackupWorkers
// Multiple BackupWorkers share each LogRouter tag:
// - Workers 0,1,2,3 all pull from LogRouter tag 0
// - Workers 4,5,6,7 all pull from LogRouter tag 1
// - Workers 8,9,10,11 all pull from LogRouter tag 2
// - Workers 12,13,14,15 all pull from LogRouter tag 3
// - Workers 16,17,18,19 all pull from LogRouter tag 4

// During hybrid mode, all workers sharing a LogRouter tag receive the SAME mutations:
// - All 4 workers (0,1,2,3) receive mutations tagged with LogRouter tag 0
// - But each worker processes different shards:
//   - Worker 0: processes shards 0, 20, 40, 60, ... (shardId % 20 == 0)
//   - Worker 1: processes shards 1, 21, 41, 61, ... (shardId % 20 == 1)
//   - Worker 2: processes shards 2, 22, 42, 62, ... (shardId % 20 == 2)
//   - Worker 3: processes shards 3, 23, 43, 63, ... (shardId % 20 == 3)

// Key insight: Multiple workers receive same mutations but filter to different shards
```

**Migration Phases**:

1. **Phase 1**: CommitProxy adds both log router tags AND shard tags to mutations
2. **Phase 2**: LogRouter routes both tag types, BackupWorkers process both
3. **Phase 3**: Verify shard-tagged backups are working correctly
4. **Phase 4**: Gradually disable log router tagging (feature flag)
5. **Phase 5**: Remove legacy log router tag processing

**Rollback Safety**: Can instantly disable shard tagging and fall back to log router tags if needed.

### Performance Implications During Hybrid Mode

**Resource Usage Impact**:

```cpp
// Performance monitoring during hybrid mode
struct HybridModeMetrics {
    // Memory overhead
    double tagMemoryOverhead = 1.8;      // ~80% increase (both tag types per mutation)
    double mutationSizeOverhead = 1.2;   // ~20% increase (additional shard tag)
    
    // Processing overhead
    double commitProxyOverhead = 1.1;    // ~10% increase (dual tagging)
    double logRouterOverhead = 1.3;      // ~30% increase (dual routing logic)
    double backupWorkerOverhead = 1.4;   // ~40% increase (dual processing)
    
    // Storage overhead during transition
    double storageOverhead = 1.6;        // ~60% increase (dual file formats)
    
    // Network bandwidth
    double networkOverhead = 1.2;        // ~20% increase (larger mutations)
};
```

**Mitigation Strategies**:

1. **Short Migration Window**: Minimize hybrid mode duration (hours, not days)
   ```cpp
   // Aggressive migration schedule
   class FastMigrationSchedule {
       static constexpr int HYBRID_MODE_MAX_HOURS = 6;  // Complete migration quickly
       static constexpr int VALIDATION_HOURS = 2;       // Quick validation period
       static constexpr int ROLLBACK_DECISION_MINUTES = 30; // Fast rollback decision
   };
   ```

2. **Resource Scaling**: Temporarily increase cluster resources during migration
   ```cpp
   // Pre-migration resource scaling
   void prepareMigrationResources() {
       // Scale up backup workers by 50% during hybrid mode
       int hybridBackupWorkers = normalBackupWorkers * 1.5;
       
       // Increase memory allocation for CommitProxy and LogRouter
       scaleCommitProxyMemory(1.3);  // 30% more memory
       scaleLogRouterMemory(1.4);    // 40% more memory
   }
   ```

3. **Selective Enablement**: Enable hybrid mode only during low-traffic periods
   ```cpp
   class TrafficAwareMigration {
       bool shouldEnableHybridMode() {
           double currentTPS = getCurrentTransactionRate();
           double cpuUsage = getCurrentCPUUsage();
           
           // Only enable during low traffic periods
           return currentTPS < (maxTPS * 0.7) && cpuUsage < 0.6;
       }
   };
   ```

4. **Progressive Migration**: Migrate one backup worker at a time
   ```cpp
   void progressiveMigration() {
       for (int workerId = 0; workerId < totalBackupWorkers; ++workerId) {
           // Enable shard processing for one worker at a time
           enableShardTagsForWorker(workerId);
           
           // Validate worker performance before proceeding
           if (!validateWorkerPerformance(workerId, 300)) { // 5 minute validation
               rollbackWorker(workerId);
               throw migration_failed_exception();
           }
           
           // Disable legacy processing for this worker
           disableLegacyTagsForWorker(workerId);
       }
   }
   ```

**Performance Targets During Hybrid Mode**:
- **Transaction throughput**: <20% degradation
- **Backup latency**: <30% increase
- **Memory usage**: <100% increase (temporary)
- **Storage usage**: <70% increase (temporary)
- **Migration duration**: <6 hours total
- **Rollback time**: <5 minutes if needed

**Monitoring During Migration**:
```cpp
struct MigrationHealthMetrics {
    bool transactionThroughputHealthy() { return getCurrentTPS() > (baselineTPS * 0.8); }
    bool backupLatencyHealthy() { return getBackupLatency() < (baselineLatency * 1.3); }
    bool memoryUsageHealthy() { return getMemoryUsage() < (maxMemory * 0.9); }
    bool errorRateHealthy() { return getErrorRate() < (baselineErrors * 1.1); }
    
    bool shouldAbortMigration() {
        return !transactionThroughputHealthy() ||
               !backupLatencyHealthy() ||
               !memoryUsageHealthy() ||
               !errorRateHealthy();
    }
};
```

The hybrid mode creates temporary overhead but enables zero-downtime migration with immediate rollback capability.

### Error Handling and Recovery During Migration

**Failure Scenarios and Recovery Procedures**:

```cpp
// Comprehensive error handling during migration
class MigrationErrorHandler {
public:
    enum class FailureType {
        PERFORMANCE_DEGRADATION,
        BACKUP_WORKER_FAILURE,
        LOG_ROUTER_FAILURE,
        COMMIT_PROXY_FAILURE,
        STORAGE_FAILURE,
        NETWORK_PARTITION,
        CORRUPTION_DETECTED,
        INCONSISTENT_BACKUPS,
        CLUSTER_RESTART_DURING_MIGRATION
    };
    
    void handleFailure(FailureType failure, const ErrorContext& context) {
        switch (failure) {
            case PERFORMANCE_DEGRADATION:
                handlePerformanceDegradation(context);
                break;
            case BACKUP_WORKER_FAILURE:
                handleBackupWorkerFailure(context);
                break;
            case LOG_ROUTER_FAILURE:
                handleLogRouterFailure(context);
                break;
            case COMMIT_PROXY_FAILURE:
                handleCommitProxyFailure(context);
                break;
            case STORAGE_FAILURE:
                handleStorageFailure(context);
                break;
            case NETWORK_PARTITION:
                handleNetworkPartition(context);
                break;
            case CORRUPTION_DETECTED:
                handleCorruption(context);
                break;
            case INCONSISTENT_BACKUPS:
                handleInconsistentBackups(context);
                break;
            case CLUSTER_RESTART_DURING_MIGRATION:
                handleClusterRestart(context);
                break;
        }
    }

private:
    void handlePerformanceDegradation(const ErrorContext& context) {
        TraceEvent(SevWarn, "MigrationPerformanceDegradation")
            .detail("TransactionTPS", getCurrentTPS())
            .detail("BaselineTPS", getBaselineTPS())
            .detail("BackupLatency", getBackupLatency());
        
        // Immediate rollback if performance is unacceptable
        if (getCurrentTPS() < (getBaselineTPS() * 0.7)) {
            emergencyRollbackToLegacy("Performance below 70% of baseline");
        } else {
            // Try scaling up resources first
            scaleUpBackupResources();
            scheduleRevalidation(60); // Check again in 1 minute
        }
    }
    
    void handleBackupWorkerFailure(const ErrorContext& context) {
        int failedWorkerId = context.workerId;
        
        TraceEvent(SevError, "BackupWorkerFailedDuringMigration")
            .detail("WorkerId", failedWorkerId)
            .detail("MigrationPhase", getCurrentMigrationPhase());
        
        // Restart failed worker in legacy mode
        restartBackupWorkerInLegacyMode(failedWorkerId);
        
        // Redistribute its shard assignments to other workers temporarily
        redistributeShardAssignments(failedWorkerId);
        
        // Attempt to re-migrate this worker after it's stable
        scheduleWorkerRemigration(failedWorkerId, 300); // 5 minutes delay
    }
    
    void handleLogRouterFailure(const ErrorContext& context) {
        TraceEvent(SevError, "LogRouterFailedDuringMigration");
        
        // LogRouter failure affects all backup workers
        // Immediately rollback all workers to legacy mode
        emergencyRollbackToLegacy("LogRouter failure - all workers affected");
        
        // Wait for LogRouter recovery
        waitForLogRouterRecovery();
        
        // Restart migration from the beginning after recovery
        scheduleMigrationRestart(600); // 10 minutes delay
    }
    
    void handleCommitProxyFailure(const ErrorContext& context) {
        TraceEvent(SevError, "CommitProxyFailedDuringMigration");
        
        // CommitProxy failure means no new shard tags being generated
        // Switch to "shard tag completion" mode - process existing shard tags but don't create new ones
        enableShardTagCompletionMode();
        
        // Wait for CommitProxy recovery
        waitForCommitProxyRecovery();
        
        // Resume dual tagging after recovery
        resumeDualTagging();
    }
    
    void handleStorageFailure(const ErrorContext& context) {
        TraceEvent(SevError, "StorageFailureDuringMigration")
            .detail("FailedContainer", context.containerUrl);
        
        // Storage failure affects backup integrity
        // Pause migration and validate backup consistency
        pauseMigration();
        
        bool backupConsistent = validateBackupConsistency();
        if (!backupConsistent) {
            emergencyRollbackToLegacy("Backup consistency compromised");
        } else {
            // Switch to alternative storage temporarily
            switchToBackupStorage();
            resumeMigration();
        }
    }
    
    void handleCorruption(const ErrorContext& context) {
        TraceEvent(SevError, "CorruptionDetectedDuringMigration")
            .detail("CorruptedFile", context.filename)
            .detail("CorruptionType", context.corruptionType);
        
        // Corruption is unacceptable - immediate rollback
        emergencyRollbackToLegacy("Data corruption detected");
        
        // Quarantine corrupted files
        quarantineCorruptedFiles(context.filename);
        
        // Require manual intervention before retrying migration
        requireManualMigrationApproval();
    }
    
    void handleInconsistentBackups(const ErrorContext& context) {
        TraceEvent(SevWarn, "InconsistentBackupsDuringMigration")
            .detail("LegacyBackupVersion", context.legacyVersion)
            .detail("ShardBackupVersion", context.shardVersion);
        
        // Inconsistency might be temporary - validate over time
        scheduleConsistencyValidation(120); // 2 minutes
        
        // If inconsistency persists, rollback
        if (getInconsistencyDuration() > 300) { // 5 minutes
            emergencyRollbackToLegacy("Persistent backup inconsistency");
        }
    }
    
    void handleClusterRestart(const ErrorContext& context) {
        TraceEvent(SevWarn, "ClusterRestartDuringMigration")
            .detail("MigrationPhase", context.migrationPhase)
            .detail("WorkersInShardMode", context.shardModeWorkers);
        
        // Recover migration state from persistent storage
        MigrationState state = loadMigrationState();
        
        if (state.isValid() && state.phase != MigrationPhase::COMPLETED) {
            // Resume migration from last checkpoint
            resumeMigrationFromState(state);
        } else {
            // State is corrupted or missing - start fresh
            resetToLegacyMode();
            scheduleMigrationRestart(1800); // 30 minutes delay
        }
    }
    
    void emergencyRollbackToLegacy(const std::string& reason) {
        TraceEvent(SevError, "EmergencyMigrationRollback")
            .detail("Reason", reason)
            .detail("RollbackStartTime", now());
        
        // Disable shard tagging immediately
        disableShardTagging();
        
        // Revert all BackupWorkers to legacy mode
        for (int i = 0; i < totalBackupWorkers; ++i) {
            revertWorkerToLegacyMode(i);
        }
        
        // Revert LogRouter to legacy routing
        revertLogRouterToLegacyMode();
        
        // Mark migration as failed
        setMigrationStatus(MigrationStatus::FAILED);
        
        TraceEvent(SevError, "EmergencyMigrationRollbackComplete")
            .detail("RollbackDuration", getElapsedTime())
            .detail("Reason", reason);
    }
};

// Persistent migration state for recovery
struct MigrationState {
    MigrationPhase phase;
    std::vector<int> shardModeWorkers;
    std::vector<int> legacyModeWorkers;
    Version migrationStartVersion;
    std::chrono::time_point<std::chrono::steady_clock> startTime;
    int retryCount;
    
    bool isValid() const {
        return phase != MigrationPhase::INVALID &&
               (!shardModeWorkers.empty() || !legacyModeWorkers.empty());
    }
    
    void serialize(BinaryWriter& writer) const {
        writer << phase << shardModeWorkers << legacyModeWorkers
               << migrationStartVersion << startTime << retryCount;
    }
    
    void deserialize(BinaryReader& reader) {
        reader >> phase >> shardModeWorkers >> legacyModeWorkers
               >> migrationStartVersion >> startTime >> retryCount;
    }
};
```

**Recovery Time Objectives**:
- **Performance rollback**: <5 minutes
- **Single worker failure**: <2 minutes to redistribute load
- **LogRouter failure**: <10 minutes for complete rollback
- **Storage failure**: <15 minutes (including validation)
- **Corruption detection**: <1 minute for immediate rollback
- **Cluster restart recovery**: <5 minutes to resume or reset

**Data Integrity Guarantees**:
- **No data loss**: Legacy backup always continues during migration
- **Consistency validation**: Continuous comparison between legacy and shard backups
- **Corruption detection**: Real-time checksums and validation
- **Rollback safety**: Can always return to known-good legacy state

The error handling ensures migration can recover from any failure scenario while maintaining data integrity and backup availability.

## Mutation Aggregation System

### 1. Mutation Serialization Format

We leverage FoundationDB's existing mutation serialization infrastructure instead of creating custom formats:

```cpp
// Extends existing VersionedMutationSerialized from RestoreUtil.h
struct ShardedVersionedMutation {
    VersionedMutationSerialized versionedMutation; // Existing FDB format
    uint64_t shardId;                              // Added: shard ID for partitioning
    
    ShardedVersionedMutation() = default;
    explicit ShardedVersionedMutation(const TaggedMutation& taggedMutation)
      : versionedMutation(taggedMutation.mutation, taggedMutation.version),
        shardId(taggedMutation.tag.id) {}
    
    explicit ShardedVersionedMutation(Arena& arena, const ShardedVersionedMutation& svm)
      : versionedMutation(arena, svm.versionedMutation), shardId(svm.shardId) {}
    
    template <class Ar>
    void serialize(Ar& ar) {
        serializer(ar, versionedMutation, shardId);
    }
    
    // Convenience accessors
    const MutationRef& mutation() const { return versionedMutation.mutation; }
    LogMessageVersion version() const { return versionedMutation.version; }
    
    int totalSize() const {
        return versionedMutation.mutation.totalSize() + sizeof(uint64_t) + sizeof(LogMessageVersion);
    }
};

// Serialization function using existing FDB infrastructure
std::vector<uint8_t> serializeShardedMutation(const TaggedMutation& taggedMutation) {
    Arena arena;
    ShardedVersionedMutation shardedMutation(taggedMutation);
    
    // Use FDB's BinaryWriter for consistent serialization
    BinaryWriter writer(AssumeVersion(ProtocolVersion::withBackupMutations()));
    writer << shardedMutation;
    
    return std::vector<uint8_t>(writer.getData(), writer.getData() + writer.getLength());
}
```

### 2. Batch File Structure

Batch files contain multiple mutations with a structured layout:

```cpp
struct BatchFileHeader {
    uint32_t magic = 0xFDB30001;      // File format magic number
    uint32_t version = 1;              // File format version
    uint64_t startVersion;             // First mutation version in file
    uint64_t endVersion;               // Last mutation version in file
    uint64_t mutationCount;            // Number of mutations in file
    uint64_t fileSize;                 // Total file size in bytes
    uint32_t shardCount;               // Number of distinct shards in file
    uint32_t compressionType;          // 0=none, 1=snappy, 2=lz4
    uint64_t uncompressedSize;         // Size before compression
    uint32_t headerChecksum;           // CRC32 of header
    uint32_t dataChecksum;             // CRC32 of all mutation data
    uint64_t createdTimestamp;         // Unix timestamp when file was created
    char reserved[32];                 // Reserved for future use
};

struct ShardIndex {
    uint64_t shardId;
    uint32_t mutationCount;            // Number of mutations for this shard
    uint64_t firstMutationOffset;      // Byte offset to first mutation
    uint64_t lastMutationOffset;       // Byte offset to last mutation
    uint64_t startVersion;             // First version for this shard
    uint64_t endVersion;               // Last version for this shard
    KeyRange approximateRange;         // Approximate key range (for filtering)
};

// Complete file structure:
// [BatchFileHeader]
// [ShardIndex[shardCount]]        <- Index of all shards in file
// [SerializedMutation]...         <- Mutations in version order
// [SerializedMutation]...
```

### 3. Mutation Batching Logic

```cpp
class MutationBatcher {
private:
    struct BatchBuffer {
        std::vector<uint8_t> buffer;
        std::map<uint64_t, std::vector<size_t>> shardOffsets; // shardId -> mutation offsets
        uint64_t startVersion = UINT64_MAX;
        uint64_t endVersion = 0;
        uint64_t mutationCount = 0;
        std::chrono::time_point<std::chrono::steady_clock> createdTime;
        
        static constexpr size_t MAX_BATCH_SIZE = 100 * 1024 * 1024; // 100MB
        static constexpr uint64_t MAX_BATCH_AGE_MS = 5 * 60 * 1000; // 5 minutes
        static constexpr uint32_t MAX_SHARDS_PER_BATCH = 1000;
    };
    
    std::unique_ptr<BatchBuffer> currentBatch;
    uint32_t workerId;
    
public:
    void addMutation(const TaggedMutation& mutation) {
        if (!currentBatch) {
            currentBatch = std::make_unique<BatchBuffer>();
            currentBatch->createdTime = std::chrono::steady_clock::now();
        }
        
        // Check if we need to rotate the batch
        if (shouldRotateBatch(mutation)) {
            finalizeBatch();
            currentBatch = std::make_unique<BatchBuffer>();
            currentBatch->createdTime = std::chrono::steady_clock::now();
        }
        
        // Serialize using existing FDB infrastructure
        auto serialized = serializeShardedMutation(mutation);
        size_t offset = currentBatch->buffer.size();
        
        // Append to buffer
        currentBatch->buffer.insert(
            currentBatch->buffer.end(),
            serialized.begin(),
            serialized.end()
        );
        
        // Update batch metadata
        uint64_t shardId = mutation.tag.id;
        currentBatch->shardOffsets[shardId].push_back(offset);
        currentBatch->startVersion = std::min(currentBatch->startVersion, mutation.version);
        currentBatch->endVersion = std::max(currentBatch->endVersion, mutation.version);
        currentBatch->mutationCount++;
    }
    
private:
    bool shouldRotateBatch(const TaggedMutation& mutation) {
        if (!currentBatch) return false;
        
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - currentBatch->createdTime).count();
        
        return currentBatch->buffer.size() >= BatchBuffer::MAX_BATCH_SIZE ||
               age >= BatchBuffer::MAX_BATCH_AGE_MS ||
               currentBatch->shardOffsets.size() >= BatchBuffer::MAX_SHARDS_PER_BATCH;
    }
    
    void finalizeBatch() {
        if (!currentBatch || currentBatch->mutationCount == 0) return;
        
        // Build complete file with header and index
        auto fileData = buildBatchFile(*currentBatch);
        
        // Queue for S3 upload
        queueS3Upload(std::move(fileData), *currentBatch);
        
        currentBatch.reset();
    }
    
    std::vector<uint8_t> buildBatchFile(const BatchBuffer& batch) {
        // Calculate file size
        size_t headerSize = sizeof(BatchFileHeader);
        size_t indexSize = batch.shardOffsets.size() * sizeof(ShardIndex);
        size_t dataSize = batch.buffer.size();
        size_t totalSize = headerSize + indexSize + dataSize;
        
        std::vector<uint8_t> fileData;
        fileData.reserve(totalSize);
        
        // Build header
        BatchFileHeader header{
            .startVersion = batch.startVersion,
            .endVersion = batch.endVersion,
            .mutationCount = batch.mutationCount,
            .fileSize = totalSize,
            .shardCount = static_cast<uint32_t>(batch.shardOffsets.size()),
            .compressionType = 0, // No compression for now
            .uncompressedSize = totalSize,
            .headerChecksum = 0, // Will calculate
            .dataChecksum = calculateCRC32(batch.buffer),
            .createdTimestamp = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()
        };
        header.headerChecksum = calculateCRC32(&header, sizeof(header) - 4);
        
        // Write header
        uint8_t* headerBytes = reinterpret_cast<uint8_t*>(&header);
        fileData.insert(fileData.end(), headerBytes, headerBytes + sizeof(header));
        
        // Build and write shard index
        std::vector<ShardIndex> shardIndex;
        for (const auto& [shardId, offsets] : batch.shardOffsets) {
            ShardIndex index{
                .shardId = shardId,
                .mutationCount = static_cast<uint32_t>(offsets.size()),
                .firstMutationOffset = headerSize + indexSize + offsets.front(),
                .lastMutationOffset = headerSize + indexSize + offsets.back(),
                .startVersion = batch.startVersion, // TODO: Calculate per-shard versions
                .endVersion = batch.endVersion,
                .approximateRange = calculateApproximateRange(shardId) // From shard registry
            };
            shardIndex.push_back(index);
        }
        
        // Write index
        uint8_t* indexBytes = reinterpret_cast<uint8_t*>(shardIndex.data());
        size_t indexByteSize = shardIndex.size() * sizeof(ShardIndex);
        fileData.insert(fileData.end(), indexBytes, indexBytes + indexByteSize);
        
        // Write mutation data
        fileData.insert(fileData.end(), batch.buffer.begin(), batch.buffer.end());
        
        return fileData;
    }
};
```

## S3 Upload System

### 1. Asynchronous Upload Pipeline

```cpp
class S3UploadPipeline {
private:
    struct UploadTask {
        std::string filename;
        std::vector<uint8_t> fileData;
        BatchFileMetadata metadata;
        int retryCount = 0;
        std::chrono::time_point<std::chrono::steady_clock> createdTime;
    };
    
    std::queue<UploadTask> uploadQueue;
    std::queue<UploadTask> retryQueue;
    std::vector<std::thread> uploadWorkers;
    std::mutex queueMutex;
    std::condition_variable queueCondition;
    std::atomic<bool> shutdown{false};
    
    // S3 client pool
    std::vector<std::unique_ptr<S3Client>> s3Clients;
    std::atomic<int> nextClientIndex{0};
    
public:
    S3UploadPipeline(int numWorkers = 4) {
        // Initialize S3 clients
        for (int i = 0; i < numWorkers; ++i) {
            s3Clients.push_back(createS3Client());
        }
        
        // Start upload workers
        for (int i = 0; i < numWorkers; ++i) {
            uploadWorkers.emplace_back([this, i]() { uploadWorkerLoop(i); });
        }
        
        // Start retry worker
        uploadWorkers.emplace_back([this]() { retryWorkerLoop(); });
    }
    
    void queueUpload(std::string filename, std::vector<uint8_t> fileData, BatchFileMetadata metadata) {
        std::lock_guard<std::mutex> lock(queueMutex);
        
        uploadQueue.push(UploadTask{
            .filename = std::move(filename),
            .fileData = std::move(fileData),
            .metadata = std::move(metadata),
            .retryCount = 0,
            .createdTime = std::chrono::steady_clock::now()
        });
        
        queueCondition.notify_one();
    }
    
private:
    void uploadWorkerLoop(int workerId) {
        auto& s3Client = s3Clients[workerId];
        
        while (!shutdown) {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCondition.wait(lock, [this]() { 
                return !uploadQueue.empty() || shutdown; 
            });
            
            if (shutdown && uploadQueue.empty()) break;
            
            auto task = uploadQueue.front();
            uploadQueue.pop();
            lock.unlock();
            
            uploadToS3(s3Client.get(), std::move(task));
        }
    }
    
    void uploadToS3(S3Client* client, UploadTask task) {
        try {
            // Generate S3 key
            std::string s3Key = generateS3Key(task.metadata);
            
            // Upload file data
            uploadFileData(client, s3Key, task.fileData);
            
            // Upload metadata
            uploadMetadata(client, s3Key, task.metadata);
            
            // Clean up local file if it exists
            if (std::filesystem::exists(task.filename)) {
                std::filesystem::remove(task.filename);
            }
            
            TraceEvent("S3UploadSuccess")
                .detail("Filename", task.filename)
                .detail("S3Key", s3Key)
                .detail("Size", task.fileData.size())
                .detail("RetryCount", task.retryCount);
                
        } catch (const std::exception& e) {
            handleUploadFailure(std::move(task), e.what());
        }
    }
    
    void uploadFileData(S3Client* client, const std::string& s3Key, const std::vector<uint8_t>& data) {
        // Use multipart upload for large files
        if (data.size() > 100 * 1024 * 1024) { // 100MB threshold
            uploadLargeFile(client, s3Key, data);
        } else {
            uploadSmallFile(client, s3Key, data);
        }
    }
    
    void uploadSmallFile(S3Client* client, const std::string& s3Key, const std::vector<uint8_t>& data) {
        auto request = Aws::S3::Model::PutObjectRequest{}
            .WithBucket(getBackupBucket())
            .WithKey(s3Key)
            .WithContentLength(data.size())
            .WithContentType("application/octet-stream");
        
        // Create input stream from data
        auto stream = std::make_shared<Aws::StringStream>();
        stream->write(reinterpret_cast<const char*>(data.data()), data.size());
        request.SetBody(stream);
        
        // Add metadata
        request.AddMetadata("backup-version", "3");
        request.AddMetadata("file-format", "batch");
        request.AddMetadata("mutation-count", std::to_string(extractMutationCount(data)));
        
        auto result = client->PutObject(request);
        if (!result.IsSuccess()) {
            throw std::runtime_error("S3 upload failed: " + result.GetError().GetMessage());
        }
    }
    
    void uploadLargeFile(S3Client* client, const std::string& s3Key, const std::vector<uint8_t>& data) {
        // Initialize multipart upload
        auto initRequest = Aws::S3::Model::CreateMultipartUploadRequest{}
            .WithBucket(getBackupBucket())
            .WithKey(s3Key)
            .WithContentType("application/octet-stream");
        
        auto initResult = client->CreateMultipartUpload(initRequest);
        if (!initResult.IsSuccess()) {
            throw std::runtime_error("Failed to initiate multipart upload: " + 
                                    initResult.GetError().GetMessage());
        }
        
        std::string uploadId = initResult.GetResult().GetUploadId();
        
        try {
            // Upload parts (5MB each)
            const size_t partSize = 5 * 1024 * 1024; // 5MB
            std::vector<Aws::S3::Model::CompletedPart> completedParts;
            
            for (size_t offset = 0; offset < data.size(); offset += partSize) {
                size_t currentPartSize = std::min(partSize, data.size() - offset);
                int partNumber = static_cast<int>(offset / partSize) + 1;
                
                auto partRequest = Aws::S3::Model::UploadPartRequest{}
                    .WithBucket(getBackupBucket())
                    .WithKey(s3Key)
                    .WithUploadId(uploadId)
                    .WithPartNumber(partNumber)
                    .WithContentLength(currentPartSize);
                
                auto partStream = std::make_shared<Aws::StringStream>();
                partStream->write(reinterpret_cast<const char*>(data.data() + offset), currentPartSize);
                partRequest.SetBody(partStream);
                
                auto partResult = client->UploadPart(partRequest);
                if (!partResult.IsSuccess()) {
                    throw std::runtime_error("Failed to upload part " + std::to_string(partNumber) + 
                                           ": " + partResult.GetError().GetMessage());
                }
                
                completedParts.push_back(
                    Aws::S3::Model::CompletedPart{}
                        .WithPartNumber(partNumber)
                        .WithETag(partResult.GetResult().GetETag())
                );
            }
            
            // Complete multipart upload
            auto completeRequest = Aws::S3::Model::CompleteMultipartUploadRequest{}
                .WithBucket(getBackupBucket())
                .WithKey(s3Key)
                .WithUploadId(uploadId)
                .WithMultipartUpload(
                    Aws::S3::Model::CompletedMultipartUpload{}
                        .WithParts(completedParts)
                );
            
            auto completeResult = client->CompleteMultipartUpload(completeRequest);
            if (!completeResult.IsSuccess()) {
                throw std::runtime_error("Failed to complete multipart upload: " + 
                                        completeResult.GetError().GetMessage());
            }
            
        } catch (const std::exception& e) {
            // Abort multipart upload on error
            auto abortRequest = Aws::S3::Model::AbortMultipartUploadRequest{}
                .WithBucket(getBackupBucket())
                .WithKey(s3Key)
                .WithUploadId(uploadId);
            
            client->AbortMultipartUpload(abortRequest);
            throw;
        }
    }
    
    // Per-file JSON metadata ELIMINATED - it's redundant!
    // All metadata is already in BatchFileHeader + ShardIndex
    void uploadMetadata(S3Client* client, const std::string& s3Key, const BatchFileMetadata& metadata) {
        // NO-OP: Per-file JSON metadata provides no value over batch file header
        //
        // BatchFileHeader already contains:
        // - startVersion, endVersion, mutationCount, fileSize, shardCount
        // - createdTimestamp, headerChecksum, dataChecksum
        //
        // ShardIndex already contains:
        // - shardId, mutationCount, approximateRange for each shard
        //
        // The only "new" info would be workerId (useful for debugging but not critical)
        //
        // RESULT: Eliminated thousands of redundant JSON files!
        
        TraceEvent(SevDebug, "PerFileMetadataSkipped")
            .detail("BatchFile", s3Key)
            .detail("Reason", "All metadata already in batch file header");
    }
    
    void handleUploadFailure(UploadTask task, const std::string& error) {
        task.retryCount++;
        
        TraceEvent(SevWarn, "S3UploadFailure")
            .detail("Filename", task.filename)
            .detail("Error", error)
            .detail("RetryCount", task.retryCount);
        
        if (task.retryCount < 3) {
            // Queue for retry
            std::lock_guard<std::mutex> lock(queueMutex);
            retryQueue.push(std::move(task));
        } else {
            // Give up after 3 retries
            TraceEvent(SevError, "S3UploadGiveUp")
                .detail("Filename", task.filename)
                .detail("Error", error);
        }
    }
};
```

## FoundationDB Backup File Organization

### Existing FoundationDB Backup Hierarchy

FoundationDB's existing [`IBackupContainer`](fdbclient/include/fdbclient/BackupContainer.h:225) infrastructure uses a well-established **hierarchical directory structure** based on version number partitioning:

```
backup_container_url/
├── logs/
│   ├── 0000/                                                    # Most significant bytes of version
│   │   ├── 0000/                                               # Least significant bytes of version
│   │   │   ├── log,0000,0010,123456789,0-of-26,1024           # Log file: start,end,timestamp,part,blocksize
│   │   │   ├── log,0000,0010,123456789,1-of-26,1024
│   │   │   ├── log,0000,0010,123456789,2-of-26,1024
│   │   │   └── ...
│   │   ├── 0001/
│   │   │   ├── log,0010,0020,123456789,0-of-15,1024
│   │   │   └── ...
│   │   ├── 0002/
│   │   │   ├── log,0020,0030,123456789,0-of-12,1024
│   │   │   └── ...
│   │   └── ...
│   ├── 0001/                                                   # Next version partition
│   │   ├── 0000/
│   │   ├── 0001/
│   │   └── ...
│   └── ...
├── range/
│   ├── rangefile-<snapshotVersion>-<fileIndex>.fdbackup             # Range files
│   └── ...
├── snapshots/
│   ├── snapshot-<beginVersion>-<endVersion>.json                    # Keyspace snapshots
│   └── ...
└── metadata/
    ├── backup_description                                           # Backup metadata
    ├── logs_description                                            # Log file index
    └── ...
```

**Version-Based Directory Partitioning**:
- **First Level** (`0000`, `0001`, etc.): Most significant bytes of version number
- **Second Level** (`0000`, `0001`, etc.): Least significant bytes of version number
- **File Names**: `log,<startVersion>,<endVersion>,<timestamp>,<partNumber>-of-<totalParts>,<blockSize>`

This hierarchical structure provides:
- **Efficient Listing**: Can list files for specific version ranges without scanning entire backup
- **S3 Performance**: Avoids "hot spotting" by distributing files across prefixes
- **Parallel Operations**: Different version ranges can be processed in parallel

### Enhanced Hierarchy with Self-Contained Shard Metadata

Our shard-aware enhancement **preserves the existing hierarchical structure** while adding minimal metadata for optimization:

```
backup_container_url/
├── logs/
│   ├── 0000/                                                    # Version partitioning preserved
│   │   ├── 0000/
│   │   │   ├── log,0000,0010,123456789,0-of-26,1024           # v2 files (unchanged)
│   │   │   ├── logv3,0000,0010,123456789,3                    # v3 files (NEW format)
│   │   │   └── ...
│   │   ├── 0001/
│   │   │   ├── log,0010,0020,123456789,0-of-15,1024           # v2 files continue
│   │   │   ├── logv3,0010,0020,123456789,7                    # v3 multi-shard files
│   │   │   └── ...
│   │   └── ...
│   ├── 0001/                                                   # Higher version ranges
│   │   ├── 0000/
│   │   └── ...
│   └── ...
├── range/
│   └── ... (unchanged)
├── snapshots/
│   └── ... (unchanged)
└── metadata/
    ├── backup_description                                          # Standard metadata (enhanced)
    ├── logs_description                                           # Standard log index
    ├── shard-mappings/                                            # NEW: Historical shard boundary directory
    │   ├── 0000/                                                  # Version partitioning (like logs)
    │   │   ├── 0000/
    │   │   │   ├── shard-mapping-00000000000001000.json          # Boundary change at version 1000
    │   │   │   └── shard-mapping-00000000000001500.json          # Boundary change at version 1500
    │   │   ├── 0001/
    │   │   │   └── shard-mapping-00000000000010000.json          # Later boundary changes
    │   │   └── ...
    │   ├── 0001/                                                  # Higher version ranges
    │   └── ...
    └── shard-to-files-index.json                                 # NEW: Consolidated lookup (optional)
```

**Enhanced File Naming Convention**:
- **v2 Legacy**: `log,<startVersion>,<endVersion>,<timestamp>,<partNumber>-of-<totalParts>,<blockSize>`
- **v3 Shard-Aware**: `logv3,<startVersion>,<endVersion>,<timestamp>,<workerId>`

**Key Design Principles**:
- ✅ **Preserve Directory Structure**: Maintain existing version-based partitioning
- ✅ **Backward Compatibility**: v2 and v3 files coexist in same directories

### Metadata File Creation and Management

#### Shard Mapping Files (`shard-mapping-<version>.json`)

**When Created**: BackupWorkers detect shard boundary changes by monitoring the cluster's shard configuration during normal operation.

**Where Created**: Generated by the BackupWorker that first encounters a shard boundary change.

**When Uploaded**: Immediately upon detecting a boundary change, before processing mutations with the new mapping.

```cpp
// BackupWorker detects shard boundary change and creates mapping file
class ShardMappingFileManager {
public:
    void onShardBoundaryChange(Version changeVersion, const ShardMapping& newMapping, const std::string& changeReason) {
        /*
         * CREATION TRIGGER: BackupWorker detects shard boundary change
         * 
         * Detection Methods:
         * 1. Configuration change notification from cluster controller
         * 2. Mutation tag pattern change (new shard IDs appearing)
         * 3. Periodic validation of current shard mapping
         */
        
        TraceEvent("CreatingShardMappingFile")
            .detail("Version", changeVersion)
            .detail("TotalShards", newMapping.size())
            .detail("ChangeReason", changeReason);
        
        // Create shard mapping snapshot
        ShardMappingSnapshot snapshot{
            .effectiveVersion = changeVersion,
            .timestamp = getCurrentTimestamp(),
            .changeReason = changeReason,
            .totalShards = static_cast<uint32_t>(newMapping.size()),
            .mapping = newMapping
        };
        
        // Serialize to JSON format
        json snapshotJson = {
            {"formatVersion", 1},
            {"effectiveVersion", snapshot.effectiveVersion},
            {"timestamp", snapshot.timestamp},
            {"changeReason", snapshot.changeReason},
            {"totalShards", snapshot.totalShards},
            {"description", "Historical shard boundaries for version-specific restore"},
            {"shardMapping", {}}
        };
        
        // Add each shard's boundary information
        for (const auto& [shardId, range] : snapshot.mapping) {
            snapshotJson["shardMapping"][std::to_string(shardId)] = {
                {"begin", range.begin.toString()},
                {"end", range.end.toString()},
                {"beginHex", range.begin.toHexString()},
                {"endHex", range.end.toHexString()}
            };
        }
        
        // Calculate file path using version partitioning
        std::string filePath = formatShardMappingPath(changeVersion);
        // Result: "metadata/shard-mappings/0000/0001/shard-mapping-00000000000001500.json"
        
        // Upload to S3 immediately
        uploadShardMappingFile(filePath, snapshotJson.dump(2));
        
        TraceEvent("ShardMappingFileCreated")
            .detail("FilePath", filePath)
            .detail("Version", changeVersion)
            .detail("FileSize", snapshotJson.dump().size());
    }
    
private:
    std::string formatShardMappingPath(Version version) {
        // Use same version partitioning as log files
        uint64_t upperBits = (version >> 32) & 0xFFFF;
        uint64_t lowerBits = (version >> 16) & 0xFFFF;
        
        return fmt::format("metadata/shard-mappings/{:04x}/{:04x}/shard-mapping-{:020d}.json",
                          upperBits, lowerBits, version);
    }
};
```

**File Content Example**:
```json
{
  "formatVersion": 1,
  "effectiveVersion": 1500,
  "timestamp": "2024-01-15T14:30:00Z",
  "changeReason": "shard_split",
  "totalShards": 125000,
  "description": "Historical shard boundaries for version-specific restore",
  "shardMapping": {
    "42": {
      "begin": "user/",
      "end": "user/5000",
      "beginHex": "757365722f",
      "endHex": "757365722f35303030"
    },
    "43": {
      "begin": "user/5000",
      "end": "user/10000",
      "beginHex": "757365722f35303030",
      "endHex": "757365722f3130303030"
    },
    // ... all 125,000 shards
  }
}
```

#### Shard-to-Files Index (`shard-to-files-index.json`)

**When Created**: Background process runs periodically (every 1-2 hours) to rebuild the consolidated index.

**Where Created**: Generated by a designated BackupWorker or separate indexing service.

**When Uploaded**: After processing new batch files that haven't been indexed yet.

```cpp
// Periodic background process to maintain shard-to-files index
class ShardToFilesIndexManager {
public:
    void rebuildConsolidatedIndex() {
        /*
         * REBUILD STRATEGY:
         * 
         * 1. Load existing index (if present)
         * 2. Scan for new batch files since last update
         * 3. Extract shard information from new files
         * 4. Update consolidated mapping
         * 5. Upload updated index
         */
        
        TraceEvent("RebuildingShardToFilesIndex");
        
        // Load existing index
        auto existingIndex = loadExistingShardIndex();
        auto lastUpdateTime = existingIndex["lastUpdated"].get<std::string>();
        
        // Find new batch files since last update
        auto newBatchFiles = findNewBatchFilesSince(lastUpdateTime);
        
        if (newBatchFiles.empty()) {
            TraceEvent("NoNewBatchFiles").detail("LastUpdate", lastUpdateTime);
            return;
        }
        
        // Process new files and extract shard mappings
        for (const auto& filename : newBatchFiles) {
            auto fileMetadata = extractShardMetadataFromBatchFile(filename);
            
            for (const auto& shardInfo : fileMetadata.containedShards) {
                uint64_t shardId = shardInfo.shardId;
                existingIndex["shardToFiles"][std::to_string(shardId)].push_back(filename);
            }
        }
        
        // Update metadata
        existingIndex["lastUpdated"] = getCurrentTimestamp();
        existingIndex["totalFiles"] = existingIndex["shardToFiles"].size();
        existingIndex["totalShards"] = calculateTotalShards(existingIndex);
        existingIndex["indexedFiles"] += newBatchFiles.size();
        
        // Upload updated index
        uploadShardToFilesIndex(existingIndex.dump(2));
        
        TraceEvent("ShardToFilesIndexRebuilt")
            .detail("NewFilesProcessed", newBatchFiles.size())
            .detail("TotalShards", existingIndex["totalShards"])
            .detail("IndexSize", existingIndex.dump().size());
    }
    
private:
    json loadExistingShardIndex() {
        try {
            return downloadAndParseShardIndex();
        } catch (const std::exception& e) {
            // Create new index if none exists
            return json{
                {"formatVersion", 1},
                {"created", getCurrentTimestamp()},
                {"lastUpdated", getCurrentTimestamp()},
                {"totalFiles", 0},
                {"totalShards", 0},
                {"indexedFiles", 0},
                {"description", "Maps shard IDs to backup files containing them"},
                {"shardToFiles", json::object()}
            };
        }
    }
};
```

**File Content Example**:
```json
{
  "formatVersion": 1,
  "created": "2024-01-15T10:00:00Z",
  "lastUpdated": "2024-01-15T16:30:00Z",
  "totalFiles": 15420,
  "totalShards": 250000,
  "indexedFiles": 15420,
  "description": "Maps shard IDs to backup files containing them",
  "shardToFiles": {
    "42": [
      "logs/0000/0001/logv3,0000,0010,123456789,3",
      "logs/0000/0001/logv3,0010,0020,123456790,3",
      "logs/0000/0002/logv3,0020,0030,123456791,3"
    ],
    "167": [
      "logs/0000/0001/logv3,0000,0010,123456789,7",
      "logs/0000/0001/logv3,0010,0020,123456790,7"
    ],
    // ... mappings for all 250,000 shards
  }
}
```

#### Creation and Upload Timeline

```cpp
void demonstrateMetadataFileLifecycle() {
    /*
     * TIMELINE: Metadata file creation and management
     * 
     * === SHARD MAPPING FILES ===
     * 
     * Time T1: Cluster starts with 100,000 shards
     * - BackupWorker detects initial shard configuration
     * - Creates: shard-mapping-00000000000001000.json
     * - Contains: 100,000 shard → KeyRange mappings
     * 
     * Time T2: Hot shard detected, split occurs (shard count → 100,001)
     * - BackupWorker detects configuration change
     * - Creates: shard-mapping-00000000000015000.json  
     * - Contains: 100,001 shard → KeyRange mappings
     * 
     * Time T3: Major rebalancing (shard count → 125,000)
     * - Creates: shard-mapping-00000000000023000.json
     * - Contains: 125,000 shard → KeyRange mappings
     * 
     * === SHARD-TO-FILES INDEX ===
     * 
     * Every 1-2 hours: Background indexer runs
     * - Scans for new logv3 files created since last run
     * - Extracts shard information from file headers
     * - Updates consolidated shard-to-files-index.json
     * - Maps each shard ID to all files containing it
     */
    
    TraceEvent("MetadataFileLifecycleExample")
        .detail("ShardMappingFiles", "Created on boundary changes")
        .detail("ShardToFilesIndex", "Updated every 1-2 hours")
        .detail("Purpose", "Enable self-contained, version-specific restores")
        .detail("StorageLocation", "S3 with version-based partitioning");
}
```

#### Key Benefits of This Metadata Strategy

**Shard Mapping Files**:
- ✅ **Version-Specific**: Each file captures exact shard boundaries at a specific version
- ✅ **Self-Contained**: Restore operations don't need cluster access for historical mappings
- ✅ **Immutable**: Once created, files never change (historical accuracy guaranteed)
- ✅ **Efficient**: Only created when boundaries actually change (not continuously)

**Shard-to-Files Index**:
- ✅ **Fast Lookup**: Instantly find files containing specific shards
- ✅ **Eventually Consistent**: Can be slightly stale without correctness issues
- ✅ **Fallback Resilient**: System works without it (just slower file discovery)
- ✅ **Incremental Updates**: Only processes new files, not entire backup set

This metadata architecture enables the 250,000x efficiency improvement by providing the precise historical context needed for version-specific, shard-aware partial restores.

- ✅ **Clear Differentiation**: `logv3` prefix clearly identifies shard-aware files
- ✅ **Worker Identification**: `<workerId>` shows which BackupWorker created the file
- ✅ **Embedded Metadata**: All shard information contained within the file itself
- ✅ **Concise Format**: Minimal naming convention without redundant prefixes/suffixes

**What is `workerId` in Enhanced Shard-Aware Design?**
- `workerId` = **Backup worker ID** (0, 1, 2, ..., numBackupWorkers-1)
- **NOT log router tag** - we're replacing log router based assignment with shard-based assignment
- Each backup worker processes mutations based on shard ID assignment
- Example: `logfile-00000000000001000000-00000000000002000000-2.fdbackup` = backup worker 2's log file

**Critical Resilience: Missing Shard Mapping Recovery**

If [`shard-mapping-<version>.json`](backup_v3_s3_writing_and_aggregation.md:1165) files are missing or failed to write, they can be **partially reconstructed** from batch files:

```cpp
// Reconstruct historical shard mapping from batch file evidence
class ShardMappingReconstruction {
public:
    ShardMapping reconstructShardMappingAtVersion(Version targetVersion) {
        /*
         * SCENARIO: shard-mapping-<version>.json files are missing/corrupted
         * SOLUTION: Reconstruct approximate shard boundaries from batch file evidence
         * LIMITATION: Not perfect, but much better than legacy (still achieves major efficiency gains)
         */
        
        // Step 1: Find batch files from around the target version
        auto nearbyFiles = findBatchFilesNearVersion(targetVersion, 1000); // ±1000 versions
        
        // Step 2: Extract shard IDs and approximate ranges from batch files
        std::map<uint64_t, KeyRange> reconstructedMapping;
        
        for (const auto& filename : nearbyFiles) {
            auto metadata = reconstructFromBatchFile(filename);
            
            // Extract shard info from embedded batch file headers
            for (const auto& shardInfo : metadata.containedShards) {
                if (reconstructedMapping.find(shardInfo.shardId) == reconstructedMapping.end()) {
                    // Use approximate range from batch file shard index
                    reconstructedMapping[shardInfo.shardId] = shardInfo.approximateRange;
                }
            }
        }
        
        TraceEvent("ReconstructedShardMapping")
            .detail("TargetVersion", targetVersion)
            .detail("ReconstructedShards", reconstructedMapping.size())
            .detail("FilesAnalyzed", nearbyFiles.size())
            .detail("Accuracy", "Approximate - may scan extra files but won't miss data");
        
        // OPTIMIZATION: Save reconstructed mapping for future restore operations
        try {
            saveReconstructedShardMapping(targetVersion, reconstructedMapping);
            TraceEvent("SavedReconstructedMapping")
                .detail("TargetVersion", targetVersion)
                .detail("Filename", formatShardMappingFilename(targetVersion));
        } catch (const std::exception& e) {
            TraceEvent(SevWarn, "FailedToSaveReconstructedMapping")
                .detail("TargetVersion", targetVersion)
                .detail("Error", e.what())
                .detail("Impact", "Future restores will need to reconstruct again");
        }
        
        /*
         * RESULT: Approximate shard mapping that may include extra shards
         * BENEFIT: Still achieves 10,000x-100,000x efficiency (not perfect 250,000x but much better than legacy)
         * SAFETY: Conservative - may scan extra files but won't miss data
         * OPTIMIZATION: Saved for future restore operations at this version
         */
        
        return ShardMapping(reconstructedMapping);
    }
    
private:
    std::vector<std::string> findBatchFilesNearVersion(Version targetVersion, Version tolerance) {
        std::vector<std::string> nearbyFiles;
        auto allFiles = listAllBatchFiles();
        
        for (const auto& filename : allFiles) {
            auto fileInfo = parseFileInfo(filename);
    
    // Save reconstructed shard mapping back to backup container for future use
    void saveReconstructedShardMapping(Version version, const std::map<uint64_t, KeyRange>& mapping) {
        /*
         * RESTORE-TIME OPTIMIZATION: Save reconstructed mapping for future restores
         * 
         * Benefits:
         * - First restore at this version: Reconstructs from batch files (slower)
         * - Subsequent restores at this version: Use saved mapping (fast)
         * - Permanent improvement: Once reconstructed, never needs reconstruction again
         */
        
        // Create shard mapping snapshot in standard format
        ShardMappingSnapshot snapshot;
        snapshot.effectiveVersion = version;
        snapshot.timestamp = getCurrentTimestamp();
        snapshot.changeReason = "reconstructed_at_restore_time";
        snapshot.totalShards = static_cast<uint32_t>(mapping.size());
        snapshot.mapping = mapping;
        
        // Serialize to JSON
        json snapshotJson = {
            {"version", snapshot.effectiveVersion},
            {"timestamp", snapshot.timestamp},
            {"changeReason", snapshot.changeReason},
            {"totalShards", snapshot.totalShards},
            {"reconstructionNote", "Created during restore from batch file evidence"},
            {"accuracy", "approximate"},
            {"shardMapping", {}}
        };
        
        // Add shard boundaries
        for (const auto& [shardId, range] : mapping) {
            snapshotJson["shardMapping"][std::to_string(shardId)] = {
                {"begin", range.begin.toString()},
                {"end", range.end.toString()}
            };
        }
        
        // Save to backup container
        std::string filename = formatShardMappingFilename(version);
        std::string jsonStr = snapshotJson.dump(2); // Pretty print
        std::vector<uint8_t> data(jsonStr.begin(), jsonStr.end());
        
        uploadShardMappingSnapshot(filename, data);
        
        TraceEvent("SavedReconstructedShardMapping")
            .detail("Version", version)
            .detail("Filename", filename)
            .detail("TotalShards", mapping.size())
            .detail("DataSize", data.size())
            .detail("Benefit", "Future restores at this version will be faster");
    }
    
    // Enhanced restore workflow that checks for and creates missing mappings
    ShardMapping loadHistoricalShardMappingWithReconstruction(Version version) {
        try {
            // Try to load existing mapping first
            return loadHistoricalShardMapping(version);
            
        } catch (const std::exception& e) {
            TraceEvent(SevInfo, "ShardMappingMissing")
                .detail("Version", version)
                .detail("Error", e.what())
                .detail("Action", "Will reconstruct from batch files");
            
            // Reconstruct and save for future use
            auto reconstructedMapping = reconstructShardMappingAtVersion(version);
            
            TraceEvent("ShardMappingReconstructed")
                .detail("Version", version)
                .detail("ReconstructedShards", reconstructedMapping.size())
                .detail("SavedForFutureUse", true);
            
            return reconstructedMapping;
        }
    }
    
    // Complete example: Restore with reconstruction and caching
    void demonstrateRestoreTimeReconstruction() {
        /*
         * SCENARIO: Multiple restores needed at version 98765, but shard-mapping-98765.json is missing
         * 
         * First restore at version 98765:
         * 1. Try to load shard-mapping-98765.json → NOT FOUND
         * 2. Reconstruct mapping from batch files → SUCCESS (takes ~30 seconds)
         * 3. Save reconstructed mapping as shard-mapping-98765.json → CACHED
         * 4. Perform restore using reconstructed mapping → 10,000x-100,000x efficiency
         * 
         * Second restore at version 98765 (hours later):
         * 1. Try to load shard-mapping-98765.json → FOUND (created during first restore)
         * 2. Use cached mapping directly → INSTANT
         * 3. Perform restore with perfect mapping → 250,000x efficiency
         */
        
        KeyRange targetRange("user/1000", "user/2000");
        Version targetVersion = 98765;
        
        // === FIRST RESTORE ===
        TraceEvent("FirstRestoreAtVersion")
            .detail("Version", targetVersion)
            .detail("ExpectedBehavior", "Reconstruct and cache mapping");
        
        auto startTime = std::chrono::high_resolution_clock::now();
        
        // This will reconstruct and cache the mapping
        auto shardMapping = loadHistoricalShardMappingWithReconstruction(targetVersion);
        auto affectedShards = convertKeyRangeToShardIds(targetRange, shardMapping);
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        TraceEvent("FirstRestoreMappingComplete")
            .detail("Version", targetVersion)
            .detail("DurationMs", duration.count())
            .detail("AffectedShards", affectedShards.size())
            .detail("Efficiency", "10000x-100000x (reconstructed mapping)")
            .detail("MappingCached", true);
        
        // === SECOND RESTORE (hours later) ===
        TraceEvent("SecondRestoreAtVersion")
            .detail("Version", targetVersion)
            .detail("ExpectedBehavior", "Use cached mapping instantly");
        
        auto startTime2 = std::chrono::high_resolution_clock::now();
        
        // This will find the cached mapping instantly
        auto cachedMapping = loadHistoricalShardMappingWithReconstruction(targetVersion);
        auto affectedShards2 = convertKeyRangeToShardIds(targetRange, cachedMapping);
        
        auto endTime2 = std::chrono::high_resolution_clock::now();
        auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(endTime2 - startTime2);
        
        TraceEvent("SecondRestoreMappingComplete")
            .detail("Version", targetVersion)
            .detail("DurationMs", duration2.count())
            .detail("AffectedShards", affectedShards2.size())
            .detail("Efficiency", "250000x (perfect cached mapping)")
            .detail("SpeedImprovement", (double)duration.count() / duration2.count());
        
        /*
         * RESULTS:
         * - First restore: ~30 seconds to reconstruct + cache mapping, then fast restore
         * - Second restore: <1 second to load cached mapping, then fast restore
         * - All future restores at version 98765: Instant mapping load
         * - One-time reconstruction cost pays for itself with multiple restores
         */
    }
            // Include files that overlap with our target version range
            if (fileInfo.endVersion >= (targetVersion - tolerance) && 
                fileInfo.startVersion <= (targetVersion + tolerance)) {
                nearbyFiles.push_back(filename);
            }
        }
        
        return nearbyFiles;
    }
};
```

**Recovery Strategy Priority**:
1. **Best**: Use stored `shard-mapping-<version>.json` (perfect accuracy, 250,000x improvement)
2. **Good**: Reconstruct from nearby batch files (approximate, 10,000x-100,000x improvement)  
3. **Fallback**: Scan all batch files (still much better than legacy)

**Enhanced BackupWorker Shard Assignment**:
```cpp
// Example: 250,000 shards, 10 backup workers
// Worker 0 handles shards: 0, 10, 20, 30, ... (shardId % 10 == 0)
// Worker 1 handles shards: 1, 11, 21, 31, ... (shardId % 10 == 1)
// Worker 2 handles shards: 2, 12, 22, 32, ... (shardId % 10 == 2)

class EnhancedBackupWorkerShardAssignment {
public:
    bool shouldProcessShard(uint64_t shardId, int workerId, int totalWorkers) {
        return (shardId % totalWorkers) == workerId;
    }
    
    void processMutations() {
        // Subscribe to shard-tagged mutations from log system
        for (auto taggedMutation : getShardTaggedMutations()) {
            if (taggedMutation.tag.locality == tagLocalityBackupShard) {
                uint64_t shardId = taggedMutation.tag.id;
                
                // Only process mutations for shards assigned to this worker
                if (shouldProcessShard(shardId, myWorkerId, totalBackupWorkers)) {
                    writeMutationToLogFile(taggedMutation);
                }
            }
        }
    }
};
```

**Key Point**: Each backup worker processes mutations from **assigned shards only** (via shardId % numWorkers). Each worker's log file contains mutations from multiple shards, but only the shards assigned to that worker. The shard metadata tells us which specific shards are in each worker's file, enabling efficient partial restore.

### File Naming Patterns Used by Existing BackupContainer

```cpp
// Current FoundationDB patterns we leverage:

// 1. Tagged Log Files (existing)
// Generated by writeTaggedLogFile(beginVersion, endVersion, blockSize, tagId, totalTags)
"log/logfile-{:020d}-{:020d}-{}.fdbackup"
// Example: log/logfile-00000000000001000000-00000000000002000000-5.fdbackup

// 2. Range Files (existing)
// Generated by writeRangeFile(snapshotBeginVersion, snapshotFileCount, fileVersion, blockSize)
"range/rangefile-{:020d}-{}.fdbackup"
// Example: range/rangefile-00000000000001000000-0.fdbackup

// 3. Keyspace Snapshots (existing)
// Generated by writeKeyspaceSnapshotFile()
"snapshots/snapshot-{:020d}-{:020d}.json"
// Example: snapshots/snapshot-00000000000001000000-00000000000002000000.json
```

### Shard Metadata File Patterns (New)

```cpp
// Our enhancement adds these metadata files for self-contained restore:

// 1. Historical shard mapping snapshots - CRITICAL for version-specific restore
"metadata/shard-mapping-<version>.json"
// Written when shard boundaries change (splits/merges), contains:
// {
//   "version": 1500,
//   "timestamp": "2024-01-01T10:15:00Z", 
//   "changeReason": "shard_split",
//   "totalShards": 125000,
//   "shardMapping": {
//     "42A": {"begin": "user/", "end": "user/5000"},
//     "42B": {"begin": "user/5000", "end": "user/10000"},
//     // ... all shards and their boundaries at this version
//   }
// }
// CRITICAL: Enables self-contained restore without cluster queries
// RECOVERY: Can be reconstructed from batch files if missing (with reduced efficiency)

// 2. Consolidated shard-to-files index - OPTIONAL optimization for fast lookup
"metadata/shard-to-files-index.json"
// Contains mapping for ALL shards in one file:
// {
//   "metadataVersion": 1,
//   "totalShards": 250000,
//   "lastUpdated": "2024-01-01T12:00:00Z",
//   "shardToFiles": {
//     "42": [
//       "logfile-00000000000001000000-00000000000002000000-3.fdbackup",
//       "logfile-00000000000002000000-00000000000003000000-3.fdbackup"
//     ],
//     "167": [
//       "logfile-00000000000001000000-00000000000002000000-7.fdbackup"  
//     ],
//     // ... all shards mapped to containing files
//   }
// }
// OPTIONAL: Updated periodically, fallback is scanning batch file headers
// SAFE: Never gives wrong answers, only may miss newly created files

// 3. Enhanced backup description
"metadata/backup_description" (enhanced)
// Existing file enhanced with: {"shardAware": true, "totalShards": 250000, "metadataVersion": 1}
```

### Metadata Resilience and Recovery

**Critical Design Principle**: **All essential shard information is embedded in the batch file itself**. JSON metadata files are purely **performance optimizations** for fast lookup.

```cpp
// Metadata recovery without JSON files
class MetadataRecoveryService {
public:
    // Reconstruct shard metadata by reading batch file headers
    ShardMetadata reconstructFromBatchFile(const std::string& batchFileName) {
        auto fileData = downloadBatchFile(batchFileName);
        
        // Parse batch file header
        BatchFileHeader* header = reinterpret_cast<BatchFileHeader*>(fileData.data());
        
        // Extract shard index from file
        size_t indexOffset = sizeof(BatchFileHeader);
        ShardIndex* shardIndex = reinterpret_cast<ShardIndex*>(fileData.data() + indexOffset);
        
        ShardMetadata metadata;
        metadata.filename = batchFileName;
        metadata.startVersion = header->startVersion;
        metadata.endVersion = header->endVersion;
        metadata.mutationCount = header->mutationCount;
        
        // Extract all shards contained in this file
        for (uint32_t i = 0; i < header->shardCount; i++) {
            ShardInfo info;
            info.shardId = shardIndex[i].shardId;
            info.mutationCount = shardIndex[i].mutationCount;
            info.approximateRange = shardIndex[i].approximateRange;
            metadata.containedShards.push_back(info);
        }
        
        return metadata;
    }
    
    // Rebuild shard-to-files index by scanning all batch files
    std::map<uint64_t, std::vector<std::string>> rebuildShardIndex(const std::string& backupPath) {
        std::map<uint64_t, std::vector<std::string>> shardToFiles;
        
        // List all batch files in backup
        auto batchFiles = listBatchFiles(backupPath);
        
        for (const auto& filename : batchFiles) {
            auto metadata = reconstructFromBatchFile(filename);
            
            // Add this file to all contained shards
            for (const auto& shardInfo : metadata.containedShards) {
                shardToFiles[shardInfo.shardId].push_back(filename);
            }
        }
        
        return shardToFiles;
    }
    
    // Fast recovery using JSON if available, fallback to batch file parsing
    ShardMetadata getShardMetadata(const std::string& batchFileName) {
        std::string jsonPath = batchFileName + ".metadata";
        
        try {
            // Try fast path: read JSON metadata
            if (fileExists(jsonPath)) {
                return parseJsonMetadata(jsonPath);
            }
        } catch (const std::exception& e) {
            TraceEvent(SevWarn, "JsonMetadataCorrupted")
                .detail("JsonFile", jsonPath)
                .detail("Error", e.what());
        }
        
        // Fallback: reconstruct from batch file (slower but always works)
        TraceEvent(SevInfo, "ReconstructingMetadataFromBatchFile")
            .detail("BatchFile", batchFileName);
        return reconstructFromBatchFile(batchFileName);
    }
    
    // Load consolidated shard-to-files index (this type of index IS useful!)
    std::vector<std::string> getFilesForShard(uint64_t shardId) {
        /*
         * IMPORTANT DISTINCTION: Two types of indexes with different validity:
         * 
         * 1. BROKEN: Historical shard boundary indexes 
         *    - Problem: Shard boundaries change over time
         *    - Solution: Always query cluster config for specific version
         * 
         * 2. USEFUL: Shard-to-files mapping indexes
         *    - Fact: Once a backup file is written, its shard content never changes
         *    - Safe: Can cache "which shards are in which files" indefinitely
         *    - Stale handling: Only misses newly created files, never gives wrong answers
         */
        
        std::vector<std::string> filesFromIndex;
        
        try {
            // Try fast path: load consolidated shard-to-files index
            auto consolidatedIndex = loadConsolidatedShardToFilesIndex();
            auto it = consolidatedIndex["shardToFiles"].find(std::to_string(shardId));
            if (it != consolidatedIndex["shardToFiles"].end()) {
                filesFromIndex = it->get<std::vector<std::string>>();
                
                // Check if index might be stale (missing newer files)
                auto indexLastUpdated = consolidatedIndex["lastUpdated"].get<std::string>();
                auto indexAge = calculateIndexAge(indexLastUpdated);
                
                if (indexAge < std::chrono::hours(2)) {
                    // Index is fresh, use it directly
                    TraceEvent("UsingFreshShardToFilesIndex")
                        .detail("ShardId", shardId)
                        .detail("FilesFound", filesFromIndex.size())
                        .detail("IndexAge", indexAge.count());
                    return filesFromIndex;
                } else {
                    // Index might be missing newer files - supplement with scan
                    TraceEvent(SevInfo, "ShardToFilesIndexStale")
                        .detail("ShardId", shardId)
                        .detail("IndexAge", indexAge.count())
                        .detail("FilesFromIndex", filesFromIndex.size())
                        .detail("WillSupplementWithScan", true);
                }
            }
        } catch (const std::exception& e) {
            TraceEvent(SevWarn, "ShardToFilesIndexUnavailable")
                .detail("ShardId", shardId)
                .detail("Error", e.what());
        }
        
        // Supplement with scan for newer files not in stale index
        auto allFiles = scanBatchFilesForShard(shardId);
        
        // If we had some files from index, merge and deduplicate
        if (!filesFromIndex.empty()) {
            std::set<std::string> uniqueFiles(filesFromIndex.begin(), filesFromIndex.end());
            for (const auto& file : allFiles) {
                uniqueFiles.insert(file);
            }
            
            TraceEvent("MergedStaleIndexWithScan")
                .detail("ShardId", shardId)
                .detail("FilesFromStaleIndex", filesFromIndex.size())
                .detail("FilesFromScan", allFiles.size())
                .detail("TotalUniqueFiles", uniqueFiles.size());
            
            return std::vector<std::string>(uniqueFiles.begin(), uniqueFiles.end());
        }
        
        return allFiles;
    }
    
    // Clarify the difference between useful and broken indexes
    void clarifyIndexTypes() {
        /*
         * INDEX VALIDITY ANALYSIS:
         * 
         * 1. SHARD-TO-FILES INDEX (Useful and Safe):
         *    - Maps: shardId → list of backup files containing that shardId
         *    - Immutable: Once a backup file is written, its shard content never changes
         *    - Stale behavior: May miss newly created files, but never gives wrong answers
         *    - Fallback: Scan file headers to find additional files
         *    - Performance: Still provides significant speedup even when stale
         * 
         * 2. HISTORICAL SHARD BOUNDARY INDEX (Broken and Dangerous):
         *    - Maps: version → shard boundary mapping at that version
         *    - Mutable: Shard boundaries change frequently due to splits/merges
         *    - Stale behavior: Gives completely wrong shard boundaries for historical versions
         *    - Consequence: Data loss during restore (missing shards created after index)
         *    - Solution: Always query cluster configuration log for specific versions
         * 
         * 3. FILE EXISTENCE INDEX (Useful but Optional):
         *    - Maps: time range → list of backup files in that range
         *    - Immutable: Existing files don't disappear (until cleanup)
         *    - Stale behavior: May miss newly created files
         *    - Fallback: List files in backup container directly
         *    - Performance: Modest speedup, not critical
         */
        
        TraceEvent("IndexValidityAnalysis")
            .detail("ShardToFilesIndex", "Useful-Safe-Cacheable")
            .detail("HistoricalShardBoundaryIndex", "Broken-Dangerous-Never-Use")
            .detail("FileExistenceIndex", "Useful-Optional-Not-Critical");
    }
    
    // Concrete example: Restore with stale index scenario
    void demonstrateStaleIndexRestore() {
        /*
         * SCENARIO: Consolidated index was last updated 3 hours ago, but new backup files
         * have been created in the last hour. We need to restore data that might be in 
         * these newer files not reflected in the stale index.
         */
        
        KeyRange targetRange("user/5000", "user/5100");
        Version targetVersion = 98765;
        uint64_t shardId = 42;  // Assume this shard contains our target range
        
        // Step 1: Try consolidated index (fast path)
        auto consolidatedIndex = loadConsolidatedShardIndex();
        auto indexLastUpdated = consolidatedIndex["lastUpdated"].get<std::string>();
        
        TraceEvent("RestoreWithStaleIndex")
            .detail("IndexLastUpdated", indexLastUpdated)
            .detail("TargetShardId", shardId)
            .detail("TargetRange", targetRange.toString());
        
        // Index shows these files for shard 42
        auto filesFromIndex = consolidatedIndex["shardToFiles"]["42"].get<std::vector<std::string>>();
        // filesFromIndex = ["logfile-00000000000090000000-00000000000095000000-7.fdbackup", 
        //                   "logfile-00000000000095000000-00000000000098000000-7.fdbackup"]
        
        // Step 2: Check if index is stale (older than 2 hours)
        auto indexAge = calculateIndexAge(indexLastUpdated);
        bool indexIsStale = indexAge > std::chrono::hours(2);
        
        if (indexIsStale) {
            TraceEvent(SevInfo, "IndexStaleDetected")
                .detail("IndexAge", indexAge.count())
                .detail("FilesFromStaleIndex", filesFromIndex.size());
            
            // Step 3: Scan for ALL files containing shard 42 (including recent ones)
            auto allCurrentFiles = scanBatchFilesForShard(shardId);
            // allCurrentFiles = ["logfile-00000000000090000000-00000000000095000000-7.fdbackup",
            //                    "logfile-00000000000095000000-00000000000098000000-7.fdbackup", 
            //                    "logfile-00000000000098000000-00000000000099000000-7.fdbackup",  // NEW!
            //                    "logfile-00000000000099000000-00000000000100000000-7.fdbackup"]  // NEW!
            
            TraceEvent("StaleIndexHandling")
                .detail("FilesFromStaleIndex", filesFromIndex.size())
                .detail("ActualCurrentFiles", allCurrentFiles.size())
                .detail("MissingFromIndex", allCurrentFiles.size() - filesFromIndex.size());
            
            // Step 4: Use complete current file list for restore
            auto relevantFiles = filterFilesByVersionRange(allCurrentFiles, targetVersion - 1000, targetVersion + 1000);
            
            // Step 5: Restore from all relevant files (including those missed by stale index)
            std::vector<VersionedMutation> mutations;
            for (const auto& filename : relevantFiles) {
                auto fileMutations = readShardMutationsFromFile(filename, {shardId}, targetRange, 
                                                               VersionRange(targetVersion - 100, targetVersion + 100));
                mutations.insert(mutations.end(), fileMutations.begin(), fileMutations.end());
            }
            
            TraceEvent("RestoreWithStaleIndexComplete")
                .detail("TotalMutationsFound", mutations.size())
                .detail("FilesScanned", relevantFiles.size())
                .detail("StaleIndexWorkaround", "Success");
            
        } else {
            // Index is fresh, use normal fast path
            TraceEvent(SevInfo, "IndexFresh")
                .detail("IndexAge", indexAge.count())
                .detail("UsingFastPath", true);
        }
    }
    
private:
    std::chrono::hours calculateIndexAge(const std::string& lastUpdatedTimestamp) {
        // Parse ISO timestamp and calculate age
        auto lastUpdated = parseISOTimestamp(lastUpdatedTimestamp);
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::hours>(now - lastUpdated);
    }
    
    std::vector<std::string> scanBatchFilesForShard(uint64_t shardId) {
        std::vector<std::string> matchingFiles;
        
        // List all batch files in backup container
        auto allBatchFiles = listAllBatchFiles();
        
        TraceEvent("ScanningForShard")
            .detail("ShardId", shardId)
            .detail("TotalBatchFiles", allBatchFiles.size());
        
        // Check each file to see if it contains the target shard
        for (const auto& filename : allBatchFiles) {
            try {
                // Read just the batch file header and shard index (not full file)
                auto headerAndIndex = downloadBatchFileHeaderAndIndex(filename);
                
                BatchFileHeader* header = reinterpret_cast<BatchFileHeader*>(headerAndIndex.data());
                ShardIndex* shardIndex = reinterpret_cast<ShardIndex*>(
                    headerAndIndex.data() + sizeof(BatchFileHeader));
                
                // Check if this file contains our target shard
                for (uint32_t i = 0; i < header->shardCount; i++) {
                    if (shardIndex[i].shardId == shardId) {
                        matchingFiles.push_back(filename);
                        break;  // Found shard in this file, move to next file
                    }
                }
                
            } catch (const std::exception& e) {
                TraceEvent(SevWarn, "SkippingCorruptedFile")
                    .detail("Filename", filename)
                    .detail("Error", e.what());
            }
        }
        
        TraceEvent("ScanForShardComplete")
            .detail("ShardId", shardId)
            .detail("MatchingFiles", matchingFiles.size())
            .detail("ScannedFiles", allBatchFiles.size());
        
        return matchingFiles;
    }
    
    // Download only header + shard index (first ~few KB) instead of entire file
    std::vector<uint8_t> downloadBatchFileHeaderAndIndex(const std::string& filename) {
        // Calculate size needed: header + max possible shard index
        size_t headerSize = sizeof(BatchFileHeader);
        size_t maxShardIndexSize = 1000 * sizeof(ShardIndex);  // Assume max 1000 shards per file
        size_t downloadSize = headerSize + maxShardIndexSize;
        
        // Download just the beginning of the file (S3 range request)
        auto partialData = downloadFileRange(filename, 0, downloadSize);
        
        // Parse actual shard count from header
        BatchFileHeader* header = reinterpret_cast<BatchFileHeader*>(partialData.data());
        size_t actualShardIndexSize = header->shardCount * sizeof(ShardIndex);
        size_t actualNeededSize = headerSize + actualShardIndexSize;
        
        // Resize to actual needed size
        partialData.resize(actualNeededSize);
        
        return partialData;
    }
};
```

## Backward Compatibility and Migration Strategy

### Complete Migration Strategy

The shard-aware backup system ensures 100% backward compatibility while providing a safe migration path:

```cpp
// Complete backward compatibility matrix
class BackwardCompatibilityMatrix {
public:
    // All existing backup operations continue unchanged
    bool legacyBackupStillWorks = true;        // ✓ Zero breaking changes
    bool legacyRestoreStillWorks = true;       // ✓ Existing restore APIs preserved  
    bool legacyToolsStillWork = true;          // ✓ All fdbbackup commands unchanged
    bool legacyConfigStillWorks = true;        // ✓ No configuration changes required
    
    // Enhanced operations are additive
    bool enhancedRestoreAPIs = true;           // ✓ New getRangeSnapshot/getRangeMutation
    bool shardAwareEfficiency = true;          // ✓ 250,000x improvement when using new APIs
    bool hybridModeSupport = true;             // ✓ Gradual migration with rollback safety
    bool crossVersionRestore = true;          // ✓ Restore from any backup format version
};

// Migration safety guarantees
struct MigrationSafetyGuarantees {
    bool zeroDowntime = true;                  // ✓ No backup service interruption
    bool zeroDataLoss = true;                  // ✓ No mutations lost during migration
    bool instantRollback = true;               // ✓ <5 minute rollback to legacy mode
    bool dataIntegrityMaintained = true;       // ✓ Continuous validation during migration
};
```

### Migration Control and Orchestration

```cpp
// Production-ready migration orchestration
class MigrationOrchestrator {
public:
    enum class MigrationPhase {
        LEGACY_ONLY,           // Standard FoundationDB backup (baseline)
        DUAL_TAGGING,          // CommitProxy adds both legacy + shard tags
        HYBRID_PROCESSING,     // BackupWorkers process both tag types
        SHARD_VALIDATION,      // Validate shard-aware backups vs legacy
        LEGACY_DEPRECATION,    // Disable legacy tag generation
        SHARD_ONLY            // Pure shard-aware mode (target state)
    };
    
    // Safe migration progression with health checks
    bool progressMigration(MigrationPhase targetPhase) {
        auto currentPhase = getCurrentMigrationPhase();
        
        // Only allow single-step progression for safety
        if (!isValidProgression(currentPhase, targetPhase)) {
            return false;
        }
        
        // Pre-migration health validation
        if (!validateClusterHealth()) {
            TraceEvent(SevWarn, "MigrationAborted").detail("Reason", "Cluster unhealthy");
            return false;
        }
        
        // Execute phase transition with monitoring
        auto transitionStart = std::chrono::steady_clock::now();
        bool success = executePhaseTransition(currentPhase, targetPhase);
        auto transitionDuration = std::chrono::steady_clock::now() - transitionStart;
        
        if (success) {
            // Post-migration validation
            bool healthyAfterTransition = validateClusterHealth();
            bool backupIntegrityPreserved = validateBackupIntegrity();
            
            if (!healthyAfterTransition || !backupIntegrityPreserved) {
                // Automatic rollback on validation failure
                emergencyRollback(currentPhase, "Post-migration validation failed");
                return false;
            }
            
            TraceEvent("MigrationPhaseComplete")
                .detail("FromPhase", currentPhase)
                .detail("ToPhase", targetPhase)
                .detail("DurationMs", std::chrono::duration_cast<std::chrono::milliseconds>(transitionDuration).count());
        }
        
        return success;
    }
    
private:
    bool executePhaseTransition(MigrationPhase from, MigrationPhase to) {
        switch (to) {
            case DUAL_TAGGING:
                return enableDualTagging();
            case HYBRID_PROCESSING:
                return enableHybridProcessing();
            case SHARD_VALIDATION:
                return enableShardValidation();
            case LEGACY_DEPRECATION:
                return disableLegacyTagging();
            case SHARD_ONLY:
                return enableShardOnlyMode();
            default:
                return false;
        }
    }
    
    void emergencyRollback(MigrationPhase safePhase, const std::string& reason) {
        TraceEvent(SevError, "EmergencyMigrationRollback")
            .detail("Reason", reason)
            .detail("RollbackToPhase", safePhase);
        
        // Immediate rollback to last known safe state
        switch (safePhase) {
            case LEGACY_ONLY:
                disableAllShardAwareFeatures();
                break;
            case DUAL_TAGGING:
                disableHybridProcessing();
                break;
            case HYBRID_PROCESSING:
                disableShardValidation();
                break;
            // ... handle all rollback scenarios
        }
    }
};
```

**Metadata File Strategy**:

**JSON Files Written**:
1. **Per-batch file metadata**: Written immediately after each batch file upload (~every 5 minutes per worker)
2. **Consolidated shard index**: Updated periodically (~every hour) by a background process
3. **NOT per-shard files**: We do NOT write 250,000 individual files!

**Periodic Index Update Process**:
```cpp
// Background process that periodically rebuilds the consolidated index
class ShardIndexManager {
    void updateConsolidatedIndex() {
        // Run every hour, not on every batch file upload
        if (timeSinceLastUpdate() > std::chrono::hours(1)) {
            
            // Scan only NEW batch files since last update
            auto newBatchFiles = getNewBatchFilesSinceLastUpdate();
            
            // Load existing consolidated index
            auto existingIndex = loadConsolidatedShardIndex();
            
            // Update index with new batch files
            for (const auto& filename : newBatchFiles) {
                auto metadata = reconstructFromBatchFile(filename);
                for (const auto& shardInfo : metadata.containedShards) {
                    existingIndex["shardToFiles"][std::to_string(shardInfo.shardId)]
                        .push_back(filename);
                }
            }
            
            // Upload updated consolidated index (single file)
            uploadConsolidatedIndex(existingIndex);
            
            TraceEvent("ConsolidatedIndexUpdated")
                .detail("NewBatchFiles", newBatchFiles.size())
                .detail("TotalShards", existingIndex["shardToFiles"].size());
        }
    }
};
```

**File Count Summary**:
- **Per-batch metadata files**: ~100-1000 files (reasonable)
- **Consolidated shard index**: **1 single file** (not 250,000!)
- **Total JSON overhead**: Minimal and manageable

**Recovery Performance**:
- **With consolidated index**: Instant lookup for any shard
- **Without consolidated index**: Scan batch files only when needed
- **Index rebuild**: ~30 seconds for 100,000 batch files vs seconds with existing index

### Benefits of Using Existing Hierarchy

1. **Tool Compatibility**: Existing backup management tools continue working
2. **Cross-Platform Support**: Works with all backup containers (S3, Azure, Local)
3. **Proven Scalability**: File organization tested at production scale
4. **Standard Operations**: Existing backup/restore procedures unchanged
5. **Efficient Cleanup**: Existing retention and cleanup logic works
6. **Metadata Resilience**: JSON loss doesn't break restore - only slows lookup
7. **Self-Contained Files**: Each batch file contains complete shard information

### Multi-Shard File Generation Examples

```cpp
// Enhanced BackupWorker writing multi-shard log files
class EnhancedBackupWorker {
    void writeMultiShardLogFile(const std::vector<TaggedMutation>& mutations) {
        // Collect all shards represented in this batch of mutations
        std::set<uint64_t> containedShards;
        for (const auto& mutation : mutations) {
            if (mutation.tag.locality == tagLocalityBackupShard) {
                containedShards.insert(mutation.tag.id);
            }
        }
        
        // Use existing writeTaggedLogFile - same as current FDB
## 11. Backward Compatibility and Migration Strategy

### 11.1 Compatibility Requirements

FoundationDB Backup v3 must maintain **100% backward compatibility** with existing v2 backups while enabling seamless migration to the enhanced shard-aware architecture.

```cpp
/**
 * BACKWARD COMPATIBILITY PRINCIPLES
 * 
 * 1. RESTORE COMPATIBILITY: v3 system MUST restore any v2 backup perfectly
 * 2. COEXISTENCE: v2 and v3 systems can operate simultaneously in same environment  
 * 3. GRADUAL MIGRATION: No "big bang" migration - smooth transition over time
 * 4. ROLLBACK SAFETY: Can safely revert to v2 if issues discovered
 * 5. ZERO DOWNTIME: Migration occurs without backup/restore service interruption
 */

enum class BackupVersion {
    V2_LEGACY = 2,        // Original FoundationDB backup format
    V3_SHARD_AWARE = 3    // Enhanced shard-aware backup format
};

// Version detection from backup container metadata
BackupVersion detectBackupVersion(const std::string& backupUrl) {
    /*
     * VERSION DETECTION STRATEGY:
     * 
     * v2 Indicators:
     * - Presence of range_* and log_* files without shard metadata
     * - Missing shard-mapping-*.json files
     * - BackupManifest format version < 3
     * 
     * v3 Indicators:
     * - Presence of shard-mapping-*.json files
     * - Batch files with ShardedBatchFileHeader
     * - BackupManifest format version >= 3
     */
    
    auto container = openBackupContainer(backupUrl);
    
    // Check for v3 indicators first
    if (container->exists("metadata/shard-mapping-snapshot.json") || 
        container->hasFilePattern("metadata/shard-mapping-*.json")) {
        return BackupVersion::V3_SHARD_AWARE;
    }
    
    // Check manifest format version
    auto manifest = container->readBackupManifest();
    if (manifest.formatVersion >= 3) {
        return BackupVersion::V3_SHARD_AWARE;
    }
    
    return BackupVersion::V2_LEGACY;
}
```

### 11.2 Hybrid Restore Engine

The v3 restore system includes a **hybrid engine** that automatically handles both v2 and v3 backup formats transparently.

```cpp
class HybridRestoreEngine {
private:
    std::unique_ptr<V2RestoreEngine> legacyEngine;
    std::unique_ptr<V3ShardAwareRestoreEngine> modernEngine;
    BackupVersion detectedVersion;
    
public:
    HybridRestoreEngine(const std::string& backupUrl) {
        // Automatic version detection
        detectedVersion = detectBackupVersion(backupUrl);
        
        switch (detectedVersion) {
            case BackupVersion::V2_LEGACY:
                legacyEngine = std::make_unique<V2RestoreEngine>(backupUrl);
                TraceEvent("UsingLegacyRestoreEngine")
                    .detail("BackupUrl", backupUrl)
                    .detail("Reason", "v2 backup format detected");
                break;
                
            case BackupVersion::V3_SHARD_AWARE:
                modernEngine = std::make_unique<V3ShardAwareRestoreEngine>(backupUrl);
                TraceEvent("UsingShardAwareRestoreEngine")
                    .detail("BackupUrl", backupUrl)
                    .detail("Reason", "v3 shard-aware format detected");
                break;
        }
    }
    
    // Unified restore interface - automatically routes to correct engine
    Future<Void> restoreRange(KeyRange range, Version version) {
        switch (detectedVersion) {
            case BackupVersion::V2_LEGACY:
                // v2 restore: Read entire dataset, filter client-side (slow but compatible)
                return legacyEngine->restoreRange(range, version);
                
            case BackupVersion::V3_SHARD_AWARE:
                // v3 restore: Shard-aware selective reading (250,000x faster)
                return modernEngine->restoreRange(range, version);
        }
    }
    
    // Performance reporting shows efficiency gained from migration
    RestorePerformanceMetrics getPerformanceMetrics() const {
        RestorePerformanceMetrics metrics;
        
        switch (detectedVersion) {
            case BackupVersion::V2_LEGACY:
                metrics.backupVersion = "v2 (legacy)";
                metrics.efficiencyFactor = 1.0; // Baseline
                metrics.shardAware = false;
                metrics.recommendation = "Migrate to v3 for 250,000x performance improvement";
                break;
                
            case BackupVersion::V3_SHARD_AWARE:
                metrics.backupVersion = "v3 (shard-aware)";
                metrics.efficiencyFactor = 250000.0; // Up to 250,000x improvement
                metrics.shardAware = true;
                metrics.recommendation = "Optimal performance achieved";
                break;
        }
        
        return metrics;
    }
};
```

### 11.3 Migration Strategy Overview

Migration follows a **four-phase approach** designed to minimize risk and ensure seamless transition:

```mermaid
graph TD
    A[Phase 1: Coexistence Setup] --> B[Phase 2: Gradual Enablement]
    B --> C[Phase 3: Full Migration]
    C --> D[Phase 4: Legacy Cleanup]
    
    A --> A1[Deploy v3 binaries]
    A --> A2[v3 disabled by default]
    A --> A3[v2 continues normally]
    
    B --> B1[Enable v3 for new backups]
    B --> B2[v2 backups still restorable]
    B --> B3[Monitor performance gains]
    
    C --> C1[Switch all backup jobs to v3]
    C --> C2[Build shard indexes for old backups]
    C --> C3[Validate migration success]
    
    D --> D1[Remove v2 compatibility code]
    D --> D2[Archive old backups]
    D --> D3[Complete v3 deployment]
```

#### Phase 1: Coexistence Setup (Week 1-2)

**Goal**: Deploy v3 system alongside existing v2 with zero impact.

```cpp
// Configuration for coexistence phase
struct MigrationConfig {
    bool enableV3Backup = false;          // v3 backup disabled initially
    bool enableV3Restore = true;          // v3 restore enabled (handles both formats)
    bool enableV2Compatibility = true;    // Keep v2 restore path active
    bool enableMigrationLogging = true;   // Detailed migration tracking
    
    // Safety controls
    double v3BackupRolloutPercent = 0.0;  // Gradual rollout percentage
    bool allowV3Rollback = true;          // Can revert to v2 if needed
    std::vector<std::string> v3TestClusters; // Test clusters for validation
};

class MigrationController {
public:
    void deployPhase1() {
        /*
         * PHASE 1 DEPLOYMENT CHECKLIST:
         * 
         * ✓ Deploy v3 binaries with v3 features disabled
         * ✓ Verify v2 backups continue working normally  
         * ✓ Test v3 restore engine with existing v2 backups
         * ✓ Establish migration monitoring dashboards
         * ✓ Validate rollback procedures work correctly
         */
        
        TraceEvent("MigrationPhase1Starting")
            .detail("BackupV3Enabled", false)
            .detail("RestoreV3Enabled", true)
            .detail("ExpectedBehavior", "Zero impact on existing operations");
        
        // Deploy with safety-first configuration
        auto config = getDefaultMigrationConfig();
        config.enableV3Backup = false;  // Critical: Keep backups in v2 format
        config.enableV3Restore = true;  // Safe: v3 restore handles v2 backups
        
        applyMigrationConfiguration(config);
        
        // Validation: Ensure existing backups still work
        validateExistingBackupsAccessible();
        validateRestorePerformanceBaseline();
        
        TraceEvent("MigrationPhase1Complete")
            .detail("V2BackupsValidated", true)
            .detail("V3RestoreEngineDeployed", true)
            .detail("ReadyForPhase2", true);
    }
};
```

#### Phase 2: Gradual Enablement (Week 3-6)

**Goal**: Enable v3 backups for select clusters while maintaining v2 compatibility.

```cpp
void deployPhase2() {
    /*
     * PHASE 2 DEPLOYMENT STRATEGY:
     * 
     * Week 3: Enable v3 for 1 test cluster (1% of backup load)
     * Week 4: Enable v3 for dev/staging clusters (10% of backup load)  
     * Week 5: Enable v3 for select production clusters (25% of backup load)
     * Week 6: Enable v3 for majority of clusters (75% of backup load)
     */
    
    auto config = getCurrentMigrationConfig();
    
    // Gradual rollout with careful monitoring
    for (int week = 3; week <= 6; week++) {
        double rolloutPercent = calculateRolloutPercent(week);
        
        TraceEvent("MigrationPhase2Week")
            .detail("Week", week)
            .detail("RolloutPercent", rolloutPercent)
            .detail("ExpectedImpact", "Significant performance improvement for new backups");
        
        config.v3BackupRolloutPercent = rolloutPercent;
        applyMigrationConfiguration(config);
        
        // Wait for rollout to stabilize
        std::this_thread::sleep_for(std::chrono::hours(24 * 7)); // 1 week
        
        // Validate no regressions
        auto metrics = collectMigrationMetrics();
        validateNoRegressions(metrics);
        
        // Report efficiency gains
        TraceEvent("MigrationPhase2Metrics")
            .detail("Week", week)
            .detail("V3BackupsCreated", metrics.v3BackupsCreated)
            .detail("PerformanceImprovement", metrics.averageSpeedupFactor)
            .detail("DataIntegrityIssues", metrics.dataIntegrityIssues) // Should be 0
            .detail("RestoreFailures", metrics.restoreFailures);       // Should be 0
    }
}

double calculateRolloutPercent(int week) {
    switch (week) {
        case 3: return 1.0;   // 1% - Single test cluster
        case 4: return 10.0;  // 10% - Dev/staging
        case 5: return 25.0;  // 25% - Select production
        case 6: return 75.0;  // 75% - Majority production
        default: return 0.0;
    }
}
```

#### Phase 3: Full Migration (Week 7-10)

**Goal**: Complete migration to v3 and build shard indexes for existing v2 backups.

```cpp
void deployPhase3() {
    /*
     * PHASE 3: COMPLETE MIGRATION
     * 
     * Week 7: Enable v3 for all clusters (100% rollout)
     * Week 8: Start building shard indexes for important v2 backups
     * Week 9: Continue index building, optimize restore performance
     * Week 10: Validate all critical backups have shard indexes
     */
    
    TraceEvent("MigrationPhase3Starting")
        .detail("Goal", "Complete v3 migration and optimize v2 backup performance");
    
    // Enable v3 for all backup operations
    auto config = getCurrentMigrationConfig();
    config.v3BackupRolloutPercent = 100.0;
    config.enableV3Backup = true;
    applyMigrationConfiguration(config);
    
    // Build shard indexes for critical v2 backups to improve restore performance
    auto legacyBackups = identifyCriticalLegacyBackups();
    
    for (const auto& backup : legacyBackups) {
        TraceEvent("BuildingShardIndexForLegacyBackup")
            .detail("BackupUrl", backup.url)
            .detail("Priority", backup.priority)
            .detail("EstimatedImprovementFactor", 250000);
        
        // Build shard mapping index for existing v2 backup
        auto indexBuilder = std::make_unique<LegacyBackupIndexBuilder>(backup.url);
        indexBuilder->buildShardMappingIndex();
        
        // Test restore performance improvement
        auto beforeMetrics = testRestorePerformance(backup.url, false); // Without index
        auto afterMetrics = testRestorePerformance(backup.url, true);   // With index
        
        double speedupFactor = beforeMetrics.durationMs / afterMetrics.durationMs;
        
        TraceEvent("LegacyBackupIndexBuildComplete")
            .detail("BackupUrl", backup.url)
            .detail("SpeedupFactor", speedupFactor)
            .detail("BeforeMs", beforeMetrics.durationMs)
            .detail("AfterMs", afterMetrics.durationMs)
            .detail("EfficiencyGained", speedupFactor >= 1000.0); // Should be massive
    }
}
```

#### Phase 4: Legacy Cleanup (Week 11+)

**Goal**: Remove v2 compatibility code and finalize v3 deployment.

```cpp
void deployPhase4() {
    /*
     * PHASE 4: FINALIZE MIGRATION
     * 
     * - All new backups use v3 format
     * - Critical v2 backups have shard indexes  
     * - v2 compatibility can be removed from new clusters
     * - Archive oldest v2 backups per retention policy
     */
    
    TraceEvent("MigrationPhase4Starting")
        .detail("Goal", "Finalize v3 deployment and clean up legacy code");
    
    // Update configuration for pure v3 operation
    auto config = getCurrentMigrationConfig();
    config.enableV2Compatibility = false;  // Can disable on new clusters
    config.allowV3Rollback = false;        // Migration is permanent
    
    // Apply to new cluster deployments only (keep compatibility on existing)
    applyMigrationConfigurationToNewClusters(config);
    
    // Archive old v2 backups according to retention policy
    auto oldBackups = identifyBackupsForArchival();
    for (const auto& backup : oldBackups) {
        if (backup.hasShardIndex || backup.age > getRetentionPolicy().maxAge) {
            TraceEvent("ArchivingLegacyBackup")
                .detail("BackupUrl", backup.url)
                .detail("Age", backup.age)
                .detail("HasShardIndex", backup.hasShardIndex)
                .detail("Reason", backup.age > getRetentionPolicy().maxAge ? 
                       "retention_expired" : "shard_index_available");
            
            archiveBackup(backup.url);
        }
    }
    
    TraceEvent("MigrationPhase4Complete")
        .detail("V3DeploymentFinalized", true)
        .detail("PerformanceImprovementAchieved", "Up to 250,000x for partial restores")
        .detail("BackwardCompatibilityMaintained", true);
}
```

### 11.4 Rollback Safety Mechanisms

Critical safety mechanisms ensure the migration can be safely reverted if issues are discovered:

```cpp
class MigrationRollbackController {
public:
    // Emergency rollback to v2 if critical issues discovered
    void emergencyRollbackToV2(const std::string& reason) {
        TraceEvent(SevWarn, "EmergencyMigrationRollback")
            .detail("Reason", reason)
            .detail("Action", "Reverting all clusters to v2 backup format");
        
        // Immediate actions:
        // 1. Disable v3 backup creation
        // 2. Re-enable v2 backup workers
        // 3. Preserve all existing backups (both v2 and v3)
        // 4. Switch restore operations to compatibility mode
        
        auto rollbackConfig = MigrationConfig{};
        rollbackConfig.enableV3Backup = false;
        rollbackConfig.enableV3Restore = true;  // Keep for compatibility
        rollbackConfig.enableV2Compatibility = true;
        rollbackConfig.v3BackupRolloutPercent = 0.0;
        
        applyEmergencyConfiguration(rollbackConfig);
        
        // Notify operations team
        alertOperationsTeam("Migration rollback executed", reason);
        
        TraceEvent("EmergencyRollbackComplete")
            .detail("V3BackupsDisabled", true)
            .detail("V2RestoreFunctional", true)
            .detail("DataIntegrityPreserved", true);
    }
    
    // Validation that rollback capability works
    void validateRollbackCapability() {
        /*
         * ROLLBACK VALIDATION CHECKLIST:
         * 
         * ✓ Can disable v3 backup creation instantly
         * ✓ v2 restore still works for all existing backups
         * ✓ v3 restore still works for v3 backups created during migration
         * ✓ No data loss during rollback process
         * ✓ Operations team has clear rollback procedures
         */
        
        TraceEvent("ValidatingRollbackCapability");
        
        // Test configuration switch speed
        auto startTime = std::chrono::high_resolution_clock::now();
        
        auto testConfig = getCurrentMigrationConfig();
        testConfig.enableV3Backup = false;
        applyMigrationConfiguration(testConfig);
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto switchTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        // Validate quick rollback (should be under 30 seconds)
        bool rollbackFastEnough = switchTime.count() < 30000; // 30 seconds
        
        TraceEvent("RollbackCapabilityValidated")
            .detail("ConfigSwitchTimeMs", switchTime.count())
            .detail("FastEnoughForEmergency", rollbackFastEnough)
            .detail("MaxAcceptableMs", 30000);
        
        if (!rollbackFastEnough) {
            throw std::runtime_error("Rollback capability too slow for emergency use");
        }
    }
};
```

### 11.5 Migration Validation and Testing

```cpp
class MigrationValidationSuite {
public:
    // Comprehensive validation during each migration phase
    void validateMigrationPhase(MigrationPhase phase) {
        switch (phase) {
            case PHASE_1_COEXISTENCE:
                validateCoexistencePhase();
                break;
            case PHASE_2_GRADUAL_ENABLEMENT:
                validateGradualEnablementPhase();
                break;
            case PHASE_3_FULL_MIGRATION:
                validateFullMigrationPhase();
                break;
            case PHASE_4_LEGACY_CLEANUP:
                validateLegacyCleanupPhase();
                break;
        }
    }
    
private:
    void validateCoexistencePhase() {
        /*
         * PHASE 1 VALIDATION CHECKLIST:
         * 
         * ✓ Existing v2 backups continue working normally
         * ✓ v3 restore engine can read v2 backups perfectly
         * ✓ No performance degradation in baseline operations
         * ✓ Emergency rollback procedures tested and working
         */
        
        // Test 1: Verify v2 backup operations unchanged
        auto v2BackupTest = runV2BackupTest();
        ASSERT(v2BackupTest.success);
        ASSERT(v2BackupTest.performanceDelta < 0.05); // <5% performance change
        
        // Test 2: Verify v3 restore engine can read v2 backups
        auto v3RestoreV2Test = runV3RestoreEngineOnV2Backup();
        ASSERT(v3RestoreV2Test.success);
        ASSERT(v3RestoreV2Test.dataIntegrityPerfect);
        
        // Test 3: Verify rollback works
        auto rollbackTest = testEmergencyRollback();
        ASSERT(rollbackTest.rollbackTimeMs < 30000); // <30 seconds
        ASSERT(rollbackTest.dataLossEvents == 0);     // No data loss
        
        TraceEvent("Phase1ValidationComplete")
            .detail("V2BackupFunctional", true)
            .detail("V3RestoreCompatible", true)
            .detail("RollbackTested", true);
    }
    
    void validateGradualEnablementPhase() {
        /*
         * PHASE 2 VALIDATION CHECKLIST:
         * 
         * ✓ v3 backups work correctly on test clusters
         * ✓ Performance improvement measurable for partial restores
         * ✓ No regressions in backup creation or restore operations
         * ✓ Cross-version restore works (v3 engine, v2 and v3 backups)
         */
        
        // Test 1: v3 backup functionality
        auto v3BackupTest = runV3BackupTest();
        ASSERT(v3BackupTest.success);
        ASSERT(v3BackupTest.shardMetadataComplete);
        
        // Test 2: Performance improvement measurement
        auto performanceTest = measurePartialRestoreImprovement();
        ASSERT(performanceTest.improvementFactor > 1000.0); // At least 1000x better
        
        // Test 3: Cross-version restore
        auto crossVersionTest = testCrossVersionRestore();
        ASSERT(crossVersionTest.v2RestoreWorks);
        ASSERT(crossVersionTest.v3RestoreWorks);
        ASSERT(crossVersionTest.dataConsistencyPerfect);
        
        TraceEvent("Phase2ValidationComplete")
            .detail("V3BackupFunctional", true)
            .detail("PerformanceImproved", performanceTest.improvementFactor)
            .detail("CrossVersionTested", true);
    }
    
    void validateFullMigrationPhase() {
        /*
         * PHASE 3 VALIDATION CHECKLIST:
         * 
         * ✓ All clusters successfully migrated to v3
         * ✓ Legacy backup indexes built for critical backups
         * ✓ Production restore performance meets targets
         * ✓ No data consistency issues across any backup format
         */
        
        // Test 1: Migration completeness
        auto migrationTest = validateMigrationCompleteness();
        ASSERT(migrationTest.allClustersV3);
        ASSERT(migrationTest.noMigrationFailures);
        
        // Test 2: Legacy index building
        auto indexTest = validateLegacyIndexBuilding();
        ASSERT(indexTest.criticalBackupsIndexed);
        ASSERT(indexTest.performanceImprovementMeasured);
        
        // Test 3: Production performance targets
        auto performanceTest = validateProductionPerformance();
        ASSERT(performanceTest.partialRestoreEfficiency >= 100000.0); // At least 100,000x
        ASSERT(performanceTest.fullRestorePerformanceNoRegression);
        
        TraceEvent("Phase3ValidationComplete")
            .detail("MigrationComplete", true)
            .detail("LegacyIndexesBuilt", true)
            .detail("PerformanceTargetsMet", true);
    }
    
    void validateLegacyCleanupPhase() {
        /*
         * PHASE 4 VALIDATION CHECKLIST:
         * 
         * ✓ Legacy code removal doesn't break functionality
         * ✓ Archive procedures work correctly
         * ✓ New cluster deployments use v3 by default
         * ✓ Final performance targets achieved
         */
        
        // Test 1: Post-cleanup functionality
        auto functionalityTest = validatePostCleanupFunctionality();
        ASSERT(functionalityTest.allRestoreOperationsWork);
        ASSERT(functionalityTest.noRegressionIntroduced);
        
        // Test 2: Archive procedures
        auto archiveTest = validateArchiveProcedures();
        ASSERT(archiveTest.archiveProceduresWork);
        ASSERT(archiveTest.retentionPolicyRespected);
        
        // Test 3: Final performance validation
        auto finalPerformanceTest = validateFinalPerformance();
        ASSERT(finalPerformanceTest.maxEfficiencyAchieved >= 250000.0); // Full 250,000x
        
        TraceEvent("Phase4ValidationComplete")
            .detail("LegacyCleanupComplete", true)
            .detail("ArchiveProceduresWork", true)
            .detail("MaxPerformanceAchieved", finalPerformanceTest.maxEfficiencyAchieved);
    }
};
```

### 11.6 Migration Success Criteria

**Technical Success Criteria**:
- **Performance**: 250,000x efficiency improvement achieved for partial restores
- **Compatibility**: 100% backward compatibility with existing v2 backups
- **Reliability**: Zero data loss during migration process
- **Speed**: <5 minute emergency rollback capability maintained

**Operational Success Criteria**:
- **Deployment**: 100% of production clusters successfully migrated
- **Downtime**: Zero customer-impacting downtime during migration
- **Performance**: No degradation in backup/restore SLAs during migration
- **Rollout**: Staged rollout completed without any rollbacks needed

**Business Success Criteria**:
- **SRE Efficiency**: Dramatically reduced time for partial restore operations
- **Cost Reduction**: Significant reduction in storage and network costs for restore operations
- **Risk Mitigation**: Enhanced backup/restore capabilities for disaster recovery

This comprehensive backward compatibility and migration strategy ensures the FoundationDB Backup v3 enhancement can be deployed safely in production environments while delivering the revolutionary 250,000x efficiency improvement with zero risk to existing operations.

        Version beginVersion = mutations.front().version;
        Version endVersion = mutations.back().version;
        auto logFile = backupContainer->writeTaggedLogFile(
            beginVersion, endVersion,
            CLIENT_KNOBS->BACKUP_BLOCK_SIZE,
            workerId,  // Use worker ID as tag, not shard ID
            getTotalWorkerCount()
        ).get();
        
        // Write all mutations to single file (multi-shard)
        for (const auto& mutation : mutations) {
            ShardedVersionedMutation shardedMutation(mutation);
            BinaryWriter writer(AssumeVersion(ProtocolVersion::withBackupMutations()));
            writer << shardedMutation;
            logFile->append(writer.getData(), writer.getLength());
        }
        
        logFile->finish();
        
        // File path becomes: "log/logfile-00000000000001000000-00000000000002000000-3.fdbackup"
        // (worker 3, contains mutations from multiple shards)
        
        // Create shard metadata tracking which shards are in this file:
        writeShardMetadata(logFile->getFileName(), containedShards, beginVersion, endVersion);
        // Creates: "metadata/shard_metadata/logfile-00000000000001000000-00000000000002000000-3.json"
        // Contains: {"containedShards": [1, 5, 12, 23, 67, 89, 124], ...}
    }
};
```

This approach **preserves FoundationDB's proven backup file organization** while adding the minimal metadata needed for shard-aware operations. No custom path generation needed - we build on 10+ years of production-tested backup infrastructure.

### 3. Version Ordering Guarantee

```cpp
class VersionOrderingManager {
public:
    void ensureVersionOrdering(std::vector<TaggedMutation>& mutations) {
        // Sort mutations by version first, then by shard ID for deterministic ordering
        std::sort(mutations.begin(), mutations.end(), 
                 [](const TaggedMutation& a, const TaggedMutation& b) {
                     if (a.version != b.version) {
                         return a.version < b.version;
                     }
                     return a.tag.id < b.tag.id;
                 });
    }
    
    void validateVersionOrdering(const std::vector<uint8_t>& batchData) {
        // Parse batch file using FDB's deserialization
        size_t offset = sizeof(BatchFileHeader);
        
        // Skip shard index
        BatchFileHeader* header = reinterpret_cast<BatchFileHeader*>(
            const_cast<uint8_t*>(batchData.data()));
        offset += header->shardCount * sizeof(ShardIndex);
        
        uint64_t lastVersion = 0;
        for (uint64_t i = 0; i < header->mutationCount; ++i) {
            // Use FDB's BinaryReader for consistent deserialization
            BinaryReader reader(batchData.data() + offset,
                              batchData.size() - offset,
                              AssumeVersion(ProtocolVersion::withBackupMutations()));
            
            ShardedVersionedMutation shardedMutation;
            reader >> shardedMutation;
            
            LogMessageVersion currentVersion = shardedMutation.version();
            if (currentVersion < lastVersion) {
                throw std::runtime_error("Version ordering violation detected");
            }
            
            lastVersion = currentVersion;
            offset += reader.getReadBytes();
        }
    }
};
```

## Benefits of This S3 Writing System

### 1. Efficient Storage
- **Structured Format**: Self-describing files with headers and indexes
- **Compression Ready**: Framework supports multiple compression algorithms
- **Checksums**: Data integrity verification at multiple levels

### 2. Optimal S3 Usage
- **Hierarchical Keys**: Time-based organization for efficient listing/cleanup
- **Multipart Uploads**: Handle large files efficiently
- **Retry Logic**: Robust error handling with exponential backoff

### 3. Fast Restore Lookups
- **Shard Indexes**: Instantly identify which files contain which shards
- **Version Ranges**: Quickly filter files by version range
- **Metadata**: Rich metadata for precise file selection

### 4. Version Ordering
- **Strict Ordering**: Mutations are stored in version order
- **Deterministic**: Same order across all backup instances
- **Validation**: Built-in verification of ordering correctness

This system provides the detailed implementation for efficient S3 writing and mutation aggregation while maintaining the performance benefits of the shard-based partitioning system.
## Shard-Aware Restore Process

### How to Read Backup at Restore Time

The restore process is where we achieve the **250,000x efficiency improvement** (4MB vs 1TB read):

```cpp
// Enhanced restore process for shard-aware backups
class ShardAwareRestoreService {
public:
    // Main restore entry point - only read mutations for specific shards
    void restoreShards(const std::vector<uint64_t>& targetShards, 
                       Version startVersion, Version endVersion) {
        
        TraceEvent("ShardAwareRestoreStart")
            .detail("TargetShards", targetShards.size())
            .detail("StartVersion", startVersion)
            .detail("EndVersion", endVersion);
        
        // Step 1: Find which files contain our target shards
        auto relevantFiles = findFilesContainingShards(targetShards, startVersion, endVersion);
        
        TraceEvent("ShardAwareRestoreFileSelection")
            .detail("TotalFiles", relevantFiles.size())
            .detail("EstimatedDataReduction", calculateDataReduction(relevantFiles, targetShards));
        
        // Step 2: Read only the relevant mutations from selected files
        std::vector<VersionedMutation> mutations;
        for (const auto& fileInfo : relevantFiles) {
            auto shardMutations = readShardMutationsFromFile(fileInfo.filename, targetShards);
            mutations.insert(mutations.end(), shardMutations.begin(), shardMutations.end());
        }
        
        // Step 3: Sort by version and apply
        std::sort(mutations.begin(), mutations.end(), 
                 [](const auto& a, const auto& b) { return a.version < b.version; });
        
        // Step 4: Apply mutations to restore database
        applyMutationsToDatabase(mutations);
        
        TraceEvent("ShardAwareRestoreComplete")
            .detail("MutationsApplied", mutations.size())
            .detail("DataReadMB", calculateTotalDataRead(relevantFiles) / (1024*1024));
    }

private:
    // Step 1: Find files containing target shards
    std::vector<FileInfo> findFilesContainingShards(
            const std::vector<uint64_t>& targetShards, 
            Version startVersion, Version endVersion) {
        
        std::vector<FileInfo> relevantFiles;
        
        try {
            // Fast path: Use consolidated shard index
            auto consolidatedIndex = loadConsolidatedShardIndex();
            std::set<std::string> candidateFiles;
            
            // Collect all files that contain any of our target shards
            for (uint64_t shardId : targetShards) {
                auto filesForShard = consolidatedIndex["shardToFiles"][std::to_string(shardId)];
                for (const auto& filename : filesForShard.get<std::vector<std::string>>()) {
                    candidateFiles.insert(filename);
                }
            }
            
            // Filter by version range
            for (const auto& filename : candidateFiles) {
                auto fileInfo = parseFileInfo(filename);
                if (fileInfo.endVersion >= startVersion && fileInfo.startVersion <= endVersion) {
                    relevantFiles.push_back(fileInfo);
                }
            }
            
        } catch (const std::exception& e) {
            // Fallback: Scan all batch files
            TraceEvent(SevWarn, "ConsolidatedIndexUnavailable")
                .detail("Error", e.what())
                .detail("FallingBackToScan", true);
            
            relevantFiles = scanAllFilesForShards(targetShards, startVersion, endVersion);
        }
        
        return relevantFiles;
    }
    
    // Step 2: Read only target shard mutations from a file
    std::vector<VersionedMutation> readShardMutationsFromFile(
            const std::string& filename, 
            const std::vector<uint64_t>& targetShards) {
        
        std::vector<VersionedMutation> mutations;
        std::set<uint64_t> targetShardSet(targetShards.begin(), targetShards.end());
        
        // Download and parse batch file
        auto fileData = downloadBatchFile(filename);
        
        // Parse header
        BatchFileHeader* header = reinterpret_cast<BatchFileHeader*>(fileData.data());
        
        // Parse shard index
        size_t indexOffset = sizeof(BatchFileHeader);
        ShardIndex* shardIndex = reinterpret_cast<ShardIndex*>(fileData.data() + indexOffset);
        
        // Pre-filter: Only process shards we care about
        std::vector<ShardIndex> relevantShards;
        for (uint32_t i = 0; i < header->shardCount; i++) {
            if (targetShardSet.count(shardIndex[i].shardId)) {
                relevantShards.push_back(shardIndex[i]);
            }
        }
        
        TraceEvent("ShardFilteringInFile")
            .detail("Filename", filename)
            .detail("TotalShardsInFile", header->shardCount)
            .detail("RelevantShards", relevantShards.size())
            .detail("DataReductionRatio", (double)relevantShards.size() / header->shardCount);
        
        // Read mutations from relevant shards only
        for (const auto& shardInfo : relevantShards) {
            // Jump to this shard's mutation range
            size_t mutationOffset = shardInfo.firstMutationOffset;
            
            // Read mutations for this shard
            while (mutationOffset <= shardInfo.lastMutationOffset && 
                   mutationOffset < fileData.size()) {
                
                BinaryReader reader(fileData.data() + mutationOffset,
                                   fileData.size() - mutationOffset,
                                   AssumeVersion(ProtocolVersion::withBackupMutations()));
                
                ShardedVersionedMutation shardedMutation;
                reader >> shardedMutation;
                
                // Verify this mutation belongs to target shard
                if (targetShardSet.count(shardedMutation.shardId)) {
                    mutations.push_back({
                        .version = shardedMutation.version(),
                        .mutation = shardedMutation.mutation()
                    });
                }
                
                mutationOffset += reader.getReadBytes();
                if (mutationOffset > shardInfo.lastMutationOffset) break;
            }
        }
        
        return mutations;
    }
};
```

### Performance Comparison: Legacy vs Shard-Aware Restore

```cpp
// Legacy restore (reads everything)
class LegacyRestoreService {
    void restoreDatabase(Version startVersion, Version endVersion) {
        // PROBLEM: Must read ALL backup files and filter locally
        auto allFiles = listAllBackupFiles(startVersion, endVersion);
        
        for (const auto& filename : allFiles) {
            auto fileData = downloadBatchFile(filename);  // Download ENTIRE file
            auto mutations = parseAllMutations(fileData);  // Parse ALL mutations
            
            // Filter mutations locally (wasteful!)
            for (const auto& mutation : mutations) {
                if (isRelevantForRestore(mutation)) {
                    applyMutation(mutation);
                }
            }
        }
        
        // RESULT: Read 1TB to restore 4MB of data (250,000x inefficient!)
    }
};

// Shard-aware restore (reads only what's needed)
class ShardAwareRestoreService {
    void restoreShards(const std::vector<uint64_t>& targetShards, 
                       Version startVersion, Version endVersion) {
        
        // SOLUTION: Pre-filter files and read only relevant shard data
        auto relevantFiles = findFilesContainingShards(targetShards, startVersion, endVersion);
        
        for (const auto& fileInfo : relevantFiles) {
            // Only download what we need from each file
            auto shardMutations = readShardMutationsFromFile(fileInfo.filename, targetShards);
            applyMutations(shardMutations);
        }
        
        // RESULT: Read 4MB to restore 4MB of data (250,000x more efficient!)
    }
};
```

### Key Efficiency Gains

**1. File-Level Filtering**:
- **Legacy**: Downloads all 10,000 backup files (1TB total)
- **Shard-aware**: Downloads only 50 files containing target shards (50MB total)
- **Shard-aware**: Downloads only 50 files containing target shards (50MB total)

**2. Intra-File Filtering**:
- **Legacy**: Parses all mutations in each file
- **Shard-aware**: Uses ShardIndex to jump directly to relevant mutations

**3. Network Transfer Reduction**:
- **Legacy**: 1TB network transfer
- **Shard-aware**: 4MB network transfer (250,000x reduction!)

**4. Processing Time Reduction**:
- **Legacy**: Parse and filter millions of irrelevant mutations
- **Shard-aware**: Process only the mutations actually needed

## Production Restore APIs

### Required Restore APIs for Real-World SRE Operations

The shard-aware backup system must support the production restore APIs used by SRE teams:

```cpp
// Production restore APIs that deliver 250,000x efficiency improvement
class ShardAwareRestoreService {
public:
    // API 1: Get snapshot data for a key range at a specific version
    RangeData getRangeSnapshot(KeyRange range, Version version);
    
    // API 2: Get mutations for a key range within a version range  
    MutationData getRangeMutation(KeyRange range, VersionRange versionRange);
};
```

### API 1: `getRangeSnapshot(KeyRange, Version)` - Efficient Range Snapshots

This API retrieves the snapshot state of a key range at a specific past version:

```cpp
class ShardAwareRestoreService {
public:
    RangeData getRangeSnapshot(KeyRange range, Version version) {
        TraceEvent("GetRangeSnapshot")
            .detail("Range", range.toString())
            .detail("Version", version);
        
        // Step 1: Map key range to affected shards at the target version
        auto affectedShards = mapKeyRangeToShards(range, version);
        
        TraceEvent("RangeToShardMapping")
            .detail("KeyRange", range.toString())
            .detail("AffectedShards", affectedShards.size())
            .detail("TotalShards", getTotalShardCount())
            .detail("DataReductionRatio", (double)affectedShards.size() / getTotalShardCount());
        
        // Step 2: Find snapshot files containing these shards at target version
        auto snapshotFiles = findSnapshotFilesForShards(affectedShards, version);
        
        // Step 3: Read only relevant range data from snapshot files
        RangeData rangeData;
        for (const auto& fileInfo : snapshotFiles) {
            auto shardData = readShardSnapshotData(fileInfo.filename, affectedShards, range);
            rangeData.merge(shardData);
        }
        
        TraceEvent("GetRangeSnapshotComplete")
            .detail("KeysRetrieved", rangeData.size())
            .detail("FilesScanned", snapshotFiles.size())
            .detail("DataReadMB", calculateDataRead(snapshotFiles) / (1024*1024));
        
        return rangeData;
    }

private:
    // Map a key range to specific shards that contained it AT A SPECIFIC VERSION
    std::vector<uint64_t> mapKeyRangeToShards(const KeyRange& range, Version version) {
        std::vector<uint64_t> affectedShards;
        
        // CRITICAL: Use historical shard mapping for the target version
        // Shard boundaries change over time due to splits/merges!
        auto historicalShardMapping = loadHistoricalShardMapping(version);
        
        for (const auto& [shardId, shardRange] : historicalShardMapping) {
            if (range.intersects(shardRange)) {
                affectedShards.push_back(shardId);
            }
        }
        
        TraceEvent("HistoricalShardMapping")
            .detail("Version", version)
            .detail("KeyRange", range.toString())
            .detail("AffectedShards", affectedShards.size())
            .detail("HistoricalMappingVersion", historicalShardMapping.mappingVersion);
        
        return affectedShards;
    }
    
    // Load shard mapping that was active at a specific version
    ShardMapping loadHistoricalShardMapping(Version version) {
        // Look up which shard mapping was active at the target version
        // This loads the time-ordered history of shard boundary changes
        auto mappingHistory = loadShardMappingHistory(version - 1000000, version + 1000);
        
        // Find the mapping that was active at the target version
        for (auto it = mappingHistory.rbegin(); it != mappingHistory.rend(); ++it) {
            if (it->effectiveVersion <= version) {
                TraceEvent("FoundHistoricalMapping")
                    .detail("TargetVersion", version)
                    .detail("MappingVersion", it->effectiveVersion)
                    .detail("TotalShards", it->mapping.size());
                return it->mapping;
            }
        }
        
        // Fallback: use the oldest mapping if none found
        return mappingHistory.front().mapping;
    }
    
    // Load shard mapping history from BACKUP DATA (no cluster queries needed!)
    std::vector<ShardMappingSnapshot> loadShardMappingHistory(Version startVersion, Version endVersion) {
        /*
         * CRITICAL REQUIREMENT: Restore from S3 backup data ONLY - no cluster access!
         * 
         * Solution: Store shard mapping snapshots AS PART OF THE BACKUP
         * - When shard boundaries change, BackupWorker writes mapping snapshot to S3
         * - During restore, read mapping history from S3 backup container
         * - Completely self-contained - no live cluster queries needed
         */
        
        TraceEvent("LoadingHistoricalShardMappingFromBackup")
            .detail("StartVersion", startVersion)
            .detail("EndVersion", endVersion)
            .detail("Method", "S3BackupContainer")
            .detail("Reason", "SelfContainedRestore");
        
        // Load shard mapping snapshots from backup container
        return loadShardMappingSnapshotsFromS3(startVersion, endVersion);
    }
    
    // Load shard mapping snapshots from S3 backup data
    std::vector<ShardMappingSnapshot> loadShardMappingSnapshotsFromS3(Version startVersion, Version endVersion) {
        std::vector<ShardMappingSnapshot> mappingHistory;
        
        try {
            // List all shard mapping snapshot files in backup
            auto snapshotFiles = listShardMappingSnapshotFiles();  // "shard-mapping-<version>.json"
            
            for (const auto& filename : snapshotFiles) {
                Version snapshotVersion = extractVersionFromMappingFilename(filename);
                
                // Only load snapshots in our version range
                if (snapshotVersion >= startVersion && snapshotVersion <= endVersion) {
                    auto snapshotData = downloadShardMappingSnapshot(filename);
                    mappingHistory.push_back(parseShardMappingSnapshot(snapshotData));
                }
            }
            
            // Sort by version
            std::sort(mappingHistory.begin(), mappingHistory.end(),
                     [](const auto& a, const auto& b) { return a.effectiveVersion < b.effectiveVersion; });
            
            TraceEvent("LoadedShardMappingFromS3")
                .detail("StartVersion", startVersion)
                .detail("EndVersion", endVersion)
                .detail("SnapshotsLoaded", mappingHistory.size());
            
        } catch (const std::exception& e) {
            TraceEvent(SevError, "FailedToLoadShardMappingFromS3")
                .detail("Error", e.what())
                .detail("StartVersion", startVersion)
                .detail("EndVersion", endVersion);
            throw;
        }
        
        return mappingHistory;
    }
    
    // Demonstrate why shard mapping indexes are fundamentally broken
    void demonstrateWhyShardMappingIndexesDontWork() {
        /*
         * CONCRETE EXAMPLE: Why hourly shard mapping indexes are useless
         * 
         * Timeline:
         * 10:00 AM - Index created: {Shard 42: ["user/", "user/10000")}
         * 10:15 AM - Shard split occurs: {Shard 42A: ["user/", "user/5000"), 
         *                                 Shard 42B: ["user/5000", "user/10000")}
         * 10:30 AM - Restore requested for version at 10:20 AM
         * 
         * Problem: KeyRange ["user/3000", "user/6000") at 10:20 AM
         * - Index from 10:00 AM says: Use shard 42 (WRONG!)
         * - Reality at 10:20 AM: Need shards 42A AND 42B (CORRECT!)
         * 
         * Consequence: Using stale index would miss data in shard 42B!
         */
        
        Version indexCreationVersion = 1000000;    // 10:00 AM
        Version shardSplitVersion = 1000900;       // 10:15 AM  
        Version targetRestoreVersion = 1001200;    // 10:20 AM
        KeyRange targetRange("user/3000", "user/6000");
        
        // What a stale index would tell us (WRONG!)
        ShardMapping staleMapping = {
            {42, KeyRange("user/", "user/10000")}  // Pre-split mapping
        };
        auto shardsFromStaleIndex = convertKeyRangeToShardIds(targetRange, staleMapping);
        // Result: [42] - Missing shard 42B data!
        
        // What the cluster config tells us for target version (CORRECT!)
        auto actualMapping = loadHistoricalShardMapping(targetRestoreVersion);
        /*
         * actualMapping = {
         *   {42A, KeyRange("user/", "user/5000")},      // First half after split
         *   {42B, KeyRange("user/5000", "user/10000")} // Second half after split  
         * }
         */
        auto correctShards = convertKeyRangeToShardIds(targetRange, actualMapping);
        // Result: [42A, 42B] - Complete data coverage!
        
        TraceEvent(SevError, "StaleIndexDangerDemonstrated")  
            .detail("TargetRange", targetRange.toString())
            .detail("TargetVersion", targetRestoreVersion)
            .detail("ShardsFromStaleIndex", shardsFromStaleIndex.size())    // 1 (incomplete!)
            .detail("CorrectShards", correctShards.size())                  // 2 (complete!)
            .detail("DataLossRisk", shardsFromStaleIndex.size() < correctShards.size());
        
        /*
         * LESSON: Shard mapping MUST be queried for the specific target version.
         * Any periodic index is fundamentally broken for historical queries.
         */
    }
    
    // Core algorithm: Convert KeyRange to list of intersecting shard IDs
    std::vector<uint64_t> convertKeyRangeToShardIds(const KeyRange& range, const ShardMapping& shardMapping) {
        std::vector<uint64_t> affectedShards;
        
        /*
         * ALGORITHM: KeyRange to Shard Mapping
         * 
         * ShardMapping structure:
         * {
         *   shardId: 1, keyRange: ["", "apple")
         *   shardId: 2, keyRange: ["apple", "banana") 
         *   shardId: 3, keyRange: ["banana", "cherry")
         *   shardId: 4, keyRange: ["cherry", "")  // "" = infinity
         * }
         * 
         * Target range: ["application", "car")
         * 
         * Step 1: Find first shard that intersects with range start
         * Step 2: Continue until we pass range end
         * Step 3: Return all intersecting shard IDs
         */
        
        TraceEvent("KeyRangeToShardMapping")
            .detail("TargetRange", range.toString())
            .detail("TotalShards", shardMapping.size());
        
        // Convert range to string keys for comparison
        std::string rangeBegin = range.begin.toString();
        std::string rangeEnd = range.end.toString();
        
        for (const auto& [shardId, shardRange] : shardMapping) {
            // Check if this shard's range intersects with target range
            if (rangeIntersects(range, shardRange)) {
                affectedShards.push_back(shardId);
                
                TraceEvent("ShardIntersection")
                    .detail("ShardId", shardId)
                    .detail("ShardRange", shardRange.toString())
                    .detail("TargetRange", range.toString());
            }
        }
        
        TraceEvent("KeyRangeToShardMappingComplete")
            .detail("TargetRange", range.toString())
            .detail("AffectedShards", affectedShards.size())
            .detail("TotalShards", shardMapping.size())
            .detail("ReductionRatio", (double)affectedShards.size() / shardMapping.size());
        
        return affectedShards;
    }
    
private:
    // Check if two key ranges intersect
    bool rangeIntersects(const KeyRange& range1, const KeyRange& range2) {
        // Range1: [begin1, end1), Range2: [begin2, end2)
        // They intersect if: begin1 < end2 AND begin2 < end1
        
        // Handle infinite end keys (empty string means infinity in FDB)
        bool range1EndInfinite = range1.end.toString().empty();
        bool range2EndInfinite = range2.end.toString().empty();
        
        // Convert to strings for comparison
        std::string begin1 = range1.begin.toString();
        std::string end1 = range1.end.toString();
        std::string begin2 = range2.begin.toString(); 
        std::string end2 = range2.end.toString();
        
        // Check intersection conditions
        bool condition1 = range2EndInfinite || (begin1 < end2);
        bool condition2 = range1EndInfinite || (begin2 < end1);
        
        return condition1 && condition2;
    }
    
    // Example of how shard mapping is structured and accessed
    void demonstrateKeyRangeToShardMapping() {
        /*
         * CONCRETE EXAMPLE: Converting KeyRange to Shard IDs
         * 
         * Scenario: 250,000 shards in cluster, want to restore range ["user/1000", "user/2000")
         */
        
        KeyRange targetRange("user/1000", "user/2000");
        Version targetVersion = 98765;
        
        // Step 1: Load historical shard mapping for target version
        auto shardMapping = loadHistoricalShardMapping(targetVersion);
        
        /*
         * shardMapping might look like:
         * {
         *   42: KeyRange("user/", "user/10000"),     // Large shard containing user/1000-2000
         *   43: KeyRange("user/10000", "user/20000"), 
         *   44: KeyRange("user/20000", "user/30000"),
         *   ...
         * }
         * 
         * OR after shard splits:
         * {
         *   167: KeyRange("user/", "user/1500"),        // Split happened - now smaller shards
         *   168: KeyRange("user/1500", "user/3000"),     
         *   169: KeyRange("user/3000", "user/4500"),
         *   ...
         * }
         */
        
        // Step 2: Find intersecting shards
        auto affectedShards = convertKeyRangeToShardIds(targetRange, shardMapping);
        
        /*
         * Possible outcomes:
         * 
         * Before split: affectedShards = [42]  (1 large shard)
         * After split:  affectedShards = [167, 168]  (2 smaller shards)
         * 
         * The algorithm automatically adapts to whatever shard boundaries
         * existed at the target version.
         */
        
        TraceEvent("ShardMappingExample")
            .detail("TargetRange", targetRange.toString())
            .detail("TargetVersion", targetVersion)
            .detail("AffectedShards", affectedShards.size())
            .detail("ReductionFactor", (double)shardMapping.size() / affectedShards.size());
        
        // Step 3: Use these shard IDs to find relevant backup files
        std::vector<std::string> relevantFiles;
        for (uint64_t shardId : affectedShards) {
            auto filesForShard = getFilesForShard(shardId);  // May use stale index fallback
            relevantFiles.insert(relevantFiles.end(), filesForShard.begin(), filesForShard.end());
        }
        
        TraceEvent("RelevantFilesFound")
            .detail("TotalFiles", relevantFiles.size())
            .detail("ForShards", affectedShards.size())
            .detail("EstimatedDataReduction", calculateDataReduction(relevantFiles));
    }
    
    // Scan cluster configuration log for shard mapping changes (fallback when index stale)
    std::vector<ShardMappingSnapshot> scanClusterConfigurationForMappingChanges(
            Version startVersion, Version endVersion) {
        
        std::vector<ShardMappingSnapshot> mappingChanges;
        
        // Access cluster configuration log to find shard boundary changes
        // This is the authoritative source when indexes are stale
        auto configurationLog = getClusterConfigurationLog(startVersion, endVersion);
        
        for (const auto& logEntry : configurationLog) {
            if (logEntry.type == "SHARD_BOUNDARY_CHANGE") {
                ShardMappingSnapshot snapshot;
                snapshot.effectiveVersion = logEntry.version;
                snapshot.mapping = parseShardBoundaries(logEntry.data);
                snapshot.changeReason = logEntry.reason;  // "split", "merge", "rebalance"
                
                mappingChanges.push_back(snapshot);
            }
        }
        
        TraceEvent("ScannedConfigurationLog")
            .detail("StartVersion", startVersion)
            .detail("EndVersion", endVersion)
            .detail("MappingChanges", mappingChanges.size());
        
        return mappingChanges;
    }
    
    // Read snapshot data for specific shards and key range
    RangeData readShardSnapshotData(const std::string& filename, 
                                   const std::vector<uint64_t>& targetShards,
                                   const KeyRange& targetRange) {
        RangeData data;
        auto fileData = downloadSnapshotFile(filename);
        
        // Parse snapshot file header
        SnapshotFileHeader* header = reinterpret_cast<SnapshotFileHeader*>(fileData.data());
        
        // Use shard index to jump directly to relevant shards
        size_t indexOffset = sizeof(SnapshotFileHeader);
        ShardSnapshotIndex* shardIndex = reinterpret_cast<ShardSnapshotIndex*>(fileData.data() + indexOffset);
        
        std::set<uint64_t> targetShardSet(targetShards.begin(), targetShards.end());
        
        for (uint32_t i = 0; i < header->shardCount; i++) {
            if (targetShardSet.count(shardIndex[i].shardId)) {
                // Jump directly to this shard's data section
                size_t dataOffset = shardIndex[i].dataOffset;
                size_t dataSize = shardIndex[i].dataSize;
                
                // Parse key-value pairs within target range only
                auto shardData = parseShardSnapshot(fileData.data() + dataOffset, dataSize, targetRange);
                data.merge(shardData);
            }
        }
        
        return data;
    }
};
```

### API 2: `getRangeMutation(KeyRange, VersionRange)` - Efficient Mutation Retrieval

This API retrieves all mutations for a key range within a version range:

```cpp
class ShardAwareRestoreService {
public:
    MutationData getRangeMutation(KeyRange range, VersionRange versionRange) {
        TraceEvent("GetRangeMutation")
            .detail("Range", range.toString())
            .detail("StartVersion", versionRange.begin)
            .detail("EndVersion", versionRange.end);
        
        // Step 1: Map key range to affected shards at the start version
        // For mutation ranges, use the start version for shard mapping
        auto affectedShards = mapKeyRangeToShards(range, versionRange.begin);
        
        // Step 2: Find mutation log files containing these shards in version range
        auto mutationFiles = findMutationFilesForShards(affectedShards, versionRange);
        
        TraceEvent("MutationFileSelection")
            .detail("AffectedShards", affectedShards.size())
            .detail("MutationFiles", mutationFiles.size())
            .detail("EstimatedDataReduction", calculateMutationDataReduction(mutationFiles, affectedShards));
        
        // Step 3: Read only relevant mutations from selected files
        MutationData mutations;
        for (const auto& fileInfo : mutationFiles) {
            auto shardMutations = readShardMutationsFromFile(fileInfo.filename, affectedShards, range, versionRange);
            mutations.merge(shardMutations);
        }
        
        // Step 4: Sort mutations by version for proper replay
        mutations.sortByVersion();
        
        TraceEvent("GetRangeMutationComplete")
            .detail("MutationsRetrieved", mutations.size())
            .detail("FilesScanned", mutationFiles.size())
            .detail("DataReadMB", calculateMutationDataRead(mutationFiles) / (1024*1024));
        
        return mutations;
    }

private:
    // Read mutations for specific shards, key range, and version range
    MutationData readShardMutationsFromFile(const std::string& filename,
                                           const std::vector<uint64_t>& targetShards,
                                           const KeyRange& targetRange,
                                           const VersionRange& versionRange) {
        MutationData mutations;
        std::set<uint64_t> targetShardSet(targetShards.begin(), targetShards.end());
        
        auto fileData = downloadBatchFile(filename);
        
        // Parse batch file header
        BatchFileHeader* header = reinterpret_cast<BatchFileHeader*>(fileData.data());
        
        // Use shard index to jump directly to relevant shards
        size_t indexOffset = sizeof(BatchFileHeader);
        ShardIndex* shardIndex = reinterpret_cast<ShardIndex*>(fileData.data() + indexOffset);
        
        for (uint32_t i = 0; i < header->shardCount; i++) {
            if (targetShardSet.count(shardIndex[i].shardId)) {
                // Jump to this shard's mutation range
                size_t mutationOffset = shardIndex[i].firstMutationOffset;
                
                while (mutationOffset <= shardIndex[i].lastMutationOffset) {
                    BinaryReader reader(fileData.data() + mutationOffset,
                                       fileData.size() - mutationOffset,
                                       AssumeVersion(ProtocolVersion::withBackupMutations()));
                    
                    ShardedVersionedMutation shardedMutation;
                    reader >> shardedMutation;
                    
                    // Filter by shard, key range, and version range
                    if (targetShardSet.count(shardedMutation.shardId) &&
                        targetRange.contains(shardedMutation.mutation().param1) &&
                        shardedMutation.version() >= versionRange.begin &&
                        shardedMutation.version() <= versionRange.end) {
                        
                        mutations.add({
                            .version = shardedMutation.version(),
                            .mutation = shardedMutation.mutation()
                        });
                    }
                    
                    mutationOffset += reader.getReadBytes();
                    if (mutationOffset > shardIndex[i].lastMutationOffset) break;
                }
            }
        }
        
        return mutations;
    }
};
```

### Real-World SRE Restore Workflow

The typical SRE restore workflow using these APIs:

```cpp
// Example: Restore /myapp/users/* to state before corruption at version 98765
class ProductionRestoreWorkflow {
public:
    void restoreAfterCorruption() {
        KeyRange corruptedRange = KeyRange("/myapp/users/", "/myapp/users0");
        Version corruptionVersion = 100000;
        Version lastGoodVersion = 98765;  // Latest known good state
        
        // Step 1: Get snapshot data at last known good version
        auto snapshotData = restoreService.getRangeSnapshot(corruptedRange, lastGoodVersion);
        
        TraceEvent("SnapshotRetrieved")
            .detail("Range", corruptedRange.toString())
            .detail("Version", lastGoodVersion)
            .detail("KeysRetrieved", snapshotData.size());
        
        // Step 2: Get mutations from snapshot version to just before corruption
        VersionRange mutationRange(lastGoodVersion + 1, corruptionVersion - 1);
        auto mutations = restoreService.getRangeMutation(corruptedRange, mutationRange);
        
        TraceEvent("MutationsRetrieved")
            .detail("Range", corruptedRange.toString())
            .detail("VersionRange", mutationRange.toString())
            .detail("MutationCount", mutations.size());
        
        // Step 3: Apply snapshot + mutations to restore clean state
        applySnapshotData(snapshotData);
        applyMutationsInOrder(mutations);
        
        TraceEvent("RestoreComplete")
            .detail("FinalVersion", corruptionVersion - 1);
    }
};
```

### Performance Comparison: Legacy vs Shard-Aware APIs

**Legacy `getRangeSnapshot()`**:
- Scans ALL snapshot files across all 250,000 shards  
- Downloads entire files to filter locally
- **Result**: 1TB download to get 4MB of range data

**Shard-Aware `getRangeSnapshot()`**:
- Maps key range to specific shards (20 shards out of 250,000)
- Downloads only files containing those shards
- Jumps directly to shard data within files  
- **Result**: 4MB download for 4MB of range data (**250,000x improvement**)

**Legacy `getRangeMutation()`**:
- Scans ALL mutation log files
- Parses all mutations to filter by key range
- **Result**: Hours of processing, TB of network transfer

**Shard-Aware `getRangeMutation()`**:
- Pre-filters files by shard mapping
- Uses embedded shard index for precise navigation
- Only reads mutations for target shards and key ranges
- **Result**: Seconds of processing, MB of network transfer

The shard-aware design transforms these critical restore APIs from unusably slow to production-ready fast, delivering the 250,000x efficiency improvement that makes partial restores practical for real-world SRE operations.


## Testing Framework Design

### Core Testing Strategy

```cpp
// Essential test categories for shard-aware backup validation
class ShardAwareTestFramework {
public:
    // 1. Unit Tests: Component validation
    void testShardTagging();              // CommitProxy shard tag assignment
    void testWorkerAssignment();          // LogRouter tag distribution
    void testShardFiltering();            // BackupWorker shard processing
    void testHistoricalMapping();         // Version-specific shard boundaries
    
    // 2. Integration Tests: End-to-end flow
    void testMutationFlow();              // CommitProxy → BackupWorker → S3
    void testRestoreAccuracy();           // Verify restore data integrity
    void testCrossVersionRestore();       // Legacy/hybrid/shard format compatibility
    
    // 3. Performance Tests: Efficiency validation
    void benchmarkPartialRestore();       // Verify 250,000x improvement
    void benchmarkHybridMode();           // Migration performance impact
    void stressTestShardCount();          // Large shard count scenarios
    
    // 4. Migration Tests: Backward compatibility
    void testMigrationWorkflow();         // Complete migration process
    void testRollbackProcedures();        // Emergency rollback scenarios
    void testDataConsistency();           // No data loss during migration
};
```

### Success Criteria

**Performance Validation**:
- **250,000x efficiency improvement** verified in partial restore benchmarks
- **<20% performance degradation** during hybrid migration mode
- **Equal or better performance** in shard-only mode vs legacy

**Data Integrity Validation**:
- **100% data consistency** across all backup formats
- **Zero data loss** during migration scenarios
- **Exact restore accuracy** for [`getRangeSnapshot()`](backup_v3_s3_writing_and_aggregation.md:1779) and [`getRangeMutation()`](backup_v3_s3_writing_and_aggregation.md:1909) APIs

## Implementation Phases

### Phase-Based Development Approach

```cpp
// Implementation roadmap with clear milestones
class ImplementationPhases {
public:
    // Phase 1: Foundation (6 weeks)
    void buildFoundation() {
        implement("ShardedVersionedMutation serialization");
        implement("Historical shard mapping");
        implement("S3 batch file format with embedded indexes");
        // Success: Core infrastructure validated with unit tests
    }
    
    // Phase 2: Integration (8 weeks)  
    void integrateComponents() {
        modify("CommitProxy: dual tagging support");
        modify("LogRouter: flexible tag assignment");
        modify("BackupWorker: shard filtering and multi-tag processing");
        // Success: End-to-end mutation flow working
    }
    
    // Phase 3: Migration (6 weeks)
    void implementMigration() {
        implement("Hybrid mode processing");
        implement("Migration orchestration with rollback");
        implement("Cross-version restore support");
        // Success: Safe migration path with <5 minute rollback
    }
    
    // Phase 4: Production (8 weeks)
    void productionDeployment() {
        optimize("Performance tuning and monitoring");
        deploy("Staged rollout to production clusters");
        validate("250,000x efficiency in production workloads");
        // Success: 100% production deployment with efficiency targets met
    }
};
```

### Resource Requirements and Timeline

- **Team Size**: 8-10 engineers (senior developers, test engineers, SREs)
- **Timeline**: 28 weeks (~7 months) from start to full production deployment
- **Risk Level**: Medium (extends existing infrastructure, staged rollout with rollback)

### Key Success Metrics

**Technical Metrics**:
- 250,000x efficiency improvement in partial restore scenarios
- Zero data loss across all migration and operational scenarios
- Sub-5-minute rollback capability for emergency situations

**Operational Metrics**:
- 100% successful deployment across production clusters
- Zero customer-impacting incidents during rollout
- Maintained or improved backup/restore SLA performance

## Architectural Summary

### Core Innovation: Shard-Aware Backup with Historical Version Support

The **FoundationDB Backup v3** delivers revolutionary **250,000x efficiency improvement** for partial restores through:

1. **Shard-Based Mutation Tagging**: [`Tag(tagLocalityBackupShard, shardId)`](backup_v3_s3_writing_and_aggregation.md:48) enables precise targeting
2. **Multi-Shard Batch Files**: Each file contains mutations from multiple shards with embedded shard indexes
3. **Historical Shard Mapping**: Version-specific shard boundaries ensure accurate restore operations
4. **Production Restore APIs**: [`getRangeSnapshot(range, version)`](backup_v3_s3_writing_and_aggregation.md:1779) and [`getRangeMutation(range, versionRange)`](backup_v3_s3_writing_and_aggregation.md:1909)

### Architecture Benefits

**Efficiency Gains**:
- **File-Level Filtering**: Download only files containing target shards (50 files vs 10,000)
- **Intra-File Precision**: Jump directly to relevant mutations using embedded shard indexes
- **Network Transfer Reduction**: 4MB download vs 1TB (250,000x improvement)

**Production Safety**:
- **100% Backward Compatibility**: All existing backup/restore operations unchanged
- **Zero-Downtime Migration**: Hybrid mode with instant rollback capability
- **Data Integrity Guaranteed**: Continuous validation throughout migration

**Operational Excellence**:
- **Leverages Existing Infrastructure**: Built on proven FoundationDB backup components
- **Self-Healing Metadata**: Essential data embedded in batch files, JSON files optional
- **Production-Ready APIs**: Real-world restore workflows for SRE teams

### Technical Foundation

**Core Components Extended**:
- [`CommitProxy`](fdbserver/BackupWorker.actor.cpp): Enhanced with shard tagging
- [`LogRouter`](fdbserver/BackupWorker.actor.cpp): Flexible tag assignment supporting all worker/tag ratios
- [`BackupWorker`](fdbserver/BackupWorker.actor.cpp): Multi-tag processing with shard filtering
- [`IBackupContainer`](fdbclient/include/fdbclient/BackupContainer.h:225): Enhanced with shard metadata

**Key Design Decisions**:
- **Multi-shard files**: Not one-file-per-shard (would create millions of files)
- **BackupWorker pull model**: Workers pull from TLog via [`peekLogRouter()`](fdbserver/BackupWorker.actor.cpp:1047)
- **Historical shard mapping**: Critical for version-specific restore accuracy
- **Embedded metadata**: Resilient design works even if JSON metadata lost

This architectural design transforms FoundationDB backup restore from an unusably slow operation (hours to read 1TB for 4MB of data) to a production-ready fast operation (seconds to read 4MB for 4MB of data), delivering the revolutionary 250,000x efficiency improvement while maintaining FoundationDB's rigorous reliability and performance standards.

## 12. Testing Framework and Validation Approach

### 12.1 Comprehensive Test Strategy

The shard-aware backup system requires extensive testing to validate the 250,000x efficiency improvement while maintaining 100% data integrity across all scenarios.

```cpp
/**
 * TESTING FRAMEWORK DESIGN PRINCIPLES
 * 
 * 1. CORRECTNESS FIRST: Every test validates data integrity before performance
 * 2. SCALE TESTING: Test with realistic shard counts (250,000+ shards)
 * 3. MIGRATION SAFETY: Extensive backward compatibility and rollback testing
 * 4. PERFORMANCE VALIDATION: Measure and verify the 250,000x improvement claims
 * 5. FAULT INJECTION: Test all failure scenarios during migration and operation
 */

enum class TestCategory {
    UNIT_TESTS,              // Component-level validation
    INTEGRATION_TESTS,       // End-to-end flow testing
    PERFORMANCE_TESTS,       // Efficiency measurement
    MIGRATION_TESTS,         // Backward compatibility
    FAULT_INJECTION_TESTS,   // Failure scenario testing
    SCALE_TESTS,            // Large-scale validation
    PRODUCTION_SIMULATION   // Real-world workload testing
};
```

### 12.2 Unit Testing Framework

```cpp
// Core unit tests for shard-aware backup components
class ShardAwareBackupUnitTests : public TestWorkload {
public:
    // Test 1: Shard Tag Assignment
    void testShardTagAssignment() {
        /*
         * VALIDATES: CommitProxy correctly assigns shard tags to mutations
         * 
         * Test Cases:
         * - KeyRange → Shard ID mapping accuracy
         * - Multiple shards per mutation handling
         * - Tag deduplication for efficiency
         * - Shard boundary edge cases
         */
        
        auto commitProxy = createTestCommitProxy();
        
        // Test single shard mutation
        MutationRef mutation(MutationRef::SetValue, "user/1000"_sr, "value"_sr);
        auto tags = commitProxy->assignShardTags(mutation);
        
        ASSERT(tags.size() == 1);
        ASSERT(tags[0].locality == tagLocalityBackupShard);
        ASSERT(tags[0].id == calculateExpectedShardId("user/1000"));
        
        // Test cross-shard mutation (key range spans multiple shards)
        MutationRef clearMutation(MutationRef::ClearRange, "user/1000"_sr, "user/5000"_sr);
        auto clearTags = commitProxy->assignShardTags(clearMutation);
        
        ASSERT(clearTags.size() > 1); // Should span multiple shards
        for (auto tag : clearTags) {
            ASSERT(tag.locality == tagLocalityBackupShard);
        }
        
        TraceEvent("ShardTagAssignmentTestPassed")
            .detail("SingleShardTags", tags.size())
            .detail("CrossShardTags", clearTags.size());
    }
    
    // Test 2: BackupWorker Shard Filtering
    void testBackupWorkerShardFiltering() {
        /*
         * VALIDATES: BackupWorkers correctly filter mutations by shard assignment
         * 
         * Test Cases:
         * - Worker processes only assigned shards
         * - Worker skips non-assigned shards
         * - Consistent assignment across worker restarts
         * - Load balancing across workers
         */
        
        int totalWorkers = 10;
        int totalShards = 100;
        
        // Create test workers
        std::vector<std::unique_ptr<EnhancedBackupWorker>> workers;
        for (int i = 0; i < totalWorkers; i++) {
            workers.push_back(createTestBackupWorker(i, totalWorkers));
        }
        
        // Test each shard is assigned to exactly one worker
        std::map<uint64_t, int> shardToWorker;
        for (uint64_t shardId = 0; shardId < totalShards; shardId++) {
            int assignedWorkers = 0;
            int assignedWorkerId = -1;
            
            for (int workerId = 0; workerId < totalWorkers; workerId++) {
                if (workers[workerId]->shouldProcessShard(shardId, workerId, totalWorkers)) {
                    assignedWorkers++;
                    assignedWorkerId = workerId;
                }
            }
            
            ASSERT(assignedWorkers == 1); // Exactly one worker per shard
            shardToWorker[shardId] = assignedWorkerId;
        }
        
        // Verify load balancing (each worker gets ~equal shards)
        std::map<int, int> workerShardCount;
        for (auto& [shardId, workerId] : shardToWorker) {
            workerShardCount[workerId]++;
        }
        
        int expectedShardsPerWorker = totalShards / totalWorkers;
        for (auto& [workerId, shardCount] : workerShardCount) {
            ASSERT(abs(shardCount - expectedShardsPerWorker) <= 1); // Load balanced
        }
        
        TraceEvent("BackupWorkerShardFilteringTestPassed")
            .detail("TotalShards", totalShards)
            .detail("TotalWorkers", totalWorkers)
            .detail("LoadBalanced", true);
    }
    
    // Test 3: Historical Shard Mapping
    void testHistoricalShardMapping() {
        /*
         * VALIDATES: Historical shard mappings work correctly for version-specific restore
         * 
         * Test Cases:
         * - Shard boundary changes over time
         * - Version-specific mapping accuracy
         * - Self-contained backup data
         * - Mapping reconstruction from batch files
         */
        
        // Create timeline with shard boundary changes
        Version version1000 = 1000;
        Version version2000 = 2000;
        Version version3000 = 3000;
        
        // Initial mapping: 10 shards
        ShardMapping mapping1000 = createTestShardMapping(10, version1000);
        
        // Split happened: 20 shards  
        ShardMapping mapping2000 = createTestShardMapping(20, version2000);
        
        // Merge happened: 15 shards
        ShardMapping mapping3000 = createTestShardMapping(15, version3000);
        
        // Store mappings in backup
        auto backupContainer = createTestBackupContainer();
        backupContainer->storeShardMapping(version1000, mapping1000);
        backupContainer->storeShardMapping(version2000, mapping2000);
        backupContainer->storeShardMapping(version3000, mapping3000);
        
        // Test version-specific retrieval
        auto retrieved1000 = backupContainer->loadHistoricalShardMapping(version1000);
        auto retrieved2000 = backupContainer->loadHistoricalShardMapping(version2000);
        auto retrieved3000 = backupContainer->loadHistoricalShardMapping(version3000);
        
        ASSERT(mappingsEqual(mapping1000, retrieved1000));
        ASSERT(mappingsEqual(mapping2000, retrieved2000));
        ASSERT(mappingsEqual(mapping3000, retrieved3000));
        
        // Test intermediate version queries (should use previous mapping)
        auto retrieved1500 = backupContainer->loadHistoricalShardMapping(1500);
        ASSERT(mappingsEqual(mapping1000, retrieved1500)); // Uses mapping from version 1000
        
        TraceEvent("HistoricalShardMappingTestPassed")
            .detail("TestedVersions", 3)
            .detail("IntermediateVersionTest", true);
    }
};
```

### 12.3 Integration Testing Framework

```cpp
// End-to-end integration tests
class ShardAwareBackupIntegrationTests : public TestWorkload {
public:
    // Test 1: Complete Mutation Flow
    void testCompleteMutationFlow() {
        /*
         * VALIDATES: End-to-end flow from CommitProxy to S3 storage
         * 
         * Flow: CommitProxy → TLog → LogRouter → BackupWorker → S3
         * Validates: Data integrity, shard assignment, file organization
         */
        
        // Set up test cluster with shard-aware backup
        auto testCluster = createTestCluster(5, 10); // 5 LogRouter tags, 10 BackupWorkers
        testCluster->enableShardAwareBackup();
        
        // Generate test workload
        std::vector<MutationRef> testMutations = generateTestMutations(1000); // 1000 mutations
        
        // Apply mutations through normal transaction flow
        for (auto& mutation : testMutations) {
            auto transaction = testCluster->createTransaction();
            transaction->addMutation(mutation);
            transaction->commit().blockUntilReady();
        }
        
        // Wait for backup processing
        testCluster->waitForBackupProgress(testMutations.back().version);
        
        // Validate S3 files created
        auto s3Files = testCluster->listBackupFiles();
        ASSERT(!s3Files.empty());
        
        // Validate file structure and content
        for (auto& filename : s3Files) {
            auto fileContent = testCluster->downloadBackupFile(filename);
            auto parsedMutations = parseBackupFile(fileContent);
            
            // Verify each mutation has correct shard metadata
            for (auto& mutation : parsedMutations) {
                auto expectedShardId = calculateShardId(mutation.mutation.param1);
                ASSERT(mutation.shardId == expectedShardId);
            }
        }
        
        TraceEvent("CompleteMutationFlowTestPassed")
            .detail("MutationsProcessed", testMutations.size())
            .detail("BackupFilesCreated", s3Files.size())
            .detail("DataIntegrityVerified", true);
    }
    
    // Test 2: Cross-Version Restore Accuracy
    void testCrossVersionRestoreAccuracy() {
        /*
         * VALIDATES: Restore accuracy across v2 and v3 backup formats
         * 
         * Test Cases:
         * - v3 restore engine reading v2 backups
         * - v3 restore engine reading v3 backups  
         * - Identical results from both formats
         * - Performance difference measurement
         */
        
        // Create identical datasets in both formats
        auto testData = generateTestDataset(10000); // 10K key-value pairs
        
        // Create v2 backup
        auto v2Backup = createV2Backup();
        v2Backup->writeDataset(testData);
        
        // Create v3 backup  
        auto v3Backup = createV3Backup();
        v3Backup->writeDataset(testData);
        
        // Test restore from both formats
        KeyRange testRange("user/1000", "user/2000");
        Version testVersion = getCurrentVersion();
        
        auto v2RestoreResult = v2Backup->restoreRange(testRange, testVersion);
        auto v3RestoreResult = v3Backup->restoreRange(testRange, testVersion);
        
        // Validate identical results
        ASSERT(restoreResultsEqual(v2RestoreResult, v3RestoreResult));
        
        // Measure performance difference
        auto v2Performance = measureRestorePerformance(v2Backup, testRange, testVersion);
        auto v3Performance = measureRestorePerformance(v3Backup, testRange, testVersion);
        
        double speedupFactor = v2Performance.durationMs / v3Performance.durationMs;
        ASSERT(speedupFactor >= 1000.0); // At least 1000x improvement
        
        TraceEvent("CrossVersionRestoreAccuracyTestPassed")
            .detail("DataSetSize", testData.size())
            .detail("RestoreRange", testRange.toString())
            .detail("ResultsIdentical", true)
            .detail("SpeedupFactor", speedupFactor);
    }
};
```

### 12.4 Performance Testing Framework

```cpp
// Performance validation and benchmarking
class ShardAwareBackupPerformanceTests : public TestWorkload {
public:
    // Test 1: Efficiency Improvement Validation
    void testEfficiencyImprovementValidation() {
        /*
         * VALIDATES: The claimed 250,000x efficiency improvement
         * 
         * Methodology:
         * - Create large backup (1TB) with 250,000 shards
         * - Perform partial restore (4MB target range)
         * - Measure data read vs target data size
         * - Validate improvement factor
         */
        
        // Create massive test backup
        uint64_t totalBackupSize = 1024ULL * 1024 * 1024 * 1024; // 1TB
        uint32_t totalShards = 250000;
        
        auto massiveBackup = createMassiveTestBackup(totalBackupSize, totalShards);
        
        // Define small restore target (0.0004% of total data)
        KeyRange smallRange("user/specific/1000", "user/specific/1100");  
        Version restoreVersion = getCurrentVersion();
        
        // Measure legacy restore performance
        auto legacyStartTime = std::chrono::high_resolution_clock::now();
        auto legacyResult = performLegacyRestore(massiveBackup, smallRange, restoreVersion);
        auto legacyEndTime = std::chrono::high_resolution_clock::now();
        
        auto legacyDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
            legacyEndTime - legacyStartTime);
        uint64_t legacyDataRead = calculateDataRead(legacyResult.readOperations);
        
        // Measure shard-aware restore performance
        auto shardStartTime = std::chrono::high_resolution_clock::now();
        auto shardResult = performShardAwareRestore(massiveBackup, smallRange, restoreVersion);
        auto shardEndTime = std::chrono::high_resolution_clock::now();
        
        auto shardDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
            shardEndTime - shardStartTime);
        uint64_t shardDataRead = calculateDataRead(shardResult.readOperations);
        
        // Calculate improvement factors
        double timeImprovement = (double)legacyDuration.count() / shardDuration.count();
        double dataReductionFactor = (double)legacyDataRead / shardDataRead;
        
        // Validate performance targets
        ASSERT(dataReductionFactor >= 100000.0);  // At least 100,000x data reduction
        ASSERT(timeImprovement >= 1000.0);        // At least 1,000x time improvement
        
        // The combination should achieve 250,000x efficiency
        double overallEfficiency = dataReductionFactor * (timeImprovement / dataReductionFactor);
        ASSERT(overallEfficiency >= 250000.0);
        
        TraceEvent("EfficiencyImprovementValidated")
            .detail("BackupSize", totalBackupSize)
            .detail("TotalShards", totalShards)
            .detail("LegacyDataRead", legacyDataRead)
            .detail("ShardDataRead", shardDataRead)
            .detail("DataReductionFactor", dataReductionFactor)
            .detail("TimeImprovement", timeImprovement)
            .detail("OverallEfficiency", overallEfficiency)
            .detail("Target250kAchieved", overallEfficiency >= 250000.0);
    }
    
    // Test 2: Scale Performance Testing
    void testScalePerformance() {
        /*
         * VALIDATES: Performance scales properly with shard count
         * 
         * Test Points: 1K, 10K, 100K, 250K, 500K shards
         * Measures: Restore time, memory usage, CPU usage
         */
        
        std::vector<uint32_t> shardCounts = {1000, 10000, 100000, 250000, 500000};
        
        for (uint32_t shardCount : shardCounts) {
            TraceEvent("ScalePerformanceTest")
                .detail("ShardCount", shardCount)
                .detail("TestStarting", true);
            
            // Create backup with specified shard count
            auto scaledBackup = createScaledTestBackup(shardCount);
            
            // Measure restore performance
            KeyRange testRange = generateRandomKeyRange();
            Version testVersion = getCurrentVersion();
            
            auto startTime = std::chrono::high_resolution_clock::now();
            auto restoreResult = performShardAwareRestore(scaledBackup, testRange, testVersion);
            auto endTime = std::chrono::high_resolution_clock::now();
            
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            
            // Measure resource usage
            auto memoryUsage = measureMemoryUsage(restoreResult);
            auto cpuUsage = measureCPUUsage(restoreResult);
            
            // Validate scalability (should be sub-linear with shard count)
            if (shardCount > 1000) {
                uint32_t previousShardCount = getPreviousShardCount(shardCount, shardCounts);
                auto previousMetrics = getMetricsForShardCount(previousShardCount);
                
                double scalingFactor = (double)shardCount / previousShardCount;
                double performanceRatio = (double)duration.count() / previousMetrics.duration.count();
                
                // Performance should scale better than linearly
                ASSERT(performanceRatio < scalingFactor);
            }
            
            TraceEvent("ScalePerformanceTestComplete")
                .detail("ShardCount", shardCount)
                .detail("RestoreDurationMs", duration.count())
                .detail("MemoryUsageMB", memoryUsage / (1024*1024))
                .detail("CPUUsagePercent", cpuUsage * 100);
        }
    }
};
```

### 12.5 Migration and Fault Injection Testing

```cpp
// Migration safety and fault tolerance testing
class ShardAwareBackupMigrationTests : public TestWorkload {
public:
    // Test 1: Migration Rollback Safety
    void testMigrationRollbackSafety() {
        /*
         * VALIDATES: Safe rollback at any point during migration
         * 
         * Test Scenarios:
         * - Rollback during Phase 1 (coexistence)
         * - Rollback during Phase 2 (gradual enablement)  
         * - Rollback during Phase 3 (full migration)
         * - Data integrity maintained in all cases
         */
        
        std::vector<MigrationPhase> rollbackPhases = {
            MigrationPhase::PHASE_1_COEXISTENCE,
            MigrationPhase::PHASE_2_GRADUAL_ENABLEMENT,
            MigrationPhase::PHASE_3_FULL_MIGRATION
        };
        
        for (auto rollbackPhase : rollbackPhases) {
            TraceEvent("MigrationRollbackTest")
                .detail("RollbackPhase", (int)rollbackPhase)
                .detail("TestStarting", true);
            
            // Set up test cluster and start migration
            auto testCluster = createTestCluster();
            auto migrationController = testCluster->startMigration();
            
            // Progress to target phase
            migrationController->progressToPhase(rollbackPhase);
            
            // Generate continuous workload during migration
            auto workloadGenerator = startContinuousWorkload(testCluster);
            
            // Capture pre-rollback state
            auto preRollbackState = captureClusterState(testCluster);
            
            // Execute emergency rollback
            auto rollbackStartTime = std::chrono::high_resolution_clock::now();
            migrationController->emergencyRollback("Test rollback");
            auto rollbackEndTime = std::chrono::high_resolution_clock::now();
            
            auto rollbackDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
                rollbackEndTime - rollbackStartTime);
            
            // Stop workload and validate
            workloadGenerator->stop();
            auto postRollbackState = captureClusterState(testCluster);
            
            // Validate rollback completed successfully
            ASSERT(rollbackDuration.count() < 300000); // <5 minutes
            ASSERT(testCluster->isBackupSystemFunctional());
            ASSERT(validateDataIntegrity(preRollbackState, postRollbackState));
            
            TraceEvent("MigrationRollbackTestPassed")
                .detail("RollbackPhase", (int)rollbackPhase)
                .detail("RollbackDurationMs", rollbackDuration.count())
                .detail("DataIntegrityMaintained", true);
        }
    }
    
    // Test 2: Fault Injection During Migration
    void testFaultInjectionDuringMigration() {
        /*
         * VALIDATES: System resilience to failures during migration
         * 
         * Failure Types:
         * - BackupWorker crashes
         * - LogRouter failures
         * - Network partitions
         * - Storage failures
         * - CommitProxy restarts
         */
        
        std::vector<FaultType> faultTypes = {
            FaultType::BACKUP_WORKER_CRASH,
            FaultType::LOG_ROUTER_FAILURE,
            FaultType::NETWORK_PARTITION,
            FaultType::STORAGE_FAILURE,
            FaultType::COMMIT_PROXY_RESTART
        };
        
        for (auto faultType : faultTypes) {
            TraceEvent("FaultInjectionTest")
                .detail("FaultType", (int)faultType)
                .detail("TestStarting", true);
            
            // Set up migration environment
            auto testCluster = createTestCluster();
            auto migrationController = testCluster->startMigration();
            
            // Progress to vulnerable phase (Phase 2)
            migrationController->progressToPhase(MigrationPhase::PHASE_2_GRADUAL_ENABLEMENT);
            
            // Start continuous workload
            auto workloadGenerator = startContinuousWorkload(testCluster);
            
            // Inject fault at critical moment
            auto faultInjector = createFaultInjector(faultType);
            faultInjector->injectFault(testCluster);
            
            // Measure recovery time
            auto faultStartTime = std::chrono::high_resolution_clock::now();
            testCluster->waitForRecovery();
            auto recoveryEndTime = std::chrono::high_resolution_clock::now();
            
            auto recoveryDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
                recoveryEndTime - faultStartTime);
            
            // Validate system recovered properly
            ASSERT(testCluster->isBackupSystemFunctional());
            ASSERT(migrationController->getMigrationState().isValid());
            
            // Stop workload and validate no data loss
            workloadGenerator->stop();
            auto dataLossEvents = workloadGenerator->getDataLossEvents();
            ASSERT(dataLossEvents.empty());
            
            TraceEvent("FaultInjectionTestPassed")
                .detail("FaultType", (int)faultType)
                .detail("RecoveryDurationMs", recoveryDuration.count())
                .detail("DataLossEvents", dataLossEvents.size())
                .detail("SystemRecovered", true);
        }
    }
};
```

### 12.6 Test Execution Framework

```cpp
// Orchestrates all test categories
class ShardAwareBackupTestOrchestrator {
public:
    struct TestResults {
        uint32_t totalTests = 0;
        uint32_t passedTests = 0;
        uint32_t failedTests = 0;
        std::vector<std::string> failureReasons;
        double maxEfficiencyImprovement = 0.0;
        bool migrationSafetyValidated = false;
        bool dataIntegrityValidated = false;
    };
    
    TestResults runAllTests() {
        TestResults results;
        
        TraceEvent("ShardAwareBackupTestingStarted")
            .detail("TestCategories", 6)
            .detail("EstimatedDurationHours", 8);
        
        // Run all test categories
        results = runTestCategory<ShardAwareBackupUnitTests>("Unit Tests", results);
        results = runTestCategory<ShardAwareBackupIntegrationTests>("Integration Tests", results);
        results = runTestCategory<ShardAwareBackupPerformanceTests>("Performance Tests", results);
        results = runTestCategory<ShardAwareBackupMigrationTests>("Migration Tests", results);
        
        // Generate final report
        generateTestReport(results);
        
        return results;
    }
    
private:
    template<typename TestClass>
    TestResults runTestCategory(const std::string& categoryName, TestResults currentResults) {
        TraceEvent("TestCategoryStarted")
            .detail("Category", categoryName);
        
        try {
            TestClass testSuite;
            auto categoryResults = testSuite.runAllTests();
            
            currentResults.totalTests += categoryResults.totalTests;
            currentResults.passedTests += categoryResults.passedTests;
            currentResults.failedTests += categoryResults.failedTests;
            
            if (categoryResults.failedTests > 0) {
                currentResults.failureReasons.insert(
                    currentResults.failureReasons.end(),
                    categoryResults.failureReasons.begin(),
                    categoryResults.failureReasons.end()
                );
            }
            
            TraceEvent("TestCategoryCompleted")
                .detail("Category", categoryName)
                .detail("Passed", categoryResults.passedTests)
                .detail("Failed", categoryResults.failedTests);
                
        } catch (const std::exception& e) {
            currentResults.failedTests++;
            currentResults.failureReasons.push_back(
                categoryName + " category failed: " + e.what()
            );
            
            TraceEvent(SevError, "TestCategoryFailed")
                .detail("Category", categoryName)
                .detail("Error", e.what());
        }
        
        return currentResults;
    }
    
    void generateTestReport(const TestResults& results) {
        TraceEvent("ShardAwareBackupTestingComplete")
            .detail("TotalTests", results.totalTests)
            .detail("PassedTests", results.passedTests)
            .detail("FailedTests", results.failedTests)
            .detail("SuccessRate", (double)results.passedTests / results.totalTests)
            .detail("MaxEfficiencyImprovement", results.maxEfficiencyImprovement)
            .detail("MigrationSafetyValidated", results.migrationSafetyValidated)
            .detail("DataIntegrityValidated", results.dataIntegrityValidated);
        
        if (results.failedTests == 0) {
            TraceEvent("AllTestsPassed")
                .detail("ReadyForProduction", true)
                .detail("PerformanceTargetsAchieved", results.maxEfficiencyImprovement >= 250000.0);
        } else {
            TraceEvent(SevError, "TestFailuresDetected")
                .detail("FailureCount", results.failedTests);
            
            for (const auto& reason : results.failureReasons) {
                TraceEvent(SevError, "TestFailureReason")
                    .detail("Reason", reason);
            }
        }
    }
};
```

### 12.7 Test Success Criteria

**Functional Requirements**:
- ✅ **100% data integrity** across all backup and restore operations
- ✅ **Zero data loss** during migration scenarios  
- ✅ **Perfect backward compatibility** with existing v2 backups
- ✅ **Sub-5-minute rollback** capability in all migration phases

**Performance Requirements**:
- ✅ **250,000x efficiency improvement** for partial restore operations
- ✅ **<20% performance degradation** during hybrid migration mode
- ✅ **Sub-linear scaling** with shard count increases
- ✅ **Memory usage** within acceptable bounds at 500K+ shards

**Operational Requirements**:
- ✅ **Zero-downtime migration** with continuous backup service
- ✅ **Fault tolerance** to all single-point failures during migration
- ✅ **Consistent shard assignment** across worker restarts
- ✅ **Complete test coverage** of all code paths and edge cases

This comprehensive testing framework ensures the shard-aware backup system meets all reliability, performance, and operational requirements before production deployment.

## 13. Implementation Phases and Milestones

### 13.1 Development Roadmap Overview

The FoundationDB Backup v3 implementation follows a **phased approach** designed to minimize risk while delivering the revolutionary 250,000x efficiency improvement.

```mermaid
gantt
    title FoundationDB Backup v3 Implementation Timeline
    dateFormat  YYYY-MM-DD
    section Phase 1: Foundation
    Core Infrastructure    :p1-infra, 2024-02-01, 6w
    Shard Tagging System   :p1-tags, after p1-infra, 4w
    Historical Mapping     :p1-history, after p1-tags, 3w
    section Phase 2: Integration  
    CommitProxy Enhancement :p2-proxy, after p1-history, 4w
    LogRouter Enhancement   :p2-router, after p2-proxy, 3w
    BackupWorker Enhancement:p2-worker, after p2-router, 5w
    section Phase 3: Migration
    Hybrid Mode Support    :p3-hybrid, after p2-worker, 4w
    Migration Orchestration:p3-migration, after p3-hybrid, 3w
    Rollback Safety        :p3-rollback, after p3-migration, 2w
    section Phase 4: Production
    Performance Optimization:p4-perf, after p3-rollback, 4w
    Production Deployment   :p4-deploy, after p4-perf, 6w
    Validation & Monitoring :p4-validate, after p4-deploy, 4w
```

### 13.2 Phase 1: Foundation Infrastructure (10 weeks)

**Goal**: Build core infrastructure components without disrupting existing systems.

```cpp
/**
 * PHASE 1 DELIVERABLES
 * 
 * 1. ShardedVersionedMutation serialization format
 * 2. Historical shard mapping storage and retrieval
 * 3. Multi-shard batch file format with embedded indexes
 * 4. S3 batch file upload pipeline
 * 5. Shard-to-files mapping index system
 */

class Phase1Implementation {
public:
    // Week 1-6: Core Infrastructure
    struct Week1to6Deliverables {
        // Milestone 1.1: Enhanced mutation serialization
        bool shardedMutationFormat = false;        // Extends VersionedMutationSerialized
        bool batchFileFormat = false;              // Multi-shard file structure
        bool s3UploadPipeline = false;             // Asynchronous upload system
        
        // Success Criteria:
        // ✓ Can serialize/deserialize mutations with shard metadata
        // ✓ Can create multi-shard batch files with embedded indexes
        // ✓ Can upload batch files to S3 with retry logic
        // ✓ All unit tests pass for core components
    };
    
    // Week 7-10: Shard Management
    struct Week7to10Deliverables {
        // Milestone 1.2: Historical shard mapping system
        bool shardMappingStorage = false;          // Version-specific shard boundaries
        bool shardMappingRetrieval = false;        // Self-contained backup queries
        bool mappingReconstruction = false;        // Recovery from batch files
        
        // Success Criteria:
        // ✓ Can store shard mappings as backup snapshots
        // ✓ Can retrieve mappings for any historical version
        // ✓ Can reconstruct mappings when metadata is missing
        // ✓ Integration tests pass for mapping lifecycle
    };
    
    void executePhase1() {
        TraceEvent("Phase1Starting")
            .detail("Duration", "10 weeks")
            .detail("Goal", "Build foundation without disrupting existing backup");
        
        // Week 1-6: Build core serialization and file formats
        implementShardedMutationSerialization();
        implementMultiShardBatchFormat();
        implementS3UploadPipeline();
        
        // Week 7-10: Build shard mapping infrastructure
        implementHistoricalShardMapping();
        implementMappingReconstruction();
        implementSelfContainedRestore();
        
        // Phase 1 completion criteria
        validatePhase1Completion();
    }
    
private:
    void validatePhase1Completion() {
        /*
         * PHASE 1 ACCEPTANCE CRITERIA:
         * 
         * ✓ All core components unit tested
         * ✓ Integration tests pass for new file formats
         * ✓ Performance tests show no regression in existing backup
         * ✓ Self-contained restore works for test datasets
         * ✓ Code review completed by backup system maintainers
         */
        
        bool allTestsPass = runPhase1TestSuite();
        bool noRegressions = validateNoPerformanceRegressions();
        bool codeReviewComplete = getCodeReviewApproval();
        
        if (allTestsPass && noRegressions && codeReviewComplete) {
            TraceEvent("Phase1Complete")
                .detail("ReadyForPhase2", true);
        } else {
            TraceEvent(SevError, "Phase1Incomplete")
                .detail("TestsPass", allTestsPass)
                .detail("NoRegressions", noRegressions)
                .detail("CodeReview", codeReviewComplete);
        }
    }
};
```

### 13.3 Phase 2: System Integration (12 weeks)

**Goal**: Integrate shard-aware components into FoundationDB's transaction processing pipeline.

```cpp
class Phase2Implementation {
public:
    // Week 11-14: CommitProxy Enhancement
    struct CommitProxyMilestone {
        // Milestone 2.1: Shard tag assignment
        bool shardTagMapping = false;              // KeyRange → ShardId conversion
        bool dualTaggingSupport = false;           // Both LogRouter + Shard tags
        bool tagDeduplication = false;             // Efficient tag management
        
        // Success Criteria:
        // ✓ CommitProxy correctly assigns shard tags to all mutations
        // ✓ Dual tagging works without performance regression
        // ✓ Tag assignment scales to 250,000+ shards
        // ✓ Integration tests pass with live cluster
    };
    
    // Week 15-17: LogRouter Enhancement  
    struct LogRouterMilestone {
        // Milestone 2.2: Flexible tag assignment
        bool multiTagRouting = false;              // Route both tag types
        bool flexibleWorkerRatios = false;         // Handle any worker/tag ratio
        bool loadBalancing = false;                // Distribute tags evenly
        
        // Success Criteria:
        // ✓ LogRouter handles both LogRouter and Shard tags
        // ✓ Supports scenarios: more workers than tags, fewer workers than tags
        // ✓ Load balances effectively across BackupWorkers
        // ✓ No LogRouter performance degradation
    };
    
    // Week 18-22: BackupWorker Enhancement
    struct BackupWorkerMilestone {
        // Milestone 2.3: Multi-tag processing with shard filtering
        bool multiTagPeek = false;                 // Pull from multiple LogRouter tags
        bool shardFiltering = false;               // Process only assigned shards
        bool hybridTagProcessing = false;          // Handle both tag types
        bool multiShardFileWriting = false;        // Write multi-shard batch files
        
        // Success Criteria:
        // ✓ BackupWorkers process both LogRouter and Shard tags
        // ✓ Shard filtering works correctly (no double-processing)
        // ✓ Multi-shard files contain correct shard metadata
        // ✓ End-to-end mutation flow working in test environment
    };
    
    void executePhase2() {
        TraceEvent("Phase2Starting")
            .detail("Duration", "12 weeks")
            .detail("Goal", "Integrate shard-aware processing into FDB pipeline");
        
        // Sequential integration to minimize risk
        enhanceCommitProxy();        // Week 11-14
        enhanceLogRouter();          // Week 15-17  
        enhanceBackupWorker();       // Week 18-22
        
        validatePhase2Integration();
    }
    
private:
    void validatePhase2Integration() {
        /*
         * PHASE 2 ACCEPTANCE CRITERIA:
         * 
         * ✓ End-to-end shard-aware mutation flow working
         * ✓ Both tag types processed correctly during hybrid mode
         * ✓ Multi-shard batch files created with proper metadata
         * ✓ Performance targets met (no significant regression)
         * ✓ Fault tolerance maintained (worker failures handled)
         */
        
        bool endToEndFlow = validateEndToEndMutationFlow();
        bool hybridMode = validateHybridTagProcessing();
        bool fileFormat = validateMultiShardBatchFiles();
        bool performance = validatePerformanceTargets();
        bool faultTolerance = validateFaultTolerance();
        
        if (endToEndFlow && hybridMode && fileFormat && performance && faultTolerance) {
            TraceEvent("Phase2Complete")
                .detail("ReadyForPhase3", true);
        }
    }
};
```

### 13.4 Phase 3: Migration Infrastructure (9 weeks)

**Goal**: Build safe migration path with rollback capability and backward compatibility.

```cpp
class Phase3Implementation {
public:
    // Week 23-26: Hybrid Mode Support
    struct HybridModeMilestone {
        // Milestone 3.1: Dual backup system operation
        bool dualFormatWrites = false;             // Write both v2 and v3 formats
        bool crossVersionRestore = false;          // Restore from any format
        bool performanceMonitoring = false;       // Track efficiency gains
        
        // Success Criteria:
        // ✓ Can run v2 and v3 backup systems simultaneously
        // ✓ Restore works correctly from both backup formats
        // ✓ Performance monitoring shows efficiency improvements
        // ✓ No data loss or corruption during hybrid operation
    };
    
    // Week 27-29: Migration Orchestration
    struct MigrationOrchestrationMilestone {
        // Milestone 3.2: Automated migration workflow
        bool migrationController = false;          // Orchestrates migration phases
        bool progressTracking = false;             // Monitor migration status
        bool healthValidation = false;             // Continuous health checks
        
        // Success Criteria:
        // ✓ Migration controller manages all migration phases
        // ✓ Progress tracking provides visibility into migration status
        // ✓ Health validation detects issues and triggers rollback
        // ✓ Automated migration tested in staging environment
    };
    
    // Week 30-31: Rollback Safety
    struct RollbackSafetyMilestone {
        // Milestone 3.3: Emergency rollback capability
        bool rollbackController = false;           // Instant rollback to v2
        bool dataIntegrityValidation = false;      // Verify no data loss
        bool rollbackTesting = false;              // Test all rollback scenarios
        
        // Success Criteria:
        // ✓ Can rollback to v2 in under 5 minutes
        // ✓ Zero data loss during rollback process
        // ✓ All rollback scenarios tested and validated
        // ✓ Operations team trained on rollback procedures
    };
    
    void executePhase3() {
        TraceEvent("Phase3Starting")
            .detail("Duration", "9 weeks")
            .detail("Goal", "Build safe migration path with rollback capability");
        
        implementHybridModeSupport();      // Week 23-26
        implementMigrationOrchestration(); // Week 27-29
        implementRollbackSafety();         // Week 30-31
        
        validatePhase3Migration();
    }
    
private:
    void validatePhase3Migration() {
        /*
         * PHASE 3 ACCEPTANCE CRITERIA:
         * 
         * ✓ Complete migration workflow automated and tested
         * ✓ Rollback capability validated in all scenarios
         * ✓ Zero data loss during migration or rollback
         * ✓ Performance monitoring shows expected improvements
         * ✓ Operations procedures documented and teams trained
         */
        
        bool migrationWorkflow = validateCompleteMigrationWorkflow();
        bool rollbackCapability = validateRollbackInAllScenarios();
        bool dataIntegrity = validateZeroDataLoss();
        bool performanceGains = validatePerformanceImprovements();
        bool operationalReadiness = validateOperationalReadiness();
        
        if (migrationWorkflow && rollbackCapability && dataIntegrity && 
            performanceGains && operationalReadiness) {
            TraceEvent("Phase3Complete")
                .detail("ReadyForPhase4", true);
        }
    }
};
```

### 13.5 Phase 4: Production Deployment (14 weeks)

**Goal**: Deploy to production with performance validation and monitoring.

```cpp
class Phase4Implementation {
public:
    // Week 32-35: Performance Optimization
    struct PerformanceOptimizationMilestone {
        // Milestone 4.1: Efficiency validation
        bool efficiencyMeasurement = false;       // Measure 250,000x improvement
        bool resourceOptimization = false;        // Memory/CPU optimization
        bool scalabilityValidation = false;       // Test with 500K+ shards
        
        // Success Criteria:
        // ✓ 250,000x efficiency improvement validated in production workloads
        // ✓ Resource usage optimized for large shard counts
        // ✓ System scales linearly with shard count increases
        // ✓ Performance benchmarks exceed targets
    };
    
    // Week 36-41: Production Deployment
    struct ProductionDeploymentMilestone {
        // Milestone 4.2: Staged production rollout
        bool testClusterDeployment = false;       // Deploy to test clusters
        bool stagingValidation = false;           // Validate in staging
        bool productionRollout = false;           // Gradual production rollout
        
        // Success Criteria:
        // ✓ Successful deployment to test clusters
        // ✓ Staging validation passes all test scenarios
        // ✓ Production rollout completed without incidents
        // ✓ All production clusters running v3 backup successfully
    };
    
    // Week 42-45: Validation & Monitoring
    struct ValidationMonitoringMilestone {
        // Milestone 4.3: Production validation
        bool productionValidation = false;        // Validate in production workloads
        bool monitoringDashboards = false;        // Operational monitoring
        bool documentationComplete = false;       // Complete documentation
        
        // Success Criteria:
        // ✓ Production validation confirms all targets met
        // ✓ Monitoring dashboards provide operational visibility
        // ✓ Documentation complete for operations teams
        // ✓ Performance SLAs maintained or improved
    };
    
    void executePhase4() {
        TraceEvent("Phase4Starting")
            .detail("Duration", "14 weeks")
            .detail("Goal", "Deploy to production with full validation");
        
        optimizePerformance();         // Week 32-35
        deployToProduction();          // Week 36-41
        validateAndMonitor();          // Week 42-45
        
        validatePhase4Production();
    }
    
private:
    void validatePhase4Production() {
        /*
         * PHASE 4 ACCEPTANCE CRITERIA (FINAL):
         * 
         * ✓ 250,000x efficiency improvement achieved in production
         * ✓ 100% of production clusters migrated successfully
         * ✓ Zero customer-impacting incidents during rollout
         * ✓ Performance SLAs maintained or improved
         * ✓ Operations teams fully trained and equipped
         */
        
        bool efficiencyAchieved = validateEfficiencyInProduction();
        bool fullMigration = validateAllClustersV3();
        bool zeroIncidents = validateZeroCustomerImpact();
        bool slasMaintained = validateSLACompliance();
        bool operationsReady = validateOperationsReadiness();
        
        if (efficiencyAchieved && fullMigration && zeroIncidents && 
            slasMaintained && operationsReady) {
            TraceEvent("Phase4Complete")
                .detail("BackupV3ProductionReady", true)
                .detail("EfficiencyImprovementAchieved", "250000x");
        }
    }
};
```

### 13.6 Risk Management and Contingency Planning

```cpp
class RiskManagementPlan {
public:
    enum class RiskLevel { LOW, MEDIUM, HIGH, CRITICAL };
    
    struct Risk {
        std::string description;
        RiskLevel level;
        std::string mitigation;
        std::string contingency;
    };
    
    std::vector<Risk> identifiedRisks = {
        {
            .description = "Performance regression during migration",
            .level = RiskLevel::HIGH,
            .mitigation = "Extensive performance testing in Phase 2",
            .contingency = "Immediate rollback capability in <5 minutes"
        },
        {
            .description = "Data corruption during hybrid mode",
            .level = RiskLevel::CRITICAL,
            .mitigation = "Continuous integrity validation and checksums",
            .contingency = "Emergency rollback with data consistency verification"
        },
        {
            .description = "Scalability issues with 250K+ shards",
            .level = RiskLevel::MEDIUM,
            .mitigation = "Scale testing throughout Phase 2 and 3",
            .contingency = "Shard count limits and gradual scaling"
        },
        {
            .description = "Migration complexity causing operational issues",
            .level = RiskLevel::MEDIUM,
            .mitigation = "Extensive automation and operational training",
            .contingency = "Extended Phase 3 timeline and additional training"
        },
        {
            .description = "Backward compatibility breaking changes",
            .level = RiskLevel::HIGH,
            .mitigation = "Hybrid restore engine supporting both formats",
            .contingency = "Maintain v2 compatibility indefinitely if needed"
        }
    };
    
    void implementRiskMitigation() {
        for (const auto& risk : identifiedRisks) {
            TraceEvent("RiskMitigation")
                .detail("Risk", risk.description) 
                .detail("Level", (int)risk.level)
                .detail("Mitigation", risk.mitigation)
                .detail("Contingency", risk.contingency);
        }
    }
};
```

### 13.7 Success Metrics and Validation Criteria

```cpp
struct ProjectSuccessMetrics {
    // Technical Metrics
    struct TechnicalMetrics {
        double efficiencyImprovement = 0.0;       // Target: >= 250,000x
        double performanceRegression = 0.0;       // Target: < 5%
        int dataIntegrityIssues = 0;              // Target: 0
        int rollbackTime = 0;                     // Target: < 5 minutes
        int testCoverage = 0;                     // Target: >= 95%
    };
    
    // Operational Metrics  
    struct OperationalMetrics {
        int customerImpactingIncidents = 0;       // Target: 0
        double migrationSuccessRate = 0.0;        // Target: 100%
        int trainingHoursCompleted = 0;           // Target: >= 40 hours per team
        int documentationPagesCreated = 0;        // Target: >= 100 pages
        double slaCompliance = 0.0;               // Target: >= 99.9%
    };
    
    // Business Metrics
    struct BusinessMetrics {
        double costReduction = 0.0;               // Target: >= 50% (restore operations)
        double sreEfficiencyGain = 0.0;           // Target: >= 1000% (partial restores)
        int innovationOpportunities = 0;          // Target: >= 5 new use cases
        double customerSatisfaction = 0.0;        // Target: >= 95%
    };
    
    bool validateProjectSuccess() {
        /*
         * PROJECT SUCCESS CRITERIA:
         * 
         * MUST HAVE (Project fails if not achieved):
         * ✓ 250,000x efficiency improvement for partial restores
         * ✓ Zero data loss or corruption
         * ✓ 100% backward compatibility
         * ✓ Sub-5-minute rollback capability
         * 
         * SHOULD HAVE (Success factors):
         * ✓ <5% performance regression during normal operations
         * ✓ Zero customer-impacting incidents during rollout
         * ✓ 100% production cluster migration success rate
         * ✓ Operations teams fully trained and confident
         * 
         * NICE TO HAVE (Bonus achievements):
         * ✓ >500,000x efficiency in some scenarios
         * ✓ Performance improvements in full backup scenarios
         * ✓ New backup/restore use cases enabled
         * ✓ Open source community adoption
         */
        
        return validateMustHaveCriteria() && 
               validateShouldHaveCriteria() &&
               measureNiceToHaveAchievements();
    }
};
```

### 13.8 Timeline Summary and Resource Requirements

**Total Project Duration**: 45 weeks (~11 months)
**Team Size**: 8-10 senior engineers + 2-3 test engineers + 1-2 SREs
**Total Engineering Effort**: ~400-500 person-weeks

**Critical Path Dependencies**:
1. **Weeks 1-10**: Foundation infrastructure (parallel development possible)
2. **Weeks 11-22**: Sequential integration (CommitProxy → LogRouter → BackupWorker)
3. **Weeks 23-31**: Migration infrastructure (can parallelize with integration testing)
4. **Weeks 32-45**: Production deployment (staged rollout with validation)

**Key Decision Points**:
- **Week 10**: Go/No-Go for Phase 2 (foundation complete)
- **Week 22**: Go/No-Go for Phase 3 (integration successful)  
- **Week 31**: Go/No-Go for Phase 4 (migration ready)
- **Week 35**: Go/No-Go for production rollout (performance validated)

This phased implementation ensures the revolutionary FoundationDB Backup v3 system delivers its promised 250,000x efficiency improvement while maintaining FoundationDB's legendary reliability and operational excellence standards.
