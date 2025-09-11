# FoundationDB Backup Worker v2 Documentation

> **Note**: For the original design and architecture details of the partitioned log backup system, see [`design/backup_v2_partitioned_logs.md`](../design/backup_v2_partitioned_logs.md). This document focuses on the current implementation details and operational aspects.

## Overview

The Backup Worker is a key component of FoundationDB's backup system v2, introduced to improve backup performance and scalability. Unlike the traditional backup approach where a single backup agent reads from the database, the backup worker system distributes the backup workload across multiple worker processes that read mutations directly from the Transaction Logs (TLogs).

## Architecture

### Key Components

1. **BackupWorker Process** ([`fdbserver/BackupWorker.actor.cpp`](../fdbserver/BackupWorker.actor.cpp))
   - Runs as a separate process type in the FoundationDB cluster
   - Each worker is assigned a specific tag to pull mutations from TLogs
   - Uses the LogRouter tag system for efficient data routing

2. **BackupInterface** ([`fdbserver/include/fdbserver/BackupInterface.h`](../fdbserver/include/fdbserver/BackupInterface.h))
   - Defines the communication interface for backup workers
   - Minimal interface with primarily a `waitFailure` endpoint for monitoring

3. **BackupData Structure**
   - Main state management structure for each backup worker
   - Tracks versions, messages, and multiple concurrent backups
   - Manages memory through a FlowLock mechanism

## How It Works

### 1. TLog Mutation Distribution and Routing

In FoundationDB's tag-partitioned log system:

#### How Mutations are Tagged and Written

**Mutations get BOTH Storage Server AND LogRouter Tags**

When a mutation is committed (from LogSystem.cpp, lines 303-310):
```cpp
if (logSystem->hasRemoteLogs()) {
    prev_tags.push_back(chooseRouterTag());  // Add LogRouter tag
}
for (auto& tag : next_message_tags) {
    prev_tags.push_back(tag);                // Add storage server tags
}
logSystem->getPushLocations(prev_tags, msg_locations);  // Determine TLogs
```

This means:
1. **Each mutation has multiple tags**: Storage server tags AND a LogRouter tag
2. **Single call to getPushLocations**: Determines TLogs for ALL tags together
3. **Union of locations**: The mutation goes to the UNION of TLogs needed for all its tags

#### Evidence: How getPushLocations Actually Works
### Understanding "Primary TLog" for Tags

The **primary TLog** for a tag is the TLog deterministically chosen as the first/preferred location for mutations with that tag. This is calculated using the `bestLocationFor` function:

From `LogSystem.cpp` lines 181-187:
```cpp
int LogSet::bestLocationFor(Tag tag) {
    if (locality == tagLocalitySatellite) {
        return satelliteTagLocations[tag.id + 1][0];
    }
    // For non-satellite: simple round-robin based on tag ID
    return tag.id % logServers.size();
}
```

**Key Points:**
- Each tag has exactly ONE primary TLog
- For normal (non-satellite) deployments: `primary_tlog_index = tag.id % number_of_TLogs`
- This is a simple modulo hash that distributes tags evenly across TLogs
- Example with 8 TLogs:
  - Tag(0) → TLog 0 (its primary)
  - Tag(1) → TLog 1 (its primary)
  - Tag(7) → TLog 7 (its primary)
  - Tag(8) → TLog 0 (its primary, wraps around)
  - Tag(-2, 0) [LogRouter] → TLog 0 (its primary)
  - Tag(-2, 1) [LogRouter] → TLog 1 (its primary)

**Why Primary TLogs Matter:**
1. **Deterministic Selection**: Every component can calculate which TLog is primary for a tag without coordination
2. **Load Distribution**: Tags are evenly distributed across TLogs via modulo arithmetic
3. **Replication Starting Point**: The primary is always included, then additional replicas are added to meet replication requirements
4. **Different Primaries Don't Increase Total Replication**: When a mutation has tags with different primaries, ALL primaries are forced into the selection, but `selectReplicas` only adds enough additional replicas to meet (not exceed) the base replication factor

### Would SS and LR Tags Have Different Primaries in Practice?

**YES, they often would!** Here's why:

1. **How Storage Server Tags are Assigned** (from [`CommitProxyServer.actor.cpp:839`](../fdbserver/CommitProxyServer.actor.cpp:839)):
   ```cpp
   auto& tags = pProxyCommitData->tagsForKey(m.param1);
   ```
   - SS tags are based on the key range the mutation affects
   - Each storage server has a specific tag ID assigned by the system
   - Example: SS handling range [A, B) might have tag(0, 5)

2. **How LogRouter Tags are Chosen** (from [`TagPartitionedLogSystem.actor.cpp:1920`](../fdbserver/TagPartitionedLogSystem.actor.cpp:1920) and [`LogSystem.cpp:304`](../fdbserver/LogSystem.cpp:304)):
   ```cpp
   Tag getRandomRouterTag() {
       return Tag(tagLocalityLogRouter, deterministicRandom()->randomInt(0, logRouterTags));
   }
   ```
   - LogRouter tags are chosen **randomly** for each batch
   - The commit proxy picks a random LogRouter tag from available ones
   - Example: Randomly chosen tag(-2, 3) for this batch

3. **In Practice**:
   - Storage server tag for key "foo": tag(0, 5) → primary TLog 5
   - Random LogRouter tag for batch: tag(-2, 3) → primary TLog 3
   - **Different primaries (5 vs 3)!**
   
4. **Why This Results in Exactly RF TLogs (Not More)**:
   
   The key insight is how `selectReplicas` works with the `alsoServers` parameter:
   
   - **Step 1**: Both primaries (3 and 5) are added to `alsoServers` - these are FORCED selections
   - **Step 2**: `selectReplicas(policy, alsoServers={3,5}, resultEntries)` is called
   - **Step 3**: The policy says "I need RF=3 total replicas"
   - **Step 4**: Since we already have 2 forced (3 and 5), the policy adds only 1 more to reach RF=3
   - **Final result**: TLogs {3, 5, X} where X is chosen by the policy
   
   **The math**:
   - Number of forced primaries: 2 (TLog 3 for LR, TLog 5 for SS)
   - Additional replicas needed: RF - len(alsoServers) = 3 - 2 = 1
   - **Total TLogs: 2 + 1 = 3 (exactly RF, not RF+1)**
   
   **Summary**: With RF=3 and different primaries, we get exactly 3 TLogs. The system efficiently handles dual-tagging without exceeding the configured replication factor.


From `LogSystem.cpp` lines 244-274 and `Replication.h` line 43-46:

```cpp
// Step 1: Collect primary TLogs for ALL tags (lines 244-249)
for (auto& t : tags) {
    if (locality == tagLocalitySpecial || t.locality == locality || t.locality < 0) {
        newLocations.push_back(bestLocationFor(t));  // Primary for each tag
    }
}

// Step 2: Remove duplicates (line 251)
uniquify(newLocations);

// Step 3: Add primaries to output AND to "alsoServers" (lines 257-260)
for (auto location : newLocations) {
    locations.push_back(locationOffset + location);  // Add to output
    alsoServers.push_back(logEntryArray[location]); // Mark as "already selected"
}

// Step 4: Find ADDITIONAL replicas (line 267)
// selectReplicas signature: (policy, alsoServers, results)
// where alsoServers = "already selected", results = "additional needed"
result = logServerSet->selectReplicas(tLogPolicy, alsoServers, resultEntries);

// Step 5: Add the additional replicas (lines 272-274)
for (auto entry : resultEntries) {
    locations.push_back(locationOffset + *logServerMap->getObject(entry));
}
```

**Key Insight**: `alsoServers` are treated as "forced" or "already selected" locations. The replication policy then adds ADDITIONAL TLogs to satisfy the replication requirements.

#### Why This Design Works for Backup Workers

1. **Storage servers read their tagged mutations**:
   - Storage server for tag (0, 5) has primary TLog 5
   - The replication policy **deterministically** adds 2 more TLogs to meet RF=3
   - **IMPORTANT**: These additional TLogs are NOT arbitrary! They're determined by:
     - The same replication policy algorithm used everywhere
     - The same cluster configuration (TLog locations, zones, etc.)
     - Result: EVERY component calculates the SAME set of TLogs for tag (0, 5)
   - Storage server knows to read from TLogs {1, 5, 7} because:
     - It runs the same `selectReplicas` algorithm as the commit proxy
     - Given the same inputs (tag, cluster config), it gets the same outputs
   - If primary (5) is down, SS can still read from 1 or 7
   - Gets only mutations tagged for it

2. **Backup workers read LogRouter-tagged mutations**:
   - Backup worker with tag (-2, 3) has primary TLog 3
   - The replication policy **deterministically** adds 2 more TLogs to meet RF=3
   - Same principle: the worker knows exactly which TLogs have its data
   - Backup worker reads from TLogs {0, 3, 6}
   - Sees ALL mutations (because all mutations get a LogRouter tag)
   - Filters to only backup-relevant mutations

**Key Insight: Deterministic Replica Selection**
The "additional" replicas aren't random - they're calculated using a deterministic algorithm based on:
- The tag being replicated
- The cluster's TLog topology (locations, zones, failure domains)
- The replication policy rules

This means:
- Commit proxy decides where to write using `getPushLocations`
- Storage servers know where to read using the same logic
- No coordination needed - everyone independently computes the same answer
- If any TLog is down, readers know the complete set of alternatives

3. **Overlap is intentional**:
   - Some TLogs have mutations for both purposes
   - Efficient use of replication while ensuring availability

#### How Backup Workers Find All Mutations

1. **Every mutation has a LogRouter tag**:
   - Added automatically when `hasRemoteLogs()` is true
   - Randomly chosen from available LogRouter tags

2. **Backup worker reads its LogRouter tag**:
   - Worker with Tag(-2, 3) connects to TLogs that have this tag
   - These TLogs have ALL mutations with Tag(-2, 3)
   - Since mutations are randomly distributed across LogRouter tags, each worker sees ~1/N of all mutations

3. **Complete coverage**:
   - With N backup workers, each with a different LogRouter tag
   - Together they cover all mutations in the system
   - No coordination needed - the tagging ensures complete partitioning

#### Actual Behavior With Evidence

With 8 TLogs and replication factor 3:

**Scenario 1: Different Primary TLogs**
- Mutation with tags [StorageTag(0, 5), LogRouterTag(-2, 2)]
- Step 1: Primary locations = {2, 5} (2 TLogs)
- Step 2: These become `alsoServers` (forced selections)
- Step 3: Policy needs total of 3 TLogs, already has 2, adds 1 more
- **Result: {2, 5, 7} - Exactly replication factor (3 TLogs)**

**Scenario 2: Same Primary TLog**
- Mutation with tags [StorageTag(0, 3), LogRouterTag(-2, 3)]
- Step 1: Primary locations = {3} (both map to same)
- Step 2: `alsoServers` = {3}
- Step 3: Policy needs 3 total, has 1, adds 2 more
- **Result: {3, 1, 6} - Exactly replication factor**

**The Correct Evidence:**
- When LogRouter tags are enabled and map to different primaries than storage tags
- The mutation is written to exactly RF TLogs (not more)
- Typical case with RF=3: 2 different primaries + 1 additional = 3 TLogs total
- This design efficiently ensures both storage servers and backup workers can read their data

#### Why This Design Is Efficient

1. **No Extra Overhead**:
   - Ensures both storage servers AND backup workers can always find their data
   - Stays within the configured replication factor (no extra writes)

2. **How It Works**:
   - With RF=3 and 2 different primaries: Still only 3 TLogs total
   - `selectReplicas` counts forced primaries toward the total RF requirement
   - Only adds enough additional replicas to reach RF, not exceed it

3. **Benefits**:
   - No write amplification beyond configured RF
   - Enables efficient parallel backup without coordination
   - Storage overhead matches exactly what was configured

4. **Real Impact**:
   - With RF=3 and different primaries: Exactly 3 TLog writes
   - With RF=3 and same primary: Also exactly 3 TLog writes
   - No performance penalty for having backup workers enabled

### 2. Worker Recruitment

Backup workers are recruited during cluster recovery when `backup_worker_enabled` is set to 1:

```cpp
// From ClusterRecovery.actor.cpp
if (self->configuration.backupWorkerEnabled) {
    self->addActor.send(recruitBackupWorkers(self, cx));
}
```

The recruitment process:
- Cluster Controller identifies available worker processes
- Assigns workers with ProcessClass::Backup designation
- Each worker receives an `InitializeBackupRequest` with:
  - `routerTag`: The specific LogRouter tag this worker should pull
    - **This tag serves TWO purposes:**
    - **1. Determines which TLogs to connect to** (using the same deterministic replica selection)
    - **2. Filters which mutations to process** (only those tagged with this LR tag)
  - `totalTags`: Total number of backup workers
  - `startVersion`: Version to begin backing up from
  - `endVersion`: Optional end version for old epoch recovery
  - `recruitedEpoch`: Current epoch being recruited
  - `backupEpoch`: Epoch to actually backup (may be older)

### How the LogRouter Tag Works for Backup Workers

When a backup worker is assigned LogRouter tag (-2, 3):

1. **It determines which TLogs to read from**:
   - Uses `bestLocationFor(tag(-2, 3))` to find primary TLog (e.g., TLog 3)
   - Uses `selectReplicas` to find the full set (e.g., TLogs {0, 3, 6})
   - **Can read from ANY of these TLogs** - they all have the same data for this tag!

2. **Fault Tolerance - What Happens When a TLog Fails**:
   - **Normal operation**: Backup worker typically reads from primary (TLog 3)
   - **If TLog 3 fails**: Worker automatically switches to TLog 0 or 6
   - **All replicas have identical data** for tag (-2, 3) due to replication
   - **No data loss**: As long as ANY replica is alive (RF provides this guarantee)
   - **Automatic failover**: The LogSystem interface handles TLog failures transparently

3. **It filters mutations from the chosen TLog**:
   - Each TLog contains mutations for MANY different tags
   - The backup worker only processes mutations that have tag (-2, 3)
   - Other mutations in the same TLog (with different tags) are ignored

**Key Point: With RF=3, the backup worker has 3 different TLogs it can read from**:
- This provides fault tolerance (can survive 2 TLog failures)
- Load balancing (can spread reads across multiple TLogs)
- Performance (can read from the fastest/closest TLog)

**Example**: If TLog 3 contains:
- Mutation A with tags [StorageTag(0, 5), LogRouterTag(-2, 3)]  ← Backup worker processes this
- Mutation B with tags [StorageTag(0, 7), LogRouterTag(-2, 1)]  ← Backup worker ignores this
- Mutation C with tags [StorageTag(0, 2), LogRouterTag(-2, 3)]  ← Backup worker processes this

The backup worker with tag (-2, 3) only processes mutations A and C.

### 3. Data Flow

```
TLogs (all mutations) → Backup Workers (partitioned by tag) → Blob Storage
         ↓
    (mutations)
         ↓
    (filter & batch)
         ↓
    (upload files)
```

Each backup worker:
1. **Pulls mutations** from TLogs using its assigned LogRouter tag
2. **Filters mutations** to only include data within backup ranges
3. **Batches mutations** into blocks (default 1MB blocks)
4. **Uploads to blob storage** in a partitioned format
5. **Tracks progress** in the system keyspace

### 3. Version Management

The backup worker maintains several important versions:

- **startVersion**: Initial version to begin backup
- **savedVersion**: Latest version successfully saved to blob storage
- **popVersion**: Latest version that can be popped from TLogs
- **minKnownCommittedVersion**: Minimum version known to be committed
- **pulledVersion**: Latest version pulled from TLogs

### 4. Operational Modes

#### Active Backup Mode
When backups are running (`backupStartedKey` is set):
- Workers actively pull mutations from TLogs
- Mutations are filtered, batched, and uploaded to blob storage
- Progress is continuously saved to the system keyspace

#### NOOP Mode
When no backups are active:
- Workers continue to pop versions from TLogs to prevent accumulation
- No data is saved to blob storage
- Tracks the maximum NOOP version for resumption

#### Pause Mode
When `backupPausedKey` is set:
- Workers temporarily stop pulling new mutations
- Existing buffered data may still be uploaded
- Resumes automatically when unpaused

## Key Features

### 1. Parallel Processing
- Multiple workers process different tags simultaneously
- Scales horizontally with the number of TLog tags
- Each worker handles approximately 1/N of the total workload

### 2. Memory Management
- Uses FlowLock to control memory usage (default `BACKUP_WORKER_LOCK_BYTES`)
- Prevents memory overflow by blocking pulls when buffer is full
- Releases memory as messages are processed and uploaded

### 3. Epoch Support
- Handles both current epoch and old epoch recovery
- Workers can be recruited to finish incomplete work from previous epochs
- Coordinates with master to ensure proper cleanup

### 4. Multiple Concurrent Backups
- Single worker can handle multiple backup jobs simultaneously
- Each backup has its own:
  - Container configuration
  - Key ranges to backup
  - Progress tracking

### 5. Encryption Support
- Handles encrypted mutations transparently
- Fetches cipher keys as needed
- Decrypts mutations for filtering, re-encrypts for storage

## Configuration

### Enabling Backup Workers

```
fdbcli> configure backup_worker_enabled:=1
```

### Key Server Knobs

| Knob | Default | Description |
|------|---------|-------------|
| `BACKUP_WORKER_LOCK_BYTES` | Varies | Memory limit per backup worker |
| `BACKUP_FILE_BLOCK_BYTES` | 1MB | Size of blocks in backup files |
| `BACKUP_UPLOAD_DELAY` | 10s | Delay between upload batches |
| `SECONDS_BEFORE_RECRUIT_BACKUP_WORKER` | 1s | Delay before recruiting workers |
| `BACKUP_TIMEOUT` | 300s | Timeout for backup worker operations |
| `BACKUP_NOOP_POP_DELAY` | 1s | Delay between NOOP pops |

## System Keys

The backup worker system uses several system keys for coordination:

| Key | Purpose |
|-----|---------|
| `\xff\x02/backupStarted` | Signals active backups to workers |
| `\xff\x02/backupPaused` | Pauses/resumes backup workers |
| `\xff\x02/backupProgress/[workerID]` | Tracks each worker's progress |
| `\xff\x02/backupWorkerMaxNoopVersion` | Maximum version popped in NOOP mode |

## Progress Monitoring

### Worker Progress
Each worker saves its progress using `WorkerBackupStatus`:
```cpp
struct WorkerBackupStatus {
    LogEpoch epoch;
    Version version;
    Tag tag;
    int32_t totalTags;
};
```

### Backup Progress
The lead worker (tag 0) monitors overall backup progress:
- Collects status from all workers
- Updates `latestBackupWorkerSavedVersion` in BackupConfig
- Ensures all workers have started before marking backup ready

## File Format

Backup workers write mutations in the **PARTITIONED_MLOG_VERSION** format (version 4110):

```
Block Header: [Version:4 bytes]
Mutation: [Version:8 bytes][SubVersion:4 bytes][Size:4 bytes][Data:variable]
```

- All multi-byte values are stored in big-endian format
- Blocks are padded with 0xFF to reach block size
- Each file contains mutations for a specific version range

### Block Handling for Small Batches

When there aren't enough mutations to fill a complete block:

1. **Padding**: If mutations don't fill the block size (default 1MB), the remaining space is padded with 0xFF bytes
2. **Upload Delay**: Workers wait up to `BACKUP_UPLOAD_DELAY` (default 10s) to accumulate more mutations
3. **Version Boundaries**: Files are always saved on version boundaries to ensure consistency
4. **Forced Upload**: When a backup completes or the worker reaches the end version, partial blocks are uploaded immediately with padding

This ensures:
- Consistent file format for readers
- Bounded latency for backup completion
- Efficient use of storage (blocks are reused across versions when possible)

## Error Handling

### Worker Displacement
- Workers detect when they've been replaced by a new recovery
- Gracefully stop processing and exit
- Do not pop versions to avoid losing data

### Backup Worker Failure
- Master detects worker failures through `waitFailure`
- Triggers cluster recovery with `backup_worker_failed` error
- New workers are recruited to continue from last saved progress

### Version Gaps
- Workers detect if TLogs have popped beyond expected versions
- Compares with saved NOOP version to verify data availability
- Raises alerts if mutations are potentially lost

### When Backup Workers Are Slow or Stop Popping

If backup workers are slow to read mutations from TLogs or stop popping entirely, several critical issues arise:

1. **TLog Memory Cache Behavior**:
   - **Cache Size**: Default configured with 1.5GB memory cache (TLOG_SPILL_THRESHOLD = 1500e6 bytes)
   - **Must Retain**: TLogs MUST retain all mutations until popped by ALL consumers (storage servers AND backup workers)
   - **Memory Growth**: Unpoppable mutations accumulate in memory cache first

2. **Spilling to Disk (Confirmed)**:
   - **When Cache Fills**: Once 1.5GB memory cache (TLOG_SPILL_THRESHOLD) is exhausted, TLogs spill to disk
   - **Disk Queue**: Mutations continue accumulating in on-disk persistent queue
   - **Unbounded Growth**: Disk queue can grow without limit (until disk space exhausted)
   - **Performance Impact**: Disk I/O becomes bottleneck, affecting write throughput

3. **TLog Accumulation for Extended Periods**:
   - **Can accumulate for hours/days**: TLogs can retain data on disk for 24+ hours when backup workers are slow
   - **Practical limit is disk space**: TLogs continue spilling to disk until storage is exhausted
   - **No hard time limit for backup workers**: Unlike datacenter lag (MAX_VERSION_DIFFERENCE), backup worker lag doesn't force recovery
   - **Operational monitoring required**: Must monitor disk usage and backup worker progress to prevent disk exhaustion
   
4. **Cascade Effects of Slow Backup Workers**:
   - **Progressive degradation**:
     - Increased memory pressure as cache fills
     - Disk I/O saturation from continuous spilling
     - Slower commit latencies due to I/O contention
     - Growing disk usage over hours/days
   - **Critical failure scenarios**:
     - Disk exhaustion after extended accumulation
     - Possible OOM if disk operations fail
     - Manual intervention required to resolve

5. **Automatic Mitigation Mechanisms**:
   - **NOOP Mode**: If no backups active, workers continue popping without processing
   - **Pop Version Monitoring**: System tracks minimum pop version across all workers
   - **Forced Recovery**: If workers lag too far behind, triggers recovery
   - **Emergency Disable**: Can disable backup workers via configuration change

### How the System Prevents Unbounded Accumulation

The system has multiple layers of protection:

1. **Continuous Popping**:
   - **NOOP Mode**: Workers pop even when no backups are active (BACKUP_NOOP_POP_DELAY = 5.0 seconds)
   - **Prevents accumulation**: Ensures TLogs can release memory/disk

2. **Version Lag Monitoring**:
   - **Tracked metric**: Difference between committed version and minimum popped version
   - **Early warning**: Alerts on growing lag to prevent disk exhaustion
   - **Operational response**: Administrators must intervene when lag grows excessive

3. **Protective Mechanisms**:
   - **Disk space monitoring**: Track available storage for TLog spilling
   - **Backup worker health checks**: Detect and replace failed workers
   - **Manual intervention options**: Can disable backup workers or adjust configuration

4. **Operational Controls**:
   - **Disable backup workers**: Can be done without recovery via configuration
   - **Adjust memory cache**: Can tune TLOG_SPILL_THRESHOLD knob for workload
   - **Monitor metrics**: Track pop version lag, disk queue size, memory usage

**Critical Insight**: While TLogs can accumulate data for extended periods (24+ hours) when backup workers are slow, this requires careful operational monitoring. The system prioritizes data durability over forced recovery, allowing administrators time to resolve backup worker issues before disk exhaustion occurs. The MAX_VERSION_DIFFERENCE limit (20 seconds) applies to datacenter replication lag, not backup worker lag.

## Performance Considerations

1. **Memory Usage**: Each worker can buffer up to `BACKUP_WORKER_LOCK_BYTES` of mutations
2. **Network Traffic**: Workers read from TLogs and write to blob storage continuously
3. **CPU Usage**: Moderate CPU for filtering, encryption/decryption, and serialization
4. **Storage IOPS**: Depends on mutation rate and number of concurrent backups

## Advantages Over Traditional Backup

1. **Scalability**: Distributes load across multiple workers
2. **Performance**: Parallel processing significantly improves throughput
3. **Reliability**: Workers can be restarted without losing progress
4. **Efficiency**: Direct reading from TLogs avoids database read overhead
5. **Flexibility**: Supports multiple concurrent backups with different configurations
6. **Completeness**: Since all TLogs have all mutations, no data can be missed

## Limitations

1. Requires configuration change to enable/disable
2. Additional processes consume cluster resources
3. Complexity in coordinating multiple workers
4. Must handle epoch transitions carefully

## Best Practices

1. **Resource Planning**: Allocate sufficient memory and network bandwidth for workers
2. **Monitoring**: Track worker progress and health through system keys
3. **Testing**: Test backup and restore procedures regularly
4. **Configuration**: Tune knobs based on workload and infrastructure
5. **Coordination**: Ensure proper coordination between backup start/stop and worker enable/disable

## Future Improvements

Potential areas for enhancement:
- Dynamic worker scaling based on load
- Improved compression for backup files
- Better integration with cloud storage APIs
- Enhanced monitoring and alerting capabilities
- Support for incremental backups at the worker level