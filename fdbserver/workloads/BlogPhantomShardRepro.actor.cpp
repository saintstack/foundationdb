/*
 * BlogPhantomShardRepro.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Workload to reproduce the blog phantom shard bug where keyServers entries
// exist for blog ranges without corresponding serverKeys entries.
//
// The bug occurs when:
// 1. Backup creates blog ranges (mutation logs at \xff\x02/blog/...)
// 2. A storage server that owns blog ranges fails
// 3. removeKeysFromFailedServer processes the blog ranges
// 4. The operation is interrupted (crash, timeout) after some keyServers
//    entries are committed but before all serverKeys are set
// 5. Result: phantom keyServers entries with no corresponding serverKeys

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// Count keyServers entries for blog ranges
ACTOR Future<int64_t> countBlogKeyServers(Database cx) {
	state Transaction tr(cx);
	state int64_t count = 0;
	state Key begin = backupLogKeys.begin;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			// Read keyServers for blog range
			KeyRange blogKeyServersRange =
			    KeyRangeRef(keyServersKey(backupLogKeys.begin), keyServersKey(backupLogKeys.end));

			RangeResult result = wait(tr.getRange(blogKeyServersRange, CLIENT_KNOBS->TOO_MANY));
			count = result.size();
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return count;
}

// Count serverKeys entries for a specific server in blog range
ACTOR Future<int64_t> countServerKeysBlog(Database cx, UID serverID) {
	state Transaction tr(cx);
	state int64_t count = 0;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			Key prefix = serverKeysPrefixFor(serverID);
			KeyRange blogServerKeysRange =
			    KeyRangeRef(prefix.withSuffix(backupLogKeys.begin), prefix.withSuffix(backupLogKeys.end));

			RangeResult result = wait(tr.getRange(blogServerKeysRange, CLIENT_KNOBS->TOO_MANY));

			// Count non-false entries
			for (const auto& kv : result) {
				if (kv.value != serverKeysFalse) {
					count++;
				}
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return count;
}

// Get list of storage servers
ACTOR Future<std::vector<UID>> getStorageServers(Database cx) {
	state Transaction tr(cx);
	state std::vector<UID> servers;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));

			for (const auto& kv : serverList) {
				UID id = decodeServerListValue(kv.value).id();
				servers.push_back(id);
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return servers;
}

// Get a storage server's address for exclusion
ACTOR Future<Optional<NetworkAddress>> getStorageServerAddress(Database cx) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));

			if (!serverList.empty()) {
				// Pick a random server
				int idx = deterministicRandom()->randomInt(0, serverList.size());
				StorageServerInterface ssi = decodeServerListValue(serverList[idx].value);
				return ssi.address();
			}
			return Optional<NetworkAddress>();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Decode source servers from keyServers value
std::vector<UID> decodeKeyServersSrc(const ValueRef& value) {
	std::vector<UID> src, dest;
	UID srcId, destId;
	decodeKeyServersValue(RangeResult(), value, src, dest, srcId, destId);
	return src;
}

// Find servers that own blog ranges
ACTOR Future<std::set<UID>> findBlogRangeOwners(Database cx) {
	state Transaction tr(cx);
	state std::set<UID> owners;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			KeyRange blogKeyServersRange =
			    KeyRangeRef(keyServersKey(backupLogKeys.begin), keyServersKey(backupLogKeys.end));

			RangeResult result = wait(tr.getRange(blogKeyServersRange, CLIENT_KNOBS->TOO_MANY));

			for (const auto& kv : result) {
				std::vector<UID> src = decodeKeyServersSrc(kv.value);
				for (const UID& id : src) {
					owners.insert(id);
				}
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return owners;
}

// Check for phantom shards: keyServers entries without corresponding serverKeys
ACTOR Future<int64_t> countPhantomBlogShards(Database cx) {
	state Transaction tr(cx);
	state int64_t phantomCount = 0;
	state RangeResult keyServersResult;
	state int idx = 0;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			// Get all blog keyServers entries
			KeyRange blogKeyServersRange =
			    KeyRangeRef(keyServersKey(backupLogKeys.begin), keyServersKey(backupLogKeys.end));
			RangeResult result = wait(tr.getRange(blogKeyServersRange, CLIENT_KNOBS->TOO_MANY));
			keyServersResult = result;

			// Process each keyServers entry
			for (idx = 0; idx < keyServersResult.size(); idx++) {
				state Key rangeBegin = keyServersResult[idx].key.removePrefix(keyServersPrefix);
				state std::vector<UID> src = decodeKeyServersSrc(keyServersResult[idx].value);

				// Check if any source server has serverKeys for this range
				state bool hasServerKeys = false;
				state int serverIdx = 0;
				for (serverIdx = 0; serverIdx < src.size(); serverIdx++) {
					Key prefix = serverKeysPrefixFor(src[serverIdx]);
					Key serverKeyBegin = prefix.withSuffix(rangeBegin);

					Optional<Value> serverKeyValue = wait(tr.get(serverKeyBegin));
					if (serverKeyValue.present() && serverKeyValue.get() != serverKeysFalse) {
						hasServerKeys = true;
						break;
					}
				}

				if (!hasServerKeys && !src.empty()) {
					phantomCount++;
				}
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return phantomCount;
}

} // anonymous namespace

struct BlogPhantomShardReproWorkload : TestWorkload {
	static constexpr auto NAME = "BlogPhantomShardRepro";

	// Configuration
	double testDuration;
	double backupStartDelay;
	double mutationPhaseDelay;
	double killServerDelay;
	int mutationCount;
	int mutationBatchSize;
	bool injectDDCrash;
	double ddCrashDelay;
	Key backupTag;

	// State
	FileBackupAgent backupAgent;
	bool backupStarted = false;
	bool serverKilled = false;

	BlogPhantomShardReproWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		backupStartDelay = getOption(options, "backupStartDelay"_sr, 5.0);
		mutationPhaseDelay = getOption(options, "mutationPhaseDelay"_sr, 30.0);
		killServerDelay = getOption(options, "killServerDelay"_sr, 60.0);
		mutationCount = getOption(options, "mutationCount"_sr, 100000);
		mutationBatchSize = getOption(options, "mutationBatchSize"_sr, 100);
		injectDDCrash = getOption(options, "injectDDCrash"_sr, true);
		ddCrashDelay = getOption(options, "ddCrashDelay"_sr, 0.5);
		backupTag = getOption(options, "backupTag"_sr, "default"_sr);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	ACTOR static Future<Void> _start(BlogPhantomShardReproWorkload* self, Database cx) {
		// If mutationCount is 0, we're in "check only" mode - skip backup/mutations
		// and rely on other workloads (e.g., BackupToDBCorrectness) to create blog data
		if (self->mutationCount > 0) {
			// Step 1: Start backup
			TraceEvent("BlogPhantomRepro_StartingBackup");
			wait(delay(self->backupStartDelay));

			state std::string backupContainer = "file://simfdb/backups/blog_phantom_test";

			try {
				wait(self->backupAgent.submitBackup(cx,
				                                    StringRef(backupContainer),
				                                    {},
				                                    0,
				                                    0,
				                                    self->backupTag.toString(),
				                                    Standalone<VectorRef<KeyRangeRef>>(),
				                                    false, // encryptionEnabled
				                                    StopWhenDone::False,
				                                    UsePartitionedLog::False,
				                                    IncrementalBackupOnly::False));
				self->backupStarted = true;
				TraceEvent("BlogPhantomRepro_BackupStarted");
			} catch (Error& e) {
				if (e.code() != error_code_backup_duplicate) {
					throw;
				}
				TraceEvent("BlogPhantomRepro_BackupAlreadyExists");
				self->backupStarted = true;
			}

			// Step 2: Generate lots of mutations to create blog entries
			TraceEvent("BlogPhantomRepro_StartingMutations").detail("Count", self->mutationCount);
			wait(generateMutations(self, cx));

			// Step 3: Wait for blog data to be created
			TraceEvent("BlogPhantomRepro_WaitingForBlogData");
			wait(delay(self->mutationPhaseDelay));
		} else {
			TraceEvent("BlogPhantomRepro_CheckOnlyMode");
			// Wait for testDuration to let other workloads create blog data
			wait(delay(self->testDuration));
		}

		// Step 4: Check blog keyServers count
		int64_t blogKeyServersCount = wait(countBlogKeyServers(cx));
		TraceEvent("BlogPhantomRepro_BlogKeyServersCount").detail("Count", blogKeyServersCount);

		if (blogKeyServersCount == 0) {
			TraceEvent(SevWarn, "BlogPhantomRepro_NoBlogKeyServers");
			// Continue anyway, the test might still be useful
		}

		// Step 5: Find servers that own blog ranges
		state std::set<UID> blogOwners = wait(findBlogRangeOwners(cx));
		TraceEvent("BlogPhantomRepro_BlogOwners").detail("Count", blogOwners.size());

		if (blogOwners.empty()) {
			TraceEvent(SevWarn, "BlogPhantomRepro_NoBlogOwners");
			// In check-only mode, continue to check for phantom shards anyway
			// Other workloads may have created and then cleared blog ranges
		}

		// Step 6: Mark a storage server as failed to trigger removeKeysFromFailedServer
		// This is the key step that triggers the code path where the bug can occur
		{
			state Optional<NetworkAddress> serverAddr = wait(getStorageServerAddress(cx));
			if (serverAddr.present()) {
				TraceEvent("BlogPhantomRepro_MarkingServerFailed").detail("Address", serverAddr.get());

				// Mark the server as failed - this triggers removeKeysFromFailedServer
				std::vector<AddressExclusion> servers;
				servers.push_back(AddressExclusion(serverAddr.get().ip, serverAddr.get().port));
				wait(excludeServers(cx, servers, true /* failed */));

				self->serverKilled = true;
				TraceEvent("BlogPhantomRepro_ServerMarkedFailed").detail("Address", serverAddr.get());

				// Wait for DD to process the failure and call removeKeysFromFailedServer
				// The BUGGIFY in removeKeysFromFailedServer should trigger during this time
				TraceEvent("BlogPhantomRepro_WaitingForRemoveKeys");
				wait(delay(30.0));
			} else {
				TraceEvent(SevWarn, "BlogPhantomRepro_NoServerToFail");
			}
		}

		// Step 7: Wait for cluster to stabilize
		TraceEvent("BlogPhantomRepro_WaitingForRecovery");
		wait(delay(30.0));

		// Step 8: Check for phantom shards
		int64_t phantomCount = wait(countPhantomBlogShards(cx));
		TraceEvent("BlogPhantomRepro_PhantomShardCount").detail("Count", phantomCount);

		if (phantomCount > 0) {
			TraceEvent(SevError, "BlogPhantomRepro_FoundPhantomShards").detail("Count", phantomCount);
		} else {
			TraceEvent("BlogPhantomRepro_NoPhantomShardsFound");
		}

		return Void();
	}

	ACTOR static Future<Void> generateMutations(BlogPhantomShardReproWorkload* self, Database cx) {
		state int completed = 0;

		while (completed < self->mutationCount) {
			state Transaction tr(cx);
			try {
				for (int i = 0; i < self->mutationBatchSize && completed < self->mutationCount; i++, completed++) {
					Key key = StringRef(format("test/key/%08d", completed));
					Value value = StringRef(
					    format("value_%08d_%s", completed, deterministicRandom()->randomAlphaNumeric(100).c_str()));
					tr.set(key, value);
				}
				wait(tr.commit());

				if (completed % 10000 == 0) {
					TraceEvent("BlogPhantomRepro_MutationProgress")
					    .detail("Completed", completed)
					    .detail("Total", self->mutationCount);
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("BlogPhantomRepro_MutationsComplete").detail("Total", completed);
		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(this, cx); }

	ACTOR static Future<bool> _check(BlogPhantomShardReproWorkload* self, Database cx) {
		// Final check for phantom shards
		state int64_t blogKeyServersCount = wait(countBlogKeyServers(cx));
		TraceEvent("BlogPhantomRepro_FinalBlogKeyServers").detail("Count", blogKeyServersCount);

		state int64_t phantomCount = wait(countPhantomBlogShards(cx));
		TraceEvent("BlogPhantomRepro_FinalPhantomCount").detail("Count", phantomCount);

		// Get all servers and check their serverKeys
		state std::vector<UID> servers = wait(getStorageServers(cx));
		state int serverIdx = 0;
		for (serverIdx = 0; serverIdx < servers.size(); serverIdx++) {
			state UID currentServerID = servers[serverIdx];
			int64_t serverKeysBlogCount = wait(countServerKeysBlog(cx, currentServerID));
			if (serverKeysBlogCount > 0) {
				TraceEvent("BlogPhantomRepro_ServerBlogKeys")
				    .detail("ServerID", currentServerID)
				    .detail("Count", serverKeysBlogCount);
			}
		}

		// The test "passes" if we either:
		// 1. Found phantom shards (reproduced the bug)
		// 2. Or didn't find phantom shards (bug might be fixed or conditions not met)
		// We trace either way for analysis
		if (phantomCount > 0) {
			TraceEvent(SevWarnAlways, "BlogPhantomRepro_BugReproduced").detail("PhantomCount", phantomCount);
		}

		return true; // Always pass - this is a repro test, not a correctness test
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(PerfMetric("BackupStarted", backupStarted ? 1 : 0, Averaged::False));
		m.push_back(PerfMetric("ServerKilled", serverKilled ? 1 : 0, Averaged::False));
	}
};

WorkloadFactory<BlogPhantomShardReproWorkload> BlogPhantomShardReproWorkloadFactory;
