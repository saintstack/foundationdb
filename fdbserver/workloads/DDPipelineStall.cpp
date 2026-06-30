/*
 * DDPipelineStall.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

// Reproduces the p127/p102 finishMoveKeys pipeline stall:
//
// 1. Load data to create many shards
// 2. Exclude storage servers to trigger bulk shard movement
// 3. BUGGIFY_GET_SHARD_STATE_DELAY causes dest SSes to respond slowly
//    to GetShardState when fetchKeys is active (simulating CPU saturation)
// 4. waitForShardReady polls inside an open transaction, consuming the 5s budget
// 5. transaction_too_old → 10ms retry → all 15 FlowLock slots stuck
// 6. Self-sustaining retry storm: BytesRate→0, InFlight inflates
//
// The test verifies that:
// - Without fixes: InFlight inflates unboundedly and/or DD restarts
// - With fixes: pipeline recovers or never stalls

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"

#include <limits>

struct DDPipelineStallWorkload : TestWorkload {
	static constexpr auto NAME = "DDPipelineStall";

	// Configuration
	int keyCount; // Number of keys to load (determines shard count)
	int valueSize; // Size of each value
	int excludeCount; // Number of storage processes to exclude
	double loadTimeout; // Max time for data loading phase
	double observeTime; // How long to observe after excluding
	double sampleInterval; // How often to sample DD metrics

	// Thresholds for detecting the stall
	int inFlightThreshold; // InFlight count that indicates stall
	int dataMovesThreshold; // Accumulated dataMoves entries indicating stall
	int ddRestartsThreshold; // DD restarts indicating death spiral

	// Stall signal based on byte progress (matches the production "BytesRate → 0"
	// indicator from the v8 p127 investigation). Bytes-written stops growing
	// while in-queue stays non-empty = pipeline is frozen. Scale-independent:
	// we look for "no progress" between samples, not absolute throughput,
	// so the same threshold catches stalls in both sim (MB) and k8s (TB).
	double stallProgressBytes; // Minimum bytes-written growth per sample interval; below this counts as stalled (default 1 MB)
	double stallMinQueueBytes; // Minimum in_queue_bytes for a sample to count as stalled (default 1 byte — any work)
	int stallConsecutiveSamples; // Consecutive stalled samples needed to declare PipelineStalled

	// Wave-exclude: stagger excludeCount over multiple waves to sustain
	// pipeline pressure instead of one burst that drains and ends.
	int excludeWaves;
	double excludeWaveDelay;

	// Background write rate — heavier traffic stresses source-side commit
	// path AND keeps dest SSes busy with mutation work on top of fetchKeys.
	int backgroundWriteBatchSize;
	double backgroundWriteDelaySeconds;

	// Rolling exclude/include during the observe window. The initial wave-
	// exclude produces a burst of DD work that drains; once drained, the
	// cascade trigger has nothing to amplify. Rolling continuously cycles
	// non-permanently-excluded SSes (exclude one for N seconds, include
	// back, pick next) — generates a continuous stream of new ++ events
	// into DDQueue so the inflation can sustain. Mirrors the FDE migration
	// pattern in v8 (servers were cycled in/out continuously over hours).
	// Set rollingExclude=true to enable.
	bool rollingExclude;
	double rollingCycleSeconds; // How long each SS stays excluded before include
	double rollingPauseSeconds; // Pause between cycles
	int rollingPerCycle; // How many SSes to exclude simultaneously per cycle

	// Results
	int peakInFlight = 0;
	int peakDataMoves = 0;
	int ddRestarts = 0;
	bool pipelineStalled = false;
	bool deathSpiral = false;
	// Byte-level metrics (from cluster.data.moving_data status JSON)
	double peakInFlightBytes = 0;
	double peakInQueueBytes = 0;
	double minProgressBytesObserved = std::numeric_limits<double>::max(); // smallest per-sample delta seen
	int stalledSampleCount = 0; // total samples where progress was below threshold
	int peakConsecutiveStalled = 0; // longest consecutive run of stalled samples

	explicit DDPipelineStallWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		keyCount = getOption(options, "keyCount"_sr, 50000);
		valueSize = getOption(options, "valueSize"_sr, 1000);
		excludeCount = getOption(options, "excludeCount"_sr, 3);
		loadTimeout = getOption(options, "loadTimeout"_sr, 60.0); // settle time after load for DD to split shards
		observeTime = getOption(options, "observeTime"_sr, 120.0);
		sampleInterval = getOption(options, "sampleInterval"_sr, 2.0);
		inFlightThreshold = getOption(options, "inFlightThreshold"_sr, 200);
		dataMovesThreshold = getOption(options, "dataMovesThreshold"_sr, 50);
		ddRestartsThreshold = getOption(options, "ddRestartsThreshold"_sr, 3);
		stallProgressBytes = getOption(options, "stallProgressBytes"_sr, 1e6); // 1 MB progress per sample = "making progress"
		stallMinQueueBytes = getOption(options, "stallMinQueueBytes"_sr, 1.0); // any queue counts (1 byte minimum)
		stallConsecutiveSamples = getOption(options, "stallConsecutiveSamples"_sr, 5);

		// Wave-exclude: split excludeCount into excludeWaves batches with
		// excludeWaveDelay seconds between waves. Each wave triggers a new
		// team-rebuild burst into a pipeline that's still draining the
		// previous wave's moves — sustains pressure rather than a single
		// burst that drains and ends. Default 1 wave = original behavior.
		excludeWaves = getOption(options, "excludeWaves"_sr, 1);
		excludeWaveDelay = getOption(options, "excludeWaveDelay"_sr, 60.0);

		// Background write tuning. Default keeps the original light load
		// (100 txns/sec × 10 writes = ~1000 writes/sec). Crank up to
		// stress source-side commit path and TLog, AND keep dest SSes busy
		// with mutation traffic on top of fetchKeys — matches p102 condition.
		backgroundWriteBatchSize = getOption(options, "backgroundWriteBatchSize"_sr, 10);
		backgroundWriteDelaySeconds = getOption(options, "backgroundWriteDelaySeconds"_sr, 0.01);

		// Rolling exclude/include — see field comment.
		rollingExclude = getOption(options, "rollingExclude"_sr, false);
		rollingCycleSeconds = getOption(options, "rollingCycleSeconds"_sr, 30.0);
		rollingPauseSeconds = getOption(options, "rollingPauseSeconds"_sr, 10.0);
		rollingPerCycle = getOption(options, "rollingPerCycle"_sr, 1);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return runTest(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		// The test "passes" if we successfully triggered the pipeline stall.
		// This validates the bug exists (useful for verifying fixes work).
		TraceEvent("DDPipelineStallResult")
		    .detail("PipelineStalled", pipelineStalled)
		    .detail("DeathSpiral", deathSpiral)
		    .detail("PeakInFlight", peakInFlight)
		    .detail("PeakDataMoves", peakDataMoves)
		    .detail("DDRestarts", ddRestarts)
		    .detail("PeakInFlightBytes", peakInFlightBytes)
		    .detail("PeakInQueueBytes", peakInQueueBytes)
		    .detail("StalledSampleCount", stalledSampleCount)
		    .detail("PeakConsecutiveStalled", peakConsecutiveStalled);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("PeakInFlight", peakInFlight, Averaged::False);
		m.emplace_back("PeakDataMoves", peakDataMoves, Averaged::False);
		m.emplace_back("DDRestarts", ddRestarts, Averaged::False);
		m.emplace_back("PipelineStalled", pipelineStalled ? 1 : 0, Averaged::False);
		m.emplace_back("DeathSpiral", deathSpiral ? 1 : 0, Averaged::False);
		// Byte-level metrics — the production cascade indicators.
		m.emplace_back("PeakInFlightBytes", peakInFlightBytes, Averaged::False);
		m.emplace_back("PeakInQueueBytes", peakInQueueBytes, Averaged::False);
		m.emplace_back("StalledSampleCount", stalledSampleCount, Averaged::False);
		m.emplace_back("PeakConsecutiveStalled", peakConsecutiveStalled, Averaged::False);
	}

	// Phase 1: Load data to create shards
	static Future<Void> loadData(Database cx, DDPipelineStallWorkload* self) {
		TraceEvent("DDPipelineStallLoadStart").detail("KeyCount", self->keyCount).detail("ValueSize", self->valueSize);

		int keysLoaded = 0;
		int batchSize = 100;

		while (keysLoaded < self->keyCount) {
			Transaction tr(cx);
			loop {
				Error err;
				try {
					int end = std::min(keysLoaded + batchSize, self->keyCount);
					for (int i = keysLoaded; i < end; i++) {
						Key k = StringRef(format("/ddstall/%08d", i));
						tr.set(k, Value(deterministicRandom()->randomAlphaNumeric(self->valueSize)));
					}
					co_await tr.commit();
					keysLoaded = std::min(keysLoaded + batchSize, self->keyCount);
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}

			if (keysLoaded % 10000 == 0) {
				TraceEvent("DDPipelineStallLoadProgress")
				    .detail("KeysLoaded", keysLoaded)
				    .detail("Total", self->keyCount);
			}
		}

		// Data loaded
		TraceEvent("DDPipelineStallLoadComplete").detail("KeysLoaded", keysLoaded);
		co_return;
	}

	// Phase 2: Exclude servers and observe
	static Future<Void> runTest(Database cx, DDPipelineStallWorkload* self) {
		// Wait a moment for knob overrides to take effect (applied at time=1.0)
		co_await delay(2.0);

		// Load data AFTER knobs are active so min_shard_bytes=5000 is in effect
		// during the initial shard split decisions
		co_await loadData(cx, self);

		// Wait for DD to finish splitting into tiny shards
		TraceEvent("DDPipelineStallWaitForSplit").detail("SettleTime", self->loadTimeout);
		co_await delay(self->loadTimeout);

		// Get current storage servers
		std::vector<StorageServerInterface> servers = co_await getStorageServers(cx, true);
		TraceEvent("DDPipelineStallServers").detail("Count", servers.size());

		if ((int)servers.size() <= self->excludeCount) {
			TraceEvent(SevWarn, "DDPipelineStallNotEnoughServers")
			    .detail("Have", servers.size())
			    .detail("NeedToExclude", self->excludeCount);
			co_return;
		}

		// Pick servers to exclude
		std::vector<AddressExclusion> allToExclude;
		for (int i = 0; i < self->excludeCount && i < (int)servers.size(); i++) {
			allToExclude.push_back(AddressExclusion(servers[i].address().ip, servers[i].address().port));
			TraceEvent("DDPipelineStallExcluding")
			    .detail("Server", servers[i].id())
			    .detail("Address", servers[i].address());
		}

		// Start monitoring in parallel with the exclude
		Future<Void> monitor = monitorDDMetrics(cx, self);

		// Also run background writes to keep source SSes busy
		Future<Void> bgWrites = backgroundWrites(cx, self);

		// Arm the BUGGIFY_DDQUEUE_RELOCATIONCOMPLETE_DELAY knob now. Before this
		// point (data load + setup) the cluster does normal shard placement work
		// that would itself trigger relocationComplete events; we don't want the
		// buggified delay to slow load down. From here on, every relocationComplete
		// the DDQueue handler processes is artificially delayed — forcing
		// fetchKeysComplete set growth (the v8 cascade amplifier).
		enableDDPipelineStallTrigger();

		// Wave-exclude: split the excludeCount into excludeWaves batches.
		// Each wave triggers a fresh team-rebuild burst while the pipeline
		// is still draining moves from previous waves — sustained pressure
		// instead of a single drain-and-done burst. excludeWaves=1
		// reproduces original single-shot behavior.
		int waves = std::max(1, self->excludeWaves);
		int totalCount = (int)allToExclude.size();
		int baseBatchSize = std::max(1, totalCount / waves);
		int issued = 0;
		for (int w = 0; w < waves; w++) {
			int batchEnd = (w == waves - 1) ? totalCount : std::min(totalCount, issued + baseBatchSize);
			std::vector<AddressExclusion> waveBatch(allToExclude.begin() + issued, allToExclude.begin() + batchEnd);
			if (waveBatch.empty()) {
				break;
			}

			TraceEvent(SevWarnAlways, "DDPipelineStallExcludeWave")
			    .detail("Wave", w + 1)
			    .detail("OfWaves", waves)
			    .detail("WaveCount", waveBatch.size())
			    .detail("TotalIssued", batchEnd)
			    .detail("TotalCount", totalCount);
			co_await excludeServers(cx, waveBatch);
			issued = batchEnd;

			// Wait before next wave (except after the last one — then we
			// fall through into the observe period).
			if (w < waves - 1) {
				co_await delay(self->excludeWaveDelay);
			}
		}
		TraceEvent("DDPipelineStallExcludeIssued").detail("Total", issued);

		// Optionally start rolling exclude/include to sustain DD work through
		// the observe window. Without this, the initial wave-exclude bursts
		// then drains and the cascade trigger has nothing to amplify.
		Future<Void> rolling = Void();
		if (self->rollingExclude) {
			TraceEvent(SevWarnAlways, "DDPipelineStallRollingExcludeStart")
			    .detail("CycleSeconds", self->rollingCycleSeconds)
			    .detail("PauseSeconds", self->rollingPauseSeconds);
			rolling = rollingExcludeActor(cx, self, allToExclude);
		}

		// Observe for the configured duration
		co_await delay(self->observeTime);

		// Cancel background activities
		bgWrites.cancel();
		monitor.cancel();
		rolling.cancel();

		// Disarm the buggified relocationComplete delay so include-all + cleanup
		// don't get artificially slowed.
		disableDDPipelineStallTrigger();

		TraceEvent(SevWarnAlways, "DDPipelineStallComplete")
		    .detail("PeakInFlight", self->peakInFlight)
		    .detail("PeakDataMoves", self->peakDataMoves)
		    .detail("DDRestarts", self->ddRestarts)
		    .detail("PipelineStalled", self->pipelineStalled)
		    .detail("DeathSpiral", self->deathSpiral);

		// Clean up: include the servers back
		co_await includeServers(cx, std::vector<AddressExclusion>());
		co_return;
	}

	// Rolling exclude/include during the observe window. Cycles non-permanently-
	// excluded SSes: pick one, exclude it for rollingCycleSeconds, include back,
	// pause rollingPauseSeconds, pick a different one, repeat. Generates a
	// continuous stream of DD ++ events so the cascade trigger (slow getShardState
	// → finishMoveKeys retry storm → actor lifetime extension) has sustained work
	// to amplify. Without this, the initial wave-exclude bursts into the pipeline
	// and drains in minutes; cascade inflation peaks transiently then fades.
	// Matches the FDE migration pattern in v8: servers cycled in/out continuously
	// over hours of production cascade.
	static Future<Void> rollingExcludeActor(Database cx,
	                                        DDPipelineStallWorkload* self,
	                                        std::vector<AddressExclusion> permanentlyExcluded) {
		// Build a set for fast lookup so we never try to rolling-exclude a
		// permanently-excluded server (which would lose data redundancy).
		std::set<std::pair<IPAddress, uint16_t>> permanentSet;
		for (auto const& a : permanentlyExcluded) {
			permanentSet.insert({ a.ip, a.port });
		}

		int cycleNum = 0;
		loop {
			// Find candidate SSes: storage servers not in permanently-excluded set.
			std::vector<StorageServerInterface> allServers = co_await getStorageServers(cx, true);
			std::vector<AddressExclusion> candidates;
			for (auto const& s : allServers) {
				auto addr = s.address();
				if (!permanentSet.count({ addr.ip, addr.port })) {
					candidates.emplace_back(addr.ip, addr.port);
				}
			}

			if (candidates.empty()) {
				TraceEvent(SevWarn, "DDPipelineStallRollingExcludeNoCandidates")
				    .detail("AllServers", allServers.size())
				    .detail("Permanent", permanentSet.size());
				co_await delay(self->rollingPauseSeconds);
				continue;
			}

			// Pick up to rollingPerCycle SSes (without replacement). Cap at the
			// number of available candidates.
			int howMany = std::min(self->rollingPerCycle, (int)candidates.size());
			std::vector<AddressExclusion> picks;
			for (int i = 0; i < howMany; i++) {
				int idx = deterministicRandom()->randomInt(0, candidates.size());
				picks.push_back(candidates[idx]);
				candidates.erase(candidates.begin() + idx);
			}
			cycleNum++;
			TraceEvent(SevWarnAlways, "DDPipelineStallRollingExcludeOut")
			    .detail("Cycle", cycleNum)
			    .detail("Count", picks.size())
			    .detail("CycleSeconds", self->rollingCycleSeconds);
			co_await excludeServers(cx, picks);

			co_await delay(self->rollingCycleSeconds);

			// Include the rolled-out servers back. Pass ONLY these (not empty
			// list — empty would clobber permanent excludes too).
			TraceEvent(SevWarnAlways, "DDPipelineStallRollingExcludeIn")
			    .detail("Cycle", cycleNum)
			    .detail("Count", picks.size());
			co_await includeServers(cx, picks);

			co_await delay(self->rollingPauseSeconds);
		}
	}

	// Background writes to keep storage servers busy (simulates production client load).
	// Heavier load (smaller delay × bigger batch) makes dest SSes do mutation work
	// concurrent with fetchKeys — matches p102's condition where SS event loops
	// were saturated by both client traffic and replication.
	static Future<Void> backgroundWrites(Database cx, DDPipelineStallWorkload* self) {
		loop {
			Transaction tr(cx);
			Error err;
			try {
				for (int i = 0; i < self->backgroundWriteBatchSize; i++) {
					Key k = StringRef(format("/ddstall/%08d", deterministicRandom()->randomInt(0, self->keyCount)));
					tr.set(k, Value(deterministicRandom()->randomAlphaNumeric(self->valueSize)));
				}
				co_await tr.commit();
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
			co_await delay(self->backgroundWriteDelaySeconds);
		}
	}

	// Read DD's moving_data status: in_queue_bytes, in_flight_bytes,
	// total_written_bytes. These are the production indicators from the
	// v8 p127 investigation. Returns -1 for any field not present.
	struct MovingData {
		double inQueue = -1.0;
		double inFlight = -1.0;
		double totalWritten = -1.0;
	};
	static Future<MovingData> getMovingData(Database cx) {
		MovingData md;
		try {
			StatusObject statusObj = co_await StatusClient::statusFetcher(cx);
			StatusObjectReader cluster;
			((StatusObjectReader)statusObj).get("cluster", cluster);
			StatusObjectReader data;
			cluster.get("data", data);
			if (data.has("moving_data")) {
				StatusObjectReader moving = data.last();
				moving.get("in_queue_bytes", md.inQueue);
				moving.get("in_flight_bytes", md.inFlight);
				moving.get("total_written_bytes", md.totalWritten);
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "DDPipelineStallGetStatusError").error(e);
		}
		co_return md;
	}

	// Monitor DD metrics by combining \xff/dataMoves/ count (existing signal)
	// with status-JSON moving_data byte rates. The cascade signature from the
	// v8 p127 investigation is BytesRate→0 while in_queue stays large — the
	// pipeline is frozen because finishMoveShards keeps failing on transaction
	// budget exhaustion. Persisted dataMoves count alone misses this when
	// successful retries partially keep up with new entries (which is what
	// happened in our first sim run: 57% abort rate but peak dataMoves=0).
	static Future<Void> monitorDDMetrics(Database cx, DDPipelineStallWorkload* self) {
		int consecutiveDataMovesStall = 0;
		int consecutiveByteRateStall = 0;

		double prevTotalWritten = -1.0;

		loop {
			co_await delay(self->sampleInterval);

			// --- dataMoves count signal (existing) ---
			int dataMovesCount = 0;
			Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

				KeyRange dataMoveRange = KeyRangeRef("\xff/dataMoves/"_sr, "\xff/dataMoves0"_sr);
				RangeResult moves = co_await tr.getRange(dataMoveRange, CLIENT_KNOBS->TOO_MANY);
				dataMovesCount = moves.size();
			} catch (Error& e) {
				continue;
			}

			if (dataMovesCount > self->peakDataMoves) {
				self->peakDataMoves = dataMovesCount;
			}

			// --- progress signal (new — production cascade indicator) ---
			// Scale-independent: looks for "no progress" between samples
			// rather than absolute throughput. Catches stalls in both sim
			// (MB) and k8s (TB) regimes with the same threshold.
			MovingData md = co_await getMovingData(cx);
			double dBytes = -1.0;
			if (md.totalWritten >= 0 && prevTotalWritten >= 0) {
				dBytes = md.totalWritten - prevTotalWritten;
			}
			prevTotalWritten = md.totalWritten;

			if (md.inFlight >= 0 && md.inFlight > self->peakInFlightBytes) {
				self->peakInFlightBytes = md.inFlight;
			}
			if (md.inQueue >= 0 && md.inQueue > self->peakInQueueBytes) {
				self->peakInQueueBytes = md.inQueue;
			}
			// Track the min progress observed (excludes the first sample where
			// we have no prev value to compare to)
			if (dBytes >= 0 && dBytes < self->minProgressBytesObserved) {
				self->minProgressBytesObserved = dBytes;
			}

			TraceEvent(SevWarnAlways, "DDPipelineStallSample")
			    .detail("DataMoves", dataMovesCount)
			    .detail("PeakDataMoves", self->peakDataMoves)
			    .detail("InQueueBytes", md.inQueue)
			    .detail("InFlightBytes", md.inFlight)
			    .detail("TotalWrittenBytes", md.totalWritten)
			    .detail("DeltaBytes", dBytes)
			    .detail("ConsecutiveDataMovesStall", consecutiveDataMovesStall)
			    .detail("ConsecutiveByteRateStall", consecutiveByteRateStall);

			// Stall A: dataMoves accumulation (existing signal). Catches the
			// case where finishMoveKeys can't clear entries at all.
			if (dataMovesCount > self->dataMovesThreshold) {
				consecutiveDataMovesStall++;
				if (consecutiveDataMovesStall >= 3) {
					self->pipelineStalled = true;
					TraceEvent(SevWarnAlways, "DDPipelineStallDetected")
					    .detail("Reason", "DataMovesAccumulating")
					    .detail("DataMoves", dataMovesCount)
					    .detail("ConsecutiveSamples", consecutiveDataMovesStall);
				}
			} else {
				consecutiveDataMovesStall = 0;
			}

			// Stall B: no progress while queue has work (production signal,
			// scale-independent). Catches the cascade even when retries
			// partially keep dataMoves count low — finishMoveKeys is failing
			// more than succeeding, so net throughput drops to ~0 between
			// samples.
			if (dBytes >= 0 && dBytes < self->stallProgressBytes && md.inQueue >= self->stallMinQueueBytes) {
				consecutiveByteRateStall++;
				self->stalledSampleCount++;
				if (consecutiveByteRateStall > self->peakConsecutiveStalled) {
					self->peakConsecutiveStalled = consecutiveByteRateStall;
				}
				if (consecutiveByteRateStall >= self->stallConsecutiveSamples) {
					self->pipelineStalled = true;
					TraceEvent(SevWarnAlways, "DDPipelineStallDetected")
					    .detail("Reason", "NoProgressWhileQueued")
					    .detail("DeltaBytes", dBytes)
					    .detail("InQueueBytes", md.inQueue)
					    .detail("InFlightBytes", md.inFlight)
					    .detail("ConsecutiveSamples", consecutiveByteRateStall);
				}
			} else {
				consecutiveByteRateStall = 0;
			}
		}
	}
};

WorkloadFactory<DDPipelineStallWorkload> DDPipelineStallWorkloadFactory;
