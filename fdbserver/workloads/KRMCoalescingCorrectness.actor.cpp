/*
 * KRMCoalescingCorrectness.actor.cpp
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

// Tests for krmSetRangeCoalescing to verify coalescing behavior and document
// a known bug where a redundant entry is left at the \xff\xff boundary.
//
// BACKGROUND
// ==========
// krmSetRangeCoalescing (fdbclient/KeyRangeMap.actor.cpp) is used by data
// distribution to update serverKeys when shards move between storage servers.
// It sets a range of keys in a key-range-map to a value, and tries to coalesce
// with adjacent ranges that already have the same value, to avoid accumulating
// redundant boundary entries.
//
// The function reads 1 entry before the range start and 2 entries at/after the
// range end, then decides how to coalesce:
//
//   Case 1: The entry AFTER range.end has the same value AND is within maxRange.
//           Coalesce fully — extend to that entry's key.
//   Case 2: The entry AT range.end has the same value but there's no next entry
//           within maxRange (e.g. we're at \xff\xff). Coalesce up to maxRange.end.
//   Case 3: Values don't match. No coalescing — write range.end as-is.
//
// THE BUG
// =======
// Case 2 has a non-coalescing bug. When the range being set ends at \xff\xff
// (which is allKeys.end, the boundary of maxRange), the function writes BOTH:
//   - beginKey (e.g. prefix+"b") = value
//   - endKey (prefix+"\xff\xff") = existingValue
//
// When value == existingValue (both FALSE), this creates two adjacent entries
// with the same value — a redundant boundary that should have been coalesced.
//
// This was found on a production cluster with 573 uncoalesced serverKeys
// entries. 19 storage servers had 2+ uncoalesced pairs, all at the \xff\xff
// boundary. Subsequent coalescing operations CAN absorb other entries via
// Case 1 (which reads 2 entries ahead), but they always re-write the \xff\xff
// boundary entry, so it never gets cleaned up — it's a permanent residue of
// the Case 2 logic.
//
// TEST SCENARIOS
// ==============
//
// Test A — Sanity check: basic coalescing works.
//   Setup:    ""=F, "c"=T
//   Action:   krmSetRangeCoalescing(["a","b"), F)
//   Expected: 0 uncoalesced pairs. The range is already within the F region.
//
// Test B — Case 2 boundary bug (the core bug, reproduced here).
//   Setup:    ""=F, "a"=T, "\xff\xff"=F   (server owns shard [a,\xff\xff))
//   Action:   krmSetRangeCoalescing(["b","\xff\xff"), F)  (move [b,\xff\xff) away)
//   Expected: Result is ""=F, "a"=T, "b"=F, "\xff\xff"=F
//             "b"=F and "\xff\xff"=F are adjacent with the same value —
//             1 uncoalesced pair. This is the bug.
//
// Test C — The \xff\xff residue survives further coalescing.
//   Setup:    ""=F, "a"=T, "b"=F, "\xff\xff"=F   (state left by Test B's bug)
//   Action:   krmSetRangeCoalescing(["a","b"), F)  (move [a,b) away)
//   Expected: Case 1 coalesces forward all the way to \xff\xff (it reads "b"=F
//             which matches, and the next entry \xff\xff is within maxRange).
//             Backward coalescing extends to "". This clears "", "a", "b" and
//             rewrites ""=F and "\xff\xff"=F.
//             Result: ""=F, "\xff\xff"=F — still 1 uncoalesced pair.
//             The intermediate entries got cleaned up, but the \xff\xff boundary
//             entry is re-written by the coalescing logic itself and can never
//             be removed. This is the same bug as Test B, not a new one.
//
// Test D — Multiple calls in one RYW transaction (sanity check).
//   Setup:    ""=F, "a"=T, "c"=T, "e"=F
//   Action:   In ONE RYW transaction:
//               krmSetRangeCoalescing(["a","b"), F)
//               krmSetRangeCoalescing(["c","d"), F)
//   Expected: Despite krmSetRangeCoalescing using Snapshot::True internally,
//             ReadYourWritesTransaction layers RYW caching on top, so the
//             second call sees the first call's writes. Both calls coalesce
//             correctly. Result: ""=F, "b"=T, "c"=F, "d"=T, "e"=F — 0
//             uncoalesced pairs. (The snapshot isolation problem would only
//             manifest with raw Transaction*, not RYW.)

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const Value FALSE_VAL = "0"_sr;
const Value TRUE_VAL = "1"_sr;

// Count adjacent entry pairs with identical values (i.e. entries that should have been coalesced).
ACTOR Future<int> countUncoalesced(Reference<ReadYourWritesTransaction> tr, Key prefix) {
	state RangeResult entries =
	    wait(tr->getRange(prefixRange(prefix), CLIENT_KNOBS->TOO_MANY));
	ASSERT(!entries.more);

	int count = 0;
	for (int i = 0; i + 1 < entries.size(); i++) {
		if (entries[i].value == entries[i + 1].value) {
			count++;
		}
	}
	return count;
}

// Read all KRM entries under a prefix.
ACTOR Future<RangeResult> readAllEntries(Reference<ReadYourWritesTransaction> tr, Key prefix) {
	RangeResult entries = wait(tr->getRange(prefixRange(prefix), CLIENT_KNOBS->TOO_MANY));
	ASSERT(!entries.more);
	return entries;
}

// Write raw KV pairs to establish preconditions for a test.
void setupEntries(Reference<ReadYourWritesTransaction> tr,
                  Key prefix,
                  std::vector<std::pair<StringRef, Value>> const& entries) {
	// Clear the entire prefix range first
	tr->clear(prefixRange(prefix));
	for (auto const& [suffix, value] : entries) {
		tr->set(prefix.withSuffix(suffix), value);
	}
}

} // anonymous namespace

struct KRMCoalescingCorrectness : TestWorkload {
	static constexpr auto NAME = "KRMCoalescingCorrectness";

	Key testPrefix;
	bool ok;

	KRMCoalescingCorrectness(WorkloadContext const& wcx) : TestWorkload(wcx), ok(true) {
		// Use a user-space prefix with a unique ID to avoid conflicts
		auto uid = deterministicRandom()->randomUniqueID();
		testPrefix = StringRef(format("\x02krmtest_%s/", uid.toString().c_str()));
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}
	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		return cleanup(cx, this);
	}
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Test A: Basic coalescing works when values match surrounding region
	ACTOR static Future<Void> testBasicCoalescing(Database cx, KRMCoalescingCorrectness* self) {
		state Key prefix = self->testPrefix.withSuffix("a/"_sr);

		// Step 1: Set up initial state
		{
			state Reference<ReadYourWritesTransaction> setupTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					setupEntries(setupTr, prefix, { { ""_sr, FALSE_VAL }, { "c"_sr, TRUE_VAL } });
					wait(setupTr->commit());
					break;
				} catch (Error& e) {
					wait(setupTr->onError(e));
				}
			}
		}

		// Step 2: Call krmSetRangeCoalescing to set [a, b) = FALSE_VAL
		// Since the region containing [a, b) already has value FALSE_VAL (from entry at ""),
		// this should coalesce and not add new entries.
		{
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					wait(krmSetRangeCoalescing(
					    tr, prefix, KeyRangeRef("a"_sr, "b"_sr), allKeys, FALSE_VAL));
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}

		// Step 3: Verify
		{
			state Reference<ReadYourWritesTransaction> verifyTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					state int uncoalesced = wait(countUncoalesced(verifyTr, prefix));
					state RangeResult entries = wait(readAllEntries(verifyTr, prefix));

					TraceEvent("KRMTestA_Result")
					    .detail("Uncoalesced", uncoalesced)
					    .detail("EntryCount", entries.size());
					for (int i = 0; i < entries.size(); i++) {
						TraceEvent("KRMTestA_Entry")
						    .detail("Index", i)
						    .detail("Key", entries[i].key.printable())
						    .detail("Value", entries[i].value.printable());
					}

					if (uncoalesced != 0) {
						TraceEvent(SevError, "KRMTestA_Failure")
						    .detail("Reason", "Basic coalescing left uncoalesced entries")
						    .detail("Uncoalesced", uncoalesced);
						self->ok = false;
					}
					break;
				} catch (Error& e) {
					wait(verifyTr->onError(e));
				}
			}
		}

		return Void();
	}

	// Test B: Case 2 boundary bug — \xff\xff boundary leaves redundant entry
	ACTOR static Future<Void> testCase2BoundaryBug(Database cx, KRMCoalescingCorrectness* self) {
		state Key prefix = self->testPrefix.withSuffix("b/"_sr);

		// Step 1: Set up initial state
		// Server owns shard [a, \xff\xff), rest is FALSE
		{
			state Reference<ReadYourWritesTransaction> setupTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					setupEntries(setupTr,
					             prefix,
					             { { ""_sr, FALSE_VAL },
					               { "a"_sr, TRUE_VAL },
					               { "\xff\xff"_sr, FALSE_VAL } });
					wait(setupTr->commit());
					break;
				} catch (Error& e) {
					wait(setupTr->onError(e));
				}
			}
		}

		// Step 2: Move shard [b, \xff\xff) away — set to FALSE
		{
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					wait(krmSetRangeCoalescing(
					    tr, prefix, KeyRangeRef("b"_sr, "\xff\xff"_sr), allKeys, FALSE_VAL));
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}

		// Step 3: Verify — expect 1 uncoalesced pair (b=FALSE, \xff\xff=FALSE)
		{
			state Reference<ReadYourWritesTransaction> verifyTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					state int uncoalesced = wait(countUncoalesced(verifyTr, prefix));
					state RangeResult entries = wait(readAllEntries(verifyTr, prefix));

					TraceEvent("KRMTestB_Result")
					    .detail("Uncoalesced", uncoalesced)
					    .detail("EntryCount", entries.size());
					for (int i = 0; i < entries.size(); i++) {
						TraceEvent("KRMTestB_Entry")
						    .detail("Index", i)
						    .detail("Key", entries[i].key.printable())
						    .detail("Value", entries[i].value.printable());
					}

					// The bug: Case 2 writes endKey=maxWithPrefix.end (\xff\xff with prefix)
					// and endValue=existingValue (FALSE_VAL). But it also writes
					// beginKey=withPrefix.begin ("b" with prefix) with value FALSE_VAL.
					// So we get adjacent entries b=FALSE and \xff\xff=FALSE.
					if (uncoalesced != 1) {
						TraceEvent(SevError, "KRMTestB_Failure")
						    .detail("Reason", "Expected exactly 1 uncoalesced pair at \\xff\\xff boundary")
						    .detail("Uncoalesced", uncoalesced);
						self->ok = false;
					}

					// Verify the \xff\xff entry exists with FALSE value
					bool foundBoundary = false;
					for (int i = 0; i < entries.size(); i++) {
						Key expectedKey = prefix.withSuffix("\xff\xff"_sr);
						if (entries[i].key == expectedKey) {
							foundBoundary = true;
							if (entries[i].value != FALSE_VAL) {
								TraceEvent(SevError, "KRMTestB_WrongBoundaryValue")
								    .detail("Expected", FALSE_VAL.printable())
								    .detail("Actual", entries[i].value.printable());
								self->ok = false;
							}
						}
					}
					if (!foundBoundary) {
						TraceEvent(SevError, "KRMTestB_MissingBoundary")
						    .detail("Reason", "\\xff\\xff boundary entry not found");
						self->ok = false;
					}
					break;
				} catch (Error& e) {
					wait(verifyTr->onError(e));
				}
			}
		}

		return Void();
	}

	// Test C: Chain scenario — backward coalescing absorbs previous entries but
	// the \xff\xff boundary entry remains, leaving 1 uncoalesced pair.
	ACTOR static Future<Void> testChainScenario(Database cx, KRMCoalescingCorrectness* self) {
		state Key prefix = self->testPrefix.withSuffix("c/"_sr);

		// Step 1: Set up state as if Case 2 already happened once
		// This is the result after Test B: "", a=TRUE, b=FALSE, \xff\xff=FALSE
		{
			state Reference<ReadYourWritesTransaction> setupTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					setupEntries(setupTr,
					             prefix,
					             { { ""_sr, FALSE_VAL },
					               { "a"_sr, TRUE_VAL },
					               { "b"_sr, FALSE_VAL },
					               { "\xff\xff"_sr, FALSE_VAL } });
					wait(setupTr->commit());
					break;
				} catch (Error& e) {
					wait(setupTr->onError(e));
				}
			}
		}

		// Step 2: Move shard [a, b) away — set to FALSE
		// Backward coalescing: entry before "a" is ""=FALSE, same value, so beginKey
		// extends back to "". Forward coalescing (Case 1): entry at "b" is FALSE, same
		// value, so endKey extends to "b". The clear(beginKey, endKey) removes "a" and
		// the set(beginKey=prefix+"", FALSE) reaffirms the start. set(endKey=prefix+"b", FALSE)
		// writes "b"=FALSE. But \xff\xff=FALSE is still there from the earlier Case 2 bug.
		// Result: ""=FALSE, "b"=FALSE, \xff\xff=FALSE? No — actually the clear removes "a"
		// and the forward coalesce to "b" means endKey="b", endValue=FALSE (from entry at "b").
		// Since value==endValue, we hit the ASSERT at line 297 which requires endKey==maxWithPrefix.end.
		// Actually Case 1 fires: hasNext=true (entry at \xff\xff), endRange.end()[-1].key is \xff\xff
		// which is <= maxWithPrefix.end (allKeys.end with prefix). So endKey=\xff\xff, endValue=FALSE.
		// Wait — re-reading: the getRange reads lastLessOrEqual(withPrefix.end="b") to
		// firstGreaterThan(withPrefix.end="b")+1, limit 2. That returns entries at "b" and "\xff\xff".
		// hasEnd=true (entry "b" <= withPrefix.end), hasNext=true (\xff\xff exists).
		// existingValue = entry at "b" = FALSE. valueMatches = (FALSE == FALSE) = true.
		// Case 1: hasNext && \xff\xff <= maxWithPrefix.end && valueMatches → endKey=\xff\xff.
		// So clear("", \xff\xff) removes "", "a", "b". set("", FALSE), set(\xff\xff, FALSE).
		// Result: ""=FALSE, \xff\xff=FALSE — 1 uncoalesced pair.
		{
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					wait(krmSetRangeCoalescing(
					    tr, prefix, KeyRangeRef("a"_sr, "b"_sr), allKeys, FALSE_VAL));
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}

		// Step 3: Verify — expect 1 uncoalesced pair
		// Result: ""=FALSE, "\xff\xff"=FALSE
		// The backward coalescing extended to "" and forward Case 1 extended to \xff\xff,
		// but the \xff\xff entry persists as the endValue write, creating 1 pair.
		{
			state Reference<ReadYourWritesTransaction> verifyTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					state int uncoalesced = wait(countUncoalesced(verifyTr, prefix));
					state RangeResult entries = wait(readAllEntries(verifyTr, prefix));

					TraceEvent("KRMTestC_Result")
					    .detail("Uncoalesced", uncoalesced)
					    .detail("EntryCount", entries.size());
					for (int i = 0; i < entries.size(); i++) {
						TraceEvent("KRMTestC_Entry")
						    .detail("Index", i)
						    .detail("Key", entries[i].key.printable())
						    .detail("Value", entries[i].value.printable());
					}

					if (uncoalesced != 1) {
						TraceEvent(SevError, "KRMTestC_Failure")
						    .detail("Reason", "Expected exactly 1 uncoalesced pair in chain scenario")
						    .detail("Uncoalesced", uncoalesced);
						self->ok = false;
					}
					break;
				} catch (Error& e) {
					wait(verifyTr->onError(e));
				}
			}
		}

		return Void();
	}

	// Test D: Multiple krmSetRangeCoalescing calls in one RYW transaction.
	// Despite using Snapshot::True for reads internally, ReadYourWritesTransaction
	// layers RYW caching on top, so the second call DOES see the first call's writes.
	// This means two calls in one RYW transaction coalesce correctly — no leftover
	// entries. This test verifies that RYW prevents the snapshot isolation problem.
	// (The snapshot isolation issue would only manifest with raw Transaction*, not RYW.)
	ACTOR static Future<Void> testMultiCallRYW(Database cx, KRMCoalescingCorrectness* self) {
		state Key prefix = self->testPrefix.withSuffix("d/"_sr);

		// Step 1: Set up initial state
		// ""=FALSE, a=TRUE, c=TRUE, e=FALSE
		{
			state Reference<ReadYourWritesTransaction> setupTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					setupEntries(setupTr,
					             prefix,
					             { { ""_sr, FALSE_VAL },
					               { "a"_sr, TRUE_VAL },
					               { "c"_sr, TRUE_VAL },
					               { "e"_sr, FALSE_VAL } });
					wait(setupTr->commit());
					break;
				} catch (Error& e) {
					wait(setupTr->onError(e));
				}
			}
		}

		// Step 2: In ONE RYW transaction, make two krmSetRangeCoalescing calls.
		// RYW sees prior writes, so the second call sees the first's mutations
		// and coalesces properly.
		{
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					wait(krmSetRangeCoalescing(
					    tr, prefix, KeyRangeRef("a"_sr, "b"_sr), allKeys, FALSE_VAL));
					wait(krmSetRangeCoalescing(
					    tr, prefix, KeyRangeRef("c"_sr, "d"_sr), allKeys, FALSE_VAL));
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}

		// Step 3: Verify — RYW means both calls see each other, so coalescing works.
		// Result: ""=F, "b"=T, "c"=F, "d"=T, "e"=F — 0 uncoalesced pairs.
		// Each adjacent pair has different values, so no redundant entries.
		{
			state Reference<ReadYourWritesTransaction> verifyTr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					state int uncoalesced = wait(countUncoalesced(verifyTr, prefix));
					state RangeResult entries = wait(readAllEntries(verifyTr, prefix));

					TraceEvent("KRMTestD_Result")
					    .detail("Uncoalesced", uncoalesced)
					    .detail("EntryCount", entries.size());
					for (int i = 0; i < entries.size(); i++) {
						TraceEvent("KRMTestD_Entry")
						    .detail("Index", i)
						    .detail("Key", entries[i].key.printable())
						    .detail("Value", entries[i].value.printable());
					}

					if (uncoalesced != 0) {
						TraceEvent(SevError, "KRMTestD_Failure")
						    .detail("Reason",
						            "Expected 0 uncoalesced entries with RYW transaction")
						    .detail("Uncoalesced", uncoalesced);
						self->ok = false;
					}
					break;
				} catch (Error& e) {
					wait(verifyTr->onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, KRMCoalescingCorrectness* self) {
		TraceEvent("KRMCoalescingCorrectnessStart");

		wait(self->testBasicCoalescing(cx, self));
		TraceEvent("KRMTestA_Done");

		wait(self->testCase2BoundaryBug(cx, self));
		TraceEvent("KRMTestB_Done");

		wait(self->testChainScenario(cx, self));
		TraceEvent("KRMTestC_Done");

		wait(self->testMultiCallRYW(cx, self));
		TraceEvent("KRMTestD_Done");

		TraceEvent("KRMCoalescingCorrectnessDone").detail("OK", self->ok);
		return Void();
	}

	ACTOR static Future<bool> cleanup(Database cx, KRMCoalescingCorrectness* self) {
		// Clean up all test data
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			try {
				tr->clear(prefixRange(self->testPrefix));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		return self->ok;
	}
};

WorkloadFactory<KRMCoalescingCorrectness> KRMCoalescingCorrectnessWorkloadFactory;
