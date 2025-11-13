/*
 * RestoreValidation.actor.cpp
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

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreValidationWorkload : TestWorkload {
	static constexpr auto NAME = "RestoreValidation";

	double validateAfter;
	KeyRange validationRange;
	int expectedPhase; // Expected AuditPhase (2 = Complete)
	bool expectSuccess;
	double checkInterval;
	double maxWaitTime;

	RestoreValidationWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		validateAfter = getOption(options, "validateAfter"_sr, 50.0);
		validationRange = normalKeys;
		expectedPhase = getOption(options, "expectedPhase"_sr, (int)AuditPhase::Complete);
		expectSuccess = getOption(options, "expectSuccess"_sr, true);
		checkInterval = getOption(options, "checkInterval"_sr, 5.0);
		maxWaitTime = getOption(options, "maxWaitTime"_sr, 300.0);

		TraceEvent("RestoreValidationWorkloadInit")
		    .detail("ValidateAfter", validateAfter)
		    .detail("ExpectedPhase", expectedPhase)
		    .detail("ExpectSuccess", expectSuccess)
		    .detail("MaxWaitTime", maxWaitTime);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(this, cx);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> _start(RestoreValidationWorkload* self, Database cx) {
		// Wait for the specified time before starting validation
		TraceEvent("RestoreValidationWorkloadWaiting").detail("WaitTime", self->validateAfter);
		wait(delay(self->validateAfter));

		// Check if restored data exists at either prefix
		// This ensures the restore has completed before we trigger validation
		// Try both restoreLogKeys (production) and restoreLogKeysSimulation (tests)
		state KeyRange restoredDataRange = prefixRange(restoreLogKeys.begin);
		state KeyRange restoredDataRangeSimulation = prefixRange(restoreLogKeysSimulation.begin);
		state bool restoredDataExists = false;
		state int checkAttempts = 0;
		state int maxCheckAttempts = 60; // Check for up to 5 minutes (60 * 5 seconds)

		TraceEvent("RestoreValidationCheckingForRestoredData")
		    .detail("PrimaryPrefix", restoredDataRange)
		    .detail("SimulationPrefix", restoredDataRangeSimulation);

		loop {
			try {
				state Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				// Try primary prefix first
				RangeResult result = wait(tr.getRange(restoredDataRange, 1));
				if (result.size() > 0) {
					restoredDataExists = true;
					TraceEvent("RestoreValidationRestoredDataFound")
					    .detail("Prefix", "Primary")
					    .detail("Keys", result.size())
					    .detail("CheckAttempts", checkAttempts);
					break;
				}

				// Try simulation prefix
				RangeResult resultSim = wait(tr.getRange(restoredDataRangeSimulation, 1));
				if (resultSim.size() > 0) {
					restoredDataExists = true;
					TraceEvent("RestoreValidationRestoredDataFound")
					    .detail("Prefix", "Simulation")
					    .detail("Keys", resultSim.size())
					    .detail("CheckAttempts", checkAttempts);
					break;
				}

				checkAttempts++;
				if (checkAttempts >= maxCheckAttempts) {
					TraceEvent(SevError, "RestoreValidationNoRestoredDataFound")
					    .detail("CheckAttempts", checkAttempts)
					    .detail("MaxCheckAttempts", maxCheckAttempts);
					throw operation_failed();
				}
				TraceEvent("RestoreValidationWaitingForRestoredData")
				    .detail("CheckAttempts", checkAttempts)
				    .detail("MaxCheckAttempts", maxCheckAttempts);
				wait(delay(5.0));
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				throw;
			}
		}

		TraceEvent("RestoreValidationWorkloadStarting").detail("Range", self->validationRange);

		try {
			// Trigger the audit_storage validate_restore command
			state AuditType auditType = AuditType::ValidateRestore;
			state Reference<IClusterConnectionRecord> clusterFile = cx->getConnectionRecord();

			TraceEvent("RestoreValidationTriggeringAudit")
			    .detail("AuditType", (int)auditType)
			    .detail("Range", self->validationRange);

			// Trigger the audit using ManagementAPI
			state UID auditId =
			    wait(auditStorage(clusterFile, self->validationRange, auditType, KeyValueStoreType::END, 300.0));

			TraceEvent("RestoreValidationAuditScheduled").detail("AuditID", auditId);

			// Monitor audit progress
			state double startTime = now();
			state double lastReportTime = startTime;
			state bool completed = false;
			state AuditPhase finalPhase = AuditPhase::Invalid;
			state std::string errorMessage;

			loop {
				wait(delay(self->checkInterval));

				// Get audit status (newFirst=true to get latest states first)
				state std::vector<AuditStorageState> auditStates = wait(getAuditStates(cx, auditType, true));

				// Filter for our audit ID
				state bool foundOurAudit = false;
				state bool allComplete = true;
				state bool anyError = false;

				for (const auto& state : auditStates) {
					if (state.id == auditId) {
						foundOurAudit = true;

						if (state.getPhase() == AuditPhase::Running) {
							allComplete = false;
						} else if (state.getPhase() == AuditPhase::Error || state.getPhase() == AuditPhase::Failed) {
							anyError = true;
							finalPhase = state.getPhase();
							if (!state.error.empty()) {
								errorMessage = state.error;
							} else {
								errorMessage = "Unknown error";
							}
						} else if (state.getPhase() == AuditPhase::Complete) {
							finalPhase = AuditPhase::Complete;
						}
					}
				}

				if (!foundOurAudit) {
					TraceEvent(SevWarn, "RestoreValidationNoAuditStates")
					    .detail("AuditID", auditId)
					    .detail("ElapsedTime", now() - startTime);
				} else {
					// Report progress periodically
					if (now() - lastReportTime >= 10.0) {
						TraceEvent("RestoreValidationProgress")
						    .detail("AuditID", auditId)
						    .detail("AllComplete", allComplete)
						    .detail("AnyError", anyError)
						    .detail("FinalPhase", (int)finalPhase)
						    .detail("ElapsedTime", now() - startTime);
						lastReportTime = now();
					}

					if (allComplete || anyError) {
						completed = true;
						break;
					}
				}

				// Check timeout
				if (now() - startTime > self->maxWaitTime) {
					TraceEvent(SevError, "RestoreValidationTimeout")
					    .detail("AuditID", auditId)
					    .detail("ElapsedTime", now() - startTime)
					    .detail("MaxWaitTime", self->maxWaitTime);
					throw timed_out();
				}
			}

			// Verify the results
			TraceEvent("RestoreValidationComplete")
			    .detail("AuditID", auditId)
			    .detail("FinalPhase", (int)finalPhase)
			    .detail("ExpectedPhase", self->expectedPhase)
			    .detail("ErrorMessage", errorMessage)
			    .detail("ElapsedTime", now() - startTime);

			if (self->expectSuccess) {
				if (finalPhase != AuditPhase::Complete) {
					TraceEvent(SevError, "RestoreValidationUnexpectedPhase")
					    .detail("AuditID", auditId)
					    .detail("FinalPhase", (int)finalPhase)
					    .detail("ExpectedPhase", self->expectedPhase)
					    .detail("ErrorMessage", errorMessage);
					throw audit_storage_failed();
				}
				if (!errorMessage.empty()) {
					TraceEvent(SevError, "RestoreValidationUnexpectedError")
					    .detail("AuditID", auditId)
					    .detail("ErrorMessage", errorMessage);
					throw audit_storage_error();
				}
			} else {
				if (finalPhase == AuditPhase::Complete) {
					TraceEvent(SevError, "RestoreValidationUnexpectedSuccess")
					    .detail("AuditID", auditId)
					    .detail("ExpectedPhase", self->expectedPhase);
					throw audit_storage_task_outdated();
				}
			}

			TraceEvent("RestoreValidationSuccess").detail("AuditID", auditId);

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevError, "RestoreValidationError").errorUnsuppressed(e);
			throw;
		}

		return Void();
	}
};

WorkloadFactory<RestoreValidationWorkload> RestoreValidationWorkloadFactory;
