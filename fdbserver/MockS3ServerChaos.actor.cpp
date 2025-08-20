/*
 * MockS3ServerChaos.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/MockS3ServerChaos.h"
#include "fdbrpc/HTTP.h"
#include "fdbrpc/simulator.h"
#include "flow/Trace.h"
#include "flow/ActorCollection.h"
#include "flow/IRandom.h"
#include "flow/ChaosMetrics.h"
#include "fdbserver/MockS3Server.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// Constructor - wraps base MockS3Server handler
MockS3ServerChaos::MockS3ServerChaos(Reference<HTTP::IRequestHandler> handler) : baseHandler(handler) {
	// Only enable chaos in simulation mode (following AsyncFileChaos pattern)
	enabled = g_network->isSimulated();
}

// Clone method for creating new instances per connection
Reference<HTTP::IRequestHandler> MockS3ServerChaos::clone() {
	return makeReference<MockS3ServerChaos>(baseHandler->clone());
}

// Main request handling with chaos injection
ACTOR Future<Void> handleRequest_impl(MockS3ServerChaos* self,
                                      Reference<HTTP::IncomingRequest> req,
                                      Reference<HTTP::OutgoingResponse> response) {

	if (!self->enabled || !g_network->isSimulated()) {
		// No chaos - delegate to base handler
		wait(self->baseHandler->handleRequest(req, response));
		return Void();
	}

	// Check if we should inject chaos using fault injector pattern
	if (self->shouldInjectChaos()) {
		if (self->shouldInjectDelay()) {
			double delayTime = self->getDelay();
			wait(self->injectDelay(delayTime));
			self->updateChaosMetrics("delay"_sr);
		}

		if (self->shouldInjectError()) {
			MockS3ServerChaos::ErrorType errorType = self->selectErrorType();
			wait(self->injectError(req, response, errorType));
			self->updateChaosMetrics("error"_sr);
			return Void(); // Error responses don't delegate to base handler
		}
	}

	// No chaos injection - delegate to base handler
	wait(self->baseHandler->handleRequest(req, response));
	return Void();
}

Future<Void> MockS3ServerChaos::handleRequest(Reference<HTTP::IncomingRequest> req,
                                              Reference<HTTP::OutgoingResponse> response) {
	return handleRequest_impl(this, req, response);
}

// Operation classification for targeted chaos injection
MockS3ServerChaos::OperationType MockS3ServerChaos::classifyOperation(StringRef verb, StringRef resource) const {
	// Simple classification for now
	if (verb == "GET"_sr)
		return OperationType::GET_OBJECT;
	if (verb == "PUT"_sr)
		return OperationType::PUT_OBJECT;
	if (verb == "DELETE"_sr)
		return OperationType::DELETE_OBJECT;
	if (verb == "HEAD"_sr)
		return OperationType::HEAD_OBJECT;

	return OperationType::BUCKET_OPERATIONS;
}

// Chaos decision logic following AsyncFileChaos pattern - use fault injectors
bool MockS3ServerChaos::shouldInjectChaos() const {
	if (!enabled || !g_network->isSimulated()) {
		return false;
	}

	// Get DiskFailureInjector for error rates (following AsyncFileChaos pattern)
	auto res = g_network->global(INetwork::enDiskFailureInjector);
	if (res) {
		DiskFailureInjector* injector = static_cast<DiskFailureInjector*>(res);
		// Use disk delay rate as S3 chaos rate (0.0-1.0)
		double chaosRate = injector->getDiskDelay();
		if (chaosRate > 0.0) {
			return deterministicRandom()->random01() < chaosRate;
		}
	}

	// Occasional BUGGIFY for extra chaos (not as master switch)
	if (BUGGIFY) {
		return deterministicRandom()->random01() < 0.01; // 1% extra chaos
	}

	return false;
}

bool MockS3ServerChaos::shouldInjectError() const {
	if (!enabled || !g_network->isSimulated()) {
		return false;
	}

	// Use DiskFailureInjector error rate
	auto res = g_network->global(INetwork::enDiskFailureInjector);
	if (res) {
		DiskFailureInjector* injector = static_cast<DiskFailureInjector*>(res);
		double errorRate = injector->getDiskDelay() * 0.5; // 50% of delay rate for errors
		return deterministicRandom()->random01() < errorRate;
	}

	return BUGGIFY && deterministicRandom()->random01() < 0.005; // 0.5% BUGGIFY errors
}

bool MockS3ServerChaos::shouldInjectDelay() const {
	if (!enabled || !g_network->isSimulated()) {
		return false;
	}

	// Use DiskFailureInjector delay rate directly
	auto res = g_network->global(INetwork::enDiskFailureInjector);
	if (res) {
		DiskFailureInjector* injector = static_cast<DiskFailureInjector*>(res);
		return injector->getDiskDelay() > 0.0;
	}

	return false;
}

bool MockS3ServerChaos::shouldInjectCorruption() const {
	if (!enabled || !g_network->isSimulated()) {
		return false;
	}

	// Use BitFlipper pattern for corruption (following AsyncFileChaos)
	auto res = g_network->global(INetwork::enBitFlipper);
	if (res) {
		auto bitFlipPercentage = static_cast<BitFlipper*>(res)->getBitFlipPercentage();
		if (bitFlipPercentage > 0.0) {
			auto bitFlipProb = bitFlipPercentage / 100;
			return deterministicRandom()->random01() < bitFlipProb;
		}
	}

	return false;
}

// Calculate delay time (following AsyncFileChaos pattern)
double MockS3ServerChaos::getDelay() const {
	if (!enabled || !g_network->isSimulated()) {
		return 0.0;
	}

	auto res = g_network->global(INetwork::enDiskFailureInjector);
	if (res) {
		DiskFailureInjector* injector = static_cast<DiskFailureInjector*>(res);
		double baseDelay = injector->getDiskDelay();

		// Scale for S3 operations (typically longer than disk I/O)
		baseDelay *= (1.0 + deterministicRandom()->random01() * 4.0); // 1x to 5x scaling

		// Add jitter
		double jitter = 1.0 + (deterministicRandom()->random01() - 0.5) * 0.4; // ±20% jitter
		return baseDelay * jitter > 0.0 ? baseDelay * jitter : 0.0;
	}

	return 0.0;
}

// Error type selection based on deterministic randomness
MockS3ServerChaos::ErrorType MockS3ServerChaos::selectErrorType() const {
	double random = deterministicRandom()->random01();

	// Weight different error types
	if (random < 0.3) {
		return ErrorType::THROTTLE_429; // 30% throttling
	}
	if (random < 0.6) {
		return ErrorType::SERVICE_UNAVAILABLE_503; // 30% service unavailable
	}
	if (random < 0.8) {
		// Server errors
		double serverRandom = deterministicRandom()->random01();
		if (serverRandom < 0.7)
			return ErrorType::INTERNAL_ERROR_500;
		return ErrorType::BAD_GATEWAY_502;
	}
	if (random < 0.9) {
		// Auth errors
		return deterministicRandom()->random01() < 0.5 ? ErrorType::INVALID_TOKEN : ErrorType::EXPIRED_TOKEN;
	}

	// Connection issues
	return ErrorType::CONNECTION_DROP;
}

// Chaos injection actors
ACTOR Future<Void> injectDelay_impl(MockS3ServerChaos* self, double delayTime) {
	wait(delay(delayTime));
	return Void();
}

Future<Void> MockS3ServerChaos::injectDelay(double delayTime) {
	return injectDelay_impl(this, delayTime);
}

ACTOR Future<Void> injectError_impl(MockS3ServerChaos* self,
                                    Reference<HTTP::IncomingRequest> req,
                                    Reference<HTTP::OutgoingResponse> response,
                                    MockS3ServerChaos::ErrorType errorType) {

	switch (errorType) {
	case MockS3ServerChaos::ErrorType::THROTTLE_429:
		self->sendThrottleError(response, 60); // 60 second retry-after
		break;

	case MockS3ServerChaos::ErrorType::SERVICE_UNAVAILABLE_503:
		self->sendS3Error(response,
		                  HTTP::HTTP_STATUS_CODE_SERVICE_UNAVAILABLE,
		                  "ServiceUnavailable"_sr,
		                  "The service is temporarily unavailable"_sr,
		                  true);
		break;

	case MockS3ServerChaos::ErrorType::INTERNAL_ERROR_500:
		self->sendS3Error(response,
		                  HTTP::HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR,
		                  "InternalError"_sr,
		                  "Internal server error occurred"_sr);
		break;

	case MockS3ServerChaos::ErrorType::BAD_GATEWAY_502:
		self->sendS3Error(response, HTTP::HTTP_STATUS_CODE_BAD_GATEWAY, "BadGateway"_sr, "Bad gateway error"_sr);
		break;

	case MockS3ServerChaos::ErrorType::INVALID_TOKEN:
	case MockS3ServerChaos::ErrorType::EXPIRED_TOKEN:
		self->sendAuthError(response, errorType);
		break;

	case MockS3ServerChaos::ErrorType::CONNECTION_DROP:
		// Simulate connection drop by throwing connection_failed
		throw connection_failed();

	case MockS3ServerChaos::ErrorType::TIMEOUT:
		throw timed_out();

	default:
		self->sendS3Error(
		    response, HTTP::HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR, "ChaosError"_sr, "Chaos-injected error"_sr);
		break;
	}

	return Void();
}

Future<Void> MockS3ServerChaos::injectError(Reference<HTTP::IncomingRequest> req,
                                            Reference<HTTP::OutgoingResponse> response,
                                            ErrorType errorType) {
	return injectError_impl(this, req, response, errorType);
}

ACTOR Future<Void> injectDataCorruption_impl(MockS3ServerChaos* self, Reference<HTTP::OutgoingResponse> response) {
	// Simulate data corruption in response (similar to AsyncFileChaos bit flipping)
	// For now, just log the event - actual corruption would modify response content
	return Void();
}

Future<Void> MockS3ServerChaos::injectDataCorruption(Reference<HTTP::OutgoingResponse> response) {
	return injectDataCorruption_impl(this, response);
}

// S3 error response generation - minimal implementation for now
void MockS3ServerChaos::sendS3Error(Reference<HTTP::OutgoingResponse> response,
                                    int httpCode,
                                    StringRef s3ErrorCode,
                                    StringRef message,
                                    bool includeRetryAfter) {
	// Minimal implementation - just log for now
	// TODO: Set proper HTTP response code and headers
}

void MockS3ServerChaos::sendThrottleError(Reference<HTTP::OutgoingResponse> response, int retryAfterSeconds) {
	// Minimal implementation - just log for now
	// TODO: Set 429 status code and Retry-After header
}

void MockS3ServerChaos::sendAuthError(Reference<HTTP::OutgoingResponse> response, ErrorType errorType) {
	// Minimal implementation - just log for now
	// TODO: Set 401 status code and auth error details
}

// Generate S3-compatible error XML - minimal implementation
Standalone<StringRef> MockS3ServerChaos::generateS3ErrorXML(StringRef errorCode, StringRef message) {
	return Standalone<StringRef>();
}

// Metrics integration (following AsyncFileChaos pattern)
void MockS3ServerChaos::updateChaosMetrics(StringRef chaosType) {
	auto chaosMetricsPointer = g_network->global(INetwork::enChaosMetrics);
	if (chaosMetricsPointer) {
		ChaosMetrics* chaosMetrics = static_cast<ChaosMetrics*>(chaosMetricsPointer);
		// Increment appropriate chaos metrics
		if (chaosType == "delay"_sr) {
			chaosMetrics->diskDelays++; // Reuse disk delay counter for S3 delays
		}
		// Future extension could add S3-specific metrics
	}
}

// Factory function to create chaos-enabled MockS3Server
ACTOR Future<Void> startMockS3ServerChaos_impl(NetworkAddress listenAddress) {
	try {
		// Create base MockS3Server handler
		Reference<HTTP::IRequestHandler> baseHandler = makeReference<MockS3RequestHandler>();

		// Wrap with chaos handler
		Reference<HTTP::IRequestHandler> chaosHandler = makeReference<MockS3ServerChaos>(baseHandler);

		// Register with simulator (similar to MockS3Server)
		wait(g_simulator->registerSimHTTPServer(
		    listenAddress.ip.toString(), format("%d", listenAddress.port), chaosHandler));

	} catch (Error& e) {
		throw;
	}

	return Void();
}

Future<Void> startMockS3ServerChaos(const NetworkAddress& listenAddress) {
	return startMockS3ServerChaos_impl(listenAddress);
}