/*
 * MockS3ServerChaos.h
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

#pragma once

#include "flow/flow.h"
#include "flow/network.h"
#include "fdbrpc/simulator.h"

// Chaos-enabled version of MockS3Server for testing S3 error handling and retries
// Follows AsyncFileChaos pattern to integrate with simulation framework
// Respects BUGGIFY, fault injection flags, and deterministic randomness
//
// Configurable chaos behaviors via fault injectors:
// - Error injection using DiskFailureInjector rates
// - Data corruption using BitFlipper rates
// - Delay patterns using DiskFailureInjector delays
//
// Simulates realistic S3 behavior including:
// - Throttling (429 Too Many Requests) with configurable burst patterns
// - Service unavailable (503) with retry-after headers
// - Internal server errors (500, 502) with exponential backoff triggers
// - Authentication errors (401, 406) with token expiration simulation
// - S3-specific errors (InvalidToken, ExpiredToken) matching AWS behavior
// - Connection failures and timeouts with realistic timing
// - Data corruption and malformed responses
// - Network delays and jitter patterns

class MockS3ServerChaos final : public HTTP::IRequestHandler, public ReferenceCounted<MockS3ServerChaos> {
public:
	// Error types to inject (matching real S3 error patterns)
	enum class ErrorType {
		THROTTLE_429, // Too Many Requests (with Retry-After)
		SERVICE_UNAVAILABLE_503, // Service Unavailable (transient)
		INTERNAL_ERROR_500, // Internal Server Error (retriable)
		BAD_GATEWAY_502, // Bad Gateway (retriable)
		AUTH_FAILED_401, // Authentication Failed (not retriable)
		NOT_ACCEPTABLE_406, // Not Acceptable (not retriable)
		INVALID_TOKEN, // S3 InvalidToken error (auth refresh needed)
		EXPIRED_TOKEN, // S3 ExpiredToken error (auth refresh needed)
		CONNECTION_DROP, // Simulate connection failure
		TIMEOUT, // Simulate timeout
		DATA_CORRUPTION, // Corrupt response data
		MALFORMED_XML // Invalid XML responses
	};

	// Operation types for targeted chaos
	enum class OperationType {
		GET_OBJECT,
		PUT_OBJECT,
		DELETE_OBJECT,
		HEAD_OBJECT,
		LIST_OBJECTS,
		MULTIPART_INITIATE,
		MULTIPART_UPLOAD_PART,
		MULTIPART_COMPLETE,
		MULTIPART_ABORT,
		BUCKET_OPERATIONS
	};

	// Public members for actor access
	Reference<HTTP::IRequestHandler> baseHandler;
	bool enabled;

public:
	explicit MockS3ServerChaos(Reference<HTTP::IRequestHandler> handler);

	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override;
	Reference<HTTP::IRequestHandler> clone() override;

	void addref() override { ReferenceCounted<MockS3ServerChaos>::addref(); }
	void delref() override { ReferenceCounted<MockS3ServerChaos>::delref(); }

	// Public methods for actor access
	bool shouldInjectChaos() const;
	bool shouldInjectError() const;
	bool shouldInjectDelay() const;
	bool shouldInjectCorruption() const;
	ErrorType selectErrorType() const;
	OperationType classifyOperation(StringRef verb, StringRef resource) const;
	double getDelay() const;
	Future<Void> injectDelay(double delayTime);
	Future<Void> injectError(Reference<HTTP::IncomingRequest> req,
	                         Reference<HTTP::OutgoingResponse> response,
	                         ErrorType errorType);
	Future<Void> injectDataCorruption(Reference<HTTP::OutgoingResponse> response);
	void updateChaosMetrics(StringRef chaosType);

	// S3 error response generation (with realistic headers) - public for actor access
	void sendS3Error(Reference<HTTP::OutgoingResponse> response,
	                 int httpCode,
	                 StringRef s3ErrorCode,
	                 StringRef message,
	                 bool includeRetryAfter = false);

	void sendThrottleError(Reference<HTTP::OutgoingResponse> response, int retryAfterSeconds);
	void sendAuthError(Reference<HTTP::OutgoingResponse> response, ErrorType errorType);
	Standalone<StringRef> generateS3ErrorXML(StringRef errorCode, StringRef message);
};

// Factory function to create chaos-enabled MockS3Server
Future<Void> startMockS3ServerChaos(const NetworkAddress& listenAddress);