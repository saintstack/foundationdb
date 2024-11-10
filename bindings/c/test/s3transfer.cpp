// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
  Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  This file is licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License. A copy of
  the License is located at

http://aws.amazon.com/apache2.0/

This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
 **/

// Taken from https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/cpp/example_code/transfer-manager/transferOnStream.cpp
// and renamed as s3transfer and amended to add directory transfer support.

#include <aws/core/Aws.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/transfer/TransferManager.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/StringUtils.h>
#include <fstream>

using namespace std;
using namespace Aws;
using namespace Aws::Utils;
using namespace Aws::S3;

static const size_t BUFFER_SIZE = 512 * 1024 * 1024; // 512MB Buffer 

/**
 * In-memory stream implementation
 */
class MyUnderlyingStream : public Aws::IOStream
{
    public:
        using Base = Aws::IOStream;
        // Provide a customer-controlled streambuf to hold data from the bucket.
        explicit MyUnderlyingStream(std::streambuf* buf)
            : Base(buf)
        {}

        ~MyUnderlyingStream() override = default;
};

int main(int argc, char** argv)
{
       if (argc < 4) 
    {
        std::cout << "This program is used to demonstrate how transfer manager transfers large object in memory without copying it to a local file." << std::endl
            << "It first uploads [LocalFilePath] to your Amazon S3 [Bucket] with object name [Key], then downloads the object to memory." << std::endl
            << "To verify the correctness of the file content in memory, the program will dump the data to a local file [LocalFilePath]_copy." << std::endl
            << "You can use md5sum [LocalFilePath] [LocalFilePath]_copy to verify they have the same content." << std::endl
            << "\tUsage: " << argv[0] << " [Bucket] [Key] [LocalFilePath]" << std::endl;
        return -1;
    }

    const char* BUCKET = argv[1];
    const char* KEY = argv[2];
    const char* LOCAL_FILE = argv[3];
    Aws::String LOCAL_FILE_COPY(LOCAL_FILE);
    LOCAL_FILE_COPY += "_copy";

    Aws::SDKOptions options;

    Aws::InitAPI(options);
    {
        Aws::Vector<std::shared_ptr<Transfer::TransferHandle>> directoryUploads;
        Aws::Vector<std::shared_ptr<Transfer::TransferHandle>> directoryDownloads;
        std::condition_variable directoryUploadSignal;
        std::condition_variable directoryDownloadSignal;
        std::mutex semaphoreLock;
        Aws::S3::S3ClientConfiguration s3Config = Aws::S3::S3ClientConfiguration(true, "legacy", true);
        s3Config.endpointOverride = "localhost:8333";
        s3Config.scheme = Aws::Http::Scheme::HTTP;
        s3Config.verifySSL = false;

        // snippet-start:[transfer-manager.cpp.transferOnStream.code]
        auto s3_client = Aws::MakeShared<Aws::S3::S3Client>("s3client", s3Config);
        auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("executor", 25);
        Aws::Transfer::TransferManagerConfiguration transfer_config(executor.get());
        //transfer_config.computeContentMD5 = true;
        transfer_config.s3Client = s3_client;
        auto transferInitCallback = [&](const Transfer::TransferManager*, const std::shared_ptr<const Transfer::TransferHandle>& handle)
        {
            std::lock_guard<std::mutex> m(semaphoreLock);

            if(handle->GetTransferDirection() == Transfer::TransferDirection::UPLOAD)
            {
                std::cout << "Transfer UP initiated " << handle->GetContentType() << " " << handle->GetBucketName() << std::endl;
                directoryUploads.push_back(std::const_pointer_cast<Transfer::TransferHandle>(handle));

/*
                if (directoryUploads.size() == 4)
                {
                    directoryUploadSignal.notify_one();
                }
*/
            } else {
                std::cout << "Transfer DOWN initiated " << " " << handle->GetBucketName() << std::endl;
                directoryDownloads.push_back(std::const_pointer_cast<Transfer::TransferHandle>(handle));
                if (directoryDownloads.size() >= 1)
                {
                    directoryDownloadSignal.notify_one();
                }
            }
        };
        transfer_config.transferInitiatedCallback = transferInitCallback;
        transfer_config.transferStatusUpdatedCallback =
            [](const Transfer::TransferManager*, const std::shared_ptr<const Transfer::TransferHandle>& handle)
        {
            // FYI, I can pass in context if needed.
            switch (handle->GetStatus())
            {
                case Transfer::TransferStatus::IN_PROGRESS:
                    std::cout << "Transfer in progress " << std::endl;
                    break;
                case Transfer::TransferStatus::COMPLETED:
                    // Verify that the upload retrieved the expected amount of data.
                    assert(handle->GetBytesTotalSize() == handle->GetBytesTransferred());
                    std::cout << "Transfer completed." << std::endl;
                    break;
                case Transfer::TransferStatus::CANCELED:
                    std::cout << "Transfer canceled." << std::endl;
                    break;
                case Transfer::TransferStatus::FAILED:
                    std::cout << "Transfer failed." << std::endl;
                    break;
                //this value is only used for directory synchronization
                case Transfer::TransferStatus::EXACT_OBJECT_ALREADY_EXISTS:
                    std::cout << "Transfer already exists." << std::endl;
                    break;
                //Operation is still queued and has not begun processing
                case Transfer::TransferStatus::NOT_STARTED:
                    std::cout << "Transfer not started." << std::endl;
                    break;
                case Transfer::TransferStatus::ABORTED:
                    std::cout << "Transfer aborted." << std::endl;
                    break;
            }
        };
        transfer_config.errorCallback = [](const Transfer::TransferManager*, const std::shared_ptr<const Transfer::TransferHandle>& handle, const Aws::Client::AWSError<Aws::S3::S3Errors>& error)
        {
            // FYI, I can pass in context if needed.
            std::cout << "Error: " << error.GetMessage() << std::endl;
        };
        transfer_config.downloadProgressCallback = [](const Transfer::TransferManager*, const std::shared_ptr<const Transfer::TransferHandle>& handle)
        {
            std::cout << "Downloaded " << handle->GetBytesTransferred() << " of " << handle->GetBytesTotalSize() << " bytes." << std::endl;
        };

        // Create buffer to hold data received by the data stream.
        Aws::Utils::Array<unsigned char> buffer(BUFFER_SIZE);

        // The local variable 'streamBuffer' is captured by reference in a lambda.
        // It must persist until all downloading by the 'transfer_manager' is complete.
        Stream::PreallocatedStreamBuf streamBuffer(buffer.GetUnderlyingData(), buffer.GetLength());
        auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);
        transfer_manager->UploadDirectory(LOCAL_FILE, BUCKET, KEY, Aws::Map<Aws::String, Aws::String>());
        auto transferStatus = transfer_manager->WaitUntilAllFinished();
        if (transferStatus != Transfer::TransferStatus::COMPLETED)
        {
            std::cout << "File upload failed:  " << transferStatus << std::endl;
        }
        std::cout << "Dir upload finished." << std::endl;
        auto handle = transfer_manager->DownloadFile("backup",
            "backup/testdir/one",
            "/tmp/onex");
        handle->WaitUntilFinished();
        std::cout << "File download finished." << handle->GetStatus() << std::endl;
        if (handle->GetStatus() != Transfer::TransferStatus::COMPLETED)
        {
            auto err = handle->GetLastError();
            std::cout << "File download failed:  " << err.GetMessage() << std::endl;
        }

        transfer_manager->DownloadToDirectory("/tmp", BUCKET, "backup/testdir/one");
        /*
        {
            std::unique_lock<std::mutex> locker(semaphoreLock);
            if (directoryDownloads.size() < 1)
            {
                directoryDownloadSignal.wait(locker);
            }
        }
        */
        transferStatus = transfer_manager->WaitUntilAllFinished();
        std::cout << "Dir download finished." << transferStatus << std::endl;
    }
    Aws::ShutdownAPI(options);
}