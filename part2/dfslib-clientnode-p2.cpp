#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <dirent.h>
#include <set>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using namespace dfs_service;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//

namespace {
    void SetDeadline(ClientContext* ctx, int timeout) {
        ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout > 0 ? timeout : 12000));
    }

    bool ValidateFilename(const std::string& name) {
        return !name.empty() && name != "." && name != ".." &&
               name[0] != '.' &&
               name.find('/') == std::string::npos &&
               name.find('\\') == std::string::npos &&
               name.find('\0') == std::string::npos;
    }

    StatusCode ConvertResultCode(const Status& s) {
        return s.ok() ? StatusCode::OK : s.error_code();
    }

    bool GetLocalStat(const std::string& path, struct stat* st) {
        return lstat(path.c_str(), st) == 0 && S_ISREG(st->st_mode);
    }
}

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {
    deadline_timeout = 12000;
}

DFSClientNodeP2::~DFSClientNodeP2() { completion_queue.Shutdown(); }

bool DFSClientNodeP2::IsValidFilename(const std::string& filename) const {
    return ValidateFilename(filename);
}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    if (!service_stub || !IsValidFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    WriteLockRequest req;
    req.set_filename(filename);
    req.set_client_id(ClientId());

    FileOpResponse response;

    // request with a context deadline
    ClientContext ctx;
    SetDeadline(&ctx, deadline_timeout);

    // rpc call to server
    Status status = service_stub->RequestWriteLock(&ctx, req, &response);

    return ConvertResultCode(status);
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    if (!service_stub || !IsValidFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    const std::string file_destination_path = WrapPath(filename);

    struct stat st{};
    if (!GetLocalStat(file_destination_path, &st)) {
        return StatusCode::NOT_FOUND;
    }

    std::ifstream file_to_store(file_destination_path, std::ios::binary);
    if (!file_to_store.is_open()) {
        return StatusCode::NOT_FOUND;
    }

    // need request for the write access
    StatusCode write_access_lock_status = RequestWriteAccess(filename);
    if (write_access_lock_status != StatusCode::OK) {
        return write_access_lock_status;
    }

    // for releasing the write clock by calling server ReleaseWriteLock rpc
    auto release_write_lock = [&]() {
        WriteLockRequest write_lock_req;
        write_lock_req.set_filename(filename);
        write_lock_req.set_client_id(ClientId());

        FileOpResponse ignored_file_op_resp;

        ClientContext release_ctx;
        SetDeadline(&release_ctx, deadline_timeout);

        service_stub->ReleaseWriteLock(&release_ctx, write_lock_req, &ignored_file_op_resp);
    };

    // the check sum will be used to compare the contents of local file against server's version
    const std::uint32_t crc_checksum = dfs_file_checksum(file_destination_path, &crc_table);

    FileRequest file_req;
    file_req.set_filename(filename);

    // check server's existing file version metadata using GetFileStatus()
    // client then compares server's modification time with its own
    {
        FileStatusResponse file_status_resp;
        ClientContext status_ctx;
        SetDeadline(&status_ctx, deadline_timeout);

        Status file_status = service_stub->GetFileStatus(&status_ctx, file_req, &file_status_resp);

        if (file_status.ok()) {
            // newer server version wins. Identical data only needs its mtime
            // reconciled; otherwise the newer local version may be uploaded.
            if (file_status_resp.mtime() >= st.st_mtime) {

                // the file content are the same, but server file has timing later than client
                // so we update client file time, no need to transfer file
                if (file_status_resp.crc() == crc_checksum && file_status_resp.mtime() != st.st_mtime) {
                    utimbuf file_meta_time{};
                    file_meta_time.actime = file_status_resp.mtime();
                    file_meta_time.modtime = file_status_resp.mtime();
                    utime(file_destination_path.c_str(), &file_meta_time);
                }

                // can straight away release write lock
                release_write_lock();

                return StatusCode::ALREADY_EXISTS;
            }
        } else if (file_status.error_code() != StatusCode::NOT_FOUND) {
            // any other error other than NOT_FOUND, we just return
            release_write_lock();

            return file_status.error_code();
        }
    }

    ClientContext ctx;
    SetDeadline(&ctx, deadline_timeout);

    FileStatusResponse file_status_resp;
    std::unique_ptr writer_chunk_to_server(service_stub->StoreFile(&ctx, &file_status_resp));

    // start writing the file onto server
    bool is_first_message = true;
    char buffer[DFS_BUFFERSIZE];
    while (file_to_store.good()) {
        file_to_store.read(buffer, sizeof(buffer));

        std::streamsize num_bytes_read = file_to_store.gcount();

        // EOF
        if (num_bytes_read == 0 && !is_first_message) {
            break;
        }

        StoreFileRequest request;

        // write first message
        if (is_first_message) {
            request.set_filename(filename);
            request.set_mtime(st.st_mtime);
            request.set_client_id(ClientId());
            request.set_crc(crc_checksum);

            is_first_message = false;
        }

        if (num_bytes_read > 0) {
            request.set_content(buffer, static_cast<size_t>(num_bytes_read));
        }

        if (!writer_chunk_to_server->Write(request)) {
            break;
        }

        if (num_bytes_read == 0) {
            break;
        }
    }

    writer_chunk_to_server->WritesDone();

    // final status
    Status status = writer_chunk_to_server->Finish();

    // The server releases the lock after a successful store. If the transfer
    // failed before it reached the server's commit path, release it explicitly.
    if (!status.ok()) {
        release_write_lock();
    }

    return ConvertResultCode(status);
}

grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //


    callback();

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //



            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//

