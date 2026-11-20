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

    // for releasing the write lock by calling server ReleaseWriteLock rpc
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

    if (!service_stub || !IsValidFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    FileRequest file_req;
    file_req.set_filename(filename);

    FileStatusResponse file_status_resp;

    // retrieve server's file metadata
    {
        ClientContext stat_ctx;
        SetDeadline(&stat_ctx, deadline_timeout);

        Status stat_status = service_stub->GetFileStatus(&stat_ctx, file_req, &file_status_resp);
        if (!stat_status.ok()) {
            // panic, file status got issue
            return stat_status.error_code();
        }
    }

    // check if local file is already identical
    const std::string file_destination_path = WrapPath(filename);
    struct stat st{};
    if (GetLocalStat(file_destination_path, &st) && dfs_file_checksum(file_destination_path, &crc_table) == file_status_resp.crc()) {

        // update the local file meta time to server if not equal
        if (file_status_resp.mtime() != st.st_mtime) {
            utimbuf file_meta_time{};
            file_meta_time.actime = file_status_resp.mtime();
            file_meta_time.modtime = file_status_resp.mtime();
            utime(file_destination_path.c_str(), &file_meta_time);
        }

        return StatusCode::ALREADY_EXISTS;
    }

    ClientContext ctx;
    SetDeadline(&ctx, deadline_timeout);

    std::unique_ptr reader_chunk_from_server(service_stub->FetchFile(&ctx, file_req));

    const std::string temporary_file_path = file_destination_path + ".downloaded_file." + std::to_string(getpid()) + "-" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));

    std::ofstream temp_output_file(temporary_file_path, std::ios::binary | std::ios::trunc);

    // unable to open output file
    if (!temp_output_file.is_open()) {
        return StatusCode::PERMISSION_DENIED;
    }

    int64_t mtime = file_status_resp.mtime(); // use the server file status time
    bool has_received_file_meta = false;
    FileChunk curr_chunk;
    while (reader_chunk_from_server->Read(&curr_chunk)) {
        // checks server is returning chunks belonging to requested file
        // when the server returns a different filename, we treat it as data-integrity problem
        if (!curr_chunk.filename().empty() && curr_chunk.filename() != filename) {
            // remember to close
            temp_output_file.close();

            // remove the temp file when there is data integrity issue
            std::remove(temporary_file_path.c_str());

            return StatusCode::DATA_LOSS;
        }

        // get file meta data
        if (!has_received_file_meta) {
            if (curr_chunk.mtime() != 0) {
                mtime = curr_chunk.mtime();
            }

            has_received_file_meta = true;
        }

        if (!curr_chunk.content().empty()) {
            temp_output_file.write(curr_chunk.content().data(), static_cast<std::streamsize>(curr_chunk.content().size()));
        }

        if (!temp_output_file.good()) {
            break;
        }
    }

    // remember to close
    temp_output_file.close();

    Status status = reader_chunk_from_server->Finish();
    if (!status.ok() || !temp_output_file.good()) {
        std::remove(temporary_file_path.c_str());
        return ConvertResultCode(status);
    }

    // verify the file checksum before renaming
    if (dfs_file_checksum(temporary_file_path, &crc_table) != file_status_resp.crc()) {
        std::remove(temporary_file_path.c_str());
        return StatusCode::DATA_LOSS;
    }

    utimbuf file_meta_time{};
    file_meta_time.actime = static_cast<time_t>(mtime);
    file_meta_time.modtime = static_cast<time_t>(mtime);

    // check if set time succeeded and rename has succeeded
    if (utime(temporary_file_path.c_str(), &file_meta_time) != 0 || std::rename(temporary_file_path.c_str(), file_destination_path.c_str()) != 0) {
        std::remove(temporary_file_path.c_str());
        return StatusCode::INTERNAL;
    }

    return StatusCode::OK;
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

    if (!service_stub || !IsValidFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status != StatusCode::OK) {
        return lock_status;
    }

    ClientContext ctx;
    SetDeadline(&ctx, deadline_timeout);

    FileRequest file_req;
    file_req.set_filename(filename);
    file_req.set_client_id(ClientId());

    FileOpResponse file_op_resp;

    Status status = service_stub->DeleteFile(&ctx, file_req, &file_op_resp);
    if (!status.ok()) {
        WriteLockRequest write_lock_req;
        write_lock_req.set_filename(filename);
        write_lock_req.set_client_id(ClientId());

        FileOpResponse ignored_file_op_resp;

        ClientContext release_ctx;
        SetDeadline(&release_ctx, deadline_timeout);

        // release write lock explicitly on status that are not ok
        service_stub->ReleaseWriteLock(&release_ctx, write_lock_req, &ignored_file_op_resp);
    }

    return ConvertResultCode(status);
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

    if (!service_stub) {
        return StatusCode::CANCELLED;
    }

    ClientContext ctx;
    SetDeadline(&ctx, deadline_timeout);

    ListFilesRequest list_files_req;

    ListFilesResponse list_files_resp;

    Status status = service_stub->ListFiles(&ctx, list_files_req, &list_files_resp);
    if (!status.ok()) {
        return status.error_code();
    }

    if (file_map) {
        file_map->clear();
    }

    for (const auto& curr_file : list_files_resp.files()) {
        if (file_map) {
            (*file_map)[curr_file.filename()] = static_cast<int>(curr_file.mtime());
        }

        if (display) {
            std::cout << curr_file.filename() << "\t" << curr_file.mtime() << "\t" << curr_file.size() << std::endl;
        }
    }

    return StatusCode::OK;
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

    if (!service_stub || !IsValidFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    ClientContext ctx;
    SetDeadline(&ctx, deadline_timeout);

    FileRequest file_req;
    file_req.set_filename(filename);

    FileStatusResponse file_Status_resp;
    
    Status status = service_stub->GetFileStatus(&ctx, file_req, &file_Status_resp);

    if (!status.ok()) {
        return status.error_code();
    }

    if (file_status) {
        *static_cast<FileStatusResponse*>(file_status) = file_Status_resp;
    } else {
        std::cout << file_Status_resp.filename() << "\t" << file_Status_resp.size() << "\t" << file_Status_resp.mtime() << std::endl;
    }

    return StatusCode::OK;
}

/**
 * The following method is part of the flow:
 *
 * 1. DFSClient::InotifyWatcher() — listens for filesystem events
 * 2. DFSClientNodeP2::InotifyWatcherCallback() — provides synchronization using sync_mutex (part of the DFSClientNodeP2 class)
 * 3. DFSClient::InotifyEventCallback() — decides whether to call Store() or Delete() to sync to server
 */
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

    std::lock_guard sync_lock_mtx(sync_mutex);

    // for example -> Store("test.txt");
    callback();
}

// this basically compare files on the server with the files on local client, then synchronize them
void DFSClientNodeP2::Reconcile(const FileList& listing) {
    // Apply server deletion notifications first, so stale local copies are not re-uploaded.
    std::set<std::string> deleted_set;
    for (const auto& curr_deleted_file : listing.deleted_files()) {
        if (!IsValidFilename(curr_deleted_file)) {
            continue;
        }

        deleted_set.insert(curr_deleted_file);

        std::string delete_filename = WrapPath(curr_deleted_file);

        std::remove(delete_filename.c_str());
    }

    // map of the server file that maps filename to the file
    std::map<std::string, FileInfo> server_filename_fileinfo_map;
    for (const auto& curr_file : listing.files()) {
        if (IsValidFilename(curr_file.filename())) {
            server_filename_fileinfo_map[curr_file.filename()] = curr_file;
        }
    }

    // server-side files: fetch missing / stale local copies, or decide which local copy to upload
    for (const auto& sever_file_info_entry : server_filename_fileinfo_map) {
        const std::string& filename = sever_file_info_entry.first;
        const FileInfo& server_file_info = sever_file_info_entry.second;

        const std::string local_path = WrapPath(filename);

        struct stat st{};
        if (!GetLocalStat(local_path, &st)) {
            Fetch(filename);
            continue;
        }

        // assignment defines mtime as the conflict winner and equal modification times require no
        // synchronization, even if CRCs differ...
        if (st.st_mtime == server_file_info.mtime()) {
            continue;
        }

        std::uint32_t local_crc_checksum = dfs_file_checksum(local_path, &crc_table);

        if (local_crc_checksum == server_file_info.crc()) {
            // same bytes, but differ in time
            if (st.st_mtime > server_file_info.mtime()) {
                Store(filename);
            } else {
                Fetch(filename);
            }
        } else if (st.st_mtime > server_file_info.mtime()) {
            // local later than server
            Store(filename);
        } else {
            // server later than local
            Fetch(filename);
        }
    }

    // on initial mount (and subsequent passes), local-only files are new client files
    DIR* dir = opendir(MountPath().c_str());
    if (!dir) {
        return;
    }

    // walk through directory to find file that aren't on server
    dirent* dir_entry;
    while ((dir_entry = readdir(dir)) != nullptr) {
        std::string filename(dir_entry->d_name);

        if (!IsValidFilename(filename) ||
            deleted_set.count(filename) || // deleted set contains the filename
            server_filename_fileinfo_map.count(filename)) {
            continue;
        }

        std::string file_not_on_server = WrapPath(filename);

        struct stat st{};
        if (GetLocalStat(file_not_on_server, &st)) {
            Store(filename);
        }
    }

    // remember to close...
    closedir(dir);
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

    void* tag = nullptr;

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

        //
        // STUDENT INSTRUCTION:
        //
        // Consider adding a critical section or RAII style lock here
        //

        // The tag is the memory location of the call_data object
        // retrieve resp from server
        std::unique_ptr<AsyncClientData<FileList>> callback_list_info(static_cast<AsyncClientData<FileList>*>(tag));

        if (ok && callback_list_info->status.ok() && !Unmounting()) {
            std::lock_guard sync_lock_mtx(sync_mutex);

            // start to reconcile onto local
            Reconcile(callback_list_info->reply);
        } else if (!Unmounting()) {
            dfs_log(LL_ERROR) << "CallbackList failure: " << callback_list_info->status.error_message();
            std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
        }

        // wait before we register the next callback request
        if (!Unmounting()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            InitCallbackList();
        }
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
    if (!service_stub || Unmounting()) {
        return;
    }

    CallbackList<FileRequest, FileList>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//

