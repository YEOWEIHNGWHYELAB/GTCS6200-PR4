#include <algorithm>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <set>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <utime.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using namespace dfs_service;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//

extern dfs_log_level_e DFS_LOG_LEVEL;

namespace {

    const std::string TEMP_PREFIX = ".dfs-tmp-upload-";

    bool ValidateFilename(const std::string& name) {
        return !name.empty() && name != "." && name != ".." &&
               name[0] != '.' &&
               name.find('/') == std::string::npos &&
               name.find('\\') == std::string::npos &&
               name.find('\0') == std::string::npos;
    }

    bool FillStatus(const std::string& path, const std::string& filename, FileStatusResponse* resp, CRC::Table<std::uint32_t, 32>* table) {
        struct stat st{};

        // // S_ISREG() checks if file is a regular file, not a sym link
        if (lstat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            return false;
        }

        resp->set_filename(filename);
        resp->set_size(static_cast<uint64_t>(st.st_size));
        resp->set_mtime(st.st_mtime);
        resp->set_crc(dfs_file_checksum(path, table));

        return true;
    }

}

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//


// inheritance is used here to tell gRPC that the CallbackList RPC should use the asynchronous server
class DFSServiceImpl final : public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
                             public DFSCallDataManager<FileRequest, FileList> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequest, FileList> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequest, FileList>> queued_tags;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;


    // filesystem operation mutex, note that reading and writing filesystem concurrently can cause inconsistent results
    // so even listing files requires this
    std::mutex filesystem_mutex;

    // ensure mutual exclusion access to write_locks_map and for deleted_files_set
    std::mutex write_lock_mutex;

    // tracks which client currently owns the write lock for each file
    std::map<std::string, std::string> write_locks_map;

    // stores filenames that have been deleted on the server
    std::set<std::string> deleted_files_set;


    void ReleaseFileWriteLock(const std::string& filename, const std::string& client) {
        std::lock_guard write_lock_mtx(write_lock_mutex);

        // find the filename key entry and delete the entry
        auto curr_it = write_locks_map.find(filename);
        if (curr_it != write_locks_map.end() && curr_it->second == client) {
            write_locks_map.erase(curr_it);
        }
    }

    bool DoesClientOwnWriteLock(const std::string& filename, const std::string& client) {
        std::lock_guard write_lock_mtx(write_lock_mutex);

        auto curr_it = write_locks_map.find(filename);

        return curr_it != write_locks_map.end() && curr_it->second == client && !client.empty();
    }

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(dfs_clean_path(mount_path)), crc_table(CRC::CRC_32()) {

        runner.SetService(this);
        runner.SetAddress(server_address);
        runner.SetNumThreads(num_async_threads);
        runner.SetQueuedRequestsCallback([this]{ ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        runner.Shutdown();
    }

    void Run() {
        runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(ServerContext* context,
                        FileRequest* request,
                        grpc::ServerAsyncResponseWriter<FileList>* response,
                        grpc::ServerCompletionQueue* cq,
                        void* tag) override {
        std::lock_guard lock(queue_mutex);
        queued_tags.emplace_back(context, request, response, cq, tag);
    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext*, FileRequest*, FileList* response) override {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //

        // lock the file system mutex first
        std::lock_guard fs_mtx(filesystem_mutex);

        DIR* mount_dir = opendir(mount_path.c_str());

        if (mount_dir) {
            dirent* dir_entry;
            while ((dir_entry = readdir(mount_dir)) != nullptr) {
                std::string filename(dir_entry->d_name);
                if (filename == "." || filename == ".." ||
                    filename.compare(0, TEMP_PREFIX.size(), TEMP_PREFIX) == 0 || // ensure that filename don't start with the TEMP_PREFIX
                    !ValidateFilename(filename)) {
                    continue;
                }

                struct stat st{};
                std::string file_destination_path = WrapPath(filename);
                if (lstat(file_destination_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
                    continue;
                }

                // add file to the response
                FileInfo* added_file = response->add_files();

                added_file->set_filename(filename);
                added_file->set_mtime(st.st_mtime);
                added_file->set_size(static_cast<uint64_t>(st.st_size));
                added_file->set_crc(dfs_file_checksum(file_destination_path, &crc_table));
            }

            // don't forget to close
            closedir(mount_dir);
        }

        // remember to lock the write lock before accessing deleted files set
        std::lock_guard write_lock_mtx(write_lock_mutex);

        // add the deletion file to response from the set
        for (const auto& name : deleted_files_set) {
            response->add_deleted_files(name);
        }
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while (true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                // dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard lock(queue_mutex);


                // register pending requests with gRPC, this is a non-blocking async call
                for (auto& queue_req : queued_tags) {
                    this->RequestCallbackList(queue_req.context, queue_req.request,
                        queue_req.response, queue_req.cq, queue_req.cq, queue_req.tag);
                    queue_req.finished = true;
                }

                // any finished tags first
                queued_tags.erase(std::remove_if( // std::remove_if() moves all unfinished requests towards beginning of vector, then return iterator pointing to new logical end
                    queued_tags.begin(),
                    queued_tags.end(),
                    [](const QueueRequest<FileRequest,FileList>& queue_req_it){ return queue_req_it.finished; }
                ), queued_tags.end());
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    Status RequestWriteLock(ServerContext*, const WriteLockRequest* req, FileOpResponse* resp) override {
        if (!ValidateFilename(req->filename()) || req->client_id().empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid filename or client id");
        }

        std::lock_guard write_lock_mtx(write_lock_mutex);

        auto curr_it = write_locks_map.find(req->filename());

        // key already exist
        if (curr_it != write_locks_map.end() && curr_it->second != req->client_id()) {
            return Status(StatusCode::RESOURCE_EXHAUSTED, "file is locked by another client already");
        }

        // obtain the write lock for the client by putting into the map
        write_locks_map[req->filename()] = req->client_id();

        // for common response for file operation
        resp->set_filename(req->filename());

        return Status::OK;
    }

    Status ReleaseWriteLock(ServerContext*, const WriteLockRequest* req, FileOpResponse* resp) override {
        if (!ValidateFilename(req->filename()) || req->client_id().empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid lock release request");
        }

        // just delete the filename entry in the map
        ReleaseFileWriteLock(req->filename(), req->client_id());

        resp->set_filename(req->filename());

        return Status::OK;
    }

    Status StoreFile(ServerContext* context, ServerReader<StoreFileRequest>* reader, FileStatusResponse* resp) override {
        StoreFileRequest req;
        if (!reader->Read(&req) || !ValidateFilename(req.filename()) || req.client_id().empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "missing or invalid upload metadata");
        }

        const std::string filename = req.filename();
        const std::string client_id = req.client_id();

        // Preserve metadata from the FIRST message.
        // Subsequent messages only contain file chunks.
        const time_t uploaded_mtime = static_cast<time_t>(req.mtime());
        const uint32_t uploaded_crc_checksum = req.crc();

        // check if client own write lock first
        if (!DoesClientOwnWriteLock(filename, client_id)) {
            return Status(StatusCode::PERMISSION_DENIED, "client does not own write lock, reject");
        }

        const std::string temporary_file_path = mount_path + TEMP_PREFIX +
                std::to_string(getpid()) + "-" +
                std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));

        std::ofstream temp_output_file(temporary_file_path, std::ios::binary | std::ios::trunc);

        // unable to create temp file
        if (!temp_output_file.is_open()) {
            ReleaseFileWriteLock(filename, client_id);
            return Status(StatusCode::PERMISSION_DENIED, "cannot create upload file");
        }

        bool write_is_successful = true;
        auto write_chunk = [&](const std::string& bytes) {
            if (!bytes.empty()) {
                temp_output_file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            }

            if (!temp_output_file.good()) {
                write_is_successful = false;
            }
        };

        write_chunk(req.content());
        while (write_is_successful && reader->Read(&req)) {
            // again to make sure that filename or client id don't change mid way through streaming by attacker
            if ((!req.filename().empty() && req.filename() != filename) || (!req.client_id().empty() && req.client_id() != client_id)) {
                write_is_successful = false;
                break;
            }

            write_chunk(req.content());
        }

        // remember to close!
        temp_output_file.close();

        if (context->IsCancelled() || !write_is_successful || temp_output_file.fail()) {
            std::remove(temporary_file_path.c_str());
            ReleaseFileWriteLock(filename, client_id);
            return Status(context->IsCancelled() ? StatusCode::CANCELLED : StatusCode::RESOURCE_EXHAUSTED, "upload failed");
        }

        utimbuf file_meta_time{};
        file_meta_time.actime = uploaded_mtime;
        file_meta_time.modtime = uploaded_mtime;

        if (utime(temporary_file_path.c_str(), &file_meta_time) != 0) {
            std::remove(temporary_file_path.c_str());
            ReleaseFileWriteLock(filename, client_id);
            return Status(StatusCode::INTERNAL, "unable to preserve mtime");
        }

        if (dfs_file_checksum(temporary_file_path, &crc_table) != uploaded_crc_checksum) {
            std::remove(temporary_file_path.c_str());
            ReleaseFileWriteLock(filename, client_id);
            return Status(StatusCode::DATA_LOSS, "upload checksum mismatch");
        }

        {
            std::lock_guard fs_mtx(filesystem_mutex);

            // rename the temp file into the actual filename
            if (std::rename(temporary_file_path.c_str(), WrapPath(filename).c_str()) != 0) {
                int rename_err = errno;
                std::remove(temporary_file_path.c_str()); ReleaseFileWriteLock(filename, client_id);
                return Status(StatusCode::INTERNAL, std::string("rename failed: ") + std::strerror(rename_err));
            }
        }

        // in the case if the file is recreated, then we must remove this filename from the set, note that the set doesn't
        // auto remove files from this set that have already been removed
        {
            std::lock_guard write_lock_mtx(write_lock_mutex);
            deleted_files_set.erase(filename);
        }

        // remember to release write lock
        ReleaseFileWriteLock(filename, client_id);

        if (!FillStatus(WrapPath(filename), filename, resp, &crc_table)) {
            return Status(StatusCode::INTERNAL, "stored file metadata unavailable");
        }

        return Status::OK;
    }

    Status FetchFile(ServerContext* context, const FileRequest* req, ServerWriter<FileChunk>* writer) override {
        if (!ValidateFilename(req->filename())) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid filename");
        }

        std::lock_guard fs_mtx(filesystem_mutex);
        std::string filename_mnt_path = WrapPath(req->filename());

        struct stat st{};
        if (lstat(filename_mnt_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        std::ifstream requested_file(filename_mnt_path, std::ios::binary);

        if (!requested_file.is_open()) {
            return Status(StatusCode::PERMISSION_DENIED, "cannot open file!");
        }

        const std::uint32_t crc_sum = dfs_file_checksum(filename_mnt_path, &crc_table);

        bool is_first_message = true;
        char buffer[DFS_BUFFERSIZE];
        while (requested_file.good()) {
            requested_file.read(buffer, sizeof(buffer));

            std::streamsize num_bytes_read = requested_file.gcount();

            // EOF
            if (num_bytes_read == 0 && !is_first_message) {
                break;
            }

            FileChunk file_chunk_to_send;
            if (is_first_message) {
                file_chunk_to_send.set_filename(req->filename());
                file_chunk_to_send.set_mtime(st.st_mtime);
                file_chunk_to_send.set_crc(crc_sum);
                is_first_message = false;
            }

            if (num_bytes_read > 0) {
                file_chunk_to_send.set_content(buffer, static_cast<size_t>(num_bytes_read));
            }

            if (!writer->Write(file_chunk_to_send)) {
                return Status(StatusCode::CANCELLED, "client stopped receiving file!");
            }

            if (num_bytes_read == 0) {
                break;
            }

            if (context->IsCancelled()) {
                return Status(StatusCode::CANCELLED, "fetch cancelled");
            }
        }

        return requested_file.bad() ? Status(StatusCode::INTERNAL, "error reading file!") : Status::OK;
    }

    Status DeleteFile(ServerContext*, const FileRequest* req, FileOpResponse* resp) override {
        if (!ValidateFilename(req->filename()) || req->client_id().empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid delete request");
        }

        // check if the client can delete the file or not
        if (!DoesClientOwnWriteLock(req->filename(), req->client_id())) {
            return Status(StatusCode::PERMISSION_DENIED, "client does not own write lock, reject");
        }

        // lock mutex then delete the file
        {
            std::lock_guard fs_mtx(filesystem_mutex);
            if (std::remove(WrapPath(req->filename()).c_str()) != 0 && errno != ENOENT) {
                // panic, failure to delete release lock first before returning error
                ReleaseFileWriteLock(req->filename(), req->client_id());
                return Status(StatusCode::PERMISSION_DENIED, std::strerror(errno));
            }
        }

        // update the deleted file set and to remove the write lock map entry for the deleted file
        {
            std::lock_guard write_lock_mtx(write_lock_mutex);
            deleted_files_set.insert(req->filename());
            write_locks_map.erase(req->filename());
        }

        // remember to return the common file operation resp
        resp->set_filename(req->filename());
        
        return Status::OK;
    }

    Status ListFiles(ServerContext*, const ListFilesRequest*, ListFilesResponse* resp) override {
        // must lock first
        std::lock_guard fs_mtx(filesystem_mutex);

        DIR* dir = opendir(mount_path.c_str());
        if (!dir) {
            return Status(StatusCode::NOT_FOUND, "mount directory not available");
        }

        // walk through the dir and then add it into the list of file info to return to client as resp
        dirent* dir_entry;
        while ((dir_entry = readdir(dir)) != nullptr) {
            std::string name(dir_entry->d_name);
            if (name == "." || name == ".." || name.compare(0,TEMP_PREFIX.size(),TEMP_PREFIX)==0 || !ValidateFilename(name)) {
                continue;
            }

            struct stat st{};

            std::string path = WrapPath(name);
            if (lstat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
                continue;
            }

            auto* added_file = resp->add_files();
            added_file->set_filename(name); added_file->set_mtime(st.st_mtime);
            added_file->set_size(static_cast<uint64_t>(st.st_size));
            added_file->set_crc(dfs_file_checksum(path, &crc_table));
        }

        // don't forget close
        closedir(dir);

        return Status::OK;
    }

    Status GetFileStatus(ServerContext*, const FileRequest* req, FileStatusResponse* resp) override {
        if (!ValidateFilename(req->filename())) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid filename");
        }

        // remember that we must lock
        std::lock_guard fs_mtx(filesystem_mutex);

        // just check if the file is there and return status
        return FillStatus(WrapPath(req->filename()), req->filename(), resp, &crc_table) ?
            Status::OK :
            Status(StatusCode::NOT_FOUND, "file not found");
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //


};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(dfs_clean_path(mount_path)),
        num_async_threads(num_async_threads),
        grader_callback(std::move(callback) // avoid unnecessary copying
    ) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

void DFSServerNode::Shutdown() {
    if (server) {
        server->Shutdown();
    }
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(mount_path, server_address, num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
