#include <map>
#include <chrono>
#include <cstdio>
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
#include <cerrno>
#include <cstring>
#include <functional>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::FileChunk;
using dfs_service::FileRequest;
using dfs_service::FileStatusResponse;
using dfs_service::ListFilesRequest;
using dfs_service::ListFilesResponse;
using dfs_service::OperationResponse;
using dfs_service::StoreFileRequest;

const std::string TEMP_FILE_PREFIX = ".dfs-tmp-upload-";

/**
 * the following is an anonymous namespace (unnamed namespace)
 *
 * only visible to current source file
 */
namespace {

    // the assignment store files in one flat directory, we reject path components
    // instead of allowing a client to escape the configured mount point
    bool ValidateFilename(const std::string& name) {
        return !name.empty() && name != "." && name != ".." &&
               name.find('/') == std::string::npos &&
               name.find('\\') == std::string::npos &&
               name.find('\0') == std::string::npos; // c style null terminator is dangerous in filenames
    }

    bool FillFileStatus(const std::string& path, const std::string& filename, FileStatusResponse* response) {
        // stat stores metadata for a filesystem object
        struct stat st;

        // lstat() retrieves metadata for specified filesystem object, 0 is success, -1 is failure
        if (lstat(path.c_str(), &st) != 0 || // note that POSIX functions - lstat() - expect a C-style string (const char*)
            !S_ISREG(st.st_mode)) { // This checks whether the filesystem object is a regular file.
            return false;
        }

        response->set_filename(filename);
        response->set_size(static_cast<uint64_t>(st.st_size));
        response->set_mtime(st.st_mtime);

        return true;
    }
}

//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    explicit DFSServiceImpl(const std::string &path) : mount_path(dfs_clean_path(path)) {}

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    Status StoreFile(ServerContext* context, ServerReader<StoreFileRequest>* reader, FileStatusResponse* response) override {
        /**
         * reader->Read(&request) attempts to read the next message from the client stream,
         * it returns true if a message was successfully received,
         * it returns false if the stream has ended or cannot provide another message.
         *
         * for the first message, we expect the message to contain the filename and the mtime which is controlled
         * by client side
         */
        StoreFileRequest request;
        if (!reader->Read(&request) || !ValidateFilename(request.filename())) {
            return Status(StatusCode::INVALID_ARGUMENT, "missing or invalid filename given!");
        }

        // get first message metadata about file
        const std::string filename = request.filename();
        const std::string file_destination_path = WrapPath(filename);
        const int64_t source_mtime = request.mtime();

        // create temp file path name
        const std::string temporary_file_path = mount_path + TEMP_FILE_PREFIX +
                                      std::to_string(static_cast<long long>(getpid())) + "-" +
                                      std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));

        // create and open the temp file
        std::ofstream temp_output_file(temporary_file_path.c_str(), std::ios::binary | std::ios::trunc);

        // panic, unable to create file
        if (!temp_output_file.is_open()) {
            return Status(StatusCode::PERMISSION_DENIED, "cannot create destination file!");
        }

        bool write_is_successful = true;

        // helper lambda to write chunks to temp file
        auto write_chunk = [&](const std::string& bytes) {
            if (!bytes.empty()) {
                temp_output_file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            }

            // good() checks if stream is in a healthy state
            if (!temp_output_file.good()) {
                write_is_successful = false;
            }
        };

        // write the chunk from first message
        write_chunk(request.content());

        // write the rest of it
        while (write_is_successful && reader->Read(&request)) {
            // should validate the filename again and again to make sure client isn't trying to change the upload's
            // file identity midway through the transfer
            if (!request.filename().empty() && request.filename() != filename) {
                write_is_successful = false;
                break;
            }

            write_chunk(request.content());
        }

        // don't forget to close it
        temp_output_file.close();

        if (context->IsCancelled()) {
            std::remove(temporary_file_path.c_str());

            return Status(StatusCode::CANCELLED, "upload cancelled or deadline exceeded!");
        }

        // check written temp file
        if (!write_is_successful || temp_output_file.fail()) {
            std::remove(temporary_file_path.c_str());
            return Status(StatusCode::RESOURCE_EXHAUSTED, "failed while writing uploaded file!");
        }

        // preserve client source file's mtime so clients can compare versions
        {
            utimbuf times{};

            times.actime = static_cast<time_t>(source_mtime);
            times.modtime = static_cast<time_t>(source_mtime);

            if (utime(temporary_file_path.c_str(), &times) != 0) {
                std::remove(temporary_file_path.c_str());

                return Status(StatusCode::INTERNAL, "could not set uploaded file timestamp!");
            }
        }

        // renaming the temp file to the actual file name
        if (std::rename(temporary_file_path.c_str(), file_destination_path.c_str()) != 0) {
            const int rename_err = errno;

            // remove temp file
            std::remove(temporary_file_path.c_str());

            return Status(rename_err == EACCES ? StatusCode::PERMISSION_DENIED : StatusCode::INTERNAL,std::string("could not commit uploaded file: ") + std::strerror(rename_err));
        }

        if (!FillFileStatus(file_destination_path, filename, response)) {
            return Status(StatusCode::INTERNAL, "could not read stored file metadata!");
        }

        // remember to return status
        return Status::OK;
    }

    Status FetchFile(ServerContext* context, const FileRequest* request, ServerWriter<FileChunk>* writer) override {
        if (!ValidateFilename(request->filename())) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid filename given!");
        }

        const std::string filename_mnt_path = WrapPath(request->filename());

        struct stat st;
        if (lstat(filename_mnt_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            return Status(StatusCode::NOT_FOUND, "file not found!");
        }

        // open the file in binary mode requested by client
        std::ifstream requested_file(filename_mnt_path.c_str(), std::ios::binary);

        // check if it can be opened
        if (!requested_file.is_open()) {
            return Status(StatusCode::PERMISSION_DENIED, "unable to open file!");
        }

        bool is_first_message = true;
        char buffer[DFS_BUFFERSIZE];
        while (requested_file.good()) {
            requested_file.read(buffer, sizeof(buffer));

            // gcount() tells how many bytes were read by the most recent unformatted input operation
            const std::streamsize num_bytes_read = requested_file.gcount();

            // EOF
            if (num_bytes_read == 0 && !is_first_message) {
                break;
            }

            FileChunk file_chunk_to_send;
            if (is_first_message) {
                file_chunk_to_send.set_filename(request->filename());
                file_chunk_to_send.set_mtime(st.st_mtime);

                // remember to set this flag to false
                is_first_message = false;
            }

            // non-empty file content to be set into chunk
            if (num_bytes_read > 0) {
                file_chunk_to_send.set_content(buffer, static_cast<size_t>(num_bytes_read));
            }

            if (!writer->Write(file_chunk_to_send)) {
                return Status(StatusCode::CANCELLED, "client stopped receiving file!");
            }

            if (num_bytes_read == 0) {
                break; // send one metadata frame for an empty file
            }
        }

        if (requested_file.bad()) {
            return Status(StatusCode::INTERNAL, "error reading file!");
        }

        return Status::OK;
    }

    Status DeleteFile(ServerContext* context, const FileRequest* request, OperationResponse* response) override {
        if (!ValidateFilename(request->filename())) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid filename given!");
        }

        const std::string filename_mnt_path = WrapPath(request->filename());

        struct stat st;

        if (lstat(filename_mnt_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            return Status(StatusCode::NOT_FOUND, "file not found!");
        }

        // delete the file directly using std::remove
        if (std::remove(filename_mnt_path.c_str()) != 0) {
            return Status(StatusCode::PERMISSION_DENIED, std::string("deletion failed: ") + std::strerror(errno));
        }

        // set the filename that has been successfully deleted
        response->set_filename(request->filename());

        return Status::OK;
    }

    Status ListFiles(ServerContext* context, const ListFilesRequest* request, ListFilesResponse* response) override {
        DIR* mnt_dir = opendir(mount_path.c_str());

        if (!mnt_dir) {
            return Status(StatusCode::NOT_FOUND, "server mount dir unavailable!");
        }

        // iterate through the current directory entries
        dirent* entry;
        while ((entry = readdir(mnt_dir)) != nullptr) {
            // converting filename to C string
            const std::string filename(entry->d_name);

            // ignore the . and ..
            // also ignore .dsf-upload- because it is used for temp storage
            if (filename == "." || filename == ".." || filename.compare(0, 12, TEMP_FILE_PREFIX) == 0) {
                continue;
            }

            const std::string filename_mnt_path = WrapPath(filename);

            struct stat st;
            if (lstat(filename_mnt_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
                continue;
            }

            // make use of grpc generated methods
            auto* info = response->add_files();

            info->set_filename(filename);

            // note that st.st_mtime contains file's last modification time
            info->set_mtime(st.st_mtime);

            info->set_size(static_cast<uint64_t>(st.st_size));
        }

        // don't forget to close the directory
        closedir(mnt_dir);

        return Status::OK;
    }

    Status GetFileStatus(ServerContext* context, const FileRequest* request, FileStatusResponse* response) override {
        // can use the following to test deadline, set less than 4 second
        // std::this_thread::sleep_for(std::chrono::seconds(4));

        if (!ValidateFilename(request->filename())) {
            return Status(StatusCode::INVALID_ARGUMENT, "invalid filename given!");
        }

        if (!FillFileStatus(WrapPath(request->filename()), request->filename(), response)) {
            return Status(StatusCode::NOT_FOUND, "file not found!");
        }

        return Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    if (this->server) {
        this->server->Shutdown();
    }
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    if (!this->server) {
        dfs_log(LL_ERROR) << "Failed to start gRPC server at " << this->server_address;
        return;
    }
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//
