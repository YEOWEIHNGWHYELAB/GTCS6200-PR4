#include <regex>
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
#include <sys/inotify.h>
#include <sys/stat.h>
#include <utime.h>
#include <fcntl.h>
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


using dfs_service::FileRequest;
using dfs_service::StoreFileRequest;
using dfs_service::FileChunk;
using dfs_service::FileStatusResponse;
using dfs_service::OperationResponse;
using dfs_service::ListFilesRequest;
using dfs_service::ListFilesResponse;

namespace {
    void SetDeadline(ClientContext* context, int timeout_ms) {
        const int safe_timeout = timeout_ms > 0 ? timeout_ms : 12000;

        // time which deadline will be reached
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(safe_timeout));
    }

    bool ValidateFilename(const std::string& name) {
        return !name.empty() && name != "." && name != ".." &&
               name.find('/') == std::string::npos &&
               name.find('\\') == std::string::npos &&
               name.find('\0') == std::string::npos;
    }

    // convert to status code
    StatusCode ConvertResultCode(const Status& status) {
        return status.ok() ? StatusCode::OK : status.error_code();
    }
}

DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //

    if (!service_stub || !ValidateFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    const std::string filename_mnt_path = WrapPath(filename);

    struct stat st;
    if (stat(filename_mnt_path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
        return StatusCode::NOT_FOUND;
    }

    // open the file in binary mode for file client want to store
    // without binary mode, some platforms may perform text-mode transformations, like converting newline chars
    std::ifstream file_to_store(filename_mnt_path.c_str(), std::ios::binary);

    // file can't be open, panic
    if (!file_to_store.is_open()) {
        return StatusCode::NOT_FOUND;
    }

    ClientContext context;
    SetDeadline(&context, deadline_timeout);

    FileStatusResponse response;

    // establishing client-side streaming RPC
    std::unique_ptr writer_chunk_to_server(service_stub->StoreFile(&context, &response));

    bool is_first_message = true;
    bool write_is_successful = true;
    char buffer[DFS_BUFFERSIZE];
    while (file_to_store.good()) {
        file_to_store.read(buffer, sizeof(buffer));

        const std::streamsize num_bytes_read = file_to_store.gcount();

        // EOF
        if (num_bytes_read == 0 && !is_first_message) {
            break;
        }

        StoreFileRequest request;

        // writing the first message with the metadata
        if (is_first_message) {
            request.set_filename(filename);
            request.set_mtime(st.st_mtime);

            is_first_message = false;
        }

        if (num_bytes_read > 0) {
            request.set_content(buffer, static_cast<size_t>(num_bytes_read));
        }

        // send request to server with the chunk
        if (!writer_chunk_to_server->Write(request)) {
            write_is_successful = false;
            break;
        }

        if (num_bytes_read == 0) {
            break; // empty file still sends the filename frame
        }
    }

    if (file_to_store.bad()) {
        write_is_successful = false;
        context.TryCancel();
    }

    // tell server that we as client has finished sending messages
    writer_chunk_to_server->WritesDone();

    // get final status
    Status status = writer_chunk_to_server->Finish();

    if (!write_is_successful && status.ok()) {
        return StatusCode::CANCELLED;
    }

    if (!status.ok()) {
        dfs_log(LL_ERROR) << "Store failed for " << filename << ": " << status.error_message();
        return ConvertResultCode(status);
    }

    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    if (!service_stub || !ValidateFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    ClientContext context;
    SetDeadline(&context, deadline_timeout);

    FileRequest request;
    request.set_filename(filename);

    std::unique_ptr reader_chunk_from_server(service_stub->FetchFile(&context, request));

    const std::string file_destination_path = WrapPath(filename);

    const std::string temporary_file_path = file_destination_path + ".downloaded_file." + std::to_string(static_cast<long long>(::getpid()));

    std::ofstream temp_output_file(temporary_file_path.c_str(), std::ios::binary | std::ios::trunc);

    // unable to open, panic
    if (!temp_output_file.is_open()) {
        context.TryCancel();
        return StatusCode::PERMISSION_DENIED;
    }

    // read in chunk
    int64_t mtime = 0;
    bool has_received_file_meta = false;
    bool write_is_successful = true;
    FileChunk curr_chunk;
    while (reader_chunk_from_server->Read(&curr_chunk)) {
        // get the file metadata
        if (!has_received_file_meta) {
            has_received_file_meta = true;
            mtime = curr_chunk.mtime();

            if (!curr_chunk.filename().empty() && curr_chunk.filename() != filename) {
                write_is_successful = false;
                context.TryCancel();
                break;
            }
        }

        // write when the content is non-empty to the temp file in chunk
        if (!curr_chunk.content().empty()) {
            temp_output_file.write(curr_chunk.content().data(), static_cast<std::streamsize>(curr_chunk.content().size()));
        }

        if (!temp_output_file.good()) {
            write_is_successful = false;
            context.TryCancel();
            break;
        }
    }

    // don't forget to close
    temp_output_file.close();

    if (temp_output_file.fail()) {
        write_is_successful = false;
    }

    // get final status
    Status status = reader_chunk_from_server->Finish();

    if (!status.ok() || !write_is_successful || !has_received_file_meta) {
        std::remove(temporary_file_path.c_str());
        if (!status.ok()) {
            dfs_log(LL_ERROR) << "Fetch failed for " << filename << ": " << status.error_message();
            return ConvertResultCode(status);
        }
        return StatusCode::INTERNAL;
    }

    {
        utimbuf file_meta_time{};
        file_meta_time.actime = static_cast<time_t>(mtime);
        file_meta_time.modtime = static_cast<time_t>(mtime);

        // set time failed, just delete temp file
        if (utime(temporary_file_path.c_str(), &file_meta_time) != 0) {
            std::remove(temporary_file_path.c_str());

            return StatusCode::INTERNAL;
        }
    }

    // rename temp file to actual file name
    if (std::rename(temporary_file_path.c_str(), file_destination_path.c_str()) != 0) {
        const int rename_err = errno;
        std::remove(temporary_file_path.c_str());
        dfs_log(LL_ERROR) << "Cannot commit fetched file: " << std::strerror(rename_err);
        return rename_err == EACCES ? StatusCode::PERMISSION_DENIED : StatusCode::INTERNAL;
    }

    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //

    if (!service_stub || !ValidateFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    ClientContext context;
    SetDeadline(&context, deadline_timeout);

    FileRequest request;
    request.set_filename(filename);

    OperationResponse response;
    Status resp_status = service_stub->DeleteFile(&context, request, &response);

    if (!resp_status.ok()) {
        dfs_log(LL_ERROR) << "Delete failed for " << filename << ": " << resp_status.error_message();
    }

    return ConvertResultCode(resp_status);
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    if (!service_stub) {
        return StatusCode::UNAVAILABLE;
    }

    ClientContext context;
    SetDeadline(&context, deadline_timeout);

    ListFilesRequest request;

    ListFilesResponse response;

    Status resp_status = service_stub->ListFiles(&context, request, &response);

    if (!resp_status.ok()) {
        dfs_log(LL_ERROR) << "List failed: " << resp_status.error_message();
        return ConvertResultCode(resp_status);
    }

    // clear the file_map if it isn't empty
    if (file_map) {
        file_map->clear();
    }

    for (const auto& file : response.files()) {
        // set the map with the filename -> mtime
        if (file_map) {
            (*file_map)[file.filename()] = static_cast<int>(file.mtime());
        }

        // print to terminal if flag is set
        if (display) {
            std::cout << file.filename() << "\t" << file.mtime() << "\t" << file.size() << " bytes\n";
        }
    }

    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    if (!service_stub || !ValidateFilename(filename)) {
        return StatusCode::INVALID_ARGUMENT;
    }

    ClientContext context;
    SetDeadline(&context, deadline_timeout);

    // server request
    FileRequest request;
    request.set_filename(filename);

    // server response
    FileStatusResponse response;

    // call server rpc func
    Status resp_status = service_stub->GetFileStatus(&context, request, &response);

    // handle error
    if (!resp_status.ok()) {
        dfs_log(LL_ERROR) << "File status failed for " << filename << ": " << resp_status.error_message();
        return ConvertResultCode(resp_status);
    }

    if (file_status) {
        // static_cast<FileStatusResponse*>(file_status) converts file_status into a pointer to FileStatusResponse.
        *static_cast<FileStatusResponse*>(file_status) = response;
    } else {
        std::cout << response.filename() << "\n" << "Size: " << response.size() << " bytes\n" << "Modified: " << response.mtime() << "\n";
    }

    return StatusCode::OK;
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//


