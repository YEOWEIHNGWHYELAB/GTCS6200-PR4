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
