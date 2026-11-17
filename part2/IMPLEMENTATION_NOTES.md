# Part 2 implementation notes

This implementation extends the Part 1 streaming protocol with:

- A server-owned per-filename writer lock, keyed by client ID.
- Explicit lock release for no-op/failed client operations.
- CRC32 metadata in file status and file listings.
- Server-side tombstones (`deleted_files`) in asynchronous callback responses.
- Client reconciliation using CRC plus mtime, including a first-mount scan of files already in the mount directory.
- A client mutex shared by watcher callbacks and callback-driven reconciliation.

## Build

From this directory, generate protobuf/gRPC sources and build:

```sh
make protos
make
```

The build requires the same Protocol Buffers and gRPC C++ development tools used for Part 1 (`protoc`, `grpc_cpp_plugin`, headers, and libraries).

## Important validation note

The source environment used to prepare this archive did not have `protoc`, `grpc_cpp_plugin`, or the gRPC/Protobuf development headers installed. Therefore protobuf generation and compilation could not be run here. Run the commands above in the assignment's configured environment before treating this as a verified build.

## Suggested multi-client checks

1. Start one server and two clients with separate mount directories.
2. Mount both clients with empty directories; create a supported file in client 1 and verify it appears on the server and client 2.
3. Modify the file in client 2 and verify the newer mtime version wins everywhere.
4. Hold a writer lock from one client and verify a second client's write attempt returns `RESOURCE_EXHAUSTED`.
5. Delete a file from client 1 and verify the server and client 2 remove it.
6. Put a file in a client mount before mounting and verify initial reconciliation uploads or fetches the appropriate version.
7. Check empty files, binary files, and files larger than `DFS_BUFFERSIZE`.

The assignment's mtime rule is implemented literally: if both copies have equal mtimes, reconciliation does nothing, even if their CRCs differ.
