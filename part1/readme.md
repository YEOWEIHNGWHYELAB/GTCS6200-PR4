# Part 1

## Command

Generate Proto: `make protos`

Build: `make`

## Testing

Setup server alias: `alias dfsserver='/cs6200/bin/dfs-server-p1 -m /cs6200/mnt/server'`

Setup client alias: `alias dfsclient='/cs6200/bin/dfs-client-p1 -m /cs6200/mnt/client'`

### Server

`dfsserver`

### Client

#### Test 1: Store a text file

`echo "Hello gRPC" > /cs6200/mnt/client/test.txt`

Note that default mount location client look for is at "/cs6200/part1/mnt/client", so you need to specify -m for the custom mount path

`dfsclient store test.txt`

#### Test 2: List remote files

`dfsclient list`

#### Test 3: Fetch the file

`rm /cs6200/mnt/client/test.txt`

`dfsclient fetch test.txt`

`cat /cs6200/mnt/client/test.txt`

#### Test 4: Delete remote file

`dfsclient list`

`dfsclient delete test.txt`

`dfsclient list`

#### Test 5: Test binary files

`cp /bin/ls /cs6200/mnt/client/binary-test`

`dfsclient store binary-test`

`rm /cs6200/mnt/client/binary-test`

`dfsclient fetch binary-test`

`sha256sum /bin/ls /cs6200/mnt/client/binary-test`

#### Test 6: Stat

`echo "hello world" > /cs6200/mnt/client/test.txt`

`dfsclient store test.txt`

`dfsclient stat test.txt`

#### Test 7: Deadline

Temporarily delay the server, in dfslib-servernode-p1.cpp, find the GetFileStatus RPC implementation

At the beginning of that method, temporarily add:

```C++
std::this_thread::sleep_for(std::chrono::seconds(3));
```

`dfsclient -t 500 stat test.txt`
