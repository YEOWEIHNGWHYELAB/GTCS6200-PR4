# Part 2

## Command

Generate Proto: `make protos`

Build: `make`

## Testing

We need to create the following:

- Server storage
- Client 1's local mount
- Client 2's local mount

### Creating Client and Server Storage

```
mkdir -p /tmp/dfs-test/server
mkdir -p /tmp/dfs-test/client1
mkdir -p /tmp/dfs-test/client2
```

### Start Server & Client

#### Terminal 1 — Server

```
cd /cs6200
./bin/dfs-server-p2 \
  -a 0.0.0.0:61277 \
  -m /tmp/dfs-test/server \
  -d 2
```

#### Terminal 2 — Client 1

```
cd /cs6200
./bin/dfs-client-p2 \
  -a 127.0.0.1:61277 \
  -m /tmp/dfs-test/client1 \
  -d 2 mount
```

#### Terminal 3 — Client 2

```
cd /cs6200
./bin/dfs-client-p2 \
  -a 127.0.0.1:61277 \
  -m /tmp/dfs-test/client2 \
  -d 2 mount
```

### Basic File Sync


#### Terminal 4

Create File: `echo "Hello from Client 1" > /tmp/dfs-test/client1/hello.txt`

Verify Directories:

```
ls -l /tmp/dfs-test/server/
ls -l /tmp/dfs-test/client1/
ls -l /tmp/dfs-test/client2/
```

There should be a hello.txt across all 3 directories

### Test Modification Propagation

#### Using Terminal 4

##### Modify Client 2 File

```
sleep 2
echo "Updated by Client 2" >> /tmp/dfs-test/client2/hello.txt
```

##### Check File Content

```
cat /tmp/dfs-test/server/hello.txt
cat /tmp/dfs-test/client1/hello.txt
cat /tmp/dfs-test/client2/hello.txt
```

### Test Deletion Propagation

#### Terminal 4


Delete Client1: `rm /tmp/dfs-test/client1/hello.txt`

##### Verify Deletion

```
ls -l /tmp/dfs-test/server/
ls -l /tmp/dfs-test/client1/
ls -l /tmp/dfs-test/client2/
```

### Test Initial Sync

For this, terminate all clients, then

Add File From Client 1: `echo "Existing local file" > /tmp/dfs-test/client1/existing.txt`

Restart Client 1:

```
./bin/dfs-client-p2 \
  -a 127.0.0.1:61277 \
  -m /tmp/dfs-test/client1 \
  -d 2 mount
```

Now look at Server: `cat /tmp/dfs-test/server/existing.txt`

### Conflict Resolution

Stop both clients

Create different versions of the same file:

```
echo "Server version" > /tmp/dfs-test/server/conflict.txt

echo "Client version" > /tmp/dfs-test/client1/conflict.txt
```

Make the client copy newer: `touch /tmp/dfs-test/client1/conflict.txt`

Restart Client 1:

```
./bin/dfs-client-p2 \
  -a 127.0.0.1:61277 \
  -m /tmp/dfs-test/client1 \
  -d 2 mount
```

We expected:

- Client 1 has the newer timestamp.
- Server should receive Client 1's version.
- The resulting server and client contents should match.

You can inspect timestamps with:

```
stat /tmp/dfs-test/server/conflict.txt
stat /tmp/dfs-test/client1/conflict.txt
```
