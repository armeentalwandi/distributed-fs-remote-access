# distributed-fs-remote-access
Distributed File System - Remote Access Mode

### Description
A transparent, FUSE-backed distributed file system that forwards all file operations to a remote server via RPC.

In this first part of this RPC-backed DFS, I built a remote-acess model on top of FUSE and a custom RPC library. Every filesystem call such as create, open, read, write, truncate, getattr, fsync, etc is packaged into an RPC, sent to a central server, and executed there against the local filesystem. Results are returned seamlessly to the client. 

### Key Features
1. RPC Client/Server in C++ for all file operations
2. Implements multiple calls over RPC such as getattr, mknod, open/release, read/write, truncate, fsync
3. Correct handling of errno and return codes for robust error reporting
