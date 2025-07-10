//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "watdfs_client.h"

#include "debug.h"
INIT_LOG

#include <algorithm>  // for std::min

#include "rpc.h"



void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {
   
    int rpc_ret = rpcClientInit();

    if (rpc_ret < 0) {
    
        *ret_code = -EINVAL;
        return nullptr;
    }

    *ret_code = 0;
    return nullptr;
}

void watdfs_cli_destroy(void *userdata) {
    int ret = rpcClientDestroy();
    if (ret < 0) {
        DLOG("RPC client destroy failed with error '%d'", ret);
    }
}

// GET FILE ATTRIBUTES
int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
    DLOG("watdfs_cli_getattr called for '%s'", path);

    int ARG_COUNT = 3;

    void **args = new void *[ARG_COUNT];

    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;

    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;

    args[0] = (void *)path;


    arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)sizeof(struct stat);  // statbuf
    args[1] = (void *)statbuf;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    int retcode = 0;
    args[2] = &retcode;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);

        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }

    if (fxn_ret < 0) {
        memset(statbuf, 0, sizeof(struct stat));
    }

    delete[] args;
    return fxn_ret;
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    int ARG_COUNT = 4;
    void **args = new void *[ARG_COUNT];

    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;

    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
    args[1] = &mode;

    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = &dev;

    arg_types[3] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int retcode = 0;
    args[3] = &retcode;

    arg_types[4] = 0;

    int rpc_ret = rpcCall((char *)"mknod", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);

        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }

    delete[] args;
    return fxn_ret;
}

int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    int ARG_COUNT = 3;
    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) |
                   (ARG_CHAR << 16u) | (uint)sizeof(struct fuse_file_info);
    args[1] = (void *)fi;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int retcode = 0;
    args[2] = &retcode;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"open", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("open rpc failed with error '%d' for path '%s'", rpc_ret, path);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }
    delete[] args;
    return fxn_ret;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
    int ARG_COUNT = 3;
    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)sizeof(struct fuse_file_info);
    args[1] = (void *)fi;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int retcode = 0;
    args[2] = &retcode;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"release", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("release rpc failed with error '%d' for path '%s'", rpc_ret, path);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }

    delete[] args;
    return fxn_ret;
}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    ssize_t total_bytes_read = 0;
    size_t bytes_remaining = size;
    off_t current_offset = offset;

    while (bytes_remaining > 0) {
        size_t bytes_to_read =
            std::min<size_t>(bytes_remaining, MAX_ARRAY_LEN);

        int ARG_COUNT = 6;
        void **args = new void *[ARG_COUNT];
        int arg_types[ARG_COUNT + 1];


        int pathlen = strlen(path) + 1;
        arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) |
                       (ARG_CHAR << 16u) | (uint)pathlen;
        args[0] = (void *)path;

        arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) |
                       (ARG_CHAR << 16u) | (uint)bytes_to_read;
        args[1] = (void *)(buf + total_bytes_read);  

        arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        args[2] = &bytes_to_read;

        arg_types[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        args[3] = &current_offset;

        arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) |
                       (ARG_CHAR << 16u) | (uint)sizeof(struct fuse_file_info);
        args[4] = (void *)fi;

        arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        int retcode = 0;
        args[5] = &retcode;

        arg_types[6] = 0;

        int rpc_ret = rpcCall((char *)"read", arg_types, args);

        if (rpc_ret < 0) {
            delete[] args;
            return -EINVAL;
        }

        if (retcode < 0) {
            delete[] args;
            return retcode;  
        }

        int bytes_read = retcode;
        if (bytes_read == 0) {  
            delete[] args;
            return total_bytes_read;
        }

        total_bytes_read += bytes_read;
        bytes_remaining -= bytes_read;
        current_offset += bytes_read;

        delete[] args;

        if ((size_t)bytes_read < bytes_to_read) {
            break;
        }
    }

    return total_bytes_read;
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    ssize_t total_written = 0;
    size_t bytes_remaining = size;
    const char *current_buf = buf;

    while (bytes_remaining > 0) {
          
        size_t chunk_size = std::min<size_t>(bytes_remaining, MAX_ARRAY_LEN);
        int ARG_COUNT = 6;
        void **args = new void *[ARG_COUNT];
        int arg_types[ARG_COUNT + 1];


        int pathlen = strlen(path) + 1;
        arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | pathlen;
        args[0] = (void *)path;

        arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) |
                       (ARG_CHAR << 16u) | chunk_size;
        args[1] = (void *)current_buf;


        arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        args[2] = &chunk_size;


        arg_types[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        args[3] = &offset;

        arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) |
                       (ARG_CHAR << 16u) | sizeof(*fi);
        args[4] = (void *)fi;

        arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        int retcode = 0;
        args[5] = &retcode;

        arg_types[6] = 0;

        int rpc_ret = rpcCall((char *)"write", arg_types, args);
        delete[] args;

        if (rpc_ret < 0 || retcode < 0) {
            return rpc_ret < 0 ? rpc_ret : retcode;
        }

        ssize_t written = retcode;
        if (written <= 0) break;

        total_written += written;
        bytes_remaining -= written;
        current_buf += written;
        offset += written;
    }

    return total_written > 0 ? total_written : -EIO;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    int ARG_COUNT = 3;
    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];


    int pathlen = strlen(path) + 1;
    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;


    arg_types[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[1] = &newsize;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int retcode = 0;
    args[2] = &retcode;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"truncate", arg_types, args);
    int fxn_ret = 0;

    if (rpc_ret < 0) {
        DLOG("truncate rpc failed for '%s' with error %d", path, rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }

    delete[] args;
    return fxn_ret;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    int ARG_COUNT = 3;
    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];



    int pathlen = strlen(path) + 1;
    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;


    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)sizeof(*fi);
    args[1] = (void *)fi;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int retcode = 0;
    args[2] = &retcode;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"fsync", arg_types, args);
    int fxn_ret = 0;

    if (rpc_ret < 0) {
        DLOG("fsync rpc failed for '%s' with error %d", path, rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }

    delete[] args;
    return fxn_ret;
}


int watdfs_cli_utimensat(void *userdata, const char *path,
                         const struct timespec ts[2]) {
    int ARG_COUNT = 3;
    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];


    int pathlen = strlen(path) + 1;
    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    size_t ts_size = 2 * sizeof(struct timespec);
    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)ts_size;
    args[1] = (void *)ts;


    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int retcode = 0;
    args[2] = &retcode;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"utimensat", arg_types, args);
    int fxn_ret = 0;

    if (rpc_ret < 0) {
        DLOG("utimensat rpc failed for '%s' with error %d", path, rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = retcode;
    }

    delete[] args;
    return fxn_ret;
}
