#define _GNU_SOURCE
#include <unistd.h>
#include <dlfcn.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <errno.h>

//gcc -shared -fPIC hack.c -o hack.so -ldl -lpthread

// example:
//  LD_PRELOAD=$PWD/hack.so ./test f2 f3
static int debug_flag = 0;

typedef long long go_int;
typedef double go_float64;
typedef struct{void *arr; go_int len; go_int cap;} go_slice;
typedef struct{const char *p; go_int len;} go_str;

typedef void (*HackGo_open_cb_f_type)(go_int tid, go_str path, go_int fd);
HackGo_open_cb_f_type HackGo_open_cb = NULL;

typedef void (*HackGo_close_cb_f_type)(go_int tid, go_int fd);
HackGo_close_cb_f_type HackGo_close_pre_cb = NULL;
HackGo_close_cb_f_type HackGo_close_post_cb = NULL;

typedef void (*HackGo_rw_cb_f_type)(go_int tid, go_int rw_type, go_int fd, go_int sz);
HackGo_rw_cb_f_type HackGo_rw_pre_cb = NULL;;
HackGo_rw_cb_f_type HackGo_rw_post_cb = NULL;;

typedef go_int (*HackGo_prw_pre_cb_f_type)(go_int tid, go_int rw_type, go_int fd, go_int offset, go_int sz);
HackGo_prw_pre_cb_f_type HackGo_prw_pre_cb = NULL;;

typedef void (*HackGo_prw_post_cb_f_type)(go_int tid, go_int rw_type, go_int fd, go_int offset, go_int sz, 
    go_int leftBytes, go_int fastPathFlag01);
HackGo_prw_post_cb_f_type HackGo_prw_post_cb = NULL;;

/*
## :-)
open
// fdopen
fopen //filen
creat
openat

close
fclose

pread
fread_unlocked
write
pwrite

## :-|
fsync
fdatasync
sync_file_range

fallocate
readahead

##
fprintf // only log printing
fflush  // only log printing
*/

typedef void (*orig_abort_f_type) ();
orig_abort_f_type orig_abort = NULL;

void assert(int b){
    if(orig_abort == NULL){
        orig_abort = dlsym(RTLD_NEXT, "abort");
    }
    if(b==0){
        printf("my abort in hack.so");
        orig_abort();
    }
}

go_int my_gettid(){
    return (go_int)syscall(SYS_gettid);
}

pthread_mutex_t openMutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t readMutex = PTHREAD_MUTEX_INITIALIZER;
int64_t readBytesReq = 0;
int64_t readBytesActual = 0;
int64_t readCt=0;

pthread_mutex_t writeMutex = PTHREAD_MUTEX_INITIALIZER;
int64_t writeBytesReq = 0;
int64_t writeBytesActual = 0;
int64_t writeCt=0;

const int64_t myMod = 1000;

void try_if_need_load_go_cb(){
    if(HackGo_open_cb == NULL){
        void *handle;
        char *error;
        handle = dlopen ("./hack_go.so", RTLD_LAZY);
        if (!handle) {
            fputs (dlerror(), stderr);
            exit(1);
        }
        HackGo_open_cb = dlsym(handle, "HackGo_open_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
        HackGo_close_pre_cb = dlsym(handle, "HackGo_close_pre_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
        HackGo_close_post_cb = dlsym(handle, "HackGo_close_post_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
        HackGo_rw_pre_cb = dlsym(handle, "HackGo_rw_pre_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
        HackGo_rw_post_cb = dlsym(handle, "HackGo_rw_post_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
        HackGo_prw_pre_cb = dlsym(handle, "HackGo_prw_pre_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
        HackGo_prw_post_cb = dlsym(handle, "HackGo_prw_post_cb");
        if ((error = dlerror()) != NULL)  {
            fputs(error, stderr);
            exit(1);
        }
    }else{

    }
}

void open_cb(int fd, const char* path){
    try_if_need_load_go_cb();
    if(debug_flag)  
        printf(">>>>>open_cb:%s %d\n", path, fd);
    go_str str;
    str.p = path;
    str.len = strlen(path);
    HackGo_open_cb((go_int)(my_gettid()),str, fd);
}

void close_pre_cb(int fd){
    try_if_need_load_go_cb();
    HackGo_close_pre_cb((go_int)(my_gettid()),fd);
}

void close_post_cb(int fd){
    try_if_need_load_go_cb();
    HackGo_close_post_cb((go_int)(my_gettid()),fd);
}

void read_pre_cb(int fd, int offset, int bytes){
    try_if_need_load_go_cb();
    HackGo_rw_pre_cb((go_int)(my_gettid()),0, fd, bytes);

    int64_t bamt = 0;
    assert(0 == pthread_mutex_lock(&readMutex));
    readBytesReq = readBytesReq + bytes;
    bamt = readBytesReq;
    assert(0 == pthread_mutex_unlock(&readMutex));
}

void read_post_cb(int fd, int offset, int bytes){
    try_if_need_load_go_cb();
    HackGo_rw_post_cb((go_int)my_gettid(), 0, fd, bytes);

    int64_t bamt = 0;
    int64_t ct = 0;
    assert(0 == pthread_mutex_lock(&readMutex));
    readBytesActual = readBytesActual + bytes;
    bamt = readBytesActual;
    readCt++;
    ct = readCt;
    assert(0 == pthread_mutex_unlock(&readMutex));
    if(debug_flag){
        if(ct % myMod == 0)
            printf("\n<<r%dm>>\n", bamt/1024/1024);
    }
}

int pread_pre_cb(int fd, int64_t offset, int bytes){
    try_if_need_load_go_cb();
    go_int fastFlag = HackGo_prw_pre_cb((go_int)my_gettid(), 0, fd, (go_int)offset, bytes);

    int64_t bamt = 0;
    assert(0 == pthread_mutex_lock(&readMutex));
    readBytesReq = readBytesReq + bytes;
    bamt = readBytesReq;
    assert(0 == pthread_mutex_unlock(&readMutex));

    return (int)fastFlag;
}

void pread_post_cb(int fd, int64_t offset, int bytes, int leftBytes , int fastFlag){
    try_if_need_load_go_cb();
    HackGo_prw_post_cb((go_int)my_gettid(), 0, fd, (go_int)offset, bytes, leftBytes, fastFlag);

    int64_t bamt = 0;
    int64_t ct = 0;
    assert(0 == pthread_mutex_lock(&readMutex));
    readBytesActual = readBytesActual + bytes;
    bamt = readBytesActual;
    readCt++;
    ct = readCt;
    assert(0 == pthread_mutex_unlock(&readMutex));
    if(debug_flag){
        if(ct % myMod == 0)
            printf("\n<<r%dm>>\n", bamt/1024/1024);
    }
}

void write_pre_cb(int fd, int offset, int bytes){
    try_if_need_load_go_cb();
    HackGo_rw_pre_cb((go_int)my_gettid(), 1, fd, bytes);

    int64_t bamt = 0;
    assert(0 == pthread_mutex_lock(&writeMutex));
    writeBytesReq = writeBytesReq + bytes;
    bamt = writeBytesReq;
    assert(0 == pthread_mutex_unlock(&writeMutex));
}

void write_post_cb(int fd, int offset, int bytes){
    try_if_need_load_go_cb();
    HackGo_rw_post_cb((go_int)my_gettid(), 1, fd, bytes);

    int64_t bamt = 0;
    int64_t ct = 0;
    assert(0 == pthread_mutex_lock(&writeMutex));
    writeBytesActual = writeBytesActual + bytes;
    bamt = writeBytesActual;
    writeCt++;
    ct = writeCt;
    assert(0 == pthread_mutex_unlock(&writeMutex));
    if(debug_flag){
        if(ct % myMod == 0)
            printf("\n<<w%dm>>\n", bamt/1024/1024);
    }
}

int pwrite_pre_cb(int fd, int64_t offset, int bytes){
    try_if_need_load_go_cb();
    go_int fastFlag = HackGo_prw_pre_cb((go_int)my_gettid(), 1, fd, (go_int)offset, bytes);

    int64_t bamt = 0;
    assert(0 == pthread_mutex_lock(&writeMutex));
    writeBytesReq = writeBytesReq + bytes;
    bamt = writeBytesReq;
    assert(0 == pthread_mutex_unlock(&writeMutex));

    return (int)fastFlag;
}

void pwrite_post_cb(int fd, int64_t offset, int bytes, int leftBytes, int fastFlag){
    try_if_need_load_go_cb();
    HackGo_prw_post_cb((go_int)my_gettid(), 1, fd, (go_int)offset, bytes, leftBytes, fastFlag);

    int64_t bamt = 0;
    int64_t ct = 0;
    assert(0 == pthread_mutex_lock(&writeMutex));
    writeBytesActual = writeBytesActual + bytes;
    bamt = writeBytesActual;
    writeCt++;
    ct = writeCt;
    assert(0 == pthread_mutex_unlock(&writeMutex));
    if(debug_flag){
        if(ct % myMod == 0)
            printf("\n<<w%dm>>\n", bamt/1024/1024);
    }
}

typedef int (*orig_open_f_type) (const char *__file, int __oflag, mode_t mode);
orig_open_f_type orig_open = NULL;

int open (const char *__file, int __oflag, mode_t mode){
    const char* fname = "open";
    if(debug_flag)
        printf("%s():%d %d\n", fname, __file, (int)(mode));
    if(orig_open == NULL){
        orig_open = dlsym(RTLD_NEXT, fname);
    }
    int ret;
    if((__oflag && 00000100)||(__oflag && 020000000)){
        ret = orig_open(__file, __oflag,mode);
    } 
    else
        ret = orig_open(__file, __oflag,0);
    if(ret >= 0){
        open_cb(ret, __file);
    }
    return ret;
}

typedef int (*orig_openat_f_type) (int dirfd, const char *pathname, int flags, mode_t mode);
orig_openat_f_type orig_openat = NULL;

int openat (int dirfd, const char *__file, int flags, mode_t mode){
    const char* fname = "openat";
    if(debug_flag)
        printf("%s():%d %d\n", fname, __file, (int)(mode));
    if(orig_openat == NULL){
        orig_openat = dlsym(RTLD_NEXT, fname);
    }
    int ret;
    if((flags && 00000100)||(flags && 020000000)){
        ret = orig_openat(dirfd, __file, flags,mode);
    } 
    else
        ret = orig_openat(dirfd, __file, flags,0);
    if(ret >= 0){
        open_cb(ret, __file);
    }
    return ret;
}


typedef int (*orig_create_f_type) (const char *pathname, mode_t mode);
orig_create_f_type orig_create = NULL;

int create (const char *__file, mode_t mode){
    const char* fname = "create";
    if(debug_flag)
        printf("%s():%d %d\n", fname, __file, (int)(mode));
    if(orig_create == NULL){
        orig_create = dlsym(RTLD_NEXT, fname);
    }
    int ret;
    ret = orig_create(__file,mode);
    if(ret >= 0){
        open_cb(ret, __file);
    }
    return ret;
}

typedef int (*orig_fileno_f_type)(FILE *stream);
orig_fileno_f_type orig_fileno = NULL;
orig_fileno_f_type get_orig_fileno(){
    if(orig_fileno == NULL){
        orig_fileno = dlsym(RTLD_NEXT, "fileno");
    }
    return orig_fileno;
}

typedef FILE * (*orig_fopen_f_type) (const char *pathname, const char *mode);
orig_fopen_f_type orig_fopen = NULL;

FILE *fopen(const char *pathname, const char *mode){
    const char* fname = "fopen";
    if(debug_flag)
        printf("%s():%s %s\n", fname, pathname, mode);
    if(orig_fopen == NULL){
        orig_fopen = dlsym(RTLD_NEXT, fname);
    }
    FILE* fp = orig_fopen(pathname, mode);
    if(fp == NULL){
        return NULL;
    }
    int fd = get_orig_fileno()(fp);
    open_cb(fd, pathname);
    return fp;
}

typedef int (*orig_close_f_type) (int fd);
orig_close_f_type orig_close = NULL;

int close(int fd){
    const char* fname = "close";
    if(debug_flag)
        printf("%s():%d\n", fname, fd);
    if(orig_close == NULL){
        orig_close = dlsym(RTLD_NEXT, fname);
    }
    close_pre_cb(fd);
    int ret = orig_close(fd);
    if(0 == ret){
        close_post_cb(fd);
    }
    return ret;
}

int fclose(FILE *stream);
typedef int (*orig_fclose_f_type) (FILE *stream);
orig_fclose_f_type orig_fclose = NULL;

int fclose(FILE *stream){
    const char* fname = "fclose";
    int fd = get_orig_fileno()(stream);
    if(debug_flag)
        printf("%s():%d\n", fname, fd);
    if(orig_fclose == NULL){
        orig_fclose = dlsym(RTLD_NEXT, fname);
    }
    close_pre_cb(fd);
    int ret = orig_fclose(stream);
    // no need to check ret since after that the stream is unavailable anyway
    close_post_cb(fd);
    return ret;
}

typedef ssize_t (*orig_pread_f_type) (int fd, void *buf, size_t count, off_t offset);
orig_pread_f_type orig_pread = NULL;

ssize_t pread(int fd, void *buf, size_t count, off_t offset){
    if(debug_flag)
        printf("mypread():%d %d\n", fd, (int)(count));
    // NOTE!!!
    // race condition may occur here
    // in this demo we just ignore this
    // Please FIXME in serious application
    if(orig_pread == NULL){
        orig_pread = dlsym(RTLD_NEXT, "pread");
    }
    int fastPathFlag = pread_pre_cb(fd, (int64_t)offset, (int)count);
    ssize_t ct = orig_pread(fd, buf,count,offset);
    if(ct > 0){
        pread_post_cb(fd, (int64_t)offset, (int)ct, (int)count - (int)ct, fastPathFlag);
    }else{
        pread_post_cb(fd, (int64_t)offset, (int)0, (int)count, fastPathFlag);
    }
    return ct;
}

typedef size_t (*orig_fread_unlocked_f_type) (void *ptr, size_t size, size_t n, FILE *stream);
orig_fread_unlocked_f_type orig_fread_unlocked = NULL;

typedef long (*orig_ftell_f_type) (FILE *stream);
orig_ftell_f_type orig_ftell = NULL;

size_t fread_unlocked(void *ptr, size_t size, size_t n,FILE *stream){
    const char* fname = "fread_unlocked";
    int fd = get_orig_fileno()(stream);
    if(debug_flag)
        printf("%s(): fd:%d sz:%d\n", fname, fd, (int)size*(int)n);
    if(orig_fread_unlocked == NULL){
        orig_fread_unlocked = dlsym(RTLD_NEXT, fname);
    }
    if(orig_ftell == NULL){
        orig_ftell = dlsym(RTLD_NEXT, "ftell");
    }
    int64_t offset = ftell(stream);
    if(offset < 0){
        offset = 0;
    }
    //assert(offset >= 0);
    //read_pre_cb(fd, (int)size*(int)n);
    int fastpathFlag = pread_pre_cb(fd, offset,  (int)size*(int)n);
    int ret = orig_fread_unlocked(ptr, size, n, stream);
    if(ret > 0){
        //read_post_cb(fd, (int)size*(int)ret);
        pread_post_cb(fd, offset, (int)size*(int)ret, (int)size*(int)n - (int)size*(int)ret, fastpathFlag);
    }else{
        //read_post_cb(fd, (int)0);
        pread_post_cb(fd, offset, 0, (int)size*(int)n, fastpathFlag);
    }
    return ret;
}

typedef ssize_t (*orig_write_f_type)(int fd, const void *buf, size_t count);
orig_write_f_type orig_write = NULL;

typedef ssize_t (*orig_lseek_f_type)(int fd, off_t offset, int whence);
orig_lseek_f_type orig_lseek = NULL;

ssize_t write(int fd, const void *buf, size_t count){
    if(debug_flag)
        printf("mywrite():%d %d\n", fd, (int)(count));
    if(orig_write == NULL){
        orig_write=dlsym(RTLD_NEXT, "write");
    }
    if(orig_lseek == NULL){
        orig_lseek=dlsym(RTLD_NEXT, "lseek");
    }
    ssize_t offset = orig_lseek(fd, 0, SEEK_CUR);
    //if(debug_flag && offset < 0){
    //    printf("hack.so: %d %s", (int)offset, strerror(errno));
    //    fflush(stdout);
    //}
    if(offset < 0){
        // maybe this is not a normal file
        // just ignore this one
        offset = 0;
    }
    //assert(offset >= 0);
    // write_pre_cb(fd, count);
    int fastpathFlag = pwrite_pre_cb(fd, offset, count);
    ssize_t ret = orig_write(fd,buf,count);
    if(ret > 0){
        //write_post_cb(fd, (int)ret);
        pwrite_post_cb(fd, (int64_t)offset, (int)ret, count-(int)ret, fastpathFlag);
    }else{
        //write_post_cb(fd, (int)0);
        pwrite_post_cb(fd, (int64_t)offset, (int)0, count, fastpathFlag);
    }
    return ret;
}

typedef ssize_t (*orig_pwrite_f_type)(int fd, const void *buf, size_t count, off_t offset);
orig_pwrite_f_type orig_pwrite = NULL;

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset){
    const char* fname = "pwrite";
    if(debug_flag)
        printf("%s(): fd:%d sz:%d\n", fname, fd, (int)count);
    if(orig_pwrite == NULL){
        orig_pwrite = dlsym(RTLD_NEXT, fname);
    }
    int fastpathFlag = pwrite_pre_cb(fd, (int64_t)offset, count);
    ssize_t ret = orig_pwrite(fd, buf, count, offset);
    if(ret > 0){
        pwrite_post_cb(fd, (int64_t)offset, (int)ret, count-(int)ret, fastpathFlag);
    }else{
        pwrite_post_cb(fd, (int64_t)offset, (int)0, count, fastpathFlag);
    }
    return ret;
}

/*
typedef int (*orig_openat_f_type)(int dirfd, const char *pathname, int flags, mode_t mode);
orig_openat_f_type orig_openat = NULL;

int openat (int dirfd, const char *pathname, int flags, mode_t mode){
    printf("myopenat():%s %d\n", pathname, (int)(mode));
    if(orig_openat == NULL){
        orig_openat=dlsym(RTLD_NEXT, "openat");
    }
    if((flags && 00000100)||(flags && 020000000)){
        return orig_openat(dirfd,pathname, flags,mode);
    } else
    return orig_openat(dirfd,pathname, flags,0);
}
*/