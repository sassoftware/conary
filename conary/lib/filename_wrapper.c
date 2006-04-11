/*
 * Copyright (c) 2005 rPath, Inc.
 *
 * This program is distributed under the terms of the Common Public License,
 * version 1.0. A copy of this license should have been distributed with this
 * source file in a file called LICENSE. If it is not present, the license
 * is always available at http://www.opensource.org/licenses/cpl.php.
 *
 * This program is distributed in the hope that it will be useful, but
 * without any warranty; without even the implied warranty of merchantability
 * or fitness for a particular purpose. See the Common Public License for
 * full details.
 *
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <utime.h>
#include <dirent.h>

#define PRINTF(...)
/* #define PRINTF(...) fprintf(stderr, __VA_ARGS__) */

#define GET_PATH(name)	PRINTF("[wrp %5d] %s %s\n", getpid(), #name, pathname); \
			if (!real_##name) real_##name = dlsym(RTLD_NEXT, #name); \
			p = prepend_destdir(pathname) ;

#define PUT_PATH(rval)	free((void *)p); \
			if (ret != rval || errno != ENOENT) return ret;

static const char *prepend_destdir(const char *pathname) {
    char *p = NULL;
    char *destdir = NULL;
    char *wrapdir = NULL;
    int destlen = 0;
    
    if (pathname[0] != '/') return NULL;
    if (!destdir) {
        destdir = getenv("DESTDIR");
        if (!destdir) {
            return NULL;
        }
        destlen = strlen(destdir);
    }

    wrapdir = getenv("WRAPDIR");
    /* if we're asked to "wrap" just one subdirectory tree and this is
       not one of those, bail out */
    if (wrapdir) {
	int wlen, plen;
	wlen = strlen(wrapdir);
	plen = strlen(pathname);
	if (plen < wlen)
	    return NULL;
	if (strncmp(wrapdir, pathname, wlen))
	    return NULL;
    }

    p = (char *) malloc(strlen(pathname)+destlen+1);
    strcpy(p, destdir);
    strcat(p, pathname);
    return p;
}

/* syscalls that have a one-one mapping to eponymous library calls.
 * N.B. this will NOT override every use of the system call; the
 * C library will still call the syscall directly from other calls.
 * For example, opendir() uses the open syscall directly, and this
 * open() library call wrapper isn't enough; we also have to wrap the
 * opendir() C library call.
 */

int access(const char *pathname, int mode) {
    static int (*real_access)(const char *pathname, int mode) = NULL;
    const char *p;
    int ret;

    GET_PATH(access);   
    if (p) {
	ret = real_access(p, mode);
	PUT_PATH(-1);
    }
    return real_access(pathname, mode);
}

int open(const char *pathname, int flags, mode_t mode) {
    static int (*real_open)(const char *pathname, int flags, mode_t mode) = NULL;
    const char *p;
    int ret;

    GET_PATH(open);
    if (p) {
	ret = real_open(p, flags, mode);
	PUT_PATH(-1);
    }
    return real_open(pathname, flags, mode);
}

int open64(const char *pathname, int flags, mode_t mode) {
    static int (*real_open64)(const char *pathname, int flags, mode_t mode) = NULL;
    const char *p;
    int ret;

    GET_PATH(open64);
    if (p) {
	ret = real_open64(p, flags, mode);
	PUT_PATH(-1);
    }
    return real_open64(pathname, flags, mode);
}

int stat(const char *pathname, struct stat *buf) {
    static int (*real_stat)(const char *pathname, struct stat *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(stat);
    if (p) {
	ret = real_stat(p, buf);
	PUT_PATH(-1);
    }
    return real_stat(pathname, buf);
}

int stat64(const char *pathname, struct stat64 *buf) {
    static int (*real_stat64)(const char *pathname, struct stat64 *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(stat64);
    if (p) {
	ret = real_stat64(p, buf);
	PUT_PATH(-1);
    }
    return real_stat64(pathname, buf);
}

int lstat(const char *pathname, struct stat *buf) {
    static int (*real_lstat)(const char *pathname, struct stat *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(lstat);
    if (p) {
	ret = real_lstat(p, buf);
	PUT_PATH(-1);
    }
    return real_lstat(pathname, buf);
}

int lstat64(const char *pathname, struct stat64 *buf) {
    static int (*real_lstat64)(const char *pathname, struct stat64 *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(lstat64);
    if (p) {
	ret = real_lstat64(p, buf);
	PUT_PATH(-1);
    }
    return real_lstat64(pathname, buf);
}

int __xstat(int ver, const char *pathname, struct stat *buf) {
    static int (*real___xstat)(int ver, const char *pathname, struct stat *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(__xstat);
    if (p) {
	ret = real___xstat(ver, p, buf);
	PUT_PATH(-1);
    }
    return real___xstat(ver, pathname, buf);
}
    
int __xstat64(int ver, const char *pathname, struct stat64 *buf) {
    static int (*real___xstat64)(int ver, const char *pathname, struct stat64 *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(__xstat64);
    if (p) {
	ret = real___xstat64(ver, p, buf);
	PUT_PATH(-1);
    }
    return real___xstat64(ver, pathname, buf);
}

int __lxstat(int ver, const char *pathname, struct stat *buf) {
    static int (*real___lxstat)(int ver, const char *pathname, struct stat *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(__lxstat);
    if (p) {
	ret = real___lxstat(ver, p, buf);
	PUT_PATH(-1);
    }
    return real___lxstat(ver, pathname, buf);
}

int __lxstat64(int ver, const char *pathname, struct stat64 *buf) {
    static int (*real___lxstat64)(int ver, const char *pathname, struct stat64 *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(__lxstat64);
    if (p) {
	ret = real___lxstat64(ver, p, buf);
	PUT_PATH(-1);
    }
    return real___lxstat64(ver, pathname, buf);
}

int chdir(const char *pathname) {
    static int (*real_chdir)(const char *pathname) = NULL;
    const char *p;
    int ret;

    GET_PATH(chdir);
    if (p) {
	ret = real_chdir(p);
	PUT_PATH(-1);
    }
    return real_chdir(pathname);
}

int mkdir(const char *pathname, mode_t mode) {
    static int (*real_mkdir)(const char *pathname, mode_t mode) = NULL;
    const char *p;
    int ret;

    GET_PATH(mkdir);
    if (p) {
	ret = real_mkdir(p, mode);
	PUT_PATH(-1);
    }
    return real_mkdir(pathname, mode);
}
    
int rmdir(const char *pathname) {
    static int (*real_rmdir)(const char *pathname) = NULL;
    const char *p;
    int ret;

    GET_PATH(rmdir);
    if (p) {
	ret = real_rmdir(p);
	PUT_PATH(-1);
    }
    return real_rmdir(pathname);
}

int chmod(const char *pathname, mode_t mode) {
    static int (*real_chmod)(const char *pathname, mode_t mode) = NULL;
    const char *p;
    int ret;

    GET_PATH(chmod);
    if (p) {
	ret = real_chmod(p, mode);
	PUT_PATH(-1);
    }
    return real_chmod(pathname, mode);
}

int lchmod(const char *pathname, mode_t mode) {
    static int (*real_lchmod)(const char *pathname, mode_t mode) = NULL;
    const char *p;
    int ret;

    GET_PATH(lchmod);
    if (p) {
	ret = real_lchmod(p, mode);
	PUT_PATH(-1);
    }
    return real_lchmod(pathname, mode);
}

int chown(const char *pathname, uid_t owner, gid_t group) {
    static int (*real_chown)(const char *pathname, uid_t owner, gid_t group) = NULL;
    const char *p;
    int ret;

    GET_PATH(chown);
    if (p) {
	ret = real_chown(p, owner, group);
	PUT_PATH(-1);
    }
    return real_chown(pathname, owner, group);
}

int lchown(const char *pathname, uid_t owner, gid_t group) {
    static int (*real_lchown)(const char *pathname, uid_t owner, gid_t group) = NULL;
    const char *p;
    int ret;

    GET_PATH(lchown);
    if (p) {
	ret = real_lchown(p, owner, group);
	PUT_PATH(-1);
    }
    return real_lchown(pathname, owner, group);
}

int unlink(const char *pathname) {
    static int (*real_unlink)(const char *pathname) = NULL;
    const char *p;
    int ret;

    GET_PATH(unlink);
    if (p) {
	ret = real_unlink(p);
	free((void*)p);
	if (ret == 0)
	    return ret;
    }
    return real_unlink(pathname);
}

int symlink(const char *oldpath, const char *pathname) {
    static int (*real_symlink)(const char *oldpath, const char *pathname) = NULL;
    const char *p;
    int ret;

    GET_PATH(symlink);
    if (p) {
	ret = real_symlink(oldpath, p);
	PUT_PATH(-1);
    }
    return real_symlink(oldpath, pathname);
}

int utime(const char *pathname, const struct utimbuf *buf) {
    static int (*real_utime)(const char *pathname, const struct utimbuf *buf) = NULL;
    const char *p;
    int ret;

    GET_PATH(utime);
    if (p) {
	ret = real_utime(p, buf);
	PUT_PATH(-1);
    }
    return real_utime(pathname, buf);
}

int utimes(const char *pathname, const struct timeval tv[2]) {
    static int (*real_utimes)(const char *pathname, const struct timeval tv[2]) = NULL;
    const char *p;
    int ret;

    GET_PATH(utimes);
    if (p) {
	ret = real_utimes(p, tv);
	PUT_PATH(-1);
    }
    return real_utimes(pathname, tv);
}

int readlink(const char *pathname, char *buf, size_t bufsiz) {
    static int (*real_readlink)(const char *pathname, char *buf, size_t bufsiz) = NULL;
    const char *p;
    int ret;

    GET_PATH(readlink);
    if (p) {
	ret = real_readlink(p, buf, bufsiz);
	PUT_PATH(-1);
    }
    return real_readlink(pathname, buf, bufsiz);
}


/* These ones are more... complex, shall we say */
int link(const char *oldpath, const char *pathname) {
    static int (*real_link)(const char *oldpath, const char *pathname) = NULL;
    const char *op, *pn;
    int ret;

    PRINTF("[wrp %5d] %s %s %s\n", getpid(), "link", oldpath, pathname);   
    if (!real_link) real_link = dlsym(RTLD_NEXT, "link"); 
    /* we can't use the std macros employed by all the other
       fuunctions because of the extra work we have to do around with
       wrapping both pathnames... */
    op = prepend_destdir(oldpath);
    pn = prepend_destdir(pathname);

    if (pn) {
	ret = real_link(op ? op : oldpath, pn);
	/* putpath */
	free((void *)pn);
	if (ret == 0) {
	    if (op) free((void *)op);
	    return ret;
	}
    }
    ret = real_link(op ? op : oldpath, pathname);
    if (op) free((void *)op);
    return ret;
}

int rename(const char *oldpath, const char *pathname) {
    static int (*real_rename)(const char *oldpath, const char *pathname) = NULL;
    const char *op, *pn;
    int ret;

    PRINTF("[wrp %5d] %s %s %s\n", getpid(), "rename", oldpath, pathname);   
    if (!real_rename) real_rename = dlsym(RTLD_NEXT, "rename"); 
    /* we can't use the std macros employed by all the other
       fuunctions because of the extra work we have to do around with
       wrapping both pathnames... */
    op = prepend_destdir(oldpath);
    pn = prepend_destdir(pathname);

    if (pn) {
	ret = real_rename(op ? op : oldpath, pn);
	/* putpath */
	free((void *)pn);
	if (ret == 0) {
	    if (op) free((void *)op);
	    return ret;
	}
    }
    ret = real_rename(op ? op : oldpath, pathname);
    if (op) free((void *)op);
    return ret;
}

/* C library bits that do not directly map to a syscall */

DIR *opendir(const char *pathname) {
    static DIR * (*real_opendir)(const char *pathname) = NULL;
    const char *p;
    DIR *ret;

    GET_PATH(opendir);
    if (p) {
	ret = real_opendir(p);
	PUT_PATH(NULL);
    }
    return real_opendir(pathname);
}

void *dlopen(const char *pathname, int flags) {
    static void * (*real_dlopen)(const char *pathname, int flags) = NULL;
    const char *p;
    void *ret;

    /* If the value of file is 0, dlopen() shall provide a handle on a global symbol object. */
    if (pathname == NULL)
	return real_dlopen(pathname, flags);
    
    GET_PATH(dlopen);
    if (p) {
	ret = real_dlopen(p, flags);
	PUT_PATH(NULL);
    }
    return real_dlopen(pathname, flags);
}

FILE *fopen(const char *pathname, const char *mode) {
    static FILE * (*real_fopen)(const char *pathname, const char *mode) = NULL;
    const char *p;
    FILE *ret;

    GET_PATH(fopen);
    if (p) {
	ret = real_fopen(p, mode);
	PUT_PATH(NULL);
    }
    return real_fopen(pathname, mode);
}

FILE *freopen(const char *pathname, const char *mode, FILE *stream) {
    static void * (*real_freopen)(const char *pathname, const char *mode, FILE *stream) = NULL;
    const char *p;
    FILE *ret;

    GET_PATH(freopen);
    if (p) {
	ret = real_freopen(p, mode, stream);
	PUT_PATH(NULL);
    }
    return real_freopen(pathname, mode, stream);
}

