/*
 * Copyright (c) 2005 Specifix, Inc.
 *
 * This program is distributed under the terms of the Common Public License,
 * version 1.0. A copy of this license should have been distributed with this
 * source file in a file called LICENSE. If it is not present, the license
 * is always available at http://www.opensource.org/licenses/cpl.php.
 *
 * This program is distributed in the hope that it will be useful, but
 * without any waranty; without even the implied warranty of merchantability
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
#include <unistd.h>

#define GET_REAL(name) if (!real_##name) real_##name = dlsym(RTLD_NEXT, #name);
#define GET_PATH() p = prepend_destdir(pathname)
#define PUT_PATH() free((void *)p)


static const char *prepend_destdir(const char *pathname) {
    char *p;
    static int destlen;
    static char *destdir;
    static int init;

    if (pathname[0] != '/') return NULL;
    if (!destdir && init) return NULL;
    if (!destdir) {
        init = 1;
        destdir = getenv("DESTDIR");
        if (!destdir) {
            return NULL;
        }
        destlen = strlen(destdir);
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

int open(const char *pathname, int flags) {
    static int (*real_open)(const char *pathname, int flags);
    const char *p;
    int ret;

    GET_REAL(open);
    GET_PATH();
    if (p) ret = real_open(p, flags);
    PUT_PATH();
    if (ret != -1 || errno != ENOENT) return ret;

    return real_open(pathname, flags);
}

int stat(const char *pathname, struct stat *buf) {
    static int (*real_stat)(const char *pathname, struct stat *buf);
    const char *p;
    int ret;

    GET_REAL(stat);
    GET_PATH();
    if (p) ret = real_stat(p, buf);
    PUT_PATH();
    if (ret != -1 || errno != ENOENT) return ret;

    return real_stat(pathname, buf);
}

int lstat(const char *pathname, struct stat *buf) {
    static int (*real_lstat)(const char *pathname, struct stat *buf);
    const char *p;
    int ret;

    GET_REAL(lstat);
    GET_PATH();
    if (p) ret = real_lstat(p, buf);
    PUT_PATH();
    if (ret != -1 || errno != ENOENT) return ret;

    return real_lstat(pathname, buf);
}

int chmod(const char *pathname, mode_t mode) {
    static int (*real_chmod)(const char *pathname, mode_t mode);
    const char *p;
    int ret;

    GET_REAL(chmod);
    GET_PATH();
    if (p) ret = real_chmod(p, mode);
    PUT_PATH();
    if (ret != -1 || errno != ENOENT) return ret;

    return real_chmod(pathname, mode);
}

int unlink(const char *pathname) {
    static int (*real_unlink)(const char *pathname);
    const char *p;
    int ret;

    GET_REAL(unlink);
    GET_PATH();
    if (p) ret = real_unlink(p);
    PUT_PATH();
    if (ret != 0 || errno != ENOENT) return ret;

    return real_unlink(pathname);
}


/* C library bits that do not directly map to a syscall */

int opendir(const char *pathname) {
    static int (*real_opendir)(const char *pathname);
    const char *p;
    int ret;

    GET_REAL(opendir);
    GET_PATH();
    if (p) ret = real_opendir(p);
    PUT_PATH();
    if (ret != 0 || errno != ENOENT) return ret;

    return real_opendir(pathname);
}

void *dlopen(const char *pathname, int flags) {
    static void * (*real_dlopen)(const char *pathname, int flags);
    const char *p;
    void *ret;

    GET_REAL(dlopen);
    GET_PATH();
    if (p) ret = real_dlopen(p, flags);
    PUT_PATH();
    if (ret != NULL || errno != ENOENT) return ret;

    return real_dlopen(pathname, flags);
}

FILE *fopen(const char *pathname, const char *mode) {
    static void * (*real_fopen)(const char *pathname, const char *mode);
    const char *p;
    FILE *ret;

    GET_REAL(fopen);
    GET_PATH();
    if (p) ret = real_fopen(p, mode);
    PUT_PATH();
    if (ret != NULL || errno != ENOENT) return ret;

    return real_fopen(pathname, mode);
}

FILE *freopen(const char *pathname, const char *mode, FILE *stream) {
    static void * (*real_freopen)(const char *pathname, const char *mode, FILE *stream);
    const char *p;
    FILE *ret;

    GET_REAL(freopen);
    GET_PATH();
    if (p) ret = real_freopen(p, mode, stream);
    PUT_PATH();
    if (ret != NULL || errno != ENOENT) return ret;

    return real_freopen(pathname, mode, stream);
}

