#
# Copyright (c) 2004-2009 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

SUBDIRS = epdb_embedded ext http

python_files=$(wildcard *.py)
python_modules=elf.so cstreams.so SHA256_nonstandard.so _versions.so
wrapper_libs=filename_wrapper.so
wrapper_objs=filename_wrapper.o
cstreamsmodule_objs=cstreams.o numstream.o streamset.o stringstream.o

libelf = $(shell \
if [ -x /usr/bin/pkg-config ] && /usr/bin/pkg-config --exists libelf; then \
    pkg-config --cflags --libs libelf; \
elif [ -d /usr/include/libelf-lgpl ]; then \
    echo "-I/usr/include/libelf-lgpl -lelf-lgpl"; \
elif [ -d ${SYSROOT}/usr/include/libelf-lgpl ]; then \
    echo "-I${SYSROOT}/usr/include/libelf-lgpl -lelf-lgpl"; \
elif [ -d /usr/include/libelf ] && grep 'GNU \(Library\|Lesser\) General Public' /usr/include/libelf/libelf.h >/dev/null; then\
    echo "-I/usr/include/libelf -lelf"; \
elif [ -d ${SYSROOT}/usr/include/libelf ] && grep 'GNU \(Library\|Lesser\) General Public' ${SYSROOT}/usr/include/libelf/libelf.h >/dev/null; then\
    echo "-I${SYSROOT}/usr/include/libelf -lelf"; \
elif [ -d /usr/include ] && grep 'GNU \(Library\|Lesser\) General Public' /usr/include/libelf.h >/dev/null; then\
    echo "-lelf"; \
elif [ -d ${SYSROOT}/usr/include ] && grep 'GNU \(Library\|Lesser\) General Public' ${SYSROOT}/usr/include/libelf.h >/dev/null; then\
    echo "-lelf"; \
else \
    echo 'No appropriate libelf found' >&2 ;\
fi)


all: default-subdirs $(python_modules) $(wrapper_libs)

$(python_modules): %.so: %.o
	$(CC) $(LDFLAGS) -shared -rdynamic -o $@ $^ $(LIBS)

elf.o: elf.c
	@if [ -z "$(libelf)" ] ; then \
	    echo 'Stopping build due to missing libelf' ; exit 1 ; fi
	$(CC) $(CFLAGS) $(libelf) -c -o $@ $^

SHA256.o: SHA256_nonstandard.c
	$(CC) $(CFLAGS) -fno-strict-aliasing -DNDEBUG -fwrapv -O3 -Wstrict-prototypes -c -o $@ $^

elf.so: LIBS = $(libelf)
SHA256_nonstandard.so: LIBS = -pthread
$(cstreamsmodule_objs): cstreams.h
cstreams.so: $(cstreamsmodule_objs)


filename_wrapper.so: filename_wrapper.c
	$(CC) -Wall -fPIC -rdynamic -c filename_wrapper.c
	$(CC) $(LDFLAGS) -shared -rdynamic -o filename_wrapper.so filename_wrapper.o -lc -ldl

install: all install-subdirs pyfiles-install
	mkdir -p $(DESTDIR)$(sitedir)/$(DIR)
	install -m 755 $(python_modules) $(DESTDIR)$(sitedir)/$(DIR)
	mkdir -p $(DESTDIR)$(conarylibdir)
	install -m 755 $(wrapper_libs) $(DESTDIR)$(conarylibdir)/

clean: clean-subdirs default-clean

dist: default-dist

include ../../Make.rules
# needs to come after including Make.rules so that PYINCLUDE is defined
CFLAGS:=-Wall -std=c99 -I$(PYINCLUDE) -fPIC $(CFLAGS) -g -D_FILE_OFFSET_BITS=64

$(cstreamsmodule_objs): cstreams.h
