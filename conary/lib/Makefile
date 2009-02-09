#
# Copyright (c) 2004-2005 rPath, Inc.
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

python_files=$(wildcard *.py)
python_modules=elfmodule.so miscmodule.so cstreamsmodule.so SHA256_nonstandardmodule.so
wrapper_libs=filename_wrapper.so
wrapper_objs=filename_wrapper.o
elfmodule_objs=elf.o
sha256module_objs=SHA256_nonstandard.o
miscmodule_objs=misc.o
cstreamsmodule_objs=cstreams.o numstream.o streamset.o stringstream.o
generated_files=$(elfmodule_objs) $(miscmodule_objs) $(cstreamsmodule_objs) $(python_modules) $(wrapper_libs) $(wrapper_objs) $(sha256_objs)
dist_files = $(python_files) elf.c misc.c cstreams.c numstream.c streamset.c \
             stringstream.c cstreams.h filename_wrapper.c SHA256_nonstandard.c Makefile
libelf = $(shell \
if [ -x /usr/bin/pkg-config ] && /usr/bin/pkg-config --exists libelf; then \
    pkg-config --cflags --libs libelf; \
elif [ -d /usr/include/libelf-lgpl ]; then \
    echo "-I/usr/include/libelf-lgpl -lelf-lgpl"; \
elif [ -d ${SYSROOT}/usr/include/libelf-lgpl ]; then \
    echo "-I${SYSROOT}/usr/include/libelf-lgpl -lelf-lgpl"; \
else \
    echo -lelf; \
fi)


all: $(python_modules) $(wrapper_libs)

elf.o: elf.c
	$(CC) $(CFLAGS) $(libelf) -c -o $@ $^

elfmodule.so: $(elfmodule_objs)
	$(CC) -shared -o $@ $< $(libelf)

SHA256.o: SHA256_nonstandard.c
	$(CC) $(CFLAGS) -fno-strict-aliasing -DNDEBUG -fwrapv -O3 -Wstrict-prototypes -c -o $@ $^

SHA256_nonstandardmodule.so: $(sha256module_objs)
	$(CC) -pthread -shared -o $@ $< 

miscmodule.so: $(miscmodule_objs)
	$(CC) -shared -lcrypto -lz -lresolv -o $@ $<

$(cstreamsmodule_objs): cstreams.h

cstreamsmodule.so: $(cstreamsmodule_objs)
	$(CC) -shared -o $@ $^

filename_wrapper.so: filename_wrapper.c
	$(CC) -Wall -fPIC -rdynamic -c filename_wrapper.c
	$(CC) -shared -rdynamic -o filename_wrapper.so filename_wrapper.o -lc -ldl

install: pyfiles-install
	mkdir -p $(DESTDIR)$(sitedir)/$(DIR)
	install -m 755 $(python_modules) $(DESTDIR)$(sitedir)/$(DIR)
	mkdir -p $(DESTDIR)$(conarylibdir)
	install -m 755 $(wrapper_libs) $(DESTDIR)$(conarylibdir)/

clean: default-clean

dist: default-dist

include ../../Make.rules
# needs to come after including Make.rules so that PYINCLUDE is defined
CFLAGS:=-Wall -std=c99 -I$(PYINCLUDE) -fPIC $(CFLAGS) -g -D_FILE_OFFSET_BITS=64

$(cstreamsmodule_objs): cstreams.h
