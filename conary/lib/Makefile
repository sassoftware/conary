#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


SUBDIRS = epdb_embedded ext http

python_files=$(wildcard *.py)
python_modules=elf.so
wrapper_libs=filename_wrapper.so
wrapper_objs=filename_wrapper.o

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
    echo >&2; \
    echo 'No appropriate libelf found' >&2 ;\
    echo 'If building for GPL distribution, please pass "libelf=-lelf"' >&2 ;\
    echo >&2; \
fi)


all: default-subdirs $(python_modules) $(wrapper_libs)

$(python_modules): %.so: %.o
	$(CC) $(LDFLAGS) -shared -rdynamic -o $@ $^ $(LIBS)

elf.o: elf.c
	@if [ -z "$(libelf)" ] ; then \
	    echo 'Stopping build due to missing libelf' ; exit 1 ; fi
	$(CC) $(CFLAGS) $(libelf) -c -o $@ $^

elf.so: LIBS = $(libelf)


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
