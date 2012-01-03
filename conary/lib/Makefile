#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
