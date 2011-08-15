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


python_files=$(wildcard *.py)

cython_modules = file_utils.so sha256_nonstandard.so system.so
other_modules = dep_freeze.so digest_uncompress.so pack.so streams.so
python_modules = $(cython_modules) $(other_modules)

helper_modules = helper_sha256_nonstandard.so

cython_output=$(cython_modules:.so=.c)
ifdef PURE
install_modules = $(helper_modules)
all_modules = $(install_modules)
else
install_modules = $(cython_modules) $(other_modules)
all_modules = $(install_modules) $(helper_modules)
endif

# Don't delete intermediate .c files -- they need to be checked into hg
.SECONDARY: $(cython_output)

all: default-subdirs $(all_modules)

install: all install-subdirs pyfiles-install
	mkdir -p $(DESTDIR)$(sitedir)/$(DIR)
	install -m 755 $(install_modules) $(DESTDIR)$(sitedir)/$(DIR)

clean: clean-subdirs default-clean

dist: default-dist

ext: ext-clean
	$(MAKE) DO_CYTHON=1 $(cython_output)

ext-clean: clean
	rm -f $(cython_output)


# Per-file settings
digest_uncompress.so: LIBS = -lcrypto -lz
sha256_nonstandard.so: helper_sha256_nonstandard.o
streams.so: streams.o streams_numeric.o streams_string.o streams_set.o
system.so: LIBS = -lresolv

# Rules
%.so: %.o
	$(CC) $(LDFLAGS) -shared -rdynamic -o $@ $^ $(LIBS)


%.c: %.pyx common.pxi
ifdef DO_CYTHON
	$(CYTHON) -f $< -o $@
endif


include ../../../Make.rules

# needs to come after including Make.rules so that PYINCLUDE is defined
CFLAGS:=-Wall -std=c99 -I$(PYINCLUDE) -I.. -fPIC $(CFLAGS) -g -D_FILE_OFFSET_BITS=64
