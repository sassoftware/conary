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
	$(CC) $(LDFLAGS) -shared -rdynamic -o $@ $^ -lpython$(PYVER) $(LIBS)


%.c: %.pyx common.pxi
ifdef DO_CYTHON
	$(CYTHON) -f $< -o $@
endif


include ../../../Make.rules

# needs to come after including Make.rules so that PYINCLUDE is defined
CFLAGS:=-Wall -std=c99 -I$(PYINCLUDE) -I.. -fPIC $(CFLAGS) -g -D_FILE_OFFSET_BITS=64
