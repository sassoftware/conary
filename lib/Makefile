#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

CFLAGS=-Wall -I$(PYINCLUDE) -fPIC

python_files=	    __init__.py		\
		    enum.py		\
		    epdb.py		\
		    erlcompleter.py	\
		    log.py		\
		    magic.py		\
		    options.py		\
		    patch.py		\
		    sha1helper.py	\
		    stackutil.py	\
		    util.py

python_modules=elfmodule.so miscmodule.so cstreamsmodule.so
elfmodule_objs=elf.o
miscmodule_objs=misc.o
cstreamsmodule_objs=cstreams.o
generated_files=$(elfmodule_objs) $(miscmodule_objs) $(cstreamsmodule_objs) $(python_modules)
dist_files = $(python_files) elf.c misc.c cstreams.c Makefile


all: $(python_modules)

elfmodule.so: $(elfmodule_objs)
	$(CC) -shared -o $@ $< -lelf

miscmodule.so: $(miscmodule_objs)
	$(CC) -shared -o $@ $< -lelf

cstreamsmodule.so: $(cstreamsmodule_objs)
	$(CC) -shared -o $@ $< -lelf

install: pyfiles-install
	mkdir -p $(DESTDIR)$(conarydir)/$(DIR)
	install -m 755 $(python_modules) $(DESTDIR)$(conarydir)/$(DIR)

clean: default-clean

dist: default-dist

include ../Make.rules
