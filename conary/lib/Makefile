#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

python_files=	    __init__.py		\
		    cfg.py		\
		    cfgtypes.py		\
		    coveragehook.py	\
		    enum.py		\
		    debugger.py		\
		    erlcompleter.py	\
		    epdb.py     	\
		    fixedglob.py	\
		    fixedthreading.py	\
		    graph.py		\
		    importer.py		\
		    javadeps.py     	\
		    log.py		\
		    logger.py		\
		    magic.py		\
		    openpgpfile.py	\
		    openpgpkey.py	\
		    options.py		\
		    patch.py		\
		    pydeps.py           \
		    sha1helper.py	\
		    stackutil.py	\
		    tracelog.py		\
		    util.py

python_modules=elfmodule.so miscmodule.so cstreamsmodule.so
wrapper_libs=filename_wrapper.so
wrapper_objs=filename_wrapper.o
elfmodule_objs=elf.o
miscmodule_objs=misc.o
cstreamsmodule_objs=cstreams.o numstream.o streamset.o stringstream.o
generated_files=$(elfmodule_objs) $(miscmodule_objs) $(cstreamsmodule_objs) $(python_modules) $(wrapper_libs) $(wrapper_objs)
dist_files = $(python_files) elf.c misc.c cstreams.c numstream.c streamset.c \
             stringstream.c cstreams.h filename_wrapper.c Makefile


all: $(python_modules) $(wrapper_libs)

elfmodule.so: $(elfmodule_objs)
	$(CC) -shared -o $@ $< -lelf

miscmodule.so: $(miscmodule_objs)
	$(CC) -shared -o $@ $<

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
CFLAGS:=-Wall -I$(PYINCLUDE) -fPIC $(CFLAGS) -g

$(cstreamsmodule_objs): cstreams.h
