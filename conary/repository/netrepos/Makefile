#
# Copyright (c) 2004-2006 rPath, Inc.
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

python_files = __init__.py	\
		cacheset.py	\
		cltable.py	\
		flavors.py	\
		fsrepos.py	\
		__init__.py	\
		instances.py	\
		items.py	\
		keytable.py	\
                netauth.py      \
		netserver.py	\
		troveinfo.py	\
		trovestore.py	\
		versionops.py

extra_dist = Makefile
dist_files = $(python_files) $(extra_dist)

all: default-all

install: pyfiles-install

dist: default-dist

clean: default-clean

include ../../../Make.rules

