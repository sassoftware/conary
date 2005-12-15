#
# Copyright (c) 2004 rPath, Inc.
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

extra_dist = DataFeed.pm Makefile

dist_files = $(extra_dist)

all: default-all

install:
	install -d -m 644 DataFeed.pm $(DESTDIR)$(prefix)/lib/conary/ScanDeps/Module/ScanDeps/

dist: default-dist

clean: default-clean

include ../../../Make.rules

