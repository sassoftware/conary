#
# Copyright (c) 2005 rPath, Inc.
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

use_files =	alternatives	\
		bootstrap	\
		builddocs	\
		buildtests	\
		desktop		\
		dietlibc	\
		emacs		\
		gcj		\
		gnome		\
		gtk		\
		ipv6		\
		kde		\
		krb		\
		ldap		\
		nptl		\
		pam		\
		pcre		\
		perl		\
		pie		\
		python		\
		qt		\
		readline	\
		sasl		\
		selinux		\
		ssl		\
		tcl		\
		tcpwrappers	\
		tk		\
		X		\
		xen		\
		xfce

dist_files = Makefile $(use_files)

all: default-all

install:
	mkdir -p $(DESTDIR)/etc/conary/use/; \
	for f in $(use_files); do \
		cp -a $$f $(DESTDIR)/etc/conary/use/$$f; \
	done

dist: default-dist

clean: default-clean

include ../../Make.rules
