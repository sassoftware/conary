#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

SUBDIRS=localrep

python_files = __init__.py	\
		changeset.py	\
		filecontents.py	\
		fsrepos.py	\
		netclient.py 	\
		repository.py	\

extra_dist = Makefile

dist_files = $(python_files) $(extra_dist)

all: default-all

install: install-subdirs pyfiles-install

dist: default-dist

clean: default-clean

include ../Make.rules

