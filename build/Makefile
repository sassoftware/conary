#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

python_files = __init__.py	\
	build.py		\
	buildpackage.py		\
	destdirpolicy.py	\
	filter.py 		\
	fixedglob.py		\
	lookaside.py		\
	macros.py		\
	packagepolicy.py	\
	policy.py		\
	recipe.py		\
	source.py		\
	use.py

extra_dist = Makefile

dist_files = $(python_files) $(extra_dist)

all: default-all

install: pyfiles-install

dist: default-dist

clean: default-clean

include ../Make.rules

