#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

python_files = __init__.py	\
	build.py		\
	destdirpolicy.py	\
	filter.py 		\
	fixedglob.py		\
	buildpackage.py		\
	lookaside.py		\
	packagepolicy.py	\
	policy.py		\
	recipe.py		\
	use.py			\
	macros.py		\
	sourcepy		\

extra_dist = Makefile

dist_files = $(python_files) $(extra_dist)

all: default-all

install: pyfiles-install

dist: default-dist

clean: default-clean

include ../Make.rules

