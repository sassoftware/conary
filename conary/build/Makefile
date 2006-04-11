#
# Copyright (c) 2004 rPath, Inc.
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
	action.py		\
	build.py		\
	buildinfo.py		\
	buildpackage.py		\
	cook.py			\
	destdirpolicy.py	\
	errors.py	        \
	filter.py 		\
	filesetrecipe.py	\
	grouprecipe.py		\
	inforecipe.py		\
	loadrecipe.py		\
	lookaside.py		\
	macros.py		\
	nextversion.py		\
	packagepolicy.py	\
	packagerecipe.py	\
	policy.py		\
	recipe.py		\
	redirectrecipe.py       \
	signtrove.py		\
	source.py		\
	tags.py			\
	use.py                  \
        usergroup.py

extra_dist = Makefile

dist_files = $(python_files) $(extra_dist)

all: default-all

install: pyfiles-install

dist: default-dist

clean: default-clean

include ../../Make.rules

