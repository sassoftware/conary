#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

CFLAGS=-Wall -I/usr/include/python2.3 -fPIC

python_files=__init__.py
python_modules=elfmodule.so
elfmodule_objs=elf.o
generated_files=$(elfmodule_objs) $(python_modules)
dist_files = $(python_files) elf.c Makefile

all: $(python_modules)

elfmodule.so: $(elfmodule_objs)
	$(CC) -shared -o $@ $< -lelf

install: pyfiles-install
	mkdir -p $(DESTDIR)$(srsdir)/$(DIR)
	install -m 755 $(python_modules) $(DESTDIR)$(srsdir)/$(DIR)

clean: default-clean

dist: default-dist

include ../Make.rules
