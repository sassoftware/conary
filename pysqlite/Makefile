#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

all:
	$(PYTHON) setup.py build
	$(PYTHON) setup.py install --install-lib=..

install:
	$(PYTHON) setup.py install --root=$(DESTDIR)

clean:
	$(PYTHON) setup.py clean

include ../Make.rules