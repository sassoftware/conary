#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


.PHONY: all dist clean install

sqlite_dir = $(wildcard sqlite-*)
sqlite_lib = $(sqlite_dir)/.libs/libsqlite3.a
sqlite_inc = $(sqlite_dir)/sqlite3*.h

all:
	if [ -f $(sqlite_lib) ]; then \
		rm -rf embed; \
		mkdir embed; \
		cp $(sqlite_lib) $(sqlite_inc) embed/; \
	fi
	$(PYTHON) setup.py build
	$(PYTHON) setup.py install --install-lib=..

install:
	$(PYTHON) setup.py install --install-lib=$(DESTDIR)/$(conarydir)

clean:
	rm -rf build dist embed
	$(PYTHON) setup.py clean

dist: clean
	mkdir -p $(DISTDIR)/$(DIR)
	tar cSpf - --exclude CVS --exclude build --exclude dist . | tar xSpf - -C $(DISTDIR)/$(DIR) 

include ../../Make.rules
