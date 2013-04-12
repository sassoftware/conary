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


all: default-subdirs default-all

install: install-subdirs
	install -D -m0644 repos/conary-repos.sysconfig $(DESTDIR)/etc/sysconfig/conary-repos
	install -D -m0644 repos/gunicorn.conf $(DESTDIR)/etc/conary-repos/gunicorn.conf
	install -D -m0644 repos/nginx-globals $(DESTDIR)/etc/conary-repos/nginx-globals
	install -D -m0644 repos/nginx-includes $(DESTDIR)/etc/conary-repos/nginx-includes
	install -d -m0755 $(DESTDIR)/etc/conary-repos/vhosts


clean: default-clean

include ../Make.rules
