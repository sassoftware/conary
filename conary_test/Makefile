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


bins = _norpmlock.so norpmlockmodule.so

all: $(bins)

clean:
	rm -f $(bins)

%.so: %.o
	$(CC) $(LDFLAGS) -shared -rdynamic -o $@ $^ -lpython$(PYVER) $(LIBS)

#_norpmlock.so: norpmlock.c
#	$(CC) $(CFLAGS) -shared -rdynamic -o $@ $^

#norpmlockmodule.so: norpmlockmodule.c
#	$(CC) $(CFLAGS) -shared -rdynamic -o $@ $^

include ../Make.rules
CFLAGS:=-Wall -std=c99 -I$(PYINCLUDE) -fPIC $(CFLAGS) -g -D_FILE_OFFSET_BITS=64
