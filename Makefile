#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

.PHONY: clean bootstrap deps.dot

clean:
	rm -f *~ .#*

bootstrap:
	@if ! [ -d /opt/ -a -w /opt/ ]; then \
		echo "/opt isn't writable, this won't work"; \
		exit 1; \
	fi
	time ./srs-bootstrap `find ../recipes/ -name "cross*.recipe" -o -name "bootstrap*.recipe"`

deps.dot:
	./srs-bootstrap --dot `find ../recipes/ -name "cross*.recipe" -o -name "bootstrap*.recipe"` > deps.dot
