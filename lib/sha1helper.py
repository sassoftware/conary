#
# Copyright (c) 2004 Specifix, Inc
# All rights reserved
#
import sha
import os

def hashFile(path):
    fd = os.open(path, os.O_RDONLY)
    m = sha.new()
    buf = os.read(fd, 40960)
    while len(buf):
	m.update(buf)
	buf = os.read(fd, 40960)
    os.close(fd)

    return m.hexdigest()

def hashString(buf):
    m = sha.new()
    m.update(buf)
    return m.hexdigest()
