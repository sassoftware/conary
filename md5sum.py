import md5
import os

def md5sum(path):
    fd = os.open(path, os.O_RDONLY)
    m = md5.new()
    buf = os.read(fd, 40960)
    while len(buf):
	m.update(buf)
	buf = os.read(fd, 40960)

    return m.hexdigest()

def md5str(str):
    m = md5.new()
    m.update(str)
    return m.hexdigest()
