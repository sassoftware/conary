import os
import string

def mkdirChain(*paths):
    for path in paths:
	if path[0] != "/":
	    path = os.getcwd() + "/" + path

	paths = string.split(path, "/")

	for n in (range(2,len(paths) + 1)):
	    p = string.join(paths[0:n], "/")
	    if not os.path.exists(p):
		os.mkdir(p)

