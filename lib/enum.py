#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

class EnumeratedType(dict):

    def __getattr__(self, item):
	if self.has_key(item): 
	    return self[item]
	raise AttributeError, "'EnumeratedType' object has no " \
		    "attribute '%s'" % item

    def __init__(self, name, *vals):
	for item in vals:
	    self[item] = "%s-%s" % (name, item)
