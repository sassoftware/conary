#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Module implementing the "macro" dictionary class
"""

class Macros(dict):
    def __setitem__(self, name, value):
        # only expand references to ourself
        d = {name: self.get(name)}
        # escape any macros in the new value
        value = value.replace('%', '%%')
        # unescape references to ourself
        value = value.replace('%%%%(%s)s' %name, '%%(%s)s'%name)
        # expand our old value when defining the new value
 	dict.__setitem__(self, name, value % d)

    def __setattr__(self, name, value):
	self.__setitem__(name, value)
        
    # we want keys that don't exist to default to empty strings
    # but warn so that we can catch bugs
    def __getitem__(self, name):
	if name in self:
	    return dict.__getitem__(self, name) %self
	log.warning('name %s does not exist in macros', name)
	return ''

    def __getattr__(self, name):
	return self.__getitem__(name)
    
    def copy(self):
	new = Macros()
	new.update(self)
	return new

