#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed with the whole that it will be usefull, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Module implementing the "macro" dictionary class
"""

class Macros(dict):
    def update(self, other):
        for key, item in other.iteritems():
            self[key] = item
    
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
        
    def __getitem__(self, name):
	return dict.__getitem__(self, name) %self

    def __getattr__(self, name):
	return self.__getitem__(name)
    
    def copy(self):
	new = Macros()
	new.update(self)
	return new

