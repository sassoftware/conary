# Copyright (c) 2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import sys
import re
import pydoc, types
from conary.build import source
from conary.build import packagerecipe

DELETE_CHAR = chr(8)

def formatString(msg):
    if msg[0] == 'B':
        res = ''
        skipIndex = 0
        for index, char in enumerate(msg[2:-1]):
            if msg[index + 3] == DELETE_CHAR:
                skipIndex = 2
            else:
                if skipIndex:
                    skipIndex = max(skipIndex - 1, 0)
                    continue
            res += char + DELETE_CHAR + char
        return res
    else:
        return msg[2:-1]

def formatDoc(obj):
    name = obj.__name__
    docString = obj.__doc__
    if not docString:
        docString = 'No documentation available.'
    docStringRe = re.compile('[A-Z]\{[^{}]*\}')
    srch = re.search(docStringRe, docString)
    while srch:
        oldString = srch.group()
        newString = formatString(oldString)
        docString = docString.replace(oldString, newString)
        srch = re.search(docStringRe, docString)
    # pydoc is fooled by conary's wrapping of stdout. override it if needed.
    if sys.stdout.isatty():
        pydoc.pager = lambda x: pydoc.pipepager(x, 'less')
    pydoc.pager("Conary API Documentation: %s\n\n" % formatString('B{' + name + '}') + docString)

def docObject(cfg, what):
    class DummyRecipe(packagerecipe.PackageRecipe):
        def __init__(self, cfg):
            self.name = 'package'
            self.version = '1.0'
            packagerecipe.PackageRecipe.__init__(self, cfg, None, None)
    r = DummyRecipe(cfg)
    r._loadSourceActions(lambda x: True)
    r.loadPolicy()
    try:
        obj = getattr(r, what)
        if hasattr(obj, 'theobject'):
            obj = obj.theobject
        elif hasattr(obj, 'theclass'):
            obj = obj.theclass
        if isinstance(obj, types.InstanceType):
            obj = obj.__class__
        formatDoc(obj)
    except AttributeError, e:
        print 'Unknown recipe method "%s"' %what
        return 1

