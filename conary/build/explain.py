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
from conary import versions
from conary.build import source
from conary.build import packagerecipe, redirectrecipe, derivedrecipe
from conary.build import filesetrecipe, grouprecipe, inforecipe

DELETE_CHAR = chr(8)

def _formatString(msg):
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

def _formatDoc(className, obj):
    name = obj.__name__
    docString = obj.__doc__
    if not docString:
        docString = 'No documentation available.'
    docStringRe = re.compile('[A-Z]\{[^{}]*\}')
    srch = re.search(docStringRe, docString)
    while srch:
        oldString = srch.group()
        newString = _formatString(oldString)
        docString = docString.replace(oldString, newString)
        srch = re.search(docStringRe, docString)
    # pydoc is fooled by conary's wrapping of stdout. override it if needed.
    if sys.stdout.isatty():
        pydoc.pager = lambda x: pydoc.pipepager(x, 'less')
    pydoc.pager("Conary API Documentation: %s\n\n" %
                _formatString('B{%s.%s}' %(className, name)) + docString)

def _parentName(klass):
    return klass.__base__.__name__

def docObject(cfg, what):
    class DummyRepos:
        def __getattr__(self, what):
            def f(*args, **kw):
                return True
            return f

    class DummyPackageRecipe(packagerecipe.PackageRecipe):
        def __init__(self, cfg):
            self.name = 'package'
            self.version = '1.0'
            packagerecipe.PackageRecipe.__init__(self, cfg, None, None)
            self._loadSourceActions(lambda x: True)
            self.loadPolicy()

    class DummyGroupRecipe(grouprecipe.GroupRecipe):
        def __init__(self, cfg):
            self.name = 'group-dummy'
            self.version = '1.0'
            troveCache = grouprecipe.TroveCache(None, None)
            repos = DummyRepos()
            grouprecipe.GroupRecipe.__init__(self, repos, cfg,
                                             versions.Label('a@b:c'), None,
                                             None)

    class DummyRedirectRecipe(redirectrecipe.RedirectRecipe):
        def __init__(self, cfg):
            self.name = 'redirect'
            self.verison = '1.0'
            redirectrecipe.RedirectRecipe.__init__(self, None, cfg, None, None)

## unused - too much overlap with PackageRecipe
##  class DummyDerivedRecipe(derivedrecipe.DerivedPackageRecipe):
##      def __init__(self, cfg):
##          self.name = 'derived'
##          self.version = '1.0'
##          derivedrecipe.DerivedPackageRecipe.__init__(self, cfg, None, None)

    class DummyInfoRecipe(inforecipe.UserGroupInfoRecipe):
        def __init__(self, cfg):
            self.name = 'info-dummy'
            self.version = '1.0'
            inforecipe.UserGroupInfoRecipe.__init__(self, cfg, None, None)

    # see if a parent class was specified (to disambiguate)
    className = None
    if '.' in what:
        split = what.split('.')
        if len(split) != 2:
            print 'Too may "." specified in "%s"' %(what)
            return 1
        className, what = split

    classList = [ DummyPackageRecipe, DummyGroupRecipe, DummyRedirectRecipe,
                  DummyInfoRecipe ]
    # filter out by the parent class specified
    if className:
        classList = [ x for x in classList if _parentName(x) == className ]

    # start looking for the object that implements the method
    found = []
    for klass in classList:
        r = klass(cfg)
        if not hasattr(r, what):
            continue

        obj = getattr(r, what)
        # The dynamic policy loader stores references to the
        # actual object or class in variables of _recipeHelper
        # and _policyUpdater classes.  This will pull the actual
        # class from those instances so we can inspect the docstring
        if hasattr(obj, 'theobject'):
            obj = obj.theobject
        elif hasattr(obj, 'theclass'):
            obj = obj.theclass
        if isinstance(obj, types.InstanceType):
            obj = obj.__class__
        found.append((klass, obj))

    if len(found) == 1:
        _formatDoc(_parentName(found[0][0]), found[0][1])
        return 0
    elif len(found) > 1:
        print ('Ambiguous recipe method "%s" is defined by %s'
               % (what, ', '.join(_parentName(x[0]) for x in found)))
        return 1
    else:
        print 'Unknown recipe method "%s"' %what
        return 1
