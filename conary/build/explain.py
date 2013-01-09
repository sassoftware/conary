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


import sys
import re
import os
import pydoc, types
from conary import versions
from conary.build import recipe
from conary.build import packagerecipe, redirectrecipe, capsulerecipe
from conary.build import filesetrecipe, grouprecipe, groupsetrecipe, inforecipe

blacklist = {'PackageRecipe': ('InstallBucket', 'reportErrors', 'reportMissingBuildRequires', 'reportExcessBuildRequires', 'setModes'),
        'GroupInfoRecipe': ('User',),
        'UserInfoRecipe': ('Group', 'SupplementalGroup'),
        'GroupRecipe' : ('reportErrors',)}

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

class DummyCapsuleRecipe(capsulerecipe.CapsuleRecipe):
    def __init__(self, cfg):
        self.name = 'capsule'
        self.version = '1.0'
        capsulerecipe.CapsuleRecipe.__init__(self, cfg, None, None)
        self._loadSourceActions(lambda x: True)
        self.loadPolicy()

class DummyGroupRecipe(grouprecipe.GroupRecipe):
    def __init__(self, cfg):
        self.name = 'group-dummy'
        self.version = '1.0'
        repos = DummyRepos()
        grouprecipe.GroupRecipe.__init__(self, repos, cfg,
                                         versions.Label('a@b:c'), None,
                                         None)
        self.loadPolicy()

class DummyGroupSetRecipe(groupsetrecipe.GroupSetRecipe):
    def __init__(self, cfg):
        self.name = 'group-dummy'
        self.version = '1.0'
        repos = DummyRepos()
        groupsetrecipe.GroupSetRecipe.__init__(self, repos, cfg,
                                               versions.Label('a@b:c'), None,
                                               None)
        self.loadPolicy()

class DummyFilesetRecipe(filesetrecipe.FilesetRecipe):
    def __init__(self, cfg):
        self.name = 'fileset'
        self.version = '1.0'
        repos = DummyRepos()
        filesetrecipe.FilesetRecipe.__init__(self, repos, cfg,
                                         versions.Label('a@b:c'), None, {})
        self._policyMap = {}

class DummyRedirectRecipe(redirectrecipe.RedirectRecipe):
    def __init__(self, cfg):
        self.name = 'redirect'
        self.verison = '1.0'
        redirectrecipe.RedirectRecipe.__init__(self, None, cfg, None, None)
        self._policyMap = {}

class DummyUserInfoRecipe(inforecipe.UserInfoRecipe):
    def __init__(self, cfg):
        self.name = 'info-dummy'
        self.version = '1.0'
        inforecipe.UserInfoRecipe.__init__(self, cfg, None, None)

class DummyGroupInfoRecipe(inforecipe.GroupInfoRecipe):
    def __init__(self, cfg):
        self.name = 'info-dummy'
        self.version = '1.0'
        inforecipe.GroupInfoRecipe.__init__(self, cfg, None, None)

class DummyTroveSet(groupsetrecipe.GroupTupleSetMethods):
    def __init__(self, *args, **kwargs):
        pass

class DummyRepository(groupsetrecipe.GroupSearchSourceTroveSet):
    def __init__(self, *args, **kwargs):
        pass

class DummySearchPath(groupsetrecipe.GroupSearchPathTroveSet):
    def __init__(self, *args, **kwargs):
        pass

class DummyScript(groupsetrecipe.GroupScript):
    def __init__(self, *args, **kwargs):
        pass

class DummyScripts(groupsetrecipe.GroupScripts):
    def __init__(self, *args, **kwargs):
        pass

classList = [ DummyPackageRecipe, DummyCapsuleRecipe,
          DummyGroupRecipe, DummyRedirectRecipe,
          DummyGroupInfoRecipe, DummyUserInfoRecipe, DummyFilesetRecipe,
          DummyGroupSetRecipe,
          DummyTroveSet, DummyRepository, DummySearchPath,
          DummyScript, DummyScripts ]

def _useLess():
    if 'PAGER' in os.environ:
        if 'less' in os.environ['PAGER']:
            return True
        return False
    return True

def _wrapString(msg, on, off):
    # on and off are ANSI color codes
    onCode = '\033[%dm' % on
    offCode = '\033[%dm' % off
    # nesting does not work
    msg = msg.replace(onCode, '').replace(offCode, '')
    return onCode + msg + offCode

def _bold(msg):
    if _useLess():
        return _wrapString(msg, 1, 21)
    return msg

def _underline(msg):
    if _useLess():
        return _wrapString(msg, 4, 24)
    return msg

def _reverse(msg):
    if _useLess():
        return _wrapString(msg, 7, 27)
    return msg

def _formatString(msg):
    if msg[0] == 'B':
        return _bold(msg[2:-1])
    elif msg[0] == 'C':
        # use underline for constant width because reverse video
        # is too distracting in practice, due to appropriate
        # widespread use of constant width
        return _underline(msg[2:-1])
    elif msg[0] == 'I':
        # use reverse video instead of underline for italic to
        # disambiguate from constant width
        return _reverse(msg[2:-1])
    return msg[2:-1]

_headerChars = set('-=')
def _isHeader(line1, line2):
    # header is two strings:
    # same length
    if len(line1) != len(line2):
        return False
    line1 = line1.lstrip()
    line2 = line2.lstrip()
    # same initial whitespace length
    if len(line1) != len(line2):
        return False
    # second line contains only - or = (this is close enough, we do not -=-=-)
    if set(line2) - _headerChars:
        return False
    return True

def _iterFormatHeaders(docLines):
    lastLine = len(docLines) - 1
    ignoreNext = False
    for i in range(0, len(docLines)):
        if ignoreNext:
            ignoreNext = False
            continue
        if i < lastLine and _isHeader(docLines[i], docLines[i+1]):
            ignoreNext = True
            yield _bold(docLines[i].lstrip())
            continue
        yield docLines[i]

def _formatHeaders(docString):
    docLines = docString.split('\n')
    return '\n'.join(_iterFormatHeaders(docLines))

def _reindentGen(lines, indentLength):
    four = '    '
    for line in lines:
        if line:
            yield four + line[indentLength:]
        else:
            yield line

def _reindent(text):
    # consistently provide no more than 4 leading spaces 
    # space sorts before any letters; use that to find shortest
    # non-blank line
    lines = text.split('\n')
    outmostLine = [x for x in sorted(lines) if x][-1]
    indentLength = len(outmostLine) - len(outmostLine.lstrip())
    if indentLength <= 4:
        return text
    return '\n'.join(_reindentGen(lines, indentLength))

def _formatDocString(docString):
    # First, reindent to be consistent:
    docString = _reindent(docString)
    # Next, handle literal blocks as much as we need do:
    docString = docString.replace('::\n', ':\n')
    # Next, reformat headers:
    docString = _formatHeaders(docString)
    docStringRe = re.compile('[A-Z]\{[^{}]*\}')
    srch = re.search(docStringRe, docString)
    while srch:
        oldString = srch.group()
        newString = _formatString(oldString)
        docString = docString.replace(oldString, newString)
        srch = re.search(docStringRe, docString)
    return docString

def _pageDoc(title, docString):
    docString = _formatDocString(docString)
    # pydoc is fooled by conary's wrapping of stdout. override it if needed.
    if sys.stdout.isatty():
        if _useLess():
            # -R parses CSR escape codes properly
            pydoc.pager = lambda x: pydoc.pipepager(x, 'less -R')
        else:
            # PAGER is set if _useLess returns False
            pydoc.pager = lambda x: pydoc.pipepager(x, os.environ['PAGER'])
    pydoc.pager("Conary API Documentation: %s\n" %
            _formatString('B{' + title + '}') + docString)

def _formatDoc(className, obj):
    name = obj.__name__
    docString = obj.__doc__
    if not docString:
        docString = 'No documentation available.'
    _pageDoc('%s.%s' % (className, name), docString)

def _parentName(klass):
    if hasattr(klass, '_explainObjectName'):
        return klass._explainObjectName

    return klass.__base__.__name__

def docObject(cfg, what):
    inspectList = sys.modules[__name__].classList
    if what in [_parentName(x).replace('Dummy', '') for x in inspectList]:
        return docClass(cfg, what)
    # see if a parent class was specified (to disambiguate)
    className = None
    if '.' in what:
        split = what.split('.')
        if len(split) != 2:
            print 'Too may "." specified in "%s"' %(what)
            return 1
        className, what = split

    # filter out by the parent class specified
    if className:
        inspectList = [ x for x in inspectList if _parentName(x) == className ]

    # start looking for the object that implements the method
    found = []
    foundDocs = set()
    for klass in inspectList:
        if issubclass(klass, recipe.Recipe):
            r = klass(cfg)
        else:
            r = klass

        if not hasattr(r, what):
            continue
        if what in blacklist.get(_parentName(klass), []):
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
        if (obj.__doc__ and obj.__doc__ not in foundDocs):
            # let the order in inspectList determine which class we
            # display if multiple classes provide this docstring
            found.append((_parentName(klass), obj))
            foundDocs.add(obj.__doc__)

    if len(found) == 1:
        _formatDoc(found[0][0], found[0][1])
        return 0
    elif len(found) > 1:
        found.sort()
        print ('Ambiguous recipe method "%s" is defined by the following '
               'classes:\n'
               '    %s\n'
               'Specify one of: %s'
               % (what, ', '.join(x[0] for x in found),
                  ', '.join('%s.%s' % (x[0], what) for x in found)))
        return 1
    else:
        print 'Unknown recipe method "%s"' %what
        return 1


def docClass(cfg, recipeType):
    classType = 'Dummy' + recipeType
    r = sys.modules[__name__].__dict__[classType](cfg)
    display = {}
    if recipeType in ('PackageRecipe', 'GroupRecipe', 'GroupSetRecipe'):
        display['Build'] = sorted(x for x in r.externalMethods if x[0] != '_' and x not in blacklist.get(recipeType, []))
    elif recipeType == 'TroveSet':
        pass
    elif 'GroupInfoRecipe' in recipeType:
        display['Build'] = ['Group', 'SupplementalGroup']
    elif 'UserInfoRecipe' in recipeType:
        display['Build'] = ['User']
    if '_policyMap' in r.__dict__:
        display['Policy'] = sorted(x for x in r._policyMap if x[0] != '_' and x not in blacklist.get(recipeType, []))
    if recipeType == 'PackageRecipe':
        Actions = display['Build'][:]
        display['Source'] = [x for x in Actions if x.startswith('add')]
        display['Build'] = [x for x in Actions if x not in display['Source'] and x not in display['Policy'] ]
    for key, val in [x for x in display.iteritems()]:
        if val:
            display[key] = '\n    '.join(val)
        else:
            del display[key]
    text = r.__class__.__base__.__doc__
    if not text:
        text = 'No documentation available.'
    text += "\n\n" + '\n\n'.join(["B{%s Actions}:\n    %s" % x for x in sorted(display.iteritems())])
    _pageDoc(recipeType, text)

def docAll(cfg):
    text = "B{Available Classes}:\n    "
    text += '\n    '.join(_parentName(x).replace('Dummy', '') for x in classList)
    text += "\n    DerivedPackageRecipe: see PackageRecipe (not all methods apply)"
    _pageDoc('All Classes', text)
