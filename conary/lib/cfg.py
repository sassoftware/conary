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


"""
Provides a generic config file format that supports creating your own config
types and value types.
"""
import copy
import inspect
import os
import socket
import sys
import textwrap
import urllib2
import urlparse

from conary.lib import util, api
from conary.lib.http import proxy_map
from conary import constants, errors
from conary.repository import transport

configVersion = 1

# NOTE: programs expect to be able to access all of the cfg types from
# lib.cfg, so we import them here.  At some point, we may wish to make this
# separation between the two modules real.
# pyflakes=ignore
from conary.lib.cfgtypes import (CfgType, CfgString, CfgPath, CfgInt, CfgBool,
        CfgRegExp, CfgSignedRegExp, CfgEnum, CfgCallBack, CfgLineList,
        CfgQuotedLineList, CfgList, CfgDict, CfgEnumDict, CfgRegExpList,
        CfgSignedRegExpList, CfgError, ParseError, CfgEnvironmentError,
        RegularExpressionList, SignedRegularExpressionList, CfgPathList)

__developer_api__ = True


class ConfigDefinition(object):
    """
    Container for the structure of a single configuration class. Currently this
    is just the list of options in the class, including those inherited from
    other classes.
    """

    def __init__(self, options, aliases, hidden, sections, directives):
        self.options = options
        self.sections = sections
        self.hidden = set(x.lower() for x in hidden)
        self.directives = directives
        self.lowerCaseMap = dict((x.lower(), y)
                for (x, y) in options.iteritems())
        for key_from, key_to in aliases:
            self.lowerCaseMap[key_from.lower()] = self[key_to]

    def __getitem__(self, key):
        lower = key.lower()
        if lower not in self.lowerCaseMap:
            raise KeyError("No such config option %r" % (key,), key)
        return self.lowerCaseMap[lower]

    def __contains__(self, key):
        return key.lower() in self.lowerCaseMap

    _SIGIL = object()
    def getExact(self, key, default=_SIGIL):
        optdef = self.options.get(key, default)
        if optdef is self._SIGIL:
            raise KeyError(key)
        return optdef

    def extend(self, other):
        self.options.update(other.options)
        self.sections.update(other.sections)
        self.hidden.update(other.sections)
        self.lowerCaseMap = dict((x.lower(), y)
                for (x, y) in self.options.iteritems())


class OptionDefinition(object):
    """
    Definition of a single configuration option.

    It also serves as a data descriptor for fetching that option from a parent
    config instance.
    """
    __slots__ = ('name', 'valueType', 'default', 'doc')

    def __init__(self, name, valueType, default=None, doc=None):
        self.name = name
        if inspect.isclass(valueType):
            valueType = valueType()
        self.valueType = valueType
        self.default = default
        self.doc = doc

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.name)

    def getDefault(self):
        return self.valueType.getDefault(self.default)

    def writeDoc(self, out, displayOptions=None):
        """ Output documentation and default information in a way that
            is parsable by ConfigFiles
        """
        if displayOptions is None:
            displayOptions = {}
        tw = textwrap.TextWrapper(initial_indent='# ',
                                  subsequent_indent='# ', width=70)
        out.write('# %s (Default: %s)\n' % (self.name,
            ', '.join(self.valueType.toStrings(self.getDefault(), displayOptions))))
        if self.doc:
            out.write('\n'.join(tw.wrap(self.doc)))
            out.write('\n')

    def __get__(self, pself, pcls):
        if pself is None:
            # SomeClass.foo -> foo_definition
            return self
        # someInst.foo -> foo_value
        return pself[self.name]

    def __set__(self, pself, value):
        pself[self.name] = value

    def __delete__(self, pself):
        pself.resetToDefault(self.name)


class OptionValue(object):
    """
    Container for the value of a single option, including peripheral data.
    """
    __slots__ = ('definition', 'listeners', 'origins', 'value', '_isDefault')

    _NOT_SET = object()

    def __init__(self, definition, value=_NOT_SET):
        self.definition = definition
        self.listeners = []
        self.origins = []
        if value is self._NOT_SET:
            self.value = definition.getDefault()
            self._isDefault = True
        else:
            # The caller is responsible for copying values before passing them
            # in if sharing would be undesirable.
            self.value = value
            self._isDefault = False

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.definition.name)

    def copy(self, memo=None, shallow=False):
        new = type(self)(self.definition)
        new.listeners = list(self.listeners)
        new.origins = list(self.origins)
        if shallow:
            new.value = self.value
        else:
            new.value = copy.deepcopy(self.value, memo)
        new._isDefault = self._isDefault
        return new
    __deepcopy__ = copy

    def __copy__(self):
        return self.copy(shallow=True)

    # Shortcuts to the option definition

    name = property(lambda self: self.definition.name)
    valueType = property(lambda self: self.definition.valueType)
    default = property(lambda self: self.definition.getDefault())
    doc = property(lambda self: self.definition.doc)

    # Storing values

    def updateFromString(self, data, path=None, line=None):
        if self.isDefault():
            value = self.valueType.setFromString(self.default, data)
        else:
            value = self.valueType.updateFromString(self.value, data)
        self.set(value, path, line)

    def updateFromContext(self, other):
        """Merge another value object into this one."""
        value = self.valueType.set(self.value, other.value)
        self.set(value)
        self.origins.extend(other.origins)

    def set(self, value, path=None, line=None):
        self._callListeners()
        self._isDefault = False
        self.value = value
        if path is not None:
            self.origins.append((path, line))

    # Other features

    def addListener(self, fn):
        self.listeners.append(fn)

    def _callListeners(self):
        for listenFn in self.listeners:
            listenFn(self.definition.name)

    def write(self, out, ignored, displayOptions=None):
        """Writes a config option name and value."""
        # "ignored" argument for bw compat with rmake
        if displayOptions is None:
            displayOptions = {}

        if displayOptions.get('showLineOrigins', False):
            lineStrs = []
            curPath = None
            for path, lineNum in self.origins:
                if path == curPath:
                    continue
                else:
                    lineStrs.append('%s' % (path,))
                    curPath = path
            if lineStrs:
                out.write('# %s: %s\n' % (self.name, ' '.join(lineStrs)))
        for line in self.valueType.toStrings(self.value, displayOptions):
            out.write('%-25s %s\n' % (self.name, line))

    def resetToDefault(self):
        """Reset to the default value.

        Returns True if the new value has any meaningful ancillary properties
        (e.g. listeners). If it does not, then the value can be removed from
        the parent object's value dictionary.
        """
        self.origins = []
        self.value = self.definition.getDefault()
        self._isDefault = True
        return bool(self.listeners)

    def isDefault(self):
        # Try to catch cases where the value was mutated externally, e.g.
        # cfg.user.append(...)
        return self._isDefault and self.value == self.definition.getDefault()

    # Backwards compatibility

    def parseString(self, curVal, newStr, path=None, line=None):
        self.updateFromString(newStr, path, line)
        return self.value

    def writeDoc(self, out, displayOptions=None):
        return self.definition.writeDoc(out, displayOptions)


def directive(func):
    """
    Decorator for 'directives' -- options that invoke a method instead of
    storing a value.

    @directive
    def someDirective(self, value):
        self.dostuff()
    """
    func.cfg_is_directive = True
    return func


class _ConfigMeta(type):
    """
    Metaclass that all configuration classes must use. This happens
    automatically if the configuration class inherits from ConfigFile.

    This handles inheritance and creation of OptionDefinition objects and
    creates and binds a ConfigDefinition to the class.
    """
    fields = ('type', 'default', 'doc')

    def __new__(metacls, name, clsbases, clsdict):
        options = {}
        sections = {}
        directives = {}
        # Merge options from bases and "config" bases
        bases = list(clsbases)
        if clsdict.get('_cfg_bases'):
            bases = list(clsdict['_cfg_bases']) + bases
        bases.reverse()
        for cls in bases:
            if hasattr(cls, '_cfg_def'):
                options.update(cls._cfg_def.options)
                sections.update(cls._cfg_def.sections)
                directives.update(cls._cfg_def.directives)
        # Add options from this class
        for key, value in clsdict.items():
            if key[0] == '_':
                continue
            elif isinstance(value, (list, tuple)):
                # foo = (CfgThing, default, doc)
                args = value
            elif value is None or isinstance(value, basestring):
                # foo = 'default'
                args = (CfgString, value)
            elif inspect.isclass(value):
                if issubclass(value, CfgType):
                    # foo = CfgThing
                    args = (value(), None)
                elif issubclass(value, ConfigSection):
                    # foo = MySectionType
                    args = (value, None, None)
                else:
                    continue
            elif isinstance(value, CfgType):
                # foo = CfgThing('frobnoz')
                args = (value, None)
            elif callable(value) and getattr(value, 'cfg_is_directive', False):
                # @directive
                # Only save the name of the function and fetch it later with
                # getattr() to avoid violating the principle of least surprise.
                # Room left for optional args and kwargs at a later date.
                directives[key.lower()] = (key, (), {})
                continue
            else:
                continue

            if inspect.isclass(args[0]) and issubclass(args[0], ConfigSection):
                sections[key] = tuple(args)
            else:
                options[key] = OptionDefinition(key, *args)
        # Add aliases from base classes, then this class
        aliases = metacls._fold(bases, clsdict, '_cfg_aliases', [], list.extend)
        hidden = metacls._fold(bases, clsdict, '_cfg_hidden', [], list.extend)
        # Copy definition to class dictionary to act as a data descriptor,
        # enabling attribute get/set/delete.
        for key, optdef in options.iteritems():
            clsdict[key] = optdef

        clsdict['_cfg_def'] = ConfigDefinition(options, aliases, hidden,
                sections, directives)
        return type.__new__(metacls, name, clsbases, clsdict)

    @staticmethod
    def _fold(bases, clsdict, name, value, func):
        for pcls in bases:
            for mcls in reversed(pcls.mro()):
                mval = getattr(mcls, name, None)
                if mval is not None:
                    func(value, mval)
        if clsdict.get(name):
            value.extend(clsdict[name])
        return value

    def extend(clself, other):
        """
        Add options in config class C{other} to this class.

        Since this method is attached to the metaclass, it is only visible on
        the config classes but not instanes of those classes.
        """
        clself._cfg_def.extend(other._cfg_def)
        for key, optdef in clself._cfg_def.options.iteritems():
            setattr(clself, key, optdef)


class _Config(object):
    """ Base configuration class.  Supports defining a configuration object,
        and displaying that object, but has no knowledge of how the input.

        Values that are supported in this configuration object are defined
        by creating class variables.  The format of such configuration items
        is as follows:

        <name> = (ConfigType, [default])

        or

        <name> = default

        In the second format, the default ConfigType is CfgString.

        ConfigType is a class or instance that describes how to parse
        a string into a configuration item, and display it.  The
        expected interface is documented in ConfigType.
    """
    __metaclass__ = _ConfigMeta

    # To be filled in by the metaclass.
    _cfg_def = None

    # Optional list of (from, to) tuples of config aliases.
    _cfg_aliases = None

    # Optional list of "base classes" to inherit options from.
    # Note that actual base classes also work, but this way prevents methods
    # and other non-option attributes from being inherited.
    _cfg_bases = None

    # Optional list of keys that should not be displayed.
    _cfg_hidden = None

    def __init__(self):
        self._values = {}
        self._displayOptions = {}

    def _cow(self, key):
        """Copy-on-write -- return a L{OptionValue} for the given key, creating
        and storing it if necessary.

        Do not use if you are not going to modify the value as that would bloat
        the value dictionary.
        """
        optdef = self._cfg_def[key]
        optval = self._values.get(optdef.name)
        if optval is None:
            optval = self._values[optdef.name] = OptionValue(optdef)
        return optval

    def addListener(self, key, fn):
        """
        Add a listener function that will be called when the given key is
        updated.  The function will be called with key as a single parameter.
        """
        value = self._cow(key)
        value.addListener(fn)

    # --- Display options allow arbitrary display parameters to be set --
    # they can be picked up by the strings printing themselves

    def _initDisplayOptions(self):
        self._displayOptions = dict(prettyPrint=False)

    def setDisplayOptions(self, **kw):
        self._displayOptions.update(kw)

    def getDisplayOption(self, key, default=None):
        if default is not None:
            return self._displayOptions.get(key, default)
        else:
            return self._displayOptions.get(key)

    # --- accessing/setting values ---
    # Both item and attribute access work. The former is implemented here, and
    # the latter is implemented using OptionDefinition as a data descriptor.

    def __getitem__(self, name):
        # Theoretically this could return the default value without COWing, but
        # then operations like "cfg.repositoryMap.update()" would either mutate
        # the default value or be discarded.
        optval = self._cow(name)
        return optval.value

    def __setitem__(self, key, value):
        optval = self._cow(key)
        optval.set(value)

    def __contains__(self, key):
        return key in self._cfg_def

    def setValue(self, key, value):
        self[key] = value

    def getDefaultValue(self, name):
        return self._cfg_def[name].getDefault()

    @api.publicApi
    def isDefault(self, key):
        # NOTE: There are ways (in code) to modify options without the
        # isDefault flag being cleared, e.g. modifying a mutable option value
        # directly. This is for advisory purposes only.
        optdef = self._cfg_def[key]
        optval = self._values.get(optdef.name)
        if optval is None:
            return True
        else:
            return optval.isDefault()

    def resetToDefault(self, key):
        self._cfg_def[key]  # test for existence
        optval = self._values.get(key)
        if optval is not None and not optval.resetToDefault():
            # Delete if we have no other reason (e.g. listeners) to keep
            # the value object around.
            del self._values[key]

    def keys(self):
        return self._cfg_def.options.keys()

    def iterkeys(self):
        return self._cfg_def.options.iterkeys()

    def items(self):
        return list(self.iteritems())

    def iteritems(self):
        for name in self._cfg_def.options:
            yield name, self[name]

    # --- displaying/writing values ---

    def display(self, out=None):
        """ Display the config file respecting all display options
        """
        if out is None:
            out = sys.stdout
        self._write(out, self._displayOptions, includeDocs=False)

    def store(self, out, includeDocs=True):
        """ Write the config file in a format that should be readable
            by the same config file.
        """
        self._write(out, dict(prettyPrint=False), includeDocs=includeDocs)

    def displayKey(self, key, out=None):
        if out is None:
            out = sys.stdout
        self._writeKey(out, self._cfg_def[key], None, self._displayOptions)

    @api.publicApi
    def storeKey(self, key, out):
        self._writeKey(out, self._cfg_def[key], None, dict(prettyPrint=False))

    def writeToFile(self, path, includeDocs=True):
        util.mkdirChain(os.path.dirname(path))
        self.store(open(path, 'w'), includeDocs)

    def _write(self, out, options, includeDocs=True):
        hidden = options.get('displayHidden', False)
        for name, optdef in sorted(self._cfg_def.options.iteritems()):
            if not hidden and optdef.name.lower() in self._cfg_def.hidden:
                continue
            if includeDocs:
                optdef.writeDoc(out, options)
            self._writeKey(out, optdef, None, options)

    def _writeKey(self, out, optdef, ignored, options):
        # "ignored" argument for bw compat with rmake
        optval = self._values.get(optdef.name) or OptionValue(optdef)
        if optval.isDefault() and optval.value is None:
            return
        optval.write(out, None, options)

    # --- pickle protocol ---

    def __getstate__(self):
        return {
                'flags': {},
                'options': [ (key, value.value)
                    for (key, value) in self._values.iteritems()
                    if not value.isDefault()
                    ],
                }

    def __setstate__(self, state):
        self.__dict__.clear()
        self.__init__(**state['flags'])

        for row in state['options']:
            key, value = row[:2]
            # If the option is unknown, skip it. This allows for a little
            # flexibility if the config definition changed.
            optdef = self._cfg_def.getExact(key, None)
            if optdef:
                self._values[key] = OptionValue(optdef, value)

    def __copy__(self):
        # Don't use getstate, we want to copy all values even if they're
        # default.
        cls = type(self)
        obj = cls.__new__(cls)
        obj.__dict__ = self.__dict__.copy()
        obj._values = dict((key, value.copy(shallow=True))
                for (key, value) in self._values.iteritems())
        return obj

    # --- metadata backwards compatibility ---

    @property
    def _options(self):
        return dict((key,
            self._values.get(key) or OptionValue(self._cfg_def.getExact(key)))
            for key in self._cfg_def.options)


class ConfigFile(_Config):
    """ _Config class + ability to read in files """

    def __init__(self):
        self._ignoreErrors = False
        self._ignoreUrlIncludes = False
        self._keyLimiters = set()
        self._configFileStack = []
        _Config.__init__(self)

    def limitToKeys(self, *keys):
        if keys == (False,):
            self._keyLimiters = None
        else:
            self._keyLimiters = set(keys)

    def ignoreUrlIncludes(self, value=True):
        self._ignoreUrlIncludes = value

    def setIgnoreErrors(self, val=True):
        self._ignoreErrors = val

    def readObject(self, path, f):
        if path in self._configFileStack:
            # File was already processed, most likely an include loop
            # This should also handle loops in URLs
            return
        self._configFileStack.append(path)
        # path is used for generating error messages
        try:
            lineno = 1
            while True:
                line = f.readline()
                if not line:
                    break

                lineCount = 1
                while len(line) > 1 and '#' not in line and line[-2] == '\\':
                    # handle \ at the end of the config line.
                    # keep track of the lines we use so that we can
                    # give accurate line #s for errors.  This config line
                    # will be considered to live on its first line even
                    # though it spans multiple lines.

                    line = line[:-2] + f.readline()
                    lineCount += 1
                self.configLine(line, path, lineno)
                lineno = lineno + lineCount
            f.close()
        except urllib2.HTTPError, err:
            raise CfgEnvironmentError(err.filename, err.filename)
        except urllib2.URLError, err:
            raise CfgEnvironmentError(path, err.reason.args[1])
        except EnvironmentError, err:
            raise CfgEnvironmentError(err.filename, err.strerror)

        # We're done with this config file, remove it from the include stack
        self._configFileStack.pop()

    def _openPath(self, path, exception=True):
        if os.path.exists(path):
            try:
                return open(path, "r")
            except EnvironmentError, err:
                if exception:
                    raise CfgEnvironmentError(err.strerror, err.filename)
                else:
                    return
        elif exception:
            raise CfgEnvironmentError(
                          path,
                          "No such file or directory")

    @api.publicApi
    def read(self, path, exception=True):
        """
        read a config file or config file section

        @param path: the OS path to the file
        @type path: string

        @param exception: if True, raise exceptions
        @type exception: bool

        @raises CfgEnvironmentError: raised if file read fails
        """
        f = self._openPath(path, exception=exception)
        if f: self.readObject(path, f)

    @api.publicApi
    def readUrl(self, url):
        """
        read a config file from a URL

        @param url: the URL to read
        @type url: string

        @raises CfgEnvironmentError: raised if file read fails
        """
        if self._ignoreUrlIncludes:
            return
        try:
            f = self._openUrl(url)
            self.readObject(url, f)
        except CfgEnvironmentError:
            if not self._ignoreErrors:
                raise

    def configLine(self, line, fileName = "override", lineno = '<No line>'):
        line = line.strip()
        line = line.replace('\\\\', '\0').replace('\\#', '\1')
        line = line.split('#', 1)[0]
        if not line:
            return
        line = line.replace('\0', '\\').replace('\1', '#')

        parts = line.split(None, 1)
        if len(parts) == 1:
            key = parts[0]
            val = ''
        else:
            (key, val) = parts

        if key.lower() in self._cfg_def.directives:
            funcName, args, kwargs = self._cfg_def.directives[key.lower()]
            fn = getattr(self, funcName)
            try:
                fn(val, *args, **kwargs)
            except Exception, err:
                if errors.exceptionIsUncatchable(err):
                    raise
                util.rethrow(ParseError("%s:%s: when processing %s: %s"
                    % (fileName, lineno, key, err)))
        else:
            self.configKey(key, val, fileName, lineno)

    def configKey(self, key, val, fileName = "override", lineno = '<No line>'):
        try:
            option = self._cfg_def[key]
        except KeyError:
            if self._ignoreErrors:
                return
            raise ParseError("%s:%s: unknown config item '%s'" % (fileName,
                lineno, key))
        try:
            if self._keyLimiters and option.name not in self._keyLimiters:
                return
            value = self._cow(key)
            value.updateFromString(val, fileName, lineno)
        except ParseError, msg:
            if not self._ignoreErrors:
                raise ParseError, "%s:%s: %s for configuration item '%s'" \
                                                            % (fileName,
                                                               lineno, msg, key)

    def getProxyMap(self):
        return proxy_map.ProxyMap()

    def _getOpener(self):
        return transport.URLOpener(proxyMap=self.getProxyMap())

    def _openUrl(self, url):
        oldTimeout = socket.getdefaulttimeout()
        timeout = 2
        socket.setdefaulttimeout(timeout)
        # Extra headers to send up
        headers = {
            'X-Conary-Version' : constants.version or "UNRELEASED",
            'X-Conary-Config-Version' : str(configVersion),
        }
        opener = self._getOpener()
        try:
            for i in range(4):
                try:
                    return opener.open(url, headers=headers)
                except socket.timeout:
                    # CNY-1161
                    # We double the socket time out after each run; this
                    # should allow very slow links to catch up while
                    # providing some feedback to the user. For now, only
                    # on stderr since logging is not enabled yet.
                    sys.stderr.write("Timeout reading configuration "
                        "file %s; retrying...\n" % url)
                    timeout *= 2
                    socket.setdefaulttimeout(timeout)
                    continue
                except (IOError, socket.error), err:
                    if len(err.args) > 1:
                        raise CfgEnvironmentError(url, err.args[1])
                    else:
                        raise CfgEnvironmentError(url, err.args[0])
                except EnvironmentError, err:
                    raise CfgEnvironmentError(err.filename, err.msg)
            else: # for
                # URL timed out
                raise CfgEnvironmentError(url, "socket timeout")
        finally:
            socket.setdefaulttimeout(oldTimeout)

    def isUrl(self, val):
        return val.startswith("http://") or val.startswith("https://")

    def _absPath(self, relpath):
        """
        Interpret C{relpath} relative to the last included config file
        (or the current working directory if no config file is being
        processed) and return the full path.

        Additionally, paths like ~/foo where the current user's home
        directory is substituted for the ~ are supported.
        """

        # Pass through URIs and absolute paths.
        if self.isUrl(relpath) or relpath[0] == '/':
            return relpath

        # This won't deal with ~user/ syntax, but it's much less
        # common anyway.
        if relpath.startswith('~/') and 'HOME' in os.environ:
            return os.path.join(os.environ['HOME'], relpath[2:])

        if self._configFileStack:
            relativeTo = os.path.dirname(self._configFileStack[-1])
        else:
            relativeTo = os.getcwd()

        if self.isUrl(relativeTo):
            parts = urlparse.urlsplit(relativeTo)
            return urlparse.urlunsplit((parts.scheme, parts.netloc, os.path.normpath(os.path.join(parts.path, relpath)), parts.query, parts.fragment))
        return os.path.normpath(os.path.join(relativeTo, relpath))

    @directive
    def includeConfigFile(self, val, fileName = "override",
                          lineno = '<No line>'):
        abspath = self._absPath(val)
        if self.isUrl(abspath):
            self.readUrl(abspath)
        else:
            for cfgfile in sorted(util.braceGlob(abspath)):
                self.read(cfgfile)

class ConfigSection(ConfigFile):
    """ A Config Section.
        Basically a separate config file, except that it knows who its
        parent config file is.
    """

    def __init__(self, parent, doc=None):
        self._parent = parent
        ConfigFile.__init__(self)
        if doc:
            self.doc = doc

    def getParent(self):
        return self._parent

    def getDisplayOption(self, key):
        return self._parent.getDisplayOption(key)

    def includeConfigFile(self, val):
        return self._parent.includeConfigFile(val)

    # --- pickle protocol ---

    def __getstate__(self):
        # Note that pickle has no problem with the reference loop here.
        state = ConfigFile.__getstate__(self)
        state['flags']['parent'] = self._parent
        return state


class SectionedConfigFile(ConfigFile):
    """
        A SectionedConfigFile allows the definition of sections
        using [foo] to delineate sections.

        When a new section is discovered, a new section with type
        self._sectionType is assigned.
    """

    _allowNewSections = False
    _defaultSectionType = None

    def __init__(self):
        ConfigFile.__init__(self)
        self._sections = {}
        self._sectionName = ''
        for key, (sectionType, _, doc) in self._cfg_def.sections.items():
            section = sectionType(self, doc)
            self._addSection(key, section)
            setattr(self, key, section)

    def iterSections(self):
        return self._sections.itervalues()

    def iterSectionNames(self):
        return self._sections.iterkeys()

    def hasSection(self, sectionName):
        return sectionName in self._sections

    def getSection(self, sectionName):
        if not self.hasSection(sectionName):
            raise ParseError, 'Unknown section "%s"' % sectionName
        return self._sections[sectionName]

    def setSection(self, sectionName, sectionType = None):
        if not self.hasSection(sectionName):
            if self._allowNewSections:
                if sectionType is None:
                    sectionType = self._defaultSectionType
                self._addSection(sectionName, sectionType(self))
            else:
                raise ParseError, 'Unknown section "%s"' % sectionName
        self._sectionName = sectionName
        return self._sections[sectionName]

    def _addSection(self, sectionName, sectionObject):
        self._sections[sectionName] = sectionObject
        sectionObject._ignoreErrors = self._ignoreErrors

    @api.publicApi
    def configLine(self, line, file = "override", lineno = '<No line>'):
        line = line.strip()
        if line and line[0] == '[' and line[-1] == ']':
            self.setSection(line[1:-1])
            return
        if self._sectionName:
            self._sections[self._sectionName].configLine(line, file, lineno)
        else:
            ConfigFile.configLine(self, line, file, lineno)

    def _writeSection(self, sectionName, options):
        """ Determine whether to write the given section
        """
        return True

    def _write(self, out, options, includeDocs=True):
        ConfigFile._write(self, out, options, includeDocs)
        for sectionName in sorted(self._sections):
            if self._writeSection(sectionName, options):
                out.write("\n\n[%s]\n" % sectionName)
                self._sections[sectionName]._write(out, options, includeDocs)

    def includeConfigFile(self, val, fileName = "override",
                          lineno = '<No line>'):
        abspath = self._absPath(val)
        if self.isUrl(abspath):
            self.readUrl(abspath, resetSection = False)
        else:
            for cfgfile in sorted(util.braceGlob(abspath)):
                self.read(cfgfile, resetSection = False)

    @api.publicApi
    def read(self, *args, **kw):
        # when reading a new config file, reset the section.
        oldSection = self._sectionName
        if kw.pop('resetSection', True):
            self._sectionName = None
        rv = ConfigFile.read(self, *args, **kw)
        self._sectionName = oldSection
        return rv

    @api.publicApi
    def readUrl(self, *args, **kw):
        oldSection = self._sectionName
        if kw.pop('resetSection', True):
            self._sectionName = None
        rv = ConfigFile.readUrl(self, *args, **kw)
        self._sectionName = oldSection
        return rv

    # --- pickle protocol ---

    def __getstate__(self):
        state = ConfigFile.__getstate__(self)
        state['sections'] = self._sections
        return state

    def __setstate__(self, state):
        ConfigFile.__setstate__(self, state)
        for name, section in state['sections'].iteritems():
            self._addSection(name, section)
