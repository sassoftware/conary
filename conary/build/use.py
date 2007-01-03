#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
# Class hierarchy:
#
# Flag
#  |
#  +--UseFlag
#  |
#  +--SubArchFlag
#  |
#  +--LocalFlag

# Collection
#  | 
#  +--UseCollection          
#  | 
#  +--ArchCollection
#  | 
#  +--LocalFlagCollection
#
# MajorArch derives from CollectionWithFlag, which is a subclass of Flag and 
# Collection. (maybe CollectionWithFlag and MajorArch should be collapsed?)

"""
Provides the build configuration as special dictionaries that directly
export their namespaces.
"""

import itertools

#conary
from conary.deps import deps
from conary.lib import log
from conary.errors import CvcError

class Flag(dict):

    def __init__(self, name, parent=None, value=False, 
                 required=True, track=False, path=None):
        self._name = name
        self._value = value
        self._parent = parent
        self._required = required
        self._tracking = track
        self._used = False
        self._alias = None
        self._path = path

    def __repr__(self):
        if self._alias: 
            return "%s (alias %s): %s" % (self._name, self._alias, self._value)
        else:
            return "%s: %s" % (self._name, self._value)

    def __str__(self):
        if self._alias:
            return "%s (alias %s): %s" % (self._fullName(), self._alias,
                                                             self._value)
        else:
            return "%s: %s" % (self._fullName(), self._value)

    def setShortDoc(self, doc):
        # XXX we don't do anything with this documentation currently.
        self._shortDoc = doc

    def setRequired(self, value=True):
        self._required = value 

    def _set(self, value=True):
        self._value = value

    def _get(self):
        """ Grab value without tracking """
        return self._value 

    def _fullName(self):
        return ('.'.join(x._name for x in self._reverseParents()) 
                                                + '.' + self._name)

    def _reverseParents(self):
        if self._parent is not None:
            for parent in self._parent._reverseParents():
                yield parent
            yield self._parent

    def _getDepSense(self):
        if self._get():
            if self._required:
                return deps.FLAG_SENSE_REQUIRED
            else: 
                return deps.FLAG_SENSE_PREFERRED
        else:
            return deps.FLAG_SENSE_PREFERNOT

    def _toDependency(self):
        """ Returns an actual Dependency Set consisting of only this flag """
        raise NotImplementedError

    def _resetUsed(self):
        self._used = False

    def _trackUsed(self, value):
        self._tracking = value


     # --- boolean operations on Flags ---

    def __nonzero__(self):
        if self._tracking:
            self._setUsed(True)
        return self._value

    def _setUsed(self, used=True):
        self._used = used

    def __eq__(self, other):
        if not isinstance(other, (Flag, bool)):
            return False
        return bool(self) == bool(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ror__(self, other):
        return bool(self) | other

    def __or__(self, other):
	return self.__ror__(other)

    def __rand__(self, other):
        return bool(self) & other

    def __and__(self, other):
	return self.__rand__(other)



class Collection(dict):

    def __init__(self, name, parent=None, track=False):
        self._name = name
        self._parent = parent
        self._strictMode = True
        self._tracking = track
        self._attrs = {}


    def _addAlias(self, realKey, alias):
        """ Add a second way to access the given item.
            Necessary if the actual name for a flag is not a valid
            python identifier. 
        """
        if alias in self or alias in self._attrs:
            raise RuntimeError, 'alias is already set'
        elif self[realKey]._alias:
            raise RuntimeError, 'key %s already has an alias' % realKey
        else:
	    self._setAttr(alias, self[realKey])
            self[realKey]._alias = alias

    def _setAttr(self, name, value):
	""" A generic way to add a temporary attribute to this collection.
	    Attributes stored in this manner will be removed when the 
	    collection is cleared, but are not tracked like flags.
	"""
	self._attrs[name] = value

    def _delAttr(self, name):
	del self._attrs[name]

    def _getAttr(self, name):
	return self._attrs[name]

    def _addFlag(self, key, *args, **kw):
	if 'track' not in kw:
	    kw = kw.copy()
	    kw['track'] = self._tracking
        dict.__setitem__(self, key, self._collectionType(key, self, 
                                                         *args, **kw))

    def __repr__(self):
        return "%s: {%s}" % (self._name,
                             ', '.join((repr(x) for x in self.values())))

    def _clear(self):
        for flag in self.keys():
            del self[flag]
	for attr in self._attrs.keys():
            del self._attrs[attr]

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        if key in self:
            return self[key]
        if key in self._attrs:
            return self._getAttr(key)
        if key[0] == '_':
            raise AttributeError, key
        return self._getNonExistantKey(key)

    def __getitem__(self, key):
        if key in self._attrs:
            return self._attrs[key]
        else:
            return dict.__getitem__(self, key)

    def __setattr__(self, key, value):
        if key[0] == '_':
            self.__dict__[key] = value
        else:
            raise RuntimeError, "Cannot set value of flags: %s" % key

    def _getNonExistantKey(self, key):
        """ Method that is called when a nonexistant key is accessed.
            Overridden by subclasses to allow for useful error messages
            or default key values to be supplied """
        raise AttributeError, key

    def _iterAll(self):
        for child in self.itervalues():
            if isinstance(child, Collection):
                for flag in child._iterAll():
                    yield flag
            else:
                yield child

    def _setStrictMode(self, value=True):
        """ Strict mode determines whether you receive an error or 
            an empty flag upon accessing a nonexistant flag
        """
        self._strictMode = value

    def _reverseParents(self):
        """ Traverse through the parents from the topmost parent down. """
        if self._parent:
            for parent in self._parent._reverseParents():
                yield parent
            yield self._parent

    # -- Tracking Commands -- 

    def _trackUsed(self, value=True):
        self._tracking = value
        for child in self.itervalues():
            child._trackUsed(value)

    def _resetUsed(self):
        for child in self.itervalues():
            child._resetUsed()

    def _getUsed(self):
        return [ x for x in self._iterUsed() ] 

    def _iterUsed(self):
        for child in self.itervalues():
            if isinstance(child, Collection):
                for flag in child._iterUsed():
                    yield flag
            else:
                if child._used:
                    yield child


class CollectionWithFlag(Flag, Collection):
    """ CollectionWithFlag.   Currently only has one child class, MajorArch. """
    def __init__(self, name, parent, track=False):
        Flag.__init__(self, name, parent, track=track)
        Collection.__init__(self, name, parent, track=track)

    def _trackUsed(self, value=True):
        Flag._trackUsed(self, value)
        Collection._trackUsed(self, value)
        
    def _resetUsed(self):
        Flag._resetUsed(self)
        Collection._resetUsed(self)

    def _iterUsed(self):
        if self._used:
            yield self
        for child in Collection._iterUsed(self):
            yield child

    def _iterAll(self):
        yield self
        for child in Collection._iterAll(self):
            yield child

    def __repr__(self):
        return "%s: %s {%s}" % (self._name, self._value, 
                                ', '.join((repr(x) for x in self.values())))

class NoSuchUseFlagError(CvcError):

    def __init__(self, key):
        self.key = key

    def __str__(self):
        return """
An unknown use flag, Use.%s, was accessed.  The default behavior
of conary is to complain about the missing flag, since it may be
a typo.  You can add the flag /etc/conary/use/%s, or
${HOME}/.conary/use/%s, or use the --unknown-flags option on
the command line to make conary assume that all unknown flags are
not relevant to your system.
""" % (self.key, self.key, self.key)
             
class NoSuchArchFlagError(CvcError):

    def __init__(self, key):
        self.key = key

    def __str__(self):
        return """


An unknown architecture, Arch.%s, was accessed.  The default 
behavior of conary is to complain about the missing flag, 
since it may be a typo.  You can add the architecture 
/etc/conary/arch/%s or ${HOME}/.conary/arch/%s, or 
use the --unknown-flags option on the command line to make 
conary assume that all unknown flags are not relevant to 
your system.

""" % (self.key, self.key, self.key)
 
class NoSuchSubArchFlagError(CvcError):

    def __init__(self, majArch, key):
        self.majArch = majArch
        self.key = key

    def __str__(self):
        return """


An unknown sub architecture, Arch.%s.%s was accessed.  The default 
behavior of conary is to complain about the missing flag, since it 
may be a typo.  You can add the subarchitecture /etc/conary/arch/%s 
or $(HOME)/.conary/architecture/%s, or use the --unknown-flags 
option on the command line to make conary assume that all unknown flags are 
not relevant to your system.  

""" % (self.majArch, self.key, self.majArch, self.majArch)
 



##########ARCH STUFF HERE######################################

class ArchCollection(Collection):

    def __init__(self):
	self._archProps = []
        self._collectionType = MajorArch
        Collection.__init__(self, 'Arch')

    def _getNonExistantKey(self, key):
        if self._strictMode:
            raise NoSuchArchFlagError(key)
        else:
            self._addFlag(key, track=False)
            self[key]._setStrictMode(False)
            return self[key]

    def _setArch(self, majArch, subArches=None):
        """ Set the current build architecture and subArches.  
            All other architectures are set to false, and not 
            tracked. 
        """
	found = False
        for key in self:
            if key == majArch:
                self[key]._set(True, subArches)
		self._setArchPropValues(self[key])
		found = True
            else:
                self[key]._set(False)
	if not found:
	    raise AttributeError, "No Such Arch %s" % majArch

    def _setArchProps(self, *archProps):
	""" Sets the required arch properties.

	    archProps are flags at the Arch level that describe
	    cross-architecture features, such as endianess or 
	    whether the arch is 32 or 64 bit oriented. 

	    For the current definition of required archProps, see flavorCfg.
	"""
	for archProp in self._archProps:
	    try:
		self._delAttr(archProp)
	    except KeyError:
		pass
	self._archProps = archProps[:]
	for archProp in self._archProps:
            self._setAttr(archProp, False)

    def _setArchPropValues(self, majArch):
	"""
	    archProps are flags at the Arch level that describe
	    cross-architecture features, such as endianess or 
	    whether the arch is 32 or 64 bit oriented. 
	    
	    For the current definition of required archProps, see flavorCfg.
	"""
	archProps = majArch._archProps.copy()
	extraKeys = tuple(set(archProps.keys()) - set(self._archProps))
	missingKeys = tuple(set(self._archProps) - set(archProps.keys()))
	if extraKeys:
	    raise RuntimeError, \
		'Extra arch properties %s provided by %s' % (extraKeys, majArch)
	if missingKeys:
	    raise RuntimeError, \
	        'Missing arch properties %s not provided by %s' % (missingKeys,
								   majArch)
	for archProp, value in archProps.iteritems():
	    self._setAttr(archProp, value)

    def _iterAll(self):
        """ Only iterate over the current architecture.  This is 
            almost always what you want, otherwise it's easy enough
            to manually go through the architectures
        """
        for child in self.itervalues():
            if child._get():
                for flag in child._iterAll():
                    yield flag

    def _getAttr(self, name):
        currentArch = self.getCurrentArch()
        # when getting an architecture prop like bits64, 
        # set the architecture flag if tracking is on
        if currentArch is not None:
            bool(currentArch)
        return Collection._getAttr(self, name)

    def _getMacro(self, key):
        """ return the given macro value, as determined by the active arch flags
        """
        arch = self.getCurrentArch()
        if arch is None:
            return None
        return arch._getMacro(key)


    def _getMacros(self):
        """ return the macros defined by the current architecture 
        """
        arch = self.getCurrentArch()
        if arch is None:
            return None
        return arch._getMacros()

    def getCurrentArch(self):
        for majarch in self.itervalues():
            if majarch._get():
                return majarch

class MajorArch(CollectionWithFlag):
    
    def __init__(self, name, parent, track=False, archProps=None, macros=None):
        self._collectionType = SubArch
        if archProps:
            self._archProps = archProps.copy()
        else:
            self._archProps = {}
        if not macros:
            self._macros = {}
        else:
            self._macros = macros
        CollectionWithFlag.__init__(self, name, parent, track=track)

    def _setUsed(self, used=True):
        CollectionWithFlag._setUsed(self, used)
        # if we are not the current architecture, find
        # the current architecture and set it
        if used and not self._get():
            currentArch = self._parent.getCurrentArch()
            currentArch._setUsed()

    def _getMacro(self, key):
        for subArch in self.itervalues():
            if subArch._get() and key in subArch._macros:
                return subArch._macros[key]
        return self._macros[key]

    def _getMacros(self):
        macros = self._macros.copy()
        for subArch in self.itervalues():
            if subArch._get():
                macros.update(subArch._macros)
        return macros

    def _getNonExistantKey(self, key):
        if self._strictMode:
            raise NoSuchSubArchFlagError(self._name, key)
        else:
            self._addFlag(key)
            return self[key]

    def _set(self, value=True, subArches=None):
        """ Allows you to set the value of this arch, and also set the 
            values of the subArches.  
            XXX hmmm...should there be a difference between subArches=None,
            and subArches=[]?  Maybe this is too complicated, and you should
            just have to set the subarches yourself.
        """
        if not subArches:
            subArches = []
        self._value = value
        for subArch in self:
            if subArches and subArch in subArches:
                continue
            self[subArch]._set(False)
        subsumed = {}
        for subArch in subArches:
            subsumed.update(dict.fromkeys(self[subArch]._subsumes))
        for subArch in subArches:
            if subArch in subsumed:
                continue
            self[subArch]._set()

    def _toDependency(self):
        set = deps.Flavor()
        sense = self._getDepSense()
        dep = deps.Dependency(self._name, [])
        set.addDep(deps.InstructionSetDependency, dep)
        return set

    def _trackUsed(self, value=True):
        CollectionWithFlag._trackUsed(self, value=value)

    def _iterUsed(self):
        if self._get():
            return CollectionWithFlag._iterUsed(self)
        return []

class SubArch(Flag):

    def __init__(self, name, parent, track=False, subsumes=None, 
                 macros=None):
        if not subsumes:
            self._subsumes = []
        else:
            self._subsumes = subsumes
        if not macros:
            self._macros = {}
        else:
            self._macros = macros
        Flag.__init__(self, name, parent, required=True, track=track)

    def _setUsed(self, used=True):
        Flag._setUsed(self, used)
        # if we are not the current architecture, find
        # the current architecture and set it
        if used and not self._parent._get():
            currentArch = self._parent._parent.getCurrentArch()
            currentArch._setUsed()

    def _toDependency(self):
        """ Creates a Flavor dep set with the subarch in it.
            Also includes any subsumed subarches if the 
            value of this subarch is true
            (better comment about why we do that here) 
        """
        set = deps.Flavor()
        sense = self._getDepSense()
        depFlags = [ (self._name, sense) ]
        parent = self._parent
        if self._get():
            depFlags.extend((parent[x]._name, sense) \
                                      for x in self._subsumes)
        dep = deps.Dependency(parent._name, depFlags)
        set.addDep(deps.InstructionSetDependency, dep)
        return set
        
####################### USE STUFF HERE ###########################

class UseFlag(Flag):
    def _toDependency(self):
        set = deps.Flavor()
        sense = self._getDepSense()
        depFlags = [ (self._name, sense) ]
        dep = deps.Dependency('use', depFlags)
        set.addDep(deps.UseDependency, dep)
        return set
    

class UseCollection(Collection):

    _collectionType = UseFlag

    def __init__(self):
        Collection.__init__(self, 'Use')

    def _getNonExistantKey(self, key):
        if self._strictMode:
            raise NoSuchUseFlagError(key)
        else:
            self._addFlag(key)
            return self[key]


####################### LOCALFLAG STUFF HERE ###########################
 
class LocalFlag(Flag):

    def __init__(self, name, parent, track=False, required=False):
        Flag.__init__(self, name, parent, track=track, required=required)
        self._override = False

    def _set(self, value=True, override=False):
        if self._override and not override:
            return
        self._value = value
        self._override = override

    def _toDependency(self, recipeName):
        depFlags =  [('.'.join((recipeName, self._name)), 
                                              self._getDepSense())]
        set = deps.Flavor()
        dep = deps.Dependency('use', depFlags)
        set.addDep(deps.UseDependency, dep)
        return set

class LocalFlagCollection(Collection):
    def __init__(self):
        self._collectionType = LocalFlag
        Collection.__init__(self, 'Flags')

    def _override(self, key, value):
        if key not in self:
            self._addFlag(key)
        self[key]._set(value, override=True)

    def _getNonExistantKey(self, key):
        raise AttributeError, 'No such local flag %s' % key

    def __setattr__(self, key, value):
        if key[0] == '_':
            self.__dict__[key] = value
        else:
            if key not in self:
                self._addFlag(key) 
            self[key]._set(value)


def allowUnknownFlags(value=True):
    Use._setStrictMode(not value)
    Arch._setStrictMode(not value)
    for majArch in Arch.values():
        Arch._setStrictMode(not value)

def setUsed(flagList):
    for flag in flagList:
        flag._used = True

def resetUsed():
    Use._resetUsed()
    Arch._resetUsed()
    LocalFlags._resetUsed()

def clearFlags():
    """ Remove all build flags so that the set of flags can 
        be repopulated 
    """
    Use._clear()
    Arch._clear()
    LocalFlags._clear()


def track(value=True):
    Arch._trackUsed(value)
    Use._trackUsed(value)
    LocalFlags._trackUsed(value)

def iterAll():
    return itertools.chain(Arch._iterAll(), 
                           Use._iterAll(), 
                           LocalFlags._iterAll())
def getUsed():
    return [ x for x in iterUsed() ]

def iterUsed():
    return itertools.chain(Arch._iterUsed(), 
                           Use._iterUsed(), 
                           LocalFlags._iterUsed())

def usedFlagsToFlavor(recipeName):
    return createFlavor(recipeName, iterUsed())

def allFlagsToFlavor(recipeName):
    return createFlavor(recipeName, iterAll())

def createFlavor(recipeName, *flagIterables):
    """ create a dependency set consisting of all of the flags in the 
        given flagIterables.  Note that is a broad category that includes
        lists, iterators, etc. RecipeName is the recipe which local flags
        should be relative to, can be set to None if there are definitely
        no local flags in the flagIterables.
    """
    majArch = None
    archFlags = {}
    subsumed = {}
    useFlags = []
    set = deps.Flavor()
    for flag in itertools.chain(*flagIterables):
        flagType = type(flag)
        if flagType == MajorArch:
            if not flag._get():
                continue
            set.union(flag._toDependency())
        elif flagType ==  SubArch:
            set.union(flag._toDependency())
        elif flagType == UseFlag:
            set.union(flag._toDependency())
        elif flagType == LocalFlag:
            assert(recipeName)
            set.union(flag._toDependency(recipeName))
    return set

def setBuildFlagsFromFlavor(recipeName, flavor, error=True, warn=False):
    """ Sets the truth of the build Flags based on the build flavor.
        All the set build flags must already exist.  Flags not mentioned
        in this flavor will be untouched.
        XXX should this create flags as well as set them?  Problem with that
        is that we don't know whether the flag is required or not based
        on the flavor; we would only be able to do as half-baked job
    """
    for depGroup in flavor.getDepClasses().values():
        if isinstance(depGroup, deps.UseDependency):
            for dep in depGroup.getDeps():
                for flag, sense in dep.flags.iteritems():
                    if sense in (deps.FLAG_SENSE_REQUIRED,
                                 deps.FLAG_SENSE_PREFERRED):
                        value = True
                    else:
                        value = False
                    # see if there is a . -- indicating this is a 
                    # local flag
                    parts = flag.split('.',1)
                    if len(parts) == 1:
                        try:
                            Use[flag]._set(value)
                        except KeyError:
                            if error:
                                raise AttributeError(
                                            "No Such Use Flag %s" % flag)
                            elif warn:
                                log.warning(
                                        'ignoring unknown Use flag %s' % flag)
                                continue
                    elif recipeName:
                        packageName, flag = parts
                        if packageName == recipeName:
                            # local flag values set from a build flavor
                            # are overrides -- the recipe should not 
                            # change these values
                            LocalFlags._override(flag, value)
                    elif error:
                        raise RuntimeError, ('Trying to set a flavor with '
                                             'localflag %s when no trove '
                                             ' name was given' % flag)

        elif isinstance(depGroup, deps.InstructionSetDependency):
            if len([ x for x in depGroup.getDeps()]) > 1:
                setOnlyIfMajArchSet = True
                found = False
            else:
                setOnlyIfMajArchSet = False

            for dep in depGroup.getDeps():
                majarch = dep.name
                if setOnlyIfMajArchSet and not Arch[majarch]:
                    continue
                found = True

                subarches = []
                for (flag, sense) in dep.flags.iteritems():
                    if sense in (deps.FLAG_SENSE_REQUIRED,
                                 deps.FLAG_SENSE_PREFERRED):
                        subarches.append(flag)
                Arch._setArch(majarch, subarches)

            if setOnlyIfMajArchSet and not found:
                if error:
                    raise RuntimeError, ('Cannot set arctitecture build flags'
                                         ' to multiple architectures:'
                                         ' %s: %s' % (recipeName, flavor))
                

Arch = ArchCollection()
Use = UseCollection()
LocalFlags = LocalFlagCollection()
