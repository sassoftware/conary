#
# Copyright (c) 2008 rPath, Inc.
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

from conary.build import recipe

class Factory:

    internalAbstractBaseClass = True
    _recipeType = recipe.RECIPE_TYPE_FACTORY
    _loadedTroves = []
    _loadedSpecs = {}
    _trackedFlags = None

    def __init__(self, packageName, sourceFiles = []):
        self.packageName = packageName
        self.sources = sourceFiles

    @classmethod
    def getType(class_):
        return class_._recipeType

    @classmethod
    def validateClass(class_):
        if class_.version == '':
            raise ParseError("empty release string")

    @classmethod
    def addLoadedTroves(class_, newTroves):
        # NOTE: we have these method to ensure that the
        # class variable we're using is assigned to _this_
        # class and not some superclass.
        class_._loadedTroves = class_._loadedTroves + newTroves

    @classmethod
    def addLoadedSpecs(class_, newSpecs):
        class_._loadedSpecs = dict(class_._loadedSpecs)
        class_._loadedSpecs.update(newSpecs)
