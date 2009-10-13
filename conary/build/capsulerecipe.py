from conary.build import defaultrecipes

from conary.build.recipe import RECIPE_TYPE_CAPSULE
from conary.build.packagerecipe import BaseRequiresRecipe, AbstractPackageRecipe

class AbstractCapsuleRecipe(AbstractPackageRecipe):
    internalAbstractBaseClass = 1
    internalPolicyModules = ( 'packagepolicy', 'capsulepolicy' )
    _recipeType = RECIPE_TYPE_CAPSULE
    def __init__(self, *args, **kwargs):
        klass = self._getParentClass('AbstractPackageRecipe')
        klass.__init__(self, *args, **kwargs)

        from conary.build import source
        self._addSourceAction('source.addCapsule', source.addCapsule)
        self._addSourceAction('source.addSource', source.addSource)


exec defaultrecipes.CapsuleRecipe
