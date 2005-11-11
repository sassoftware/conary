
class BuildError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
    
class RecipeFileError(BuildError):
    pass


class RecipeDependencyError(RecipeFileError):
    pass

class BadRecipeNameError(RecipeFileError):
    pass
