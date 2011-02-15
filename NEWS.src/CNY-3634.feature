GroupRecipe PathConflicts policy now records the conflicting paths which were
explicitly allowed and the system model based install code treats those paths
as if --replace-managed-files were enabled whenever that group would be
present at the end of the operation
