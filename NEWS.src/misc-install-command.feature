The "conary install" command has been added.  It is equivalent to the
"conary update --keep-existing" command.  While the "--keep-existing"
argument option has been not commonly recommended in the old update
model because it can easily introduce packages with versions that are
unintentionally out of sync, it is commonly useful with a system model
because the search path used for a system model can keep versions in
sync.
