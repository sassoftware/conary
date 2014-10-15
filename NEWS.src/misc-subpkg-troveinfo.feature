Added a new 'subPackages' troveinfo that enumerates the names of packages
created via PackageSpec and groups created via createGroup from the same
source. Query, repquery, and showchangeset commands with the --info and --debug
flag set will display the list of subpackages.
