#!/usr/bin/perl
use Module::ScanDeps;

$map = scan_deps(files=>[$ARGV[0]], recurse => 0);

# Do as little as possible in this bootstrapping script; do all the
# processing in Python.  We use // as a separator because it will
# never be found within a normalized POSIX path.
foreach $item (values %$map) {
    # since we depend on the path being normalized, make sure they
    # are!
    $file = $item->{file};
    $file = s/\/\/+/\//g;
    print $item->{type} . "//" . $file . "//" . $item->{key} . "\n";
}
