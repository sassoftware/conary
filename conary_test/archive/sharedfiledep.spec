Summary: Test that dependencies are shared with files shared across capsules
Name: sharedfiledep
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something

%description
junk

%package -n sharedfiledep-secondary
Group: something
Summary: foo

%description -n sharedfiledep-secondary
What not to wear, III


%prep

%build
echo 'int foo(void) {return 0;}' > foo.c
make CFLAGS="-g -fPIC" foo.o
gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o

%install
mkdir -p $RPM_BUILD_ROOT/usr/{lib,bin}
cp libfoo.so.0.0 $RPM_BUILD_ROOT/usr/lib/
ldconfig -n $RPM_BUILD_ROOT/usr/lib
echo -e '#!/bin/sh\nexit 0' > $RPM_BUILD_ROOT/usr/bin/script

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(755,root,root) /usr/lib/libfoo*
%attr(755,root,root) /usr/bin/script

%files -n sharedfiledep-secondary
%attr(755,root,root) /usr/lib/libfoo*
%attr(755,root,root) /usr/bin/script
