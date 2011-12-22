Summary: Two rpms that share directories
Name: shareddirs
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something

%description
junk

%package -n shareddirs-secondary
Group: something
Summary: foo

%description -n shareddirs-secondary
What not to wear, III


%prep

%build
echo 'int foo(void) {return 0;}' > foo.c
make CFLAGS="-g -fPIC" foo.o
gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o

%install
mkdir -p $RPM_BUILD_ROOT/usr/{lib,bin}
mkdir $RPM_BUILD_ROOT/shareddir
cp libfoo.so.0.0 $RPM_BUILD_ROOT/usr/lib/
ldconfig -n $RPM_BUILD_ROOT/usr/lib
echo -e '#!/bin/sh\nexit 0' > $RPM_BUILD_ROOT/usr/bin/script


%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(755,root,root) /usr/lib/libfoo*
%attr(755,root,root) /usr/bin/script
%attr(755,root,root) /shareddir

%files -n shareddirs-secondary
%attr(755,root,root) /usr/lib/libfoo*
%attr(755,root,root) /usr/bin/script
%attr(755,root,root) /shareddir