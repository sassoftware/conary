Summary: Test of obsolete metadata
Name: obsolete
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something
Obsoletes: foo, bar < 1.0, baz > 2.0

%description
junk

%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT/foo
touch $RPM_BUILD_ROOT/foo/ghost

%clean
rm -rf $RPM_BUILD_ROOT

%files
%dir /foo
%ghost /foo/ghost
