Summary: package to test _netsharedpath implementation
Name: netshared
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
BuildArch: noarch
Group: something
License: something

%description
junk

%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT/local
echo > $RPM_BUILD_ROOT/local/shouldexist
mkdir -p $RPM_BUILD_ROOT/excluded
echo > $RPM_BUILD_ROOT/excluded/shouldnotexist

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) /local/shouldexist
%attr(-,root,root) /excluded/shouldnotexist
