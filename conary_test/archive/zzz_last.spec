Summary: late-install-order RPM
Name: zzz_last
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
BuildArch: noarch
Group: something
License: something
Requires: problem

%description
junk

%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT/dummy
echo > $RPM_BUILD_ROOT/dummy/zzz_last

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) /dummy/zzz_last
