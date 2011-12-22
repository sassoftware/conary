Summary: early-install-order RPM
Name: aaa_first
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
mkdir -p $RPM_BUILD_ROOT/dummy
echo > $RPM_BUILD_ROOT/dummy/aaa_first

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) /dummy/aaa_first
