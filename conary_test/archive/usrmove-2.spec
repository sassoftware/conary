Summary: Post-usrmove, has a file in /usr/sbin
Name: usrmove
Version: 2.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something

%description
junk

%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT/usr/sbin
echo 2.0 > $RPM_BUILD_ROOT/usr/sbin/usrmove

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) /usr/sbin/usrmove
