Summary: Test of owners and groups
Name: simple
Version: 1.0.1
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
mkdir -p $RPM_BUILD_ROOT/dir
echo "config" > $RPM_BUILD_ROOT/config
echo "normal" > $RPM_BUILD_ROOT/normal

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) %dir /dir
%config %attr(-,root,root) /config
%attr(-,root,root) /normal
