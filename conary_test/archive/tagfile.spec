Summary: Test that tags are not applied to capsules
Name: tagfile
Version: 1.0
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
mkdir -p $RPM_BUILD_ROOT/usr/share/info/
echo "tagfile.info" > $RPM_BUILD_ROOT/usr/share/info/tagfile.info

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) /usr/share/info/tagfile.info.gz
