Summary: Test of owners and groups
Name: overlap-conflict
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something
Summary: foo

%description
junk

%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT
echo "conflicting contents" > $RPM_BUILD_ROOT/file

%clean
rm -rf $RPM_BUILD_ROOT

%files
%config %attr(-,root,root) /file
