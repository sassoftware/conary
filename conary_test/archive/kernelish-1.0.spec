Summary: Test of kernel provide/requires
Name: kernelish
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something
Provides: kernel(foo) = 123456789abcdef
Provides: kernel(bar) = 123456789abcdef
Requires: ksym(foo) = 123456789abcdef
Requires: ksym(bar) = 123456789abcdef

%description
junk

%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT
echo "normal" > $RPM_BUILD_ROOT/normal

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) /normal
