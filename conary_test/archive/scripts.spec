Summary: test looking through scripts
Name: scripts
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
mkdir -p $RPM_BUILD_ROOT/dummy
echo > $RPM_BUILD_ROOT/dummy/file

%clean
rm -rf $RPM_BUILD_ROOT

%triggerin -- other1

%triggerun -p /other2/interp -- other2

%triggerpostun -- other3 < 4

%triggerprein -- other4
echo triggerprein has non-sequential sense flag >/dev/null

%pre
echo this script mentions /etc/ld.so.conf >/dev/null

%post -p /sbin/ldconfig

%preun
echo this script mentions /etc/ld.so.conf from preun >/dev/null

%postun
echo this script mentions /etc/ld.so.conf from postun >/dev/null

%files
%attr(-,root,root) /dummy/file
