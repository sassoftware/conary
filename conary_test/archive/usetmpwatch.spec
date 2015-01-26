Summary: test using shared file from script
Name: usetmpwatch
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

%post -p <lua>
if posix.access ("/usr/sbin/tmpwatch", "x")
then
    io.write("PRESENT\n")
else
    error("MISSING")
end

%files
%attr(-,root,root) /dummy/file
