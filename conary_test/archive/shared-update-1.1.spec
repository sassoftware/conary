Name: shared-update
Version: 1.1
Release: 1
Group: Dummy
License: GPL
BuildRoot: /var/tmp/%{name}-%{version}-root
Summary: RPM shared update handling

%description
Description

%prep
%setup -c -T

%build

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT/usr/share
echo contents1.1 > $RPM_BUILD_ROOT/usr/share/test

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0644,root,root)
/usr/share/test
