Name: dev
Version: 1.0
Release: 1
Group: Dummy
License: GPL
#Source: %{name}-%{version}.tar.gz
BuildRoot: /var/tmp/%{name}-%{version}-root
Summary: RPM with device file

%description
Description

%prep
%setup -c -T

%build

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0644,root,root)
%dev(b,8,0) /dev/sda
