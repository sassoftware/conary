Name: ghost-conflict
Version: 1.0
Release: 1
Group: Dummy
License: GPL
BuildRoot: /var/tmp/%{name}-%{version}-root
Summary: RPM ghost conflict handling

%description
Description

%prep
%setup -c -T

%build

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT/etc
touch $RPM_BUILD_ROOT/etc/fake

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0644,root,root)
%ghost /etc/fake
