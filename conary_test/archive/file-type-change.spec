# build w/ rpmbuild -D "%ver 1" or -D "%ver 2"
Name: file-type-change
Version: %ver
Release: 1
Group: Dummy
License: GPL
#Source: %{name}-%{version}.tar.gz
BuildRoot: /var/tmp/%{name}-%{version}-root
Summary: RPM with special files

%description
Description

%prep
%setup -c -T

%build

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT

%dump
set -x
if [ %{ver} = "1" ]; then
    ln -s foo $RPM_BUILD_ROOT/test
else
    touch $RPM_BUILD_ROOT/test
fi

%files
%defattr(0644,root,root)
/test
