Name: doc-conflict
Version: 1.0
Release: 1
Group: Dummy
License: GPL
BuildRoot: /var/tmp/%{name}-%{version}-root
Summary: RPM doc conflict handling

%description
Description

%prep
%setup -c -T

%build

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT

# A regular file, hopefully always different
echo $RANDOM > docfile
%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%doc docfile
