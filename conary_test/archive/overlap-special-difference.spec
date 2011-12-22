Summary: Test overlapping differences in various files
Name: overlap-special-difference
Version: 1.0
Release: 1
ExclusiveOs: Linux
BuildRoot: %{_tmppath}/%{name}-root
Group: something
License: something
Summary: foo

%description
What not to wear

%package -n overlap-special-other
Group: something
Summary: foo

%description -n overlap-special-other
What not to wear, II


%prep

%build

%install
mkdir -p $RPM_BUILD_ROOT/etc
# empty config file is initial contents
touch $RPM_BUILD_ROOT/etc/conf
# this is a real config file to conary
echo 'realstuff' >  $RPM_BUILD_ROOT/etc/conf2
mkdir -p $RPM_BUILD_ROOT/ghostly
touch $RPM_BUILD_ROOT/ghostly/file
# test noverify
echo 'ignorethis' > $RPM_BUILD_ROOT/etc/noverify
echo 'ignorethissometimes' > $RPM_BUILD_ROOT/etc/maybeverify
# test a normal file
mkdir -p $RPM_BUILD_ROOT/usr
echo 'normal is the new weird' > $RPM_BUILD_ROOT/usr/normal
# test a file that will get synthetically flavored (CNY-3277)
mkdir -p $RPM_BUILD_ROOT/usr/lib64/python2.4/config/
touch  $RPM_BUILD_ROOT/usr/lib64/python2.4/config/Makefile
# ghost symlink overlap (CNY-3336)
ln -s conf $RPM_BUILD_ROOT/etc/ghostconf

%clean
rm -rf $RPM_BUILD_ROOT

%files
%config %attr(0600,root,root) /etc/conf
%config %attr(0600,root,root) /etc/conf2
%ghost %attr(0700,root,root) /ghostly
%ghost %attr(0600,root,root) /ghostly/file
%ghost /etc/ghostconf
%dev(b,8,0) %attr(0600,root,root) /dev/sda
%verify(not md5) %attr(0600,root,root) /etc/noverify
%verify(not md5) %attr(0600,root,root) /etc/maybeverify
%attr(0600,root,root) /usr/normal
%attr(0600,root,root) /usr/lib64/python2.4/config/Makefile

%files -n overlap-special-other
%config %attr(0644,oot,oot) /etc/conf
%config %attr(0644,oot,oot) /etc/conf2
%ghost %attr(0750,oot,oot) /ghostly
%ghost %attr(0640,oot,oot) /ghostly/file
%ghost /etc/ghostconf
%dev(b,9,0) %attr(0660,oot,oot) /dev/sda
%verify(not md5) %attr(0660,oot,oot) /etc/noverify
%attr(0600,oot,oot) /etc/maybeverify
%attr(0640,oot,oot) /usr/normal
# "root" not a typo below -- this is a test of flavoring (CNY-3277)
%attr(0600,root,root) /usr/lib64/python2.4/config/Makefile
