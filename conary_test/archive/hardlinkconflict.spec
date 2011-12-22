name: hardlinkconflict
version: 1.0
release: %release
summary: foo
license: bsd

%description
foo

%install
mkdir -p $RPM_BUILD_ROOT
echo "hello" > $RPM_BUILD_ROOT/foo
ln $RPM_BUILD_ROOT/foo $RPM_BUILD_ROOT/bar%{release}

%clean
rm -rf $RPM_BUILD_ROOT

%files
/foo
/bar%{release}
