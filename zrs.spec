Name: zc.zrs
Version: 2.0.3b4
Release: 0

Summary: Zope Replication Service
Group: Applications/Database
Requires: cleanpython24
# BuildRequires: ???
%define python /opt/cleanpython24/bin/python

##########################################################################
# Lines below this point normally shouldn't change

%define source %{name}-%{version}-%{release}

Copyright: ZVSL
Vendor: Zope Corporation
Packager: Zope Corporation <sales@zope.com>
AutoReqProv: no
Source: %{source}.tgz

%description
%{summary}

%prep
rm -rf $RPM_BUILD_DIR/%{source}
zcat $RPM_SOURCE_DIR/%{source}.tgz | tar -xvf -

%build
if [ -d /opt/%{name} ] ; then chmod -R +w /opt/%{name} ; fi
rm -rf /opt/%{name}
cp -r $RPM_BUILD_DIR/%{source} /opt/%{name}
%{python} /opt/%{name}/install.py bootstrap
%{python} /opt/%{name}/install.py buildout:extensions=
chmod -R -w /opt/%{name}

%files
%attr(-, root, root) /opt/%{name}
