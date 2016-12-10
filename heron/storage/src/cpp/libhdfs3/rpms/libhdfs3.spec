# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
%define name libhdfs3
%define release 1%{?dist}

Name: %{name}
Version: %{version}
Release: %{release}
Summary: Native C/C++ HDFS Client.
Group: Development/Libraries
Source0: libhdfs3-%{version}.tar.gz

License: Apache-2.0
BuildRoot:  %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: cmake
BuildRequires: libuuid-devel
BuildRequires: libxml2-devel
BuildRequires: krb5-devel
BuildRequires: libgsasl-devel
BuildRequires: protobuf-devel

%description
Libhdfs3, designed as an alternative implementation of libhdfs,
is implemented based on native Hadoop RPC protocol and
HDFS data transfer protocol.
It gets rid of the drawbacks of JNI, and it has a lightweight,
small memory footprint code base. In addition, it is easy to use and deploy.
.

%package devel
Summary: Native C/C++ HDFS Client - development files
Requires: %{name} = %{version}-%{release}
Group: Development/Libraries
Requires: libhdfs3 = %{version}-%{release}
Requires: libuuid-devel libxml2-devel krb5-devel libgsasl-devel protobuf-devel pkgconfig

%description devel
Libhdfs3, designed as an alternative implementation of libhdfs,
is implemented based on native Hadoop RPC protocol and
HDFS data transfer protocol.
It gets rid of the drawbacks of JNI, and it has a lightweight,
small memory footprint code base. In addition, it is easy to use and deploy.

%build
%{_sourcedir}/../../bootstrap --prefix=${RPM_BUILD_ROOT}/usr
%{__make}

%install
%{__rm} -rf $RPM_BUILD_ROOT
%{__make} install

%clean
%{__rm} -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_prefix}/lib/lib*.so.*

%files devel
%defattr(-,root,root,-)
%{_prefix}/lib/lib*.so
%{_prefix}/lib/*.a
%{_prefix}/lib/pkgconfig/*
%{_prefix}/include/*

%post
/sbin/ldconfig

%postun
/sbin/ldconfig

%changelog
* Sun Oct 04 2015 Zhanwei Wang <wangzw@wangzw.org> - 2.2.30-1
- Initial RPM release
