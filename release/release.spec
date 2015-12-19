Name: heron-cli
Version: %version
Release: %qualification
Summary: Heron CLI tool
Group: streaming-compute@twitter.com
License: N/A

%description
Tool for submitting Heron jobs to Aurora

%files
/opt/heron-cli/

%post
ln -sf /opt/heron-cli/bin/heron-cli /usr/local/bin/heron-cli

%postun
if [ $1 = 0 ] ; then
  rm -f /usr/local/bin/heron-cli
fi
