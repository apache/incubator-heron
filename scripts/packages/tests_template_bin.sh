# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Heron self-extractable installer for integration tests package
# used only for integration tests

# Set help URL
getting_started_url=https://heron.apache.org/docs/getting-started-local-single-node/

# Installation and etc prefix can be overriden from command line
install_prefix=${1:-"/usr/local/herontests"}

progname="$0"

echo "Heron tests installer"
echo "---------------------"
echo

function usage() {
  echo "Usage: $progname [options]" >&2
  echo "Options are:" >&2
  echo "  --prefix=/some/path set the prefix path (default=/usr/local)." >&2
  echo "  --user configure for user install, expands to" >&2
  echo '           `--prefix=$HOME/.herontests`.' >&2
  exit 1
}

prefix="/usr/local"
bin="%prefix%/bin"
base="%prefix%/herontests"

for opt in "${@}"; do
  case $opt in
    --prefix=*)
      prefix="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --user)
      bin="$HOME/bin"
      base="$HOME/.herontests"
      ;;
    *)
      usage
      ;;
  esac
done

bin="${bin//%prefix%/${prefix}}"
base="${base//%prefix%/${prefix}}"

check_unzip; check_tar; check_java

# Test for write access
test_write "${bin}"
test_write "${base}"

# Do the actual installation
echo -n "Uncompressing."

# Cleaning-up, with some guards.
if [ -L "${bin}/test-runner" ]; then
  rm -f "${bin}/test-runner"
fi
if [ -L "${bin}/topology-test-runner" ]; then
  rm -f "${bin}/topology-test-runner"
fi
if [ -L "${bin}/http-server" ]; then
  rm -f "${bin}/http-server"
fi
if [ -d "${base}" -a -x "${base}/bin/test-runner" ]; then
  rm -fr "${base}"
fi
if [ -d "${base}" -a -x "${base}/bin/topology-test-runner" ]; then
  rm -fr "${base}"
fi

mkdir -p ${bin} ${base}
echo -n .

unzip -q -o "${BASH_SOURCE[0]}" -d "${base}"
untar ${base}/heron-tests.tar.gz ${base}
echo -n .
chmod 0755 ${base}/bin/test-runner ${base}/bin/http-server ${base}/bin/topology-test-runner
echo -n .
chmod -R og-w "${base}"
chmod -R og+rX "${base}"
chmod -R u+rwX "${base}"
echo -n .

ln -s "${base}/bin/test-runner" "${bin}/test-runner"
ln -s "${base}/bin/topology-test-runner" "${bin}/topology-test-runner"
ln -s "${base}/bin/http-server" "${bin}/http-server"
mv "${base}/lib/heron_integ_topology" "${base}/lib/heron_integ_topology.pex"
echo -n .

rm "${base}/heron-tests.tar.gz"

cat <<EOF

Heron Tests is now installed!

Make sure you have "${bin}" in your path.

EOF
echo
cat <<'EOF'
%release_info%
EOF
exit 0
