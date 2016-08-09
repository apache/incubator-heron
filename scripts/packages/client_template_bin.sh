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

# Heron self-extractable installer for client package

# Set help URL
getting_started_url=http://heronstreaming.io/docs/getting-started

# Installation and etc prefix can be overriden from command line
install_prefix=${1:-"/usr/local/heron"}
heronrc=${2:-"/usr/local/heron/etc/heron.heronrc"}

progname="$0"

echo "Heron client installer"
echo "----------------------"
echo

function usage() {
  echo "Usage: $progname [options]" >&2
  echo "Options are:" >&2
  echo "  --prefix=/some/path set the prefix path (default=/usr/local)." >&2
  echo "  --heronrc= set the heronrc path (default=/usr/local/heron/etc/heron.heronrc)." >&2
  echo "  --user configure for user install, expands to" >&2
  echo '           `--prefix=$HOME/.heron --heronrc=$HOME/.heronrc`.' >&2
  exit 1
}

prefix="/usr/local"
bin="%prefix%/bin"
base="%prefix%/heron"
conf="%prefix%/heron/conf"
heronrc="%prefix%/heron/etc/heron.heronrc"

for opt in "${@}"; do
  case $opt in
    --prefix=*)
      prefix="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --heronrc=*)
      heronrc="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    --user)
      bin="$HOME/bin"
      base="$HOME/.heron"
      heronrc="$HOME/.heronrc"
      ;;
    *)
      usage
      ;;
  esac
done

bin="${bin//%prefix%/${prefix}}"
base="${base//%prefix%/${prefix}}"
heronrc="${heronrc//%prefix%/${prefix}}"

check_unzip; check_tar; check_java

# Test for write access
test_write "${bin}"
test_write "${base}"
test_write "${heronrc}"

# Do the actual installation
echo -n "Uncompressing."

# Cleaning-up, with some guards.
if [ -L "${bin}/heron" ]; then
  rm -f "${bin}/heron"
fi

if [ -L "${bin}/heron-explorer" ]; then
  rm -f "${bin}/heron-explorer"
fi

if [ -L "${bin}/heron-cli3" ]; then
  rm -f "${bin}/heron-cli3"
fi

if [ -d "${base}" -a -x "${base}/bin/heron" ]; then
  rm -fr "${base}"
fi

mkdir -p ${bin} ${base} ${base}/etc
echo -n .

unzip -q -o "${BASH_SOURCE[0]}" -d "${base}"
untar ${base}/heron-client.tar.gz ${base}
echo -n .
chmod 0755 ${base}/bin/heron
echo -n .
chmod -R og-w "${base}"
chmod -R og+rX "${base}"
chmod -R u+rwX "${base}"
echo -n .

ln -s "${base}/bin/heron" "${bin}/heron"
ln -s "${base}/bin/heron" "${bin}/heron-cli3"
ln -s "${base}/bin/heron-explorer" "${bin}/heron-explorer"
echo -n .

if [ -f "${heronrc}" ]; then
  echo
  echo "${heronrc} already exists, not modifying it"
else
  touch "${heronrc}"
  if [ "${UID}" -eq 0 ]; then
    chmod 0644 "${heronrc}"
  fi
fi

rm "${base}/heron-client.tar.gz"

cat <<EOF

Heron is now installed!

Make sure you have "${bin}" in your path.

See ${getting_started_url} for how to use Heron.
EOF
echo
cat <<'EOF'
%release_info%
EOF
exit 0
