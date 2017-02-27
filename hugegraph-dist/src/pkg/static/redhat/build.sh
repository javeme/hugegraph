#!/bin/bash

set -e
set -u

TOPDIR=~/rpmbuild
cd "`dirname $0`/.."
REPO_ROOT="`pwd`"
cd - >/dev/null

cd "`dirname $0`"/.. 
. pkgcommon/etc/config.sh
. pkgcommon/etc/version.sh
[ ! -h "$TOPDIR/BUILD/hugegraph-$RPM_VERSION" ] && ln -s "$REPO_ROOT" "$TOPDIR"/BUILD/hugegraph-$RPM_VERSION

# Prepend version and release macro definitions to specfile
cd "`dirname $0`"
cat > hugegraph.spec <<EOF
%define hugegraph_version $RPM_VERSION
%define hugegraph_release $RPM_RELEASE
EOF
cat hugegraph.spec.base >> hugegraph.spec

rpmbuild -bb hugegraph.spec
rpmsign --addsign --key-id=$SIGNING_KEY_ID "$TOPDIR"/RPMS/noarch/hugegraph*-$RPM_VERSION-$RPM_RELEASE.noarch.rpm
