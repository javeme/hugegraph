#!/bin/bash

# Backends contains [memory, rocksdb, cassandra, scylladb]
export BACKEND=memory

export SCRIPT_DIR="hugegraph-dist/src/assembly/jenkins"

export ACTION=${ACTION}
export TRIGGER=${TRIGGER}
export RUNMODE=${RUNMODE}

export BUILD_ID=${AGILE_COMPILE_BUILD_ID}
export BRANCH=${AGILE_COMPILE_BRANCH}
export BRANCH_REF=${AGILE_COMPILE_BRANCH_REF}

export USER="liunanke"
export REPO="hugegraph"
export REPO_URL="icode.baidu.com:8235/baidu/xbu-data/$REPO"

export RELEASE_SERVER="yq01-sw-hdsserver16.yq01.baidu.com 8322"
export RELEASE_SERVER_USER=${FTP_USER}

# Clone code from repo if necessary
if [ ! -d $REPO ]; then
    echo "Clone code from repo..."
    git clone ssh://$USER@$REPO_URL
    if [ $? -ne 0 ]; then
        echo "Failed to clone code."
        exit 1
    fi
fi

# Change dir into local repo
cd $REPO
if [ $? -ne 0 ]; then
    echo "Failed to cd $REPO."
    exit 1
fi

if [ -n "$BRANCH_REF" ]; then
    # Fetch code from repo if necessary
    echo "Fetch code from repo: ${BRANCH_REF}..."
    git checkout . && git checkout $BRANCH
    git fetch ssh://$USER@$REPO_URL ${BRANCH_REF} && git checkout FETCH_HEAD
    if [ $? -ne 0 ]; then
        echo "Failed to fetch code."
        exit 1
    fi
else
    # Pull or checkout release branch
    git rev-parse --verify $BRANCH
    if [ $? -eq 0 ]; then
        git checkout $BRANCH && git pull
    else
        git pull && git checkout -b $BRANCH origin/$BRANCH
    fi

    if [ $? -ne 0 ]; then
        echo "Failed to pull code."
        exit 1
    fi
fi

sh $SCRIPT_DIR/build.sh
